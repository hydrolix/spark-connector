// NOTE: this is in the spark.sql package because we make use of a few private[sql] members in there 😐
package org.apache.spark.sql

import net.openhft.hashing.LongHashFunction
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.expressions.filter._
import org.apache.spark.sql.connector.expressions.{Expression, FieldReference, GeneralScalarExpression, LiteralValue}
import org.apache.spark.sql.types.{DataType, DataTypes}

import java.{lang => jl}
import java.time.Instant

/**
 * TODO:
 *  - see if multi-part names will ever show up (e.g. in a join?); that would break [[GetField]] but hopefully only
 *    in a way that would allow fewer pushdown opportunities rather than incorrect results.
 */
object HdxPredicatePushdown extends Logging {
  private val LT = "<"
  private val GT = ">"
  private val NE = "<>"
  private val EQ = "="
  private val GE = ">="
  private val LE = "<="

  /**
   * Note, [[Comparison.unapply]] assumes `shardOps` is a subset of `timeOps`; fix that if this assumption
   * is ever falsified!
   */
  private val timeOps = Set(LT, LE, GT, GE, EQ, NE)
  private val shardOps = Set(EQ, NE)

  private val allTimestamps = new AllLiterals(DataTypes.TimestampType)
  private val allStrings = new AllLiterals(DataTypes.StringType)

  /**
   * Tests whether a given predicate should be pushable for the given timestamp and shard key field names. Note that we
   * have no concept of a unique key, so we can never return 1 here. However, `prunePartition` should be able to
   * authoritatively prune particular partitions, since it will have their specific min/max timestamps and shard keys.
   *
   * @param timeField      name of the timestamp ("primary key") field for this table
   * @param mShardKeyField name of the shard key field for this table
   * @param predicate      predicate to test for pushability
   * @return (see [[org.apache.spark.sql.connector.read.SupportsPushDownV2Filters]])
   *          - 1 if this predicate doesn't need to be evaluated again after scanning
   *          - 2 if this predicate still needs to be evaluated again after scanning
   *          - 3 if this predicate is not pushable
   */
  def pushable(timeField: String, mShardKeyField: Option[String], predicate: Expression): Int = {
    predicate match {
      case Comparison(GetField(`timeField`), op, LiteralValue(_: Long, DataTypes.TimestampType)) if timeOps.contains(op) =>
        // Comparison between the time field and a timestamp literal (TODO TimestampNTZType?)
        2
      case In(GetField(`timeField`), allTimestamps(_)) =>
        // timeField IN (timestamp literals)
        2
      case Comparison(GetField(field), op, LiteralValue(_: String, DataTypes.StringType)) if mShardKeyField.contains(field) && shardOps.contains(op) =>
        // shardKeyField == string or shardKeyField <> string
        // Note: this is a 2 even when shardKeyField == string because in the rare case of a hash collision we still
        // need to compare the raw strings.
        2
      case In(GetField(field), allStrings(_)) if mShardKeyField.contains(field) =>
        // shardKeyField IN (string literals)
        2
      case not: Not =>
        // child is pushable
        pushable(timeField, mShardKeyField, not.child())
      case and: And =>
        // max of childrens' pushability
        pushable(timeField, mShardKeyField, and.left()) max pushable(timeField, mShardKeyField, and.right())
      case or: Or =>
        // max of childrens' pushability
        pushable(timeField, mShardKeyField, or.left()) max pushable(timeField, mShardKeyField, or.right())
      case _: AlwaysTrue | _: AlwaysFalse =>
        // Literal true/false are pushable (probably not required)
        1
      case _ =>
        3
    }
  }

  /**
   * Evaluate the min/max timestamps and shard key of a single partition against a single predicate
   * to determine if the partition CAN be pruned, i.e. doesn't need to be scanned.
   *
   * @param timeField         the name of the timestamp field for this partition
   * @param mShardKeyField    the name of the shard key field for this partition
   * @param predicate         the predicate to evaluate
   * @param partitionMin      the minimum timestamp for data in this partition
   * @param partitionMax      the maximum timestamp for data in this partition
   * @param partitionShardKey the shard key of this partition. Not optional because `42bc986dc5eec4d3` (`wyhash("")`)
   *                          appears when there's no shard key.
   * @return `true` if this partition should be pruned (i.e. NOT scanned) according to `predicate`,
   *         or `false` if it MUST be scanned
   */
  def prunePartition(timeField: String,
                mShardKeyField: Option[String],
                     predicate: Expression,
                  partitionMin: Instant,
                  partitionMax: Instant,
             partitionShardKey: String)
                              : Boolean =
  {
      predicate match {
        case Comparison(GetField(`timeField`), op, LiteralValue(micros: Long, DataTypes.TimestampType)) if timeOps.contains(op) =>
          // [`timestampField` <op> <timestampLiteral>], where op ∈ `timeOps`
          val timestamp = Instant.ofEpochSecond(micros / 1_000_000, (micros % 1_000_000) * 1000)

          op match {
            case EQ => // prune if timestamp IS OUTSIDE partition min/max
              timestamp.compareTo(partitionMin) < 0 || timestamp.compareTo(partitionMax) > 0

            case NE => // prune if timestamp IS INSIDE partition min/max
              timestamp.compareTo(partitionMin) >= 0 && timestamp.compareTo(partitionMax) <= 0

            case GE | GT => // prune if timestamp IS AFTER partition max
              // TODO seriously consider whether > and >= should be treated the same given mismatched time grain
              timestamp.compareTo(partitionMax) >= 0

            case LE | LT => // prune if timestamp IS BEFORE partition min
              // TODO seriously consider whether < and <= should be treated the same given mismatched time grain
              timestamp.compareTo(partitionMin) <= 0

            case _ =>
              // Shouldn't happen because the pattern guard already checked in timeOps
              sys.error(s"Unsupported comparison operator for timestamp: $op")
          }

        case Comparison(GetField(field), op, LiteralValue(shardKey: String, DataTypes.StringType)) if mShardKeyField.contains(field) && shardOps.contains(op) =>
          // [`shardKeyField` <op> <stringLiteral>], where op ∈ `shardOps`
          // TODO do we need to care about 42bc986dc5eec4d3 here?
          val hashed = jl.Long.toHexString(LongHashFunction.wy_3().hashBytes(shardKey.getBytes("UTF-8")))

          op match {
            case EQ =>
              // Shard key must match partition's; prune if NOT EQUAL
              partitionShardKey != hashed
            case NE =>
              // Shard key must NOT match partition's; prune if EQUAL
              partitionShardKey == hashed
            case _ =>
              // Shouldn't happen because the pattern guard already checked in shardOps
              sys.error(s"Unsupported comparison operator for shard key: $op")
          }

        case In(f@GetField(`timeField`), allTimestamps(ts)) =>
          // [`timeField` IN (<timestampLiterals>)]
          val comparisons = ts.map(Comparison(f, EQ, _))
          val results = comparisons.map(prunePartition(timeField, mShardKeyField, _, partitionMin, partitionMax, partitionShardKey))
          // This partition can be pruned if _every_ literal IS NOT within this partition's time bounds
          !results.contains(false)

        case In(gf@GetField(f), allStrings(ts)) if mShardKeyField.contains(f) =>
          // [`shardKeyField` IN (<stringLiterals>)]
          val comparisons = ts.map(Comparison(gf, EQ, _))
          val results = comparisons.map(prunePartition(timeField, mShardKeyField, _, partitionMin, partitionMax, partitionShardKey))
          // This partition can be pruned if _every_ literal IS NOT this partition's shard key
          !results.contains(false)

        case and: And =>
          val pruneLeft = prunePartition(timeField, mShardKeyField, and.left(), partitionMin, partitionMax, partitionShardKey)
          val pruneRight = prunePartition(timeField, mShardKeyField, and.left(), partitionMin, partitionMax, partitionShardKey)

          pruneLeft || pruneRight // TODO!!

        case or: Or =>
          val pruneLeft = prunePartition(timeField, mShardKeyField, or.left(), partitionMin, partitionMax, partitionShardKey)
          val pruneRight = prunePartition(timeField, mShardKeyField, or.left(), partitionMin, partitionMax, partitionShardKey)

          pruneLeft && pruneRight // TODO!!

        case not: Not =>
          val pruneChild = prunePartition(timeField, mShardKeyField, not.child(), partitionMin, partitionMax, partitionShardKey)
          !pruneChild // TODO!!

        case _ =>
          // This predicate shouldn't have gotten this far; log it but don't crash
          log.warn(s"Unsupported predicate for partition pruning: $predicate")
          false
      }
  }

  /**
   * Looks at an expression and, if it's a binary comparison operator of the form `<left> <op> <right>`
   * where `op` is one of [[timeOps]], returns a tuple of (`left`, `op`, `right`).
   *
   * Note that this is also used to match shard key comparisons, but the caller needs to check that `op` is in
   * [[shardOps]].
   */
  private object Comparison {
    def apply(l: Expression, op: String, r: Expression) = new GeneralScalarExpression(op, Array(l, r))
    def unapply(expr: Expression): Option[(Expression, String, Expression)] = {
      val kids = expr.children()
      expr match {
        case gse: GeneralScalarExpression if timeOps.contains(gse.name()) && kids.size == 2 =>
          // TODO this assumes timeOps is a superset of shardOps, change it if that's no longer the case
          Some(kids(0), gse.name(), kids(1))
        case _ => None
      }
    }
  }

  /**
   * Looks at an expression and, if it's of the form `<left> IN (<rights>)` where `rights` has at least one value,
   * returns a tuple of (`left`, `rights`).
   *
   * Note that this assumes `foo IN ()` would have already been optimized away as tautological.
   */
  private object In {
    def unapply(expr: Expression): Option[(Expression, List[Expression])] = {
      val kids = expr.children()
      expr match {
        case gse: GeneralScalarExpression if gse.name() == "IN" && kids.size >= 2 =>
          Some(kids(0) -> kids.drop(1).toList)
        case _ => None
      }
    }
  }

  /**
   * Looks at a list of expressions, and if it's not empty, and EVERY value is a literals of type `typ`,
   * returns them as a list of [[LiteralValue]].
   *
   * Returns nothing if:
   *  - the list is empty
   *  - ANY value is not a literal
   *  - ANY value is a literal, but not of the `desired` type
   *
   * @param desired the type to search for
   */
  private class AllLiterals(desired: DataType) {
    def unapply(expressions: List[Expression]): Option[List[LiteralValue[_]]] = {
      val literals = expressions.flatMap {
        case lit: LiteralValue[_] => if (lit.dataType == desired) Some(lit) else None
        case _ => None
      }
      if (expressions.nonEmpty && literals.size == expressions.size) Some(literals) else None
    }
  }

  /**
   * Looks at an expression, and if it's a `FieldReference(<ss>)` (where `ss` is a single string),
   * returns that string, otherwise returns nothing.
   */
  private object GetField {
    def unapply(expr: Expression): Option[String] = {
      expr match {
        case FieldReference(Seq(f)) => Some(f)
        case _ => None
      }
    }
  }
}
