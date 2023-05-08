// NOTE: this is in the spark.sql package because we make use of a few private[sql] members in there 😐
package org.apache.spark.sql

import io.hydrolix.spark.model.HdxColumnInfo

import net.openhft.hashing.LongHashFunction
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.DateTimeUtils.microsToInstant
import org.apache.spark.sql.connector.expressions.aggregate._
import org.apache.spark.sql.connector.expressions.filter.{And, Not, Or}
import org.apache.spark.sql.connector.expressions.{Expression, FieldReference, GeneralScalarExpression, LiteralValue}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField}

import java.time.Instant
import java.{lang => jl}

/**
 * TODO:
 *  - see if multi-part names will ever show up (e.g. in a join?); that would break [[GetField]] but hopefully only
 *    in a way that would allow fewer pushdown opportunities rather than incorrect results.
 */
object HdxPushdown extends Logging {
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
  private val hdxOps = Map(
    LT -> LT,
    LE -> LE,
    GT -> GT,
    GE -> GE,
    EQ -> EQ,
    NE -> "!="
  )
  private val hdxSimpleTypes = Set(
    DataTypes.BooleanType,
    DataTypes.StringType,
    DataTypes.ByteType,
    DataTypes.ShortType,
    DataTypes.IntegerType,
    DataTypes.LongType,
    DataTypes.DoubleType,
    DataTypes.FloatType,
  )

  private val allTimestamps = new AllLiterals(DataTypes.TimestampType)
  private val allStrings = new AllLiterals(DataTypes.StringType)

  /**
   * Tests whether a given predicate should be pushable for the given timestamp and shard key field names. Note that we
   * have no concept of a unique key, so we can never return 1 here. However, `prunePartition` should be able to
   * authoritatively prune particular partitions, since it will have their specific min/max timestamps and shard keys.
   *
   * TODO:
   *  - once we figure out precisely what FilterExpr can do, change this to return `1` sometimes!
   *  - Decide whether we need to care about [[org.apache.spark.sql.types.TimestampNTZType]]
   *
   * @param primaryKeyField name of the timestamp ("primary key") field for this table
   * @param mShardKeyField  name of the shard key field for this table
   * @param predicate       predicate to test for pushability
   * @param cols            the Hydrolix column metadata
   * @return (see [[org.apache.spark.sql.connector.read.SupportsPushDownV2Filters]])
   *          - 1 if this predicate doesn't need to be evaluated again after scanning
   *          - 2 if this predicate still needs to be evaluated again after scanning
   *          - 3 if this predicate is not pushable
   */
  def pushable(primaryKeyField: String, mShardKeyField: Option[String], predicate: Expression, cols: Map[String, HdxColumnInfo]): Int = {
    predicate match {
      case Comparison(GetField(`primaryKeyField`), op, LiteralValue(_: Long, DataTypes.TimestampType)) if timeOps.contains(op) =>
        // Comparison between the primary key field and a timestamp literal:
        // 2 because FilterInterpreter doesn't look at the primary key field
        2
      case In(GetField(`primaryKeyField`), allTimestamps(_)) =>
        // primaryKeyField IN (timestamp literals):
        // 2 because FilterInterpreter doesn't look at the primary key field
        2
      case Comparison(GetField(field), op, LiteralValue(_: String, DataTypes.StringType)) if mShardKeyField.contains(field) && shardOps.contains(op) =>
        // shardKeyField == string or shardKeyField <> string
        // Note: this is a 2 even when shardKeyField == string because in the rare case of a hash collision we still
        // need to compare the raw strings.
        2
      case In(GetField(field), allStrings(_)) if mShardKeyField.contains(field) =>
        // shardKeyField IN (string literals)
        2
      case Comparison(GetField(f), op, LiteralValue(_, typ)) if hdxOps.contains(op) && hdxSimpleTypes.contains(typ) =>
        // field op literal
        val hcol = cols.getOrElse(f, sys.error(s"No HdxColumnInfo for $f"))
        if (hcol.indexed == 2) {
          // This field is indexed in all partitions, it can be pushed down completely!
          // TODO not so fast! filter_interpreter is only doing block filtering; we still need to eval after scan, but
          //  it'll be much smaller
          2
        } else {
          2
        }
      case not: Not =>
        // child is pushable
        pushable(primaryKeyField, mShardKeyField, not.child(), cols)
      case and: And =>
        // max of childrens' pushability
        pushable(primaryKeyField, mShardKeyField, and.left(), cols) max pushable(primaryKeyField, mShardKeyField, and.right(), cols)
      case or: Or =>
        // max of childrens' pushability
        pushable(primaryKeyField, mShardKeyField, or.left(), cols) max pushable(primaryKeyField, mShardKeyField, or.right(), cols)
      case _ =>
        // Something else; it should be logged by the caller as non-pushable
        3
    }
  }

  /**
   * Evaluate the min/max timestamps and shard key of a single partition against a single predicate
   * to determine if the partition CAN be pruned, i.e. doesn't need to be scanned.
   *
   * @param primaryKeyField   the name of the timestamp field for this partition
   * @param mShardKeyField    the name of the shard key field for this partition
   * @param predicate         the predicate to evaluate
   * @param partitionMin      the minimum timestamp for data in this partition
   * @param partitionMax      the maximum timestamp for data in this partition
   * @param partitionShardKey the shard key of this partition. Not optional because `42bc986dc5eec4d3` (`wyhash("")`)
   *                          appears when there's no shard key.
   * @return `true` if this partition should be pruned (i.e. NOT scanned) according to `predicate`, or
   *         `false` if it MUST be scanned
   */
  def prunePartition(primaryKeyField: String,
                      mShardKeyField: Option[String],
                           predicate: Expression,
                        partitionMin: Instant,
                        partitionMax: Instant,
                   partitionShardKey: String)
                                    : Boolean =
  {
      predicate match {
        case Comparison(GetField(`primaryKeyField`), op, LiteralValue(micros: Long, DataTypes.TimestampType)) if timeOps.contains(op) =>
          // [`timestampField` <op> <timestampLiteral>], where op ∈ `timeOps`
          val timestamp = microsToInstant(micros)

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

        case In(f@GetField(`primaryKeyField`), allTimestamps(ts)) =>
          // [`timeField` IN (<timestampLiterals>)]
          val comparisons = ts.map(Comparison(f, EQ, _))
          val results = comparisons.map(prunePartition(primaryKeyField, mShardKeyField, _, partitionMin, partitionMax, partitionShardKey))
          // This partition can be pruned if _every_ literal IS NOT within this partition's time bounds
          !results.contains(false)

        case In(gf@GetField(f), allStrings(ts)) if mShardKeyField.contains(f) =>
          // [`shardKeyField` IN (<stringLiterals>)]
          val comparisons = ts.map(Comparison(gf, EQ, _))
          val results = comparisons.map(prunePartition(primaryKeyField, mShardKeyField, _, partitionMin, partitionMax, partitionShardKey))
          // This partition can be pruned if _every_ literal IS NOT this partition's shard key
          // TODO do we need care about hash collisions here? It might depend on whether op is EQ or NE
          !results.contains(false)

        case and: And =>
          val pruneLeft = prunePartition(primaryKeyField, mShardKeyField, and.left(), partitionMin, partitionMax, partitionShardKey)
          val pruneRight = prunePartition(primaryKeyField, mShardKeyField, and.left(), partitionMin, partitionMax, partitionShardKey)

          pruneLeft || pruneRight // TODO!!

        case or: Or =>
          val pruneLeft = prunePartition(primaryKeyField, mShardKeyField, or.left(), partitionMin, partitionMax, partitionShardKey)
          val pruneRight = prunePartition(primaryKeyField, mShardKeyField, or.left(), partitionMin, partitionMax, partitionShardKey)

          pruneLeft && pruneRight // TODO!!

        case not: Not =>
          val pruneChild = prunePartition(primaryKeyField, mShardKeyField, not.child(), partitionMin, partitionMax, partitionShardKey)
          !pruneChild // TODO!!

        case _ =>
          false
      }
  }

  /**
   * Try to render a Spark predicate into something that will hopefully be acceptable to FilterExprParser
   *
   * @return Some(s) if the predicate was rendered successfully; None if not
   */
  def renderHdxFilterExpr(expr: Expression, primaryKeyField: String, cols: Map[String, HdxColumnInfo]): Option[String] = {
    expr match {
      case Comparison(GetField(field), op, lit@LiteralValue(_, typ)) if timeOps.contains(op) && hdxSimpleTypes.contains(typ) =>
        val hcol = cols.getOrElse(field, sys.error(s"No HdxColumnInfo for $field"))
        val hdxOp = hdxOps.getOrElse(op, sys.error(s"No hydrolix operator for Spark operator $op"))

        if (hcol.indexed == 2) {
          // This field is indexed in all partitions, make it so
          Some(s""""$field" $hdxOp $lit""")
        } else {
          None
        }

      case Comparison(GetField(field), op, LiteralValue(lit, DataTypes.TimestampType)) if timeOps.contains(op) =>
        if (field == primaryKeyField) {
          // FilterInterpreter specifically doesn't try to use the primary key field
          None
        } else {
          // Note, at this point we don't care if it's the partition min/max timestamp; any timestamp will do
          val hdxOp = hdxOps.getOrElse(op, sys.error(s"No hydrolix operator for Spark operator $op"))
          val hcol = cols.getOrElse(field, sys.error(s"No HdxColumnInfo for $field"))

          val t = microsToInstant(lit.asInstanceOf[Long])
          val long = if (hcol.clickhouseType.contains("datetime64")) t.toEpochMilli else t.getEpochSecond

          if (hcol.indexed == 2) {
            Some(s""""$field" $hdxOp '$long'""")
          } else {
            None
          }
        }

      case and: And =>
        val results = and.children().map(kid => renderHdxFilterExpr(kid, primaryKeyField, cols))
        if (results.contains(None)) None else Some(results.flatten.mkString("(", " AND ", ")"))

      case or: Or =>
        val results = or.children().map(kid => renderHdxFilterExpr(kid, primaryKeyField, cols))
        if (results.contains(None)) None else Some(results.flatten.mkString("(", " OR ", ")"))

      case not: Not =>
        not.child() match {
          case Comparison(l, EQ, r) =>
            // Spark turns `foo <> bar` into `NOT (foo = bar)`, put it back so FilterInterpreter likes it better
            renderHdxFilterExpr(Comparison(l, NE, r), primaryKeyField, cols)
          case other =>
            renderHdxFilterExpr(other, primaryKeyField, cols)
              .map(res => s"NOT ($res)")
        }

      case _ =>
        None
    }
  }

  def pushableAggs(aggregation: Aggregation, primaryKeyField: String): List[(AggregateFunc, StructField)] = {
    if (aggregation.groupByExpressions().nonEmpty) return Nil

    aggregation.aggregateExpressions().flatMap {
      case cs: CountStar => Some(cs -> StructField("COUNT(*)", DataTypes.LongType))
      case mf @ MinField(`primaryKeyField`) => Some(mf -> StructField(s"MIN($primaryKeyField)", DataTypes.TimestampType))
      case mf @ MaxField(`primaryKeyField`) => Some(mf -> StructField(s"MAX($primaryKeyField)", DataTypes.TimestampType))
      case _ => None
    }.toList
  }

  /**
   * Looks at an expression, and if it's a Min(FieldReference(`field`)) where `field` is single-valued, returns `field`.
   */
  private object MinField {
    def unapply(expr: Expression): Option[String] = {
      expr match {
        case min: Min =>
          min.children() match {
            case Array(FieldReference(Seq(fieldName))) => Some(fieldName)
            case _ => None
          }
        case _ => None
      }
    }
  }

  /**
   * Looks at an expression, and if it's a Max(FieldReference(`field`)) where `field` is single-valued, returns `field`.
   */
  private object MaxField {
    def unapply(expr: Expression): Option[String] = {
      expr match {
        case max: Max =>
          max.children() match {
            case Array(FieldReference(Seq(fieldName))) => Some(fieldName)
            case _ => None
          }
        case _ => None
      }
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
    def unapply(expressions: List[Expression]): Option[List[LiteralValue[Any]]] = {
      val literals = expressions.flatMap {
        case lit: LiteralValue[Any] if lit.dataType == desired => Some(lit)
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
