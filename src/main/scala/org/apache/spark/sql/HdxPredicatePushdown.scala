// NOTE: this is in the spark.sql package because we make use of a few private[sql] members in there 😐
package org.apache.spark.sql

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.expressions.filter.{AlwaysFalse, AlwaysTrue, And, Not}
import org.apache.spark.sql.connector.expressions.{Expression, FieldReference, GeneralScalarExpression, LiteralValue}
import org.apache.spark.sql.types.DataTypes

object HdxPredicatePushdown extends Logging {
  /**
   * Note, [[Comparison.unapply()]] assumes shardOps is a subset of timeOps; fix that if this assumption is ever falsified!
   */
  private val timeOps = Set("<", "<=", ">", ">=", "==", "!=")
  private val shardOps = Set("==", "!=")

  /**
   * An expression is pushable if:
   *  - it refers to the single known timestamp field, and the expression is one of:
   *    - any binary comparison with a timestamp literal
   *    - Not(above)
   *    - And(above)
   *  - it refers to the single known "shard key" field, if any, and the expression is one of:
   *    - equality or not-equal comparison with a string literal
   *    - Not(above)
   *    - And(above)
   */
  def pushable(timeField: String, shardKeyField: Option[String], filter: Expression): Boolean = {
    val timeMatch = filter match {
      case Comparison(FieldReference(List(`timeField`)), operator, LiteralValue(t, DataTypes.TimestampType)) =>
        // Comparison between the time field and a timestamp literal (TODO TimestampNTZType?)
        log.warn(s"$timeField $operator $t")
        true
      case not: Not =>
        // Pushable if child is pushable
        pushable(timeField, shardKeyField, not.child())
      case and: And =>
        // Pushable if both children are pushable
        pushable(timeField, shardKeyField, and.left()) && pushable(timeField, shardKeyField, and.right())
      case _: AlwaysTrue | _: AlwaysFalse =>
        // Literal true/false are pushable (probably not required)
        true
      case _ => false
    }

    // forall returns true on a None (i.e. when there's no shard key)
    val shardMatch = shardKeyField.forall { sk =>
      filter match {
        case Comparison(FieldReference(List(`sk`)), op, LiteralValue(sk, DataTypes.StringType)) if shardOps.contains(op) =>
          // Equality comparison between the shard key field and a string literal
          log.warn(s"$shardKeyField $op $sk")
          true
        case not: Not =>
          // Pushable if child is pushable
          pushable(timeField, shardKeyField, not.child())
        case and: And =>
          // Pushable if both children are pushable
          pushable(timeField, shardKeyField, and.left()) && pushable(timeField, shardKeyField, and.right())
        case _: AlwaysTrue | _: AlwaysFalse =>
          // Literal true/false are pushable (probably not required)
          true
        case _ =>
          // There is a shard key field, but the filter doesn't satisfy our criteria
          false
      }
    }

    timeMatch && shardMatch
  }

  // This lets us say `case Comparison(l, op, r)` on an arbitrary expression and only match when it's actually a
  // comparison operator
  private object Comparison {
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
}
