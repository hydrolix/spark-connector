package org.apache.spark.sql

import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.expressions.{filter => sparkfilter}

import io.hydrolix.connectors.expr._
import io.hydrolix.connectors.{types => coretypes}

object SparkPredicates {
  def coreToSpark(expr: Expr[Boolean]): Predicate = {
    expr match {
      case Comparison(l, op, r) =>
        val sl = SparkExpressions.coreToSpark(l)
        val sr = SparkExpressions.coreToSpark(r)

        new Predicate(op.getSymbol, Array(sl, sr)) // This assumes our symbols are the same as Spark's

      case IsNull(expr) => new Predicate("IS_NULL", Array(SparkExpressions.coreToSpark(expr)))
      case Not(IsNull(expr)) => new Predicate("IS_NOT_NULL", Array(SparkExpressions.coreToSpark(expr)))
      case And(kids) =>
        require(kids.size == 2, "AND must have exactly two children")
        val skids = kids.map(coreToSpark)
        new sparkfilter.And(skids.head, skids(1))
      case Or(kids) =>
        require(kids.size == 2, "OR must have exactly two children")
        val skids = kids.map(coreToSpark)
        new sparkfilter.Or(skids.head, skids(1))
      case Not(expr) =>
        new sparkfilter.Not(coreToSpark(expr))
      case other => sys.error(s"Can't convert predicate from core to spark: $other")
    }
  }

  def sparkToCore(pred: Predicate, schema: coretypes.StructType): Expr[Boolean] = {
    pred match {
      case pred: Predicate if ComparisonOp.bySymbol.keySet.contains(pred.name()) && pred.children().length == 2 =>
        Comparison(
          SparkExpressions.sparkToCore(pred.children()(0), schema),
          ComparisonOp.bySymbol.get(pred.name()),
          SparkExpressions.sparkToCore(pred.children()(1), schema)
        )
      case pred: Predicate if pred.name() == "IS_NULL" => IsNull(SparkExpressions.sparkToCore(pred.children()(0), schema))
      case pred: Predicate if pred.name() == "IS_NOT_NULL" => Not(IsNull(SparkExpressions.sparkToCore(pred.children()(0), schema)))
      case and: sparkfilter.And => And(List(sparkToCore(and.left(), schema), sparkToCore(and.right(), schema)))
      case or: sparkfilter.Or => Or(List(sparkToCore(or.left(), schema), sparkToCore(or.right(), schema)))
      case not: sparkfilter.Not => Not(sparkToCore(not.child(), schema))
      case other => sys.error(s"Can't convert predicate from spark to core: $other")
    }
  }
}
