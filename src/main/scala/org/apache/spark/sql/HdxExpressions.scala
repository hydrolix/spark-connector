package org.apache.spark.sql

import org.apache.spark.sql.catalyst.util.{DateTimeUtils, quoteIfNeeded}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.expressions.{Expression, FieldReference, LiteralValue, filter => sparkfilter}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.unsafe.types.UTF8String

import io.hydrolix.connectors.expr._
import io.hydrolix.connectors.types.AnyType
import io.hydrolix.connectors.{types => coretypes}

object HdxExpressions {
  def coreToSpark(expr: Expr[Any]): Expression = {
    expr match {
      case expr if expr.`type` == coretypes.BooleanType => HdxPredicates.coreToSpark(expr.asInstanceOf[Expr[Boolean]])
      case GetField(name, _)       => FieldReference(name)
      case BooleanLiteral(value)   => LiteralValue(value, DataTypes.BooleanType)
      case StringLiteral(value)    => LiteralValue(UTF8String.fromString(value), DataTypes.StringType)
      case Int8Literal(value)      => LiteralValue(value, DataTypes.ByteType)
      case UInt8Literal(value)     => LiteralValue(value, DataTypes.ShortType)
      case Int16Literal(value)     => LiteralValue(value, DataTypes.ShortType)
      case UInt16Literal(value)    => LiteralValue(value, DataTypes.IntegerType)
      case Int32Literal(value)     => LiteralValue(value, DataTypes.IntegerType)
      case UInt32Literal(value)    => LiteralValue(value, DataTypes.LongType)
      case Int64Literal(value)     => LiteralValue(value, DataTypes.LongType)
      case UInt64Literal(value)    => LiteralValue(value, DataTypes.createDecimalType(20,0))
      case TimestampLiteral(value) => LiteralValue(DateTimeUtils.instantToMicros(value), DataTypes.TimestampType)
      case other                   => sys.error(s"Can't convert expression from core to spark: $other")
    }
  }

  def sparkToCore(expr: Expression, schema: coretypes.StructType): Expr[Any] = {
    expr match {
      case FieldReference(parts) =>
        val name = parts.map(quoteIfNeeded).mkString(".")
        GetField(name, schema.byName.get(name).map(_.`type`).getOrElse(AnyType))

      case pred: Predicate => HdxPredicates.sparkToCore(pred, schema)

      case LiteralValue(value: Boolean,    DataTypes.BooleanType)   => BooleanLiteral(value)
      case LiteralValue(value: String,     DataTypes.StringType)    => StringLiteral(value)
      case LiteralValue(value: UTF8String, DataTypes.StringType)    => StringLiteral(value.toString)
      case LiteralValue(value: Byte,       DataTypes.ByteType)      => Int8Literal(value)
      case LiteralValue(value: Short,      DataTypes.ShortType)     => Int16Literal(value)
      case LiteralValue(value: Int,        DataTypes.IntegerType)   => Int32Literal(value)
      case LiteralValue(value: Long,       DataTypes.LongType)      => Int64Literal(value)
      case LiteralValue(value: Float,      DataTypes.FloatType)     => Float32Literal(value)
      case LiteralValue(value: Double,     DataTypes.DoubleType)    => Float64Literal(value)
      case LiteralValue(value: Long,       DataTypes.TimestampType) => TimestampLiteral(DateTimeUtils.microsToInstant(value))
      // TODO array literals?
      case other => sys.error(s"Can't convert expression from spark to core: $other")
    }
  }
}

object HdxPredicates {
  def coreToSpark(expr: Expr[Boolean]): Predicate = {
    expr match {
      case Comparison(l, op, r) =>
        val sl = HdxExpressions.coreToSpark(l)
        val sr = HdxExpressions.coreToSpark(r)

        new Predicate(op.getSymbol, Array(sl, sr)) // This assumes our symbols are the same as Spark's

      case IsNull(expr)      => new Predicate("IS_NULL", Array(HdxExpressions.coreToSpark(expr)))
      case Not(IsNull(expr)) => new Predicate("IS_NOT_NULL", Array(HdxExpressions.coreToSpark(expr)))
      case And(kids)         => new Predicate("AND", kids.map(coreToSpark).toArray)
      case Or(kids)          => new Predicate("OR", kids.map(coreToSpark).toArray)
      case Not(expr)         => new Predicate("NOT", Array(coreToSpark(expr)))
      case other             => sys.error(s"Can't convert predicate from core to spark: $other")
    }
  }

  def sparkToCore(pred: Predicate, schema: coretypes.StructType): Expr[Boolean] = {
    pred match {
      case pred: Predicate if ComparisonOp.bySymbol.keySet.contains(pred.name()) && pred.children().length == 2 =>
        Comparison(
          HdxExpressions.sparkToCore(pred.children()(0), schema),
          ComparisonOp.bySymbol.get(pred.name()),
          HdxExpressions.sparkToCore(pred.children()(1), schema)
        )
      case pred: Predicate if pred.name() == "IS_NULL" => IsNull(HdxExpressions.sparkToCore(pred.children()(0), schema))
      case pred: Predicate if pred.name() == "IS_NOT_NULL" => Not(IsNull(HdxExpressions.sparkToCore(pred.children()(0), schema)))
      case and: sparkfilter.And => And(List(sparkToCore(and.left(), schema), sparkToCore(and.right(), schema)))
      case or: sparkfilter.Or => Or(List(sparkToCore(or.left(), schema), sparkToCore(or.right(), schema)))
      case not: sparkfilter.Not => Not(sparkToCore(not.child(), schema))
      case other => sys.error(s"Can't convert predicate from spark to core: $other")
    }
  }
}
