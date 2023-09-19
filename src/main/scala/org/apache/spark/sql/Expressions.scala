package org.apache.spark.sql

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.expressions.{Expression, FieldReference, LiteralValue}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.unsafe.types.UTF8String

import io.hydrolix.connectors.expr._

object Expressions {
  def coreToSpark(expr: Expr[Any]): Expression = {
    expr match {
      case Comparison(l, op, r) =>
        val sl = coreToSpark(l)
        val sr = coreToSpark(r)

        new Predicate(op.getSymbol, Array(sl, sr)) // This assumes our symbols are the same as Spark's

      case IsNull(expr) => new Predicate("IS_NULL", Array(coreToSpark(expr)))
      case Not(expr) => new Predicate("NOT", Array(coreToSpark(expr)))
      case And(kids) => new Predicate("AND", kids.map(coreToSpark(_)).toArray)
      case Or(kids) => new Predicate("OR", kids.map(coreToSpark(_)).toArray)

      case GetField(name, _) => FieldReference(name)

      case BooleanLiteral(value) => LiteralValue(value, DataTypes.BooleanType)
      case StringLiteral(value) => LiteralValue(UTF8String.fromString(value), DataTypes.StringType)
      case Int8Literal(value) => LiteralValue(value, DataTypes.ByteType)
      case UInt8Literal(value) => LiteralValue(value, DataTypes.ShortType)
      case Int16Literal(value) => LiteralValue(value, DataTypes.ShortType)
      case UInt16Literal(value) => LiteralValue(value, DataTypes.IntegerType)
      case Int32Literal(value) => LiteralValue(value, DataTypes.IntegerType)
      case UInt32Literal(value) => LiteralValue(value, DataTypes.LongType)
      case Int64Literal(value) => LiteralValue(value, DataTypes.LongType)
      case UInt64Literal(value) => LiteralValue(value, DataTypes.createDecimalType(20,0))
      case TimestampLiteral(value) => LiteralValue(DateTimeUtils.instantToMicros(value), DataTypes.TimestampType)
    }
  }
}
