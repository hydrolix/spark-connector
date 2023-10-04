package org.apache.spark.sql

import org.apache.spark.sql.catalyst.util.{DateTimeUtils, quoteIfNeeded}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.expressions.{Expression, FieldReference, LiteralValue, filter => sparkfilter}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.unsafe.types.UTF8String

import io.hydrolix.connectors.expr._
import io.hydrolix.connectors.types.AnyType
import io.hydrolix.connectors.{types => coretypes}

object SparkExpressions {
  def coreToSpark(expr: Expr[Any]): Expression = {
    expr match {
      case expr if expr.`type` == coretypes.BooleanType => SparkPredicates.coreToSpark(expr.asInstanceOf[Expr[Boolean]])
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

      case pred: Predicate => SparkPredicates.sparkToCore(pred, schema)

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
