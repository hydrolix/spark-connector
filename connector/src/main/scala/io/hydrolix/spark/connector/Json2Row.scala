package io.hydrolix.spark.connector

import io.hydrolix.spark.model.JSON

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.time.Instant
import java.time.format.DateTimeFormatter
import scala.collection.JavaConverters._
import scala.sys.error

object Json2Row {
  def row(schema: StructType, jsonLine: String): InternalRow = {
    val obj = JSON.objectMapper.readValue[ObjectNode](jsonLine)

    val values = schema.map { col =>
      val node = obj.get(col.name) // TODO can we be sure the names match exactly?
      node2Any(node, col.name, col.dataType)
    }

    InternalRow.fromSeq(values)
  }

  private def node2Any(node: JsonNode, name: String, dt: DataType): Any = {
    node match {
      case n if n.isNull => null
      case s: TextNode => str(s, dt)
      case n: NumericNode => num(n, dt)
      case b: BooleanNode => bool(b, dt)
      case a: ArrayNode =>
        dt match {
          case ArrayType(elementType, _) =>
            val values = a.asScala.map(node2Any(_, name, elementType)).toList
            new GenericArrayData(values)

          case other => error(s"TODO JSON array field $name needs conversion from $other to $dt")
        }
      case obj: ObjectNode =>
        dt match {
          case MapType(keyType, valueType, _) =>
            if (keyType != DataTypes.StringType) error(s"TODO JSON map field $name keys are $keyType, not strings")
            val keys = obj.fieldNames().asScala.map(UTF8String.fromString).toArray
            val values = obj.fields().asScala.map(entry => node2Any(entry.getValue, name, valueType)).toArray
            new ArrayBasedMapData(new GenericArrayData(keys), new GenericArrayData(values))

          case other => error(s"TODO JSON map field $name needs conversion from $other to $dt")
        }
    }
  }

  private def str(s: TextNode, dt: DataType): Any = {
    dt match {
      case StringType => UTF8String.fromString(s.textValue())
      case DataTypes.TimestampType =>
        val inst = Instant.from(DateTimeFormatter.ISO_DATE_TIME.parse(s.textValue()))
        (inst.getEpochSecond * 1000000) + inst.getNano / 1000
      case other => error(s"TODO make a $other from string value '$s'")
    }
  }

  private def num(n: NumericNode, dt: DataType): Any = {
    dt match {
      case _: DecimalType => n.decimalValue() // TODO do we care to preserve the precision and scale here?
      case LongType => n.longValue()
      case DoubleType => n.doubleValue()
      case FloatType => n.floatValue()
      case IntegerType => n.intValue()
      case ShortType => n.shortValue()
      case other => error(s"TODO make a $other from JSON value '$n'")
    }
  }


  private def bool(n: BooleanNode, dt: DataType) = {
    dt match {
      case BooleanType => n.booleanValue()
      case IntegerType => if (n.booleanValue()) 1 else 0
      case LongType => if (n.booleanValue()) 1L else 0L
      case DoubleType => if (n.booleanValue()) 1.0 else 0.0
      case other => error(s"TODO make a $other from JSON value '$n'")
    }
  }
}
