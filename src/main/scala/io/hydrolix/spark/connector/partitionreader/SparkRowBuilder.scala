package io.hydrolix.spark.connector.partitionreader

import java.time.Instant
import java.time.format.DateTimeFormatter
import scala.collection.JavaConverters._
import scala.collection.mutable

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.UTF8String

import io.hydrolix.connectors.types._
import io.hydrolix.connectors.instantToMicros

final class SparkRowBuilder(override val `type`: StructType) extends RowBuilder[InternalRow, GenericArrayData, ArrayBasedMapData] {
  private val data = mutable.HashMap[String, Any]()

  type AB = SparkArrayBuilder
  type MB = SparkMapBuilder

  override def newArrayBuilder(`type`: ArrayType): SparkArrayBuilder = new SparkArrayBuilder(`type`)

  override def newMapBuilder(`type`: MapType): SparkMapBuilder = new SparkMapBuilder(`type`)

  override def setField(name: String, value: Any) = data(name) = value
  override def setNull(name: String): Unit = ()

  private def node2Any(node: JsonNode, name: Option[String], dt: ValueType): Any = {
    node match {
      case null => null
      case n if n.isNull => null
      case s: TextNode => str(s, dt)
      case n: NumericNode => num(n, dt)
      case b: BooleanNode => bool(b, dt)
      case a: ArrayNode =>
        dt match {
          case ArrayType(elementType, _) =>
            val values = a.asScala.map(node2Any(_, name, elementType)).toList
            new GenericArrayData(values)

          case other => sys.error(s"TODO JSON array field $name needs conversion from $other to $dt")
        }
      case obj: ObjectNode =>
        dt match {
          case MapType(keyType, valueType, _) =>
            if (keyType != StringType) sys.error(s"TODO JSON map field $name keys are $keyType, not strings")
            val keys = obj.fieldNames().asScala.map(UTF8String.fromString).toArray
            val values = obj.fields().asScala.map(entry => node2Any(entry.getValue, name, valueType)).toArray
            new ArrayBasedMapData(new GenericArrayData(keys), new GenericArrayData(values))

          case other => sys.error(s"TODO JSON map field $name needs conversion from $other to $dt")
        }
    }
  }

  private def str(s: TextNode, dt: ValueType): Any = {
    dt match {
      case StringType => UTF8String.fromString(s.textValue())
      case TimestampType(_) =>
        val inst = Instant.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(s.textValue()))
        instantToMicros(inst)
      case other => sys.error(s"TODO make a $other from string value '$s'")
    }
  }

  private def num(n: NumericNode, dt: ValueType): Any = {
    dt match {
      case DecimalType(_, _) => Decimal(n.decimalValue()) // TODO do we care to preserve the precision and scale here?
      case Int64Type => n.longValue()
      case Float64Type => n.doubleValue()
      case Float32Type => n.floatValue()
      case Int32Type => n.intValue()
      case Int16Type => n.shortValue()
      case other => sys.error(s"TODO make a $other from JSON value '$n'")
    }
  }

  private def bool(n: BooleanNode, dt: ValueType) = {
    dt match {
      case BooleanType => n.booleanValue()
      case Int32Type => if (n.booleanValue()) 1 else 0
      case Int64Type => if (n.booleanValue()) 1L else 0L
      case Float64Type => if (n.booleanValue()) 1.0 else 0.0
      case other => sys.error(s"TODO make a $other from JSON value '$n'")
    }
  }

  override def convertJsonValue(`type`: ValueType, jvalue: JsonNode): Any = {
    node2Any(jvalue, None, `type`)
  }

  override def build: InternalRow = {
    val values = for (field <- `type`.fields) yield data.get(field.name).orNull

    new GenericInternalRow(values.toArray)
  }

  final class SparkArrayBuilder(val `type`: ArrayType) extends ArrayBuilder {
    private val elements = new java.util.ArrayList[Any]()
    private val nulls = mutable.BitSet()

    override def setNull(pos: Int): Unit = {
      nulls += pos
    }

    override def set(pos: Int, value: Any): Unit = {
      elements.ensureCapacity(pos+1)
      elements.set(pos, value)
    }

    override def build: GenericArrayData = {
      val data = new GenericArrayData(elements)
      for (pos <- nulls) {
        data.setNullAt(pos)
      }
      data
    }
  }

  final class SparkMapBuilder(val `type`: MapType) extends MapBuilder {
    private val keys = new java.util.ArrayList[Any]()
    private val values = new java.util.ArrayList[Any]()

    override def putNull(key: Any): Unit = ()

    override def put(key: Any, value: Any): Unit = {
      keys.add(key)
      values.add(value)
    }

    override def build: ArrayBasedMapData = new ArrayBasedMapData(new GenericArrayData(keys), new GenericArrayData(values))
  }
}
