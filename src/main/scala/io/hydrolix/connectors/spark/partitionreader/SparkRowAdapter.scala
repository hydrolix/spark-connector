package io.hydrolix.connectors.spark.partitionreader

import java.time.Instant
import java.time.format.DateTimeFormatter
import scala.collection.mutable

import com.fasterxml.jackson.databind.node._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.UTF8String

import io.hydrolix.connectors.data.RowAdapter
import io.hydrolix.connectors.instantToMicros
import io.hydrolix.connectors.types._

object SparkRowAdapter extends RowAdapter[InternalRow, GenericArrayData, ArrayBasedMapData] {
  type RB = SparkRowBuilder
  type AB = SparkArrayBuilder
  type MB = SparkMapBuilder

  override def newRowBuilder(`type`: StructType, rowId: Int): SparkRowBuilder = new SparkRowBuilder(`type`)

  override def newArrayBuilder(`type`: ArrayType): SparkArrayBuilder = new SparkArrayBuilder(`type`)

  override def newMapBuilder(`type`: MapType): SparkMapBuilder = new SparkMapBuilder(`type`)


  override def string(value: String): Any = UTF8String.fromString(value)

  override def jsonString(s: TextNode, dt: ValueType): Any = {
    dt match {
      case StringType => UTF8String.fromString(s.textValue())
      case TimestampType(_) =>
        val inst = Instant.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(s.textValue()))
        instantToMicros(inst)
      case other => sys.error(s"TODO make a $other from string value '$s'")
    }
  }

  override def jsonNumber(n: NumericNode, dt: ValueType): Any = {
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

  override def jsonBoolean(n: BooleanNode, dt: ValueType): Any = {
    dt match {
      case BooleanType => n.booleanValue()
      case Int32Type => if (n.booleanValue()) 1 else 0
      case Int64Type => if (n.booleanValue()) 1L else 0L
      case Float64Type => if (n.booleanValue()) 1.0 else 0.0
      case other => sys.error(s"TODO make a $other from JSON value '$n'")
    }
  }

  final class SparkRowBuilder(val `type`: StructType) extends RowBuilder {
    private val data = mutable.HashMap[String, Any]()

    override def setNull(name: String): Unit = ()

    override def setField(name: String, value: Any): Unit = data(name) = value

    override def build: InternalRow = {
      val values = for (field <- `type`.fields) yield data.get(field.name).orNull

      new GenericInternalRow(values.toArray)
    }
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

  override def row(rowId: Int, `type`: StructType, obj: ObjectNode): InternalRow = {
    val rb = newRowBuilder(`type`, rowId)

    for (sf <- `type`.fields) {
      val node = obj.get(sf.name)
      val value = node2Any(node, sf.`type`)
      if (value == null) {
        rb.setNull(sf.name)
      } else {
        rb.setField(sf.name, value)
      }
    }
    rb.build
  }
}

