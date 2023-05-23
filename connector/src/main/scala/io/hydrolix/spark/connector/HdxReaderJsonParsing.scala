package io.hydrolix.spark.connector

import io.hydrolix.spark.model.JSON

import com.fasterxml.jackson.core.{JsonParser, JsonToken}
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.execution.vectorized.{OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import org.apache.spark.unsafe.types.UTF8String

import java.io.InputStream
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util
import scala.collection.JavaConverters._
import scala.sys.error

object HdxReaderJsonParsing extends Logging {
  def row(schema: StructType, jsonLine: String): InternalRow = {
    val obj = JSON.objectMapper.readValue[ObjectNode](jsonLine)

    val values = schema.map { col =>
      val node = obj.get(col.name) // TODO can we be sure the names match exactly?
      node2Any(node, col.name, col.dataType)
    }

    InternalRow.fromSeq(values)
  }

  def batches(schema: StructType, stdout: InputStream, onBatch: ColumnarBatch => Unit, onDone: => Unit): Unit = {
    val parser = JSON.objectMapper.createParser(stdout)

    parser.nextToken() // Advance to start object if present, or null if empty stream

    while (true) {
      block(parser, schema) match {
        case Some(batch) =>
          onBatch(batch)
        case None =>
          onDone
          return
      }
    }
  }

  private def block(parser: JsonParser, schema: StructType): Option[ColumnarBatch] = {
    parser.currentToken() match {
      case JsonToken.START_OBJECT => // OK
      case null                   => return None // Done
      case other                  => sys.error(s"Expected object start, got $other")
    }

    // TODO this expects `rows` to come before `cols` and it would be tricky to change that, maybe document it
    if (parser.nextFieldName() != "rows") sys.error("Expected `rows` field")
    val rows = parser.nextIntValue(-1)
    if (rows == -1) sys.error("`rows` was not an Int value")

    val cols = OnHeapColumnVector.allocateColumns(rows, schema)

    val schemaFields = schema.fields.zipWithIndex.map { case (field, i) => field.name -> (field -> i) }.toMap

    if (parser.nextFieldName() != "cols") sys.error("Expected `cols` field")
    if (parser.nextToken() != JsonToken.START_OBJECT) sys.error("Expected object start for `cols`")

    while (parser.nextToken() != JsonToken.END_OBJECT) {
      // For each column in `cols`...
      val name = parser.currentName()
      if (parser.nextToken() != JsonToken.START_ARRAY) sys.error(s"Expected array start for column $name") // TODO maybe allow a null column instead of empty array?
      val (field, pos) = schemaFields.getOrElse(name, sys.error(s"Couldn't find field $name in schema"))

      var rowNo = -1
      while (parser.nextToken() != JsonToken.END_ARRAY) {
        // For each value in this column...
        rowNo += 1

        parser.currentToken() match {
          case JsonToken.VALUE_NULL => cols(pos).putNull(rowNo)
          case JsonToken.VALUE_STRING =>
            val s = parser.getText
            val bytes = s.getBytes("UTF-8")
            field.dataType match {
              case DataTypes.StringType => cols(pos).putByteArray(rowNo, bytes)
              case other => sys.error(s"Can't decode a $other from a JSON String")
            }

          case JsonToken.VALUE_NUMBER_INT =>
            field.dataType match {
              case DataTypes.ShortType => cols(pos).putShort(rowNo, parser.getShortValue)
              case DataTypes.IntegerType => cols(pos).putInt(rowNo, parser.getIntValue)
              case DataTypes.LongType => cols(pos).putLong(rowNo, parser.getLongValue)
              case dt: DecimalType if dt.precision == 0 => cols(pos).putDecimal(rowNo, Decimal(parser.getBigIntegerValue), 0)
              case other =>
                sys.error(s"Can't decode a $other from a JSON Int")
            }

          case JsonToken.VALUE_NUMBER_FLOAT =>
            field.dataType match {
              case DataTypes.FloatType => cols(pos).putFloat(rowNo, parser.getFloatValue)
              case DataTypes.DoubleType => cols(pos).putDouble(rowNo, parser.getDoubleValue)
              case dt: DecimalType =>
                val bd = parser.getDecimalValue
                cols(pos).putDecimal(rowNo, Decimal(bd), bd.precision()) // TODO we're ignoring dt.precision here...
              case other => sys.error(s"Can't decode a $other from a JSON Float")
            }

          case JsonToken.VALUE_TRUE =>
            field.dataType match {
              case DataTypes.BooleanType => cols(pos).putBoolean(rowNo, true)
              case other => sys.error(s"Can't decode a $other from a JSON Boolean")
            }

          case JsonToken.VALUE_FALSE =>
            field.dataType match {
              case DataTypes.BooleanType => cols(pos).putBoolean(rowNo, false)
              case other => sys.error(s"Can't decode a $other from a JSON Boolean")
            }

          case JsonToken.START_ARRAY =>
            field.dataType match {
              case ArrayType(elt, _) =>
            }


          case other => sys.error(s"Don't know how to decode a $other token")
        }
      }

      if (rowNo+1 < rows) {
        sys.error(s"$name only had ${rowNo+1} value(s); expected $rows")
      }
    }

    parser.nextToken() // Advance past the end of the `cols` object
    parser.nextToken() // Advance past the end of the block object

    Some(new ColumnarBatch(cols.toArray, rows))
  }

  private def putScalar(parser: JsonParser, dataType: DataType, rowId: Int, col: WritableColumnVector): Int = {
    val tok = parser.currentToken()

    if (tok == JsonToken.VALUE_NULL) {
      col.putNull(rowId)
      return rowId
    }

    if (dataType == DataTypes.StringType && tok == JsonToken.VALUE_STRING) {
      val bytes = parser.getText.getBytes("UTF-8")
      col.putByteArray(rowId, bytes)
    } else if (dataType == DataTypes.BooleanType && tok.isBoolean) {
      col.putBoolean(rowId, tok == JsonToken.VALUE_TRUE)
      rowId
    } else if (tok == JsonToken.VALUE_NUMBER_INT) {
      dataType match {
        case DataTypes.ByteType => col.putByte(rowId, parser.getByteValue)
        case DataTypes.ShortType => col.putShort(rowId, parser.getShortValue)
        case DataTypes.IntegerType => col.putInt(rowId, parser.getIntValue)
        case DataTypes.LongType => col.putLong(rowId, parser.getLongValue)
        case dt: DecimalType if dt.scale == 0 =>
          val bd = parser.getDecimalValue
          col.putDecimal(rowId, Decimal(bd), bd.precision())
        case other => sys.error(s"Can't decode a $other value from a $tok")
      }
      rowId
    } else if (tok == JsonToken.VALUE_NUMBER_FLOAT) {
      dataType match {
        case DataTypes.FloatType => col.putFloat(rowId, parser.getFloatValue)
        case DataTypes.DoubleType => col.putDouble(rowId, parser.getDoubleValue)
        case _: DecimalType =>
          val bd = parser.getDecimalValue
          col.putDecimal(rowId, Decimal(bd), bd.precision())
        case other => sys.error(s"Can't decode a $other value from a $tok")
      }
      rowId
    } else {
      sys.error(s"Can't putScalar a $dataType")
    }
  }

  private val scalarTypes = Set(
    DataTypes.StringType,
    DataTypes.ByteType,
    DataTypes.ShortType,
    DataTypes.IntegerType,
    DataTypes.LongType,
    DataTypes.BooleanType,
    DataTypes.FloatType,
    DataTypes.DoubleType,
    DataTypes.TimestampType,
  )

/*
  private def doStuff(parser: JsonParser, dataType: DataType, rowId: Int, col: WritableColumnVector): Unit = {
    if (scalarTypes.contains(dataType)) {
      putScalar(parser, dataType, rowId, col)
      return
    }

    dataType match {
      case ArrayType(elt, _) =>
        val kid = col.getChild(0)
        var kidRowId = -1
        var firstOffset = -1
        var sumOffsets = 0
        while (parser.nextToken() != JsonToken.END_ARRAY) {
          kidRowId += 1
          val offset = putScalar(parser, elt, kidRowId, kid)
          if (firstOffset != -1) firstOffset = offset
          sumOffsets += offset
        }
        col.putArray(rowId, firstOffset, sumOffsets)
      case MapType(kt, vt, _) =>
    }
    } else if (tok == JsonToken.START_ARRAY ) {
      dataType match {
        case ArrayType(elt, _) =>
        case other => sys.error(s"Can't decode a $other value from a $tok")
      }
    } else if (tok == JsonToken.START_OBJECT) {
      dataType match {
        case MapType(DataTypes.StringType, vt, _) =>
          val kid1 = col.getChild(0)
          val kid2 = col.getChild(1)

          var kidRowId = -1
          while (parser.nextToken() != JsonToken.END_OBJECT) {
            kidRowId += 1
            kid1.putByteArray(kidRowId, parser.currentName().getBytes("UTF-8"))
            doStuff(parser, vt, kidRowId, kid2)
          }
        case other => sys.error(s"Can't decode a $other value from a $tok")
      }
    } else {
      sys.error(s"Don't know how to handle a $dataType from a $tok")
    }
  }
*/


  private def consumeArray(parser: JsonParser, tokens: util.EnumSet[JsonToken], col: WritableColumnVector, f: (Int, JsonToken) => Unit): Unit = {
    var rowId = -1
    while (parser.nextToken() != JsonToken.END_ARRAY) {
      rowId += 1
      parser.currentToken() match {
        case JsonToken.VALUE_NULL => col.putNull(rowId)
        case tok if tokens.contains(tok) => f(rowId, tok)
      }
    }
  }




  private def consumeColumn(parser: JsonParser, field: StructField, col: OnHeapColumnVector): Unit = {
    parser.currentToken() match {
      case JsonToken.START_ARRAY => // OK
      case JsonToken.VALUE_NULL =>
        col.setAllNull()
        return 0
      case other =>
        sys.error(s"Can't read column of values starting with a $other")
    }

    field.dataType match {
      case DataTypes.StringType =>
        consumeArray(parser, util.EnumSet.of(JsonToken.VALUE_STRING), col, { (rowId, _) =>
          val bytes = parser.getText.getBytes("UTF-8")
          col.putByteArray(rowId, bytes)
        })

      case DataTypes.BooleanType =>
        consumeArray(parser, util.EnumSet.of(JsonToken.VALUE_TRUE, JsonToken.VALUE_FALSE), col, { (rowId, tok) =>
          col.putBoolean(rowId, tok == JsonToken.VALUE_TRUE)
        })

      case DataTypes.ShortType =>
        consumeArray(parser, util.EnumSet.of(JsonToken.VALUE_NUMBER_INT), col, { (rowId, _) =>
          col.putShort(rowId, parser.getShortValue)
        })

      case DataTypes.IntegerType =>
        consumeArray(parser, util.EnumSet.of(JsonToken.VALUE_NUMBER_INT), col, { (rowId, _) =>
          col.putInt(rowId, parser.getIntValue)
        })

      case DataTypes.LongType =>
        consumeArray(parser, util.EnumSet.of(JsonToken.VALUE_NUMBER_INT), col, { (rowId, _) =>
          col.putLong(rowId, parser.getLongValue)
        })

      case DataTypes.FloatType =>
        consumeArray(parser, util.EnumSet.of(JsonToken.VALUE_NUMBER_FLOAT), col, { (rowId, _) =>
          col.putFloat(rowId, parser.getFloatValue)
        })

      case DataTypes.DoubleType =>
        consumeArray(parser, util.EnumSet.of(JsonToken.VALUE_NUMBER_FLOAT), col, { (rowId, _) =>
          col.putDouble(rowId, parser.getDoubleValue)
        })

      case dt: DecimalType if dt.scale == 0 =>
        consumeArray(parser, util.EnumSet.of(JsonToken.VALUE_NUMBER_INT), col, { (rowId, _) =>
          val dec = parser.getDecimalValue
          col.putDecimal(rowId, Decimal(dec), dec.precision())
        })

      case MapType(kt, vt, _) =>
        val kid1 = col.getChild(0)
        val kid2 = col.getChild(1)

        consumeArray(parser, util.EnumSet.allOf(classOf[JsonToken]), col, { (rowId, tok) =>
          if (parser.currentToken() != JsonToken.START_OBJECT) sys.error("Expected an Object")
          while (parser.nextToken() != JsonToken.END_OBJECT) {
            val key = parser.currentName()
            val value = parser.currentValue()
          }
        })

      case ArrayType(elt, _) =>
    }
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
      case _: DecimalType => Decimal(n.decimalValue()) // TODO do we care to preserve the precision and scale here?
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
