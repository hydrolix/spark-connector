package io.hydrolix.spark.connector

import io.hydrolix.spark.model.JSON

import com.fasterxml.jackson.core.{JsonParser, JsonToken}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.vectorized.{OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.io.InputStream
import java.time.{LocalDate, OffsetDateTime}

object HdxReaderColumnarJson extends Logging {
  private val scalarTypes = Set(
    DataTypes.StringType,
    DataTypes.BooleanType,
    DataTypes.ByteType,
    DataTypes.ShortType,
    DataTypes.IntegerType,
    DataTypes.LongType,
    DataTypes.FloatType,
    DataTypes.DoubleType,
    DataTypes.DateType,
    DataTypes.TimestampType,
  )

  /**
   * Must be called in its own thread, because it does blocking reads from `stream`! Doesn't close the stream.
   *
   * @param schema  the column names and types expected
   * @param stream  the input stream to read
   * @param onBatch a callback to send a completed batch
   * @param onDone  a callback when the stream is completely consumed
   */
  def batches(schema: StructType,
              stream: InputStream,
             onBatch: ColumnarBatch => Unit,
              onDone: => Unit)
                    : Unit =
  {
    val parser = JSON.objectMapper.createParser(stream)

    parser.nextToken() // Advance to start object if present, or null if empty stream

    val cols = OnHeapColumnVector.allocateColumns(8192, schema) // TODO make the batch size configurable maybe
    val colsByName = schema.zip(cols).map {
      case (field, col) =>
        field.name -> (field, col)
    }.toMap

    while (true) {
      if (parser.currentToken() == null) {
        onDone
        return
      }

      val rows = block(parser, colsByName)

      onBatch(new ColumnarBatch(cols.toArray, rows))

      cols.foreach(_.reset())
    }
  }

  /**
   * Reads a single columnar batch from the parser into `cols`. The parser must be positioned on a START_OBJECT,
   * and will be left '''after''' the corresponding END_OBJECT on successful completion.
   */
  private def block(parser: JsonParser, colsByName: Map[String, (StructField, WritableColumnVector)]): Int = {
    if (!parser.isExpectedStartObjectToken) sys.error(s"Expected object start, got ${parser.currentToken()}")

    // TODO this expects `rows` to come before `cols` and it would be tricky to change that, maybe document it
    if (parser.nextFieldName() != "rows") sys.error("Expected `rows` field")
    val rows = parser.nextIntValue(-1)
    if (rows == -1) sys.error("`rows` was not an Int value")

    if (parser.nextFieldName() != "cols") sys.error("Expected `cols` field")
    if (parser.nextToken() != JsonToken.START_OBJECT) sys.error("Expected object start for `cols`")

    while (parser.nextToken() != JsonToken.END_OBJECT) {
      // For each column in `cols`...
      val name = parser.currentName()
      val (field, col) = colsByName.getOrElse(name, sys.error(s"Couldn't find field $name in schema"))

      if (parser.nextToken() != JsonToken.START_ARRAY) sys.error(s"Expected array start for column $name, got ${parser.currentToken()}")

      val written = readArray(parser, field.name, field.dataType, col, 0)

      if (written != rows) {
        sys.error(s"$name had $written value(s); expected $rows")
      }
    }

    parser.nextToken() // Advance past the end of the `cols` object
    parser.nextToken() // Advance past the end of the block object

    rows
  }

  private def scalarType(typ: DataType) = scalarTypes.contains(typ) || typ.isInstanceOf[DecimalType]

  private def readValue(parser: JsonParser, name: String, valueType: DataType, col: WritableColumnVector, rowId: Int, offset: Int): Int = {
    try {
      if (parser.currentToken() == JsonToken.VALUE_NULL) {
        col.putNull(offset)
        return 1
      }

      valueType match {
        case typ if scalarType(typ) =>
          readScalar(parser, valueType, name, col, offset)
          1

        case ArrayType(elementType, _) =>
          if (!parser.isExpectedStartArrayToken) sys.error("Expected array start")

          readArray(parser, name + "[]", elementType, col, offset)

        case MapType(DataTypes.StringType, valueType, _) =>
          if (!parser.isExpectedStartObjectToken) sys.error("Expected object start")

          readMap(parser, name + "{}", valueType, col, rowId, offset)
      }
    } catch {
      case e: Exception =>
        throw new RuntimeException(s"Couldn't read a value $name: $valueType", e)
    }
  }

  /**
   * Reads an array of whatever from a streaming JSON parser.
   * On entry, parser must already be at a START_ARRAY token. On exit, parser will be at the corresponding
   * END_ARRAY token, unless an exception is thrown
   *
   * @param parser      must be at a START_ARRAY token.
   * @param name        the name of the field we're reading, only for error messages
   * @param elementType the Spark type of the array's elements, not the array itself!
   * @param col         the column we're writing into
   * @param offset      the offset to start rows at
   * @return the number of values read. Parser will be at an END_ARRAY token.
   */
  private def readArray(parser: JsonParser, name: String, elementType: DataType, col: WritableColumnVector, offset: Int): Int = {
    require(parser.isExpectedStartArrayToken, s"Expected START_ARRAY at beginning of $name; got ${parser.currentToken()}")

    // If this is a top-level column, this is the rowId.
    // If this is a child structure, it's the position within the per-row struct.
    var pos = -1
    var valuesWritten = 0

    while (parser.nextToken() != JsonToken.END_ARRAY) {
      pos += 1

      elementType match {
        case typ if scalarType(typ) && col.dataType() == typ =>
          readScalar(parser, elementType, name, col, offset + pos)

        case typ if scalarType(typ) =>
          readScalar(parser, elementType, name, col.getChild(0), offset + pos)

        case at@ArrayType(_, _) =>
          // Array of arrays

          val elements = col.getChild(0)

          // Since child arrays are stored in concatenated form, we need to distinguish between the rowId and the offset
          // where subsequent values should be appended.

          // Read a sub-array into the `elements` child column starting at offset `valuesWritten`
          val len = readValue(parser, s"$name[$pos]", at, elements, pos, offset + valuesWritten)

          // Record the child offset and length for this rowId into the parent array
          col.putArray(pos, offset + valuesWritten, len)

          // Bump the offset for the next write
          valuesWritten += len

        case MapType(DataTypes.StringType, valueType, _) =>
          // Array of maps

          val len = readMap(parser, s"$name[$pos]{}", valueType, col, pos, offset + valuesWritten)

          valuesWritten += len

        case other =>
          sys.error(s"readArray can't read $name: $other!")
      }
    }

    pos + 1
  }

  private def readMap(parser: JsonParser, name: String, valueType: DataType, col: WritableColumnVector, rowId: Int, offset: Int): Int = {
    val keys = col.getChild(0)
    val values = col.getChild(1)

    var pos = -1
    var valuesWritten = 0
    while (parser.nextToken() != JsonToken.END_OBJECT) {
      pos += 1
      val key = parser.currentName()
      keys.reserve(offset + pos + 1)
      keys.putByteArray(offset + pos, key.getBytes("UTF-8"))

      parser.nextToken() // Advance to value
      val len = readValue(parser, s"$name.$key", valueType, values, rowId, offset + pos)
      valuesWritten += len
    }

    col.putArray(rowId, offset, valuesWritten)

    valuesWritten
  }

  /**
   * Reads a scalar value from the parser, and stores it in `col` at position `pos`. Always succeeds in writing a
   * single value unless an exception is thrown. Does not advance the parser.
   *
   * @param parser   must be positioned at the scalar value token (may be null), not the enclosing structure start
   * @param dataType the Spark type expected to be read
   * @param name     the name of the current column, only used for error messages
   * @param col      the column where the scalar value will be stored
   * @param pos      the position at which the value will be stored
   */
  private def readScalar(parser: JsonParser,
                       dataType: DataType,
                           name: String,
                            col: WritableColumnVector,
                            pos: Int)
                               : Unit =
  {
    val tok = parser.currentToken()

    col.reserve(pos+1)

    if (tok == JsonToken.VALUE_NULL) {
      col.putNull(pos)
    } else {
      dataType match {
        case DataTypes.StringType if tok == JsonToken.VALUE_STRING =>
          val s = parser.getText
          val bytes = s.getBytes("UTF-8")
          col.putByteArray(pos, bytes)

        case DataTypes.BooleanType if tok.isBoolean =>
          // TODO maybe booleans from ints etc?
          col.putBoolean(pos, tok == JsonToken.VALUE_TRUE)

        case DataTypes.ByteType if tok == JsonToken.VALUE_NUMBER_INT =>
          col.putByte(pos, parser.getByteValue)

        case DataTypes.ShortType if tok == JsonToken.VALUE_NUMBER_INT =>
          col.putShort(pos, parser.getShortValue)

        case DataTypes.IntegerType if tok == JsonToken.VALUE_NUMBER_INT =>
          col.putInt(pos, parser.getIntValue)

        case DataTypes.LongType if tok == JsonToken.VALUE_NUMBER_INT =>
          col.putLong(pos, parser.getLongValue)

        case dt: DecimalType if dt.scale == 0 && tok == JsonToken.VALUE_NUMBER_INT =>
          val bd = parser.getDecimalValue
          col.putDecimal(pos, Decimal(bd), bd.precision())

        case DataTypes.FloatType if tok.isNumeric =>
          col.putFloat(pos, parser.getFloatValue)

        case DataTypes.DoubleType if tok.isNumeric =>
          col.putDouble(pos, parser.getDoubleValue)

        case _: DecimalType if tok.isNumeric =>
          val bd = parser.getDecimalValue
          col.putDecimal(pos, Decimal(bd), bd.precision())

        case DataTypes.TimestampType =>
          val time = OffsetDateTime.parse(parser.getText)

          col.putLong(pos, DateTimeUtils.instantToMicros(time.toInstant))

        case DataTypes.DateType =>
          val date = LocalDate.parse(parser.getText)
          col.putInt(pos, DateTimeUtils.localDateToDays(date))

        case other =>
          sys.error(s"Can't get a value for $name of type $other from a $tok token!")
      }
    }
  }
}
