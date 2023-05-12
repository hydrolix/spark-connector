package io.hydrolix.spark.model

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.types._

object Types {
  private val arrayR = """array\((.*?)\)""".r
  private val mapR = """map\((.*?),\s*(.*?)\)""".r
  private val nullableR = """nullable\((.*?)\)""".r
  private val datetime64R = """datetime64\((.*?)\)""".r
  private val datetimeR = """datetime\((.*?)\)""".r
  private val encodingR = """(.*?)\s*encoding=(.*?)""".r

  /**
   * TODO this is conservative about making space for rare, high-magnitude values, e.g. uint64 -> decimal ... should
   * probably be optional
   *
   * @return (sparkType, hdxColumnDataType, nullable?)
   */
  def decodeClickhouseType(s: String): (DataType, HdxColumnDatatype, Boolean) = {
    s.toLowerCase match {
      case "int8" => (DataTypes.ByteType, HdxColumnDatatype(HdxValueType.Int8, true, false), false)    // signed 8-bit => byte
      case "uint8" => (DataTypes.ShortType, HdxColumnDatatype(HdxValueType.UInt8, true, false), false) // unsigned 8-bit => short

      case "int16" => (DataTypes.ShortType, HdxColumnDatatype(HdxValueType.Int32, true, false), false) // signed 16-bit => short
      case "uint16" => (DataTypes.IntegerType, HdxColumnDatatype(HdxValueType.UInt32, true, false), false) // unsigned 16-bit => int

      case "int32" => (DataTypes.IntegerType, HdxColumnDatatype(HdxValueType.Int32, true, false), false) // signed 32-bit => int
      case "uint32" => (DataTypes.LongType, HdxColumnDatatype(HdxValueType.UInt32, true, false), false) // unsigned 32-bit => long

      case "int64" => (DataTypes.LongType, HdxColumnDatatype(HdxValueType.Int64, true, false), false) // signed 64-bit => long
      case "uint64" => (DataTypes.createDecimalType(20, 0), HdxColumnDatatype(HdxValueType.UInt64, true, false), false) // unsigned 64-bit => 20-digit decimal

      case "float32" => (DataTypes.FloatType, HdxColumnDatatype(HdxValueType.Double, true, false), false) // float32 => double
      case "float64" => (DataTypes.DoubleType, HdxColumnDatatype(HdxValueType.Double, true, false), false) // float64 => double

      case "string" => (DataTypes.StringType, HdxColumnDatatype(HdxValueType.String, true, false), false)

      case datetime64R(_) => (DataTypes.TimestampType, HdxColumnDatatype(HdxValueType.DateTime64, true, false), false) // TODO OK to discard precision here?
      case datetimeR(_) => (DataTypes.TimestampType, HdxColumnDatatype(HdxValueType.DateTime, true, false), false) // TODO OK to discard precision here?

      case "datetime64" => (DataTypes.TimestampType, HdxColumnDatatype(HdxValueType.DateTime64, true, false), false)
      case "datetime" => (DataTypes.TimestampType, HdxColumnDatatype(HdxValueType.DateTime64, true, false), false)

      case arrayR(elementTypeName) =>
        val (typ, hdxType, nullable) = decodeClickhouseType(elementTypeName)

        (DataTypes.createArrayType(typ), HdxColumnDatatype(HdxValueType.Array, true, false, elements = Some(List(hdxType))), nullable)

      case mapR(keyTypeName, valueTypeName) =>
        val (keyType, hdxKeyType, _) = decodeClickhouseType(keyTypeName)
        val (valueType, hdxValueType, valueNull) = decodeClickhouseType(valueTypeName)

        (DataTypes.createMapType(keyType, valueType, valueNull), HdxColumnDatatype(HdxValueType.Map, true, false, elements = Some(List(hdxKeyType, hdxValueType))), false)

      case nullableR(typeName) =>
        val (typ, hdxType, _) = decodeClickhouseType(typeName)

        (typ, hdxType, true)

      case encodingR(name, _) =>
        // TODO we might want the encoding somewhere but not for Spark per se
        decodeClickhouseType(name)
    }
  }
}
