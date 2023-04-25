package io.hydrolix.spark

import com.fasterxml.jackson.databind.JsonNode
import io.hydrolix.spark.model.HdxColumnDatatype
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, DecimalType, MapType}

object Types {
  private val arrayR = """array\((.*?)\)""".r
  private val mapR = """array\((.*?),\s*(.*?)\)""".r
  private val nullableR = """nullable\((.*?)\)""".r
  private val datetime64R = """datetime64\((.*?)\)""".r
  private val datetimeR = """datetime\((.*?)\)""".r

  /**
   * TODO this is conservative about making space for rare, high-magnitude values, e.g. uint64 -> decimal ... should
   *  probably be optional
   *
   * @return (sparkType, nullable?)
   */
  def clickhouseToSpark(s: String): (DataType, Boolean) = {
    // array(nullable(string))
    s.toLowerCase match {
      case "uint8" | "int8" | "int32" | "int16" | "uint16" => DataTypes.IntegerType -> false
      case "uint32" | "int64" => DataTypes.LongType -> false
      case "uint64" => DataTypes.createDecimalType(19, 0) -> false
      case "int128" | "uint128" => DataTypes.createDecimalType(39, 0) -> false
      case "int256" | "uint256" => DataTypes.createDecimalType(78, 0) -> false
      case "float32" => DataTypes.FloatType -> false
      case "float64" => DataTypes.DoubleType -> false
      case "string" => DataTypes.StringType -> false
      case datetime64R(_) => DataTypes.TimestampType -> false // TODO OK to discard precision here?
      case datetimeR(_) => DataTypes.TimestampType -> false // TODO OK to discard precision here?

      case arrayR(elementTypeName) =>
        val (typ, nullable) = clickhouseToSpark(elementTypeName)
        DataTypes.createArrayType(typ) -> nullable

      case mapR(keyTypeName, valueTypeName) =>
        val (keyType, _) = clickhouseToSpark(keyTypeName)
        val (valueType, valueNull) = clickhouseToSpark(valueTypeName)
        DataTypes.createMapType(keyType, valueType, valueNull) -> false

      case nullableR(typeName) =>
        val (typ, _) = clickhouseToSpark(typeName)
        typ -> true
    }
  }

  def sparkToHdx(name: String, sparkType: DataType, primaryKeyName: String): HdxColumnDatatype = {
    sparkType match {
      case DataTypes.StringType => HdxColumnDatatype("string", true, false)
      case DataTypes.IntegerType => HdxColumnDatatype("int32", true, false)
      case DataTypes.ShortType => HdxColumnDatatype("int32", true, false)
      case DataTypes.LongType => HdxColumnDatatype("int64", true, false)
      case DataTypes.BooleanType => HdxColumnDatatype("uint8", true, false)
      case DataTypes.DoubleType => HdxColumnDatatype("double", false, false)
      case DataTypes.FloatType => HdxColumnDatatype("double", false, false)
      case DataTypes.TimestampType =>
        // TODO assumes all timestamps are 64-bit millis, maybe sneak in some out-of-band info
        HdxColumnDatatype("datetime64", true, name == primaryKeyName, resolution = Some("ms"))

      case ArrayType(elementType, _) =>
        val elt = sparkToHdx(name, elementType, primaryKeyName).copy(index = false)

        val arr = JSON.objectMapper.getNodeFactory.arrayNode()
        arr.add(JSON.objectMapper.convertValue[JsonNode](elt))

        HdxColumnDatatype("array", false, false, elements = Some(arr))

      case MapType(keyType, valueType, _) =>
        val kt = sparkToHdx(name, keyType, primaryKeyName).copy(index = false)
        val vt = sparkToHdx(name, valueType, primaryKeyName).copy(index = false)

        val arr = JSON.objectMapper.getNodeFactory.arrayNode()
        arr.add(JSON.objectMapper.convertValue[JsonNode](kt))
        arr.add(JSON.objectMapper.convertValue[JsonNode](vt))

        HdxColumnDatatype("map", false, false, elements = Some(arr))

      case dt: DecimalType if dt.scale == 0 => HdxColumnDatatype("int64", true, false)
      case DecimalType() => HdxColumnDatatype("double", false, false)
    }
  }
}
