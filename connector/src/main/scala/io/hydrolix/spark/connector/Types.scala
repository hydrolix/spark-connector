package io.hydrolix.spark.connector

import io.hydrolix.spark.model.{HdxColumnDatatype, HdxColumnInfo, JSON}

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.types._

object Types {
  private val arrayR = """array\((.*?)\)""".r
  private val mapR = """array\((.*?),\s*(.*?)\)""".r
  private val nullableR = """nullable\((.*?)\)""".r
  private val datetime64R = """datetime64\((.*?)\)""".r
  private val datetimeR = """datetime\((.*?)\)""".r
  private val encodingR = """(.*?)\s*encoding=(.*?)""".r

  /**
   * TODO this is conservative about making space for rare, high-magnitude values, e.g. uint64 -> decimal ... should
   * probably be optional
   *
   * @return (sparkType, nullable?)
   */
  def clickhouseToSpark(s: String): (DataType, Boolean) = {
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
      case "datetime64" => DataTypes.TimestampType -> false
      case "datetime" => DataTypes.TimestampType -> false

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

      case encodingR(name, _) =>
        // TODO we might want the encoding somewhere but not for Spark per se
        clickhouseToSpark(name)
    }
  }

  def sparkToHdx(name: String, sparkType: DataType, primaryKeyName: String, cols: Map[String, HdxColumnInfo]): HdxColumnDatatype = {
    val hcol = cols.getOrElse(name, sys.error(s"No HdxColumnInfo for $name"))
    val index = hcol.indexed == 2 // TODO maybe OK to index=true if the column is indexed in only some partitions?

    sparkType match {
      case DataTypes.StringType => HdxColumnDatatype("string", index, false)
      case DataTypes.IntegerType => HdxColumnDatatype("int32", index, false)
      case DataTypes.ShortType => HdxColumnDatatype("int32", index, false)
      case DataTypes.LongType => HdxColumnDatatype("int64", index, false)
      case DataTypes.BooleanType => HdxColumnDatatype("uint8", index, false)
      case DataTypes.DoubleType => HdxColumnDatatype("double", index, false)
      case DataTypes.FloatType => HdxColumnDatatype("double", index, false)
      case DataTypes.TimestampType =>
        val (typ, res) = if (hcol.clickhouseType.toLowerCase().contains("datetime64")) {
          "datetime64" -> "ms"
        } else {
          "datetime" -> "s"
        }

        HdxColumnDatatype(typ, index, name == primaryKeyName, resolution = Some(res))

      case ArrayType(elementType, _) =>
        val elt = sparkToHdx(name, elementType, primaryKeyName, cols).copy(index = false)

        val arr = JSON.objectMapper.getNodeFactory.arrayNode()
        arr.add(JSON.objectMapper.convertValue[JsonNode](elt))

        HdxColumnDatatype("array", index, false, elements = Some(arr))

      case MapType(keyType, valueType, _) =>
        val kt = sparkToHdx(name, keyType, primaryKeyName, cols).copy(index = false)
        val vt = sparkToHdx(name, valueType, primaryKeyName, cols).copy(index = false)

        val arr = JSON.objectMapper.getNodeFactory.arrayNode()
        arr.add(JSON.objectMapper.convertValue[JsonNode](kt))
        arr.add(JSON.objectMapper.convertValue[JsonNode](vt))

        HdxColumnDatatype("map", index, false, elements = Some(arr))

      case dt: DecimalType if dt.scale == 0 => HdxColumnDatatype("int64", index, false)
      case DecimalType() => HdxColumnDatatype("double", index, false)
    }
  }
}
