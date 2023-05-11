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
   * @return (sparkType, hdxValueType, nullable?)
   */
  def decodeClickhouseType(s: String): (DataType, HdxValueType, Boolean) = {
    s.toLowerCase match {
      case "int8" => (DataTypes.ByteType, HdxValueType.Int8, false)    // signed 8-bit => byte
      case "uint8" => (DataTypes.ShortType, HdxValueType.UInt8, false) // unsigned 8-bit => short

      case "int16" => (DataTypes.ShortType, HdxValueType.Int32, false) // signed 16-bit => short
      case "uint16" => (DataTypes.IntegerType, HdxValueType.UInt32, false) // unsigned 16-bit => int

      case "int32" => (DataTypes.IntegerType, HdxValueType.Int32, false) // signed 32-bit => int
      case "uint32" => (DataTypes.LongType, HdxValueType.UInt32, false) // unsigned 32-bit => long

      case "int64" => (DataTypes.LongType, HdxValueType.Int64, false) // signed 64-bit => long
      case "uint64" => (DataTypes.createDecimalType(20, 0), HdxValueType.UInt64, false) // unsigned 64-bit => 20-digit decimal

      case "float32" => (DataTypes.FloatType, HdxValueType.Double, false) // float32 => double
      case "float64" => (DataTypes.DoubleType, HdxValueType.Double, false) // float64 => double

      case "string" => (DataTypes.StringType, HdxValueType.String, false)

      case datetime64R(_) => (DataTypes.TimestampType, HdxValueType.DateTime64, false) // TODO OK to discard precision here?
      case datetimeR(_) => (DataTypes.TimestampType, HdxValueType.DateTime, false) // TODO OK to discard precision here?

      case "datetime64" => (DataTypes.TimestampType, HdxValueType.DateTime64, false)
      case "datetime" => (DataTypes.TimestampType, HdxValueType.DateTime64, false)

      case arrayR(elementTypeName) =>
        val (typ, _, nullable) = decodeClickhouseType(elementTypeName)
        (DataTypes.createArrayType(typ), HdxValueType.Array, nullable)

      case mapR(keyTypeName, valueTypeName) =>
        val (keyType, _, _) = decodeClickhouseType(keyTypeName)
        val (valueType, _, valueNull) = decodeClickhouseType(valueTypeName)
        (DataTypes.createMapType(keyType, valueType, valueNull), HdxValueType.Map, false)

      case nullableR(typeName) =>
        val (typ, hdxType, _) = decodeClickhouseType(typeName)

        (typ, hdxType, true)

      case encodingR(name, _) =>
        // TODO we might want the encoding somewhere but not for Spark per se
        decodeClickhouseType(name)
    }
  }

  def sparkToHdx(name: String, sparkType: DataType, primaryKeyName: String, cols: Map[String, HdxColumnInfo]): HdxColumnDatatype = {
    val hcol = cols.getOrElse(name, sys.error(s"No HdxColumnInfo for $name"))
    val htype = hcol.hdxType
    val index = hcol.indexed == 2 // TODO maybe OK to index=true if the column is indexed in only some partitions?

    sparkType match {
      case DataTypes.StringType => HdxColumnDatatype(htype, index, false)
      case DataTypes.IntegerType => HdxColumnDatatype(htype, index, false)
      case DataTypes.ShortType => HdxColumnDatatype(htype, index, false)
      case DataTypes.LongType => HdxColumnDatatype(htype, index, false)
      case DataTypes.BooleanType => HdxColumnDatatype(htype, index, false)
      case DataTypes.DoubleType => HdxColumnDatatype(htype, index, false)
      case DataTypes.FloatType => HdxColumnDatatype(htype, index, false)
      case DataTypes.TimestampType =>
        val (typ, res) = if (hcol.clickhouseType.toLowerCase().contains("datetime64")) {
          htype -> "ms"
        } else {
          htype -> "s"
        }

        HdxColumnDatatype(typ, index, name == primaryKeyName, resolution = Some(res))

      case ArrayType(elementType, _) =>
        val elt = sparkToHdx(name, elementType, primaryKeyName, cols).copy(index = false)

        val arr = JSON.objectMapper.getNodeFactory.arrayNode()
        arr.add(JSON.objectMapper.convertValue[JsonNode](elt))

        HdxColumnDatatype(HdxValueType.Array, index, false, elements = Some(arr))

      case MapType(keyType, valueType, _) =>
        val kt = sparkToHdx(name, keyType, primaryKeyName, cols).copy(index = false)
        val vt = sparkToHdx(name, valueType, primaryKeyName, cols).copy(index = false)

        val arr = JSON.objectMapper.getNodeFactory.arrayNode()
        arr.add(JSON.objectMapper.convertValue[JsonNode](kt))
        arr.add(JSON.objectMapper.convertValue[JsonNode](vt))

        HdxColumnDatatype(HdxValueType.Map, index, false, elements = Some(arr))

      case DecimalType() => HdxColumnDatatype(htype, index, false)

      case other => sys.error(s"Can't convert $other to a HdxColumnDatatype")
    }
  }
}
