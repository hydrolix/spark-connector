/*
 * Copyright (c) 2023 Hydrolix Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hydrolix.spark.model

import io.hydrolix.connectors
import io.hydrolix.connectors.{types => coretypes}
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

  def hdxToSpark(htype: HdxColumnDatatype): DataType = {
    htype.`type` match {
      case HdxValueType.Int8 => DataTypes.ByteType
      case HdxValueType.UInt8 => DataTypes.ShortType
      case HdxValueType.Int32 => DataTypes.IntegerType
      case HdxValueType.UInt32 => DataTypes.LongType
      case HdxValueType.Int64 => DataTypes.LongType
      case HdxValueType.UInt64 => DataTypes.createDecimalType(20, 0)
      case HdxValueType.Double => DataTypes.DoubleType
      case HdxValueType.String => DataTypes.StringType
      case HdxValueType.Boolean => DataTypes.BooleanType
      case HdxValueType.DateTime64 => DataTypes.TimestampType
      case HdxValueType.DateTime => DataTypes.TimestampType
      case HdxValueType.Epoch => DataTypes.TimestampType
      case HdxValueType.Array =>
        val elt = hdxToSpark(htype.elements.get.head)
        DataTypes.createArrayType(elt)
      case HdxValueType.Map =>
        val kt = hdxToSpark(htype.elements.get.apply(0))
        val vt = hdxToSpark(htype.elements.get.apply(1))
        DataTypes.createMapType(kt, vt)
    }
  }

  /**
   * TODO this is lossy
   */
  def sparkToCore(stype: DataType): connectors.types.ValueType = {
    stype match {
      case DataTypes.BooleanType   => coretypes.BooleanType
      case DataTypes.StringType    => coretypes.StringType
      case DataTypes.ByteType      => coretypes.Int8Type
      case DataTypes.ShortType     => coretypes.Int16Type
      case DataTypes.IntegerType   => coretypes.Int32Type
      case DataTypes.LongType      => coretypes.Int64Type
      case DataTypes.FloatType     => coretypes.Float32Type
      case DataTypes.DoubleType    => coretypes.Float64Type
      case DataTypes.TimestampType => coretypes.TimestampType(6)
      case dt: DecimalType if dt.precision == 20 && dt.scale == 0 => coretypes.UInt64Type
      case ArrayType(elementType, containsNull) =>
        val el = sparkToCore(elementType)
        coretypes.ArrayType(el, containsNull)
      case MapType(keyType, valueType, valuesNull) =>
        val kt = sparkToCore(keyType)
        val vt = sparkToCore(valueType)
        coretypes.MapType(kt, vt, valuesNull)
      case StructType(fields) =>
        coretypes.StructType(fields.map { sf =>
          coretypes.StructField(sf.name, sparkToCore(sf.dataType), sf.nullable)
        }: _*)
      case other => sys.error(s"Can't translate Spark type $other to connectors-core equivalent")
    }
  }
}
