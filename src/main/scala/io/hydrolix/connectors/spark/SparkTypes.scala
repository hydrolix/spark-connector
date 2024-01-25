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
package io.hydrolix.connectors.spark

import org.apache.spark.sql.types.{DataType, DataTypes}
import org.apache.spark.sql.{types => sparktypes}

import io.hydrolix.connectors
import io.hydrolix.connectors.types._
import io.hydrolix.connectors.{types => coretypes}

object SparkTypes {
  /**
   * TODO this is lossy
   */
  def sparkToCore(stype: DataType): connectors.types.ConcreteType = {
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
      case dt: sparktypes.DecimalType if dt.precision == 20 && dt.scale == 0 => coretypes.UInt64Type
      case sparktypes.ArrayType(elementType, containsNull) =>
        val el = sparkToCore(elementType)
        coretypes.ArrayType(el, containsNull)
      case sparktypes.MapType(keyType, valueType, valuesNull) =>
        val kt = sparkToCore(keyType)
        val vt = sparkToCore(valueType)
        coretypes.MapType(kt, vt, valuesNull)
      case sparktypes.StructType(fields) =>
        coretypes.StructType(fields.map { sf =>
          coretypes.StructField(sf.name, sparkToCore(sf.dataType), sf.nullable)
        }.toList)
      case other => sys.error(s"Can't translate Spark type $other to connectors-core equivalent")
    }
  }

  def coreToSpark(core: ValueType): DataType = {
    core match {
      case coretypes.BooleanType => DataTypes.BooleanType
      case coretypes.StringType => DataTypes.StringType
      case coretypes.TimestampType(_) => DataTypes.TimestampType
      case Int8Type => DataTypes.ByteType
      case UInt8Type => DataTypes.ShortType
      case Int16Type => DataTypes.ShortType
      case UInt16Type => DataTypes.IntegerType
      case Int32Type => DataTypes.IntegerType
      case Int64Type => DataTypes.LongType
      case UInt32Type => DataTypes.LongType
      case UInt64Type => DataTypes.createDecimalType(20, 0)
      case Float32Type => DataTypes.FloatType
      case Float64Type => DataTypes.DoubleType
      case coretypes.ArrayType(elementType, nulls) =>
        DataTypes.createArrayType(coreToSpark(elementType), nulls)
      case coretypes.MapType(keyType, valueType, nulls) =>
        DataTypes.createMapType(coreToSpark(keyType), coreToSpark(valueType), nulls)
      case coretypes.StructType(fields) =>
        DataTypes.createStructType(fields.map { corefield =>
          DataTypes.createStructField(corefield.name, coreToSpark(corefield.`type`), corefield.nullable)
        }.toArray)
      case other => sys.error(s"Can't translate connectors-core type $other to Spark equivalent")
    }
  }
}
