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

package io.hydrolix.spark.connector.partitionreader

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node._
import io.hydrolix.connectors.types
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, DateTimeUtils, GenericArrayData}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.io._
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.UUID
import scala.collection.JavaConverters._
import scala.sys.error
import scala.util.Using
import scala.util.control.Breaks.{break, breakable}

import io.hydrolix.spark.connector.HdxScanPartition
import io.hydrolix.spark.model._

final class RowPartitionReaderFactory(info: HdxConnectionInfo,
                                  storages: Map[UUID, HdxStorageSettings],
                                    pkName: String)
  extends PartitionReaderFactory
{
  override def supportColumnarReads(partition: InputPartition) = false

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val hdxPart = partition.asInstanceOf[HdxScanPartition]
    val storage = storages.getOrElse(hdxPart.storageId, sys.error(s"Partition ${hdxPart.path} refers to unknown storage #${hdxPart.storageId}"))
    new RowPartitionReader(info, storage, pkName, partition.asInstanceOf[HdxScanPartition])
  }
}

final class RowPartitionReader(val           info: HdxConnectionInfo,
                               val        storage: HdxStorageSettings,
                               val primaryKeyName: String,
                               val           scan: HdxScanPartition)
  extends HdxPartitionReader[InternalRow]
{
  private val coreschema = Types.sparkToCore(scan.schema).asInstanceOf[StructType]

  override val doneSignal = new GenericInternalRow(0)

  override def outputFormat = "json"

  override def handleStdout(stdout: InputStream): Unit = {
    Using.Manager { use =>
      val reader = use(new BufferedReader(new InputStreamReader(stdout)))
      breakable {
        while (true) {
          val line = reader.readLine()
          if (line == null) {
            stdoutQueue.put(doneSignal)
            break()
          } else {
            expectedLines.incrementAndGet()
            stdoutQueue.put(HdxReaderRowJson(coreschema, line))
          }
        }
      }
    }.get
  }
}

object HdxReaderRowJson extends Logging {
  def apply(schema: StructType, jsonLine: String): InternalRow = {
    val obj = JSON.objectMapper.readValue[ObjectNode](jsonLine)

    val values = schema.fields.map { col =>
      val node = obj.get(col.name) // TODO can we be sure the names match exactly?
      node2Any(node, col.name, col.`type`)
    }

    InternalRow.fromSeq(values)
  }

  private def node2Any(node: JsonNode, name: String, dt: ValueType): Any = {
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
        val inst = Instant.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(s.textValue()))
        DateTimeUtils.instantToMicros(inst)
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
