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

package io.hydrolix.connectors.spark.partitionreader

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.io._
import java.util.UUID

import io.hydrolix.connectors
import io.hydrolix.connectors.HdxPartitionScanPlan
import io.hydrolix.connectors.partitionreader.HdxPartitionReader
import io.hydrolix.connectors.spark.SparkScanPartition

final class ColumnarPartitionReaderFactory(info: connectors.HdxConnectionInfo,
                                           storages: Map[UUID, connectors.HdxStorageSettings],
                                           pkName: String)
  extends PartitionReaderFactory
{
  override def supportColumnarReads(partition: InputPartition) = true

  override def createColumnarReader(partition: InputPartition): SparkColumnarPartitionReader = {
    val hdxPart = partition.asInstanceOf[SparkScanPartition]
    val storage = storages.getOrElse(hdxPart.coreScan.storageId, sys.error(s"Partition ${hdxPart.coreScan.partitionPath} refers to unknown storage #${hdxPart.coreScan.storageId}"))
    new SparkColumnarPartitionReader(info, storage, pkName, hdxPart.coreScan)
  }

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    sys.error("This flavour of PartitionReaderFactory doesn't support row-oriented reads")
  }
}

/**
 * TODO port this to connectors-core
 */
final class SparkColumnarPartitionReader(val           info: connectors.HdxConnectionInfo,
                                         val        storage: connectors.HdxStorageSettings,
                                         val primaryKeyName: String,
                                         val           scan: HdxPartitionScanPlan)
  extends HdxPartitionReader[ColumnarBatch]
     with PartitionReader[ColumnarBatch]
{
  override def outputFormat = "jsonc"

  override val doneSignal = new ColumnarBatch(Array())

  override def handleStdout(stdout: InputStream): Unit = {
      // TODO wrap a GZIPInputStream etc. around stdout once we get that working on the turbine side
      HdxReaderColumnarJson(
        scan.schema,
        stdout,
        { batch =>
          expectedLines.incrementAndGet()
          stdoutQueue.put(batch)
        },
        {
          stdoutQueue.put(doneSignal)
          stdout.close()
        }
      )
  }
}
