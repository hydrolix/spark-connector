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
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}

import java.util.UUID

import io.hydrolix.connectors
import io.hydrolix.connectors.partitionreader.RowPartitionReader
import io.hydrolix.connectors.spark.SparkScanPartition
import io.hydrolix.connectors.spark.partitionreader.SparkRowPartitionReader.doneSignal

final class SparkRowPartitionReaderFactory(info: connectors.HdxConnectionInfo,
                                           storages: Map[UUID, connectors.HdxStorageSettings],
                                           pkName: String)
  extends PartitionReaderFactory
{
  override def supportColumnarReads(partition: InputPartition) = false

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val hdxPart = partition.asInstanceOf[SparkScanPartition]
    val storage = storages.getOrElse(hdxPart.coreScan.storageId, sys.error(s"Partition ${hdxPart.coreScan.partitionPath} refers to unknown storage #${hdxPart.coreScan.storageId}"))
    new SparkRowPartitionReader(info, storage, pkName, partition.asInstanceOf[SparkScanPartition])
  }
}

object SparkRowPartitionReader {
  val doneSignal = new GenericInternalRow(0)
}

final class SparkRowPartitionReader(info: connectors.HdxConnectionInfo,
                                 storage: connectors.HdxStorageSettings,
                          primaryKeyName: String,
                                    scan: SparkScanPartition)
  extends PartitionReader[InternalRow]
{
  private val corePartitionReader = new RowPartitionReader[InternalRow](info, storage, primaryKeyName, scan.coreScan, SparkRowAdapter, doneSignal)

  override def next(): Boolean = corePartitionReader.next()

  override def get(): InternalRow = corePartitionReader.get()

  override def close(): Unit = corePartitionReader.close()
}
