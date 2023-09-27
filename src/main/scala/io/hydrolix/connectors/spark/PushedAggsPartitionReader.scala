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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}

case class HdxPushedAggsPartition(row: InternalRow) extends InputPartition

final class PushedAggsPartitionReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new PushedAggsPartitionReader(partition.asInstanceOf[HdxPushedAggsPartition].row)
  }
}

/**
 * A trivial PartitionReader for the special case when a query contains only pushed aggregations
 *
 * @param row the row containing the pushed aggregation results
 */
final class PushedAggsPartitionReader(row: InternalRow) extends PartitionReader[InternalRow] {
  private var consumed = false

  override def next(): Boolean = {
    if (consumed) {
      false
    } else {
      consumed = true
      true
    }
  }

  override def get(): InternalRow = row

  override def close(): Unit = ()
}