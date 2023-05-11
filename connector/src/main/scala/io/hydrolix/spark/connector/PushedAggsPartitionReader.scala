package io.hydrolix.spark.connector

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