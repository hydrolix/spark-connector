package io.hydrolix.spark.connector

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}

class HdxPartitionReaderFactory(info: HdxConnectionInfo, pkName: String)
  extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new HdxPartitionReader(info, pkName, partition.asInstanceOf[HdxPartitionScan])
  }
}
