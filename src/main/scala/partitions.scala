package io.hydrolix.spark

import model.HdxConnectionInfo

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}

case class HdxPartition(db: String,
                     table: String,
                      path: String,
                    pkName: String,
                      cols: List[String])
  extends InputPartition

class HdxPartitionReaderFactory(info: HdxConnectionInfo) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new HdxPartitionReader(info, partition.asInstanceOf[HdxPartition])
  }
}
