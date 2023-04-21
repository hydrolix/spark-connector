package io.hydrolix.spark

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

case class HdxPartition(db: String,
                     table: String,
                      path: String,
                    pkName: String,
                    schema: StructType)
  extends InputPartition

class HdxPartitionReaderFactory(info: HdxConnectionInfo) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new HdxPartitionReader(info, partition.asInstanceOf[HdxPartition])
  }
}
