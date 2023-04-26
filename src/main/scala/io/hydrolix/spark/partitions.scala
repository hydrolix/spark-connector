package io.hydrolix.spark

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

case class HdxPartitionScan(db: String,
                         table: String,
                          path: String,
                        schema: StructType,
                        pushed: List[Predicate])
  extends InputPartition

class HdxPartitionReaderFactory(info: HdxConnectionInfo, pkName: String)
  extends PartitionReaderFactory
{
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new HdxPartitionReader(info, pkName, partition.asInstanceOf[HdxPartitionScan])
  }
}
