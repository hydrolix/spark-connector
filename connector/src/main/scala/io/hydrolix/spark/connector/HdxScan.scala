package io.hydrolix.spark.connector

import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Batch, Scan}
import org.apache.spark.sql.types.StructType

class HdxScan(info: HdxConnectionInfo,
              table: HdxTable,
              cols: StructType,
              pushed: List[Predicate])
  extends Scan {
  override def toBatch: Batch = {
    new HdxBatch(info, table, cols, pushed)
  }

  override def description(): String = super.description()

  override def readSchema(): StructType = cols
}
