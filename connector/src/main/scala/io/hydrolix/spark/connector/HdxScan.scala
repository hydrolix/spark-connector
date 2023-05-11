package io.hydrolix.spark.connector

import io.hydrolix.spark.model.HdxConnectionInfo

import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Batch, Scan}
import org.apache.spark.sql.types.StructType

class HdxScan(info: HdxConnectionInfo,
             table: HdxTable,
              cols: StructType,
       pushedPreds: List[Predicate],
        pushedAggs: List[AggregateFunc])
  extends Scan
{
  // TODO add SupportsRuntimeFiltering maybe
  private val batch = new HdxBatch(info, table, cols, pushedPreds, pushedAggs)

  override def toBatch: Batch = batch

  override def description(): String = super.description()

  override def readSchema(): StructType = cols
}
