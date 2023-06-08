package io.hydrolix.spark.connector

import io.hydrolix.spark.model.{HdxConnectionInfo, HdxStorageSettings}

import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Batch, Scan}
import org.apache.spark.sql.types.StructType

final class HdxScan(info: HdxConnectionInfo,
                 storage: HdxStorageSettings,
                   table: HdxTable,
                    cols: StructType,
             pushedPreds: List[Predicate],
              pushedAggs: List[AggregateFunc])
  extends Scan
{
  // TODO consider adding SupportsRuntimeFiltering
  private val batch = new HdxBatch(info, storage, table, cols, pushedPreds, pushedAggs)

  override def toBatch: Batch = batch

  override def description(): String = super.description()

  override def readSchema(): StructType = cols
}
