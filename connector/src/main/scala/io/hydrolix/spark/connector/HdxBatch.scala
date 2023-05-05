package io.hydrolix.spark.connector

import io.hydrolix.spark.model.{HdxConnectionInfo, HdxJdbcSession}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.HdxPredicatePushdown
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

class HdxBatch(info: HdxConnectionInfo,
               table: HdxTable,
               cols: StructType,
               pushed: List[Predicate])
  extends Batch
    with Logging
{
  private val jdbc = HdxJdbcSession(info)

  override def planInputPartitions(): Array[InputPartition] = {
    val parts = jdbc.collectPartitions(table.ident.namespace().head, table.ident.name())
    val db = table.ident.namespace().head
    val tbl = table.ident.name()

    parts.flatMap { hp =>
      val max = hp.maxTimestamp
      val min = hp.minTimestamp
      val sk = hp.shardKey

      if (pushed.nonEmpty && pushed.forall(HdxPredicatePushdown.prunePartition(table.primaryKeyField, table.shardKeyField, _, min, max, sk))) {
        // All pushed predicates found this partition can be pruned; skip it
        None
      } else {
        // Either nothing was pushed, or at least one predicate didn't want to prune this partition; scan it
        Some(
          HdxPartitionScan(
            db,
            tbl,
            hp.partition,
            cols,
            pushed,
            table.hdxCols
          )
        )
      }
    }.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = new HdxPartitionReaderFactory(info, table.primaryKeyField)
}
