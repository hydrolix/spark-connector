package io.hydrolix.spark.connector

import io.hydrolix.spark.model.{HdxConnectionInfo, HdxJdbcSession}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.HdxPushdown
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.expressions.aggregate.{AggregateFunc, CountStar, Max, Min}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

class HdxBatch(info: HdxConnectionInfo,
              table: HdxTable,
               cols: StructType,
        pushedPreds: List[Predicate],
         pushedAggs: List[AggregateFunc])
  extends Batch
    with Logging
{
  private var planned: Array[InputPartition] = _

  override def planInputPartitions(): Array[InputPartition] = {
    if (planned == null) {
      planned = plan()
    }

    planned
  }

  private def plan(): Array[InputPartition] = {
    val jdbc = HdxJdbcSession(info)

    if (pushedAggs.nonEmpty) {
      // TODO make agg pushdown work when the GROUP BY is the shard key, and perhaps a primary timestamp derivation?
      val (rows, min, max) = jdbc.collectPartitionAggs(table.ident.namespace().head, table.ident.name())

      // Build a row containing only the values of the pushed aggregates
      val row = InternalRow(
        pushedAggs.map {
          case _: CountStar => rows
          case _: Min => DateTimeUtils.instantToMicros(min)
          case _: Max => DateTimeUtils.instantToMicros(max)
        }: _*
      )

      Array(HdxPushedAggsPartition(row))
    } else {
      // TODO we have `pushedPreds`, we can make this query a lot more selective (carefully!)
      val parts = jdbc.collectPartitions(table.ident.namespace().head, table.ident.name())
      val db = table.ident.namespace().head
      val tbl = table.ident.name()

      parts.zipWithIndex.flatMap { case (hp, i) =>
        val max = hp.maxTimestamp
        val min = hp.minTimestamp
        val sk = hp.shardKey

        // pushedPreds is implicitly an AND here
        val pushResults = pushedPreds.map(HdxPushdown.prunePartition(table.primaryKeyField, table.shardKeyField, _, min, max, sk))
        if (pushedPreds.nonEmpty && pushResults.contains(true)) {
          // At least one pushed predicate said we could skip this partition
          log.debug(s"Skipping partition ${i + 1}: $hp")
          None
        } else {
          log.info(s"Scanning partition ${i + 1}: $hp. Per-predicate results: ${pushedPreds.zip(pushResults).mkString("\n  ", "\n  ", "\n")}")
          // Either nothing was pushed, or at least one predicate didn't want to prune this partition; scan it
          Some(
            HdxScanPartition(
              db,
              tbl,
              hp.partition,
              cols,
              pushedPreds,
              table.hdxCols)
          )
        }
      }.toArray
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    if (pushedAggs.nonEmpty) {
      new PushedAggsPartitionReaderFactory()
    } else {
      new HdxPartitionReaderFactory(info, table.primaryKeyField)
    }
  }
}
