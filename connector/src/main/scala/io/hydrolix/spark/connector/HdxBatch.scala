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
  private val jdbc = HdxJdbcSession(info)

  override def planInputPartitions(): Array[InputPartition] = {
    // TODO we have `pushedPreds`, we can make this query a lot more selective (carefully!)
    // TODO when `pushedAggs` is non-empty, select only what we need from here
    // TODO make agg pushdown work when the GROUP BY is the shard key, and perhaps a primary timestamp derivation?
    val parts = jdbc.collectPartitions(table.ident.namespace().head, table.ident.name())
    val db = table.ident.namespace().head
    val tbl = table.ident.name()

    parts.flatMap { hp =>
      val max = hp.maxTimestamp
      val min = hp.minTimestamp
      val sk = hp.shardKey

      if (pushedPreds.nonEmpty && pushedPreds.forall(HdxPushdown.prunePartition(table.primaryKeyField, table.shardKeyField, _, min, max, sk))) {
        // All pushed predicates found this partition can be pruned; skip it
        None
      } else {
        if (pushedAggs.nonEmpty) {
          // Build a row containing only the values of the pushed aggregates
          val row = InternalRow(
            pushedAggs.map {
              case _: CountStar => hp.rows
              case _: Min => DateTimeUtils.instantToMicros(min)
              case _: Max => DateTimeUtils.instantToMicros(max)
            }: _*
          )
          Some(HdxPushedAggsPartition(row))
        } else {
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
      }
    }.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    if (pushedAggs.nonEmpty) {
      new PushedCountStarPartitionReaderFactory()
    } else {
      new HdxPartitionReaderFactory(info, table.primaryKeyField)
    }
  }
}

final class PushedCountStarPartitionReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new PushedCountStarPartitionReader(partition.asInstanceOf[HdxPushedAggsPartition].row)
  }
}

final class PushedCountStarPartitionReader(row: InternalRow) extends PartitionReader[InternalRow] {
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