package io.hydrolix.spark.connector

import io.hydrolix.spark.model.{HdxConnectionInfo, HdxJdbcSession}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.HdxPredicatePushdown
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

class HdxBatch(info: HdxConnectionInfo,
              table: HdxTable,
               cols: StructType,
        pushedPreds: List[Predicate],
    pushedCountStar: Boolean)
  extends Batch
    with Logging
{
  private val jdbc = HdxJdbcSession(info)

  override def planInputPartitions(): Array[InputPartition] = {
    // TODO we have `pushed`, we can make this query a lot more selective (carefully!)
    val parts = jdbc.collectPartitions(table.ident.namespace().head, table.ident.name())
    val db = table.ident.namespace().head
    val tbl = table.ident.name()

    parts.flatMap { hp =>
      val max = hp.maxTimestamp
      val min = hp.minTimestamp
      val sk = hp.shardKey

      if (pushedPreds.nonEmpty && pushedPreds.forall(HdxPredicatePushdown.prunePartition(table.primaryKeyField, table.shardKeyField, _, min, max, sk))) {
        // All pushed predicates found this partition can be pruned; skip it
        None
      } else {
        if (pushedCountStar) {
          Some(HdxPushedCountStarPartition(hp.rows))
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
    if (pushedCountStar) {
      new PushedCountStarPartitionReaderFactory
    } else {
      new HdxPartitionReaderFactory(info, table.primaryKeyField)
    }
  }
}

final class PushedCountStarPartitionReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new PushedCountStarPartitionReader(partition.asInstanceOf[HdxPushedCountStarPartition].rows)
  }
}

final class PushedCountStarPartitionReader(rows: Long) extends PartitionReader[InternalRow] {
  private val row = InternalRow.apply(rows)
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