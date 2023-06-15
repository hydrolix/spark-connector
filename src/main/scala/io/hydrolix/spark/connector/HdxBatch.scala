/*
 * Copyright (c) 2023 Hydrolix Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hydrolix.spark.connector

import io.hydrolix.spark.model.{HdxConnectionInfo, HdxJdbcSession, HdxStorageSettings}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.HdxPushdown
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.expressions.aggregate.{AggregateFunc, CountStar, Max, Min}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

final class HdxBatch(info: HdxConnectionInfo,
                  storage: HdxStorageSettings,
                    table: HdxTable,
                     cols: StructType,
              pushedPreds: List[Predicate],
               pushedAggs: List[AggregateFunc])
  extends Batch
     with Logging
{
  private var planned: Array[InputPartition] = _
  private val hdxCols = table.hdxCols
    .filterKeys(cols.fieldNames.contains(_))
    .map(identity) // because Map.filterKeys produces something non-Serializable

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
        val min = hp.minTimestamp
        val max = hp.maxTimestamp
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

          val path = hp.storageId match {
            case Some(id) if hp.partition.startsWith(id.toString + "/") =>
              log.info(s"storage_id = ${hp.storageId}, partition = ${hp.partition}")
              // Remove storage ID prefix if present; it's not there physically
              "db/hdx/" + hp.partition.drop(id.toString.length + 1)
            case _ =>
              // No storage ID or not present in the path, assume the prefix is there
              info.partitionPrefix.getOrElse("") + hp.partition
          }

          Some(
            HdxScanPartition(
              db,
              tbl,
              path,
              cols,
              pushedPreds,
              hdxCols)
          )
        }
      }.toArray
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    if (pushedAggs.nonEmpty) {
      new PushedAggsPartitionReaderFactory()
    } else {
      new HdxPartitionReaderFactory(info, storage, table.primaryKeyField)
    }
  }
}
