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
package io.hydrolix.connectors.spark

import org.apache.spark.internal.Logging
import org.apache.spark.sql.HdxPredicates
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.expressions.aggregate.{AggregateFunc, CountStar, Max, Min}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

import io.hydrolix.connectors
import io.hydrolix.connectors.spark.partitionreader.{ColumnarPartitionReaderFactory, SparkRowPartitionReaderFactory}
import io.hydrolix.connectors.{HdxConnectionInfo, HdxJdbcSession, HdxPartitionScanPlan, HdxQueryMode, HdxTable, types}

final class SparkBatch(info: HdxConnectionInfo,
                      table: HdxTable,
                       cols: StructType,
                pushedPreds: List[Predicate],
                 pushedAggs: List[AggregateFunc])
  extends Batch
     with Logging
{
  private var planned: Array[InputPartition] = _
  //noinspection RedundantCollectionConversion -- Scala 2.13
  private val hdxCols = table.hdxCols
    .filterKeys(col => cols.fields.exists(_.name == col))
    .map(identity) // because Map.filterKeys produces something non-Serializable
    .toMap
  private val pushedCore = pushedPreds.map(HdxPredicates.sparkToCore)
  private val colsCore = Types.sparkToCore(cols).asInstanceOf[types.StructType]

  private lazy val schemaContainsMap = cols.fields.exists(col => hasMap(col.dataType))

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
      val (rows, min, max) = jdbc.collectPartitionAggs(table.ident.head, table.ident(1))

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
      val parts = jdbc.collectPartitions(table.ident.head, table.ident(1))
      val db = table.ident.head
      val tbl = table.ident(1)

      parts.zipWithIndex.flatMap { case (hp, i) =>
        val min = hp.minTimestamp
        val max = hp.maxTimestamp
        val sk = hp.shardKey

        // pushedPreds is implicitly an AND here
        val pushResults = pushedCore.map(connectors.HdxPushdown.prunePartition(table.primaryKeyField, table.shardKeyField, _, min, max, sk))
        if (pushedPreds.nonEmpty && pushResults.contains(true)) {
          // At least one pushed predicate said we could skip this partition
          log.debug(s"Skipping partition ${i + 1}: $hp")
          None
        } else {
          log.debug(s"Scanning partition ${i + 1}: $hp. Per-predicate results: ${pushedPreds.zip(pushResults).mkString("\n  ", "\n  ", "\n")}")
          // Either nothing was pushed, or at least one predicate didn't want to prune this partition; scan it

          val (path, storageId) = hp.storageId match {
            case Some(id) if hp.partition.startsWith(id.toString + "/") =>
              log.debug(s"storage_id = ${hp.storageId}, partition = ${hp.partition}")
              // Remove storage ID prefix if present; it's not there physically
              ("db/hdx/" + hp.partition.drop(id.toString.length + 1), id)
            case _ =>
              // No storage ID from catalog or not present in the path, assume the prefix is there
              val defaults = table.storages.filter(_._2.isDefault)
              if (defaults.isEmpty) {
                if (table.storages.isEmpty) {
                  // Note: this won't be empty if the storage settings override is used
                  sys.error(s"No storage found for partition ${hp.partition}")
                } else {
                  val firstId = table.storages.head._1
                  log.warn(s"Partition ${hp.partition} had no `storage_id`, and cluster has no default storage; using the first (#$firstId)")
                  (info.partitionPrefix.getOrElse("") + hp.partition, firstId)
                }
              } else {
                val firstDefault = defaults.head._1
                if (defaults.size > 1) {
                  log.warn(s"Partition ${hp.partition} had no `storage_id`, and cluster has multiple default storages; using the first (#$firstDefault)")
                }
                (info.partitionPrefix.getOrElse("") + hp.partition, firstDefault)
              }
          }

          Some(SparkScanPartition(HdxPartitionScanPlan(
            db,
            tbl,
            storageId,
            path,
            colsCore,
            pushedCore,
            hdxCols)))
        }
      }.toArray
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    if (pushedAggs.nonEmpty) {
      new PushedAggsPartitionReaderFactory()
    } else {
      val useRowOriented = if (table.queryMode == HdxQueryMode.FORCE_ROW) {
        log.info("Forcing row-oriented query mode")
        true
      } else if (table.queryMode == HdxQueryMode.AUTO && schemaContainsMap) {
        log.info("Schema includes at least one Map type; using row-oriented reader")
        true
      } else if (table.queryMode == HdxQueryMode.AUTO && !schemaContainsMap) {
        log.info("Schema does not include a Map type; using columnar reader")
        false
      } else {
        log.info("Forcing columnar query mode")
        false
      }

      if (useRowOriented) {
        new SparkRowPartitionReaderFactory(info, table.storages, table.primaryKeyField)
      } else {
        new ColumnarPartitionReaderFactory(info, table.storages, table.primaryKeyField)
      }
    }
  }

  private def hasMap(typ: DataType): Boolean = {
    typ match {
      case MapType(_, _, _) => true
      case ArrayType(elt, _) => hasMap(elt)
      case StructType(fields) => fields.exists(fld => hasMap(fld.dataType))
      case _ => false
    }
  }
}
