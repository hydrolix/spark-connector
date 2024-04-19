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

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkPredicates
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.expressions.aggregate.{AggregateFunc, CountStar, Max, Min}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

import io.hydrolix.connectors.spark.partitionreader.{ColumnarPartitionReaderFactory, SparkRowPartitionReaderFactory}
import io.hydrolix.connectors.{HdxConnectionInfo, HdxJdbcSession, HdxPushdown, HdxQueryMode, HdxTable, types}

final class SparkBatch(info: HdxConnectionInfo,
                      table: HdxTable,
                       cols: StructType,
                pushedPreds: List[Predicate],
                 pushedAggs: List[AggregateFunc])
  extends Batch
{
  private val logger = Logger(getClass)

  private var planned: Array[InputPartition] = _
  //noinspection RedundantCollectionConversion -- Scala 2.13
  private val colsCore = SparkTypes.sparkToCore(cols).asInstanceOf[types.StructType]
  private val pushedCore = pushedPreds.map(SparkPredicates.sparkToCore(_, table.schema))

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
      HdxPushdown
        .planPartitions(info, jdbc, table, colsCore, pushedCore)
        .map(SparkScanPartition)
        .toArray
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    if (pushedAggs.nonEmpty) {
      new PushedAggsPartitionReaderFactory()
    } else {
      val useRowOriented = if (table.queryMode == HdxQueryMode.FORCE_ROW) {
        logger.info("Forcing row-oriented query mode")
        true
      } else if (table.queryMode == HdxQueryMode.AUTO && schemaContainsMap) {
        logger.info("Schema includes at least one Map type; using row-oriented reader")
        true
      } else if (table.queryMode == HdxQueryMode.AUTO && !schemaContainsMap) {
        logger.info("Schema does not include a Map type; using columnar reader")
        false
      } else {
        logger.info("Forcing columnar query mode")
        false
      }

      if (useRowOriented) {
        new SparkRowPartitionReaderFactory(info, table.storages)
      } else {
        new ColumnarPartitionReaderFactory(info, table.storages)
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
