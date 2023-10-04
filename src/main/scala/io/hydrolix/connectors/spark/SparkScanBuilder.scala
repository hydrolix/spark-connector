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
import org.apache.spark.sql.connector.expressions.aggregate.{AggregateFunc, Aggregation}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.{SparkExpressions, SparkPushdown, types => sparktypes}

import io.hydrolix.connectors
import io.hydrolix.connectors.expr.Expr
import io.hydrolix.connectors.{HdxConnectionInfo, HdxTable}

final class SparkScanBuilder(info: HdxConnectionInfo,
                            table: HdxTable)
  extends ScanBuilder
     with SupportsPushDownV2Filters
     with SupportsPushDownRequiredColumns
     with SupportsPushDownAggregates
     with Logging
{
  private var pushedPreds: List[Predicate] = Nil
  private var pushedAggs: List[AggregateFunc] = Nil
  private var cols: sparktypes.StructType = _
  private val pkField = table.hdxCols.getOrElse(table.primaryKeyField, sys.error("No PK field"))

  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    val pushable = predicates.toList.groupBy { pred =>
      connectors.HdxPushdown.pushable(
        table.primaryKeyField,
        table.shardKeyField,
        SparkExpressions.sparkToCore(pred, table.schema).asInstanceOf[Expr[Boolean]],
        table.hdxCols
      )
    }

    val type1 = pushable.getOrElse(1, Nil)
    val type2 = pushable.getOrElse(2, Nil)
    val type3 = pushable.getOrElse(3, Nil)

    if (type1.nonEmpty || type2.nonEmpty) log.info(s"These predicates are pushable: 1:[$type1], 2:[$type2]")
    if (type3.nonEmpty) log.info(s"These predicates are NOT pushable: 3:[$type3]")

    // Types 1 & 2 will be pushed
    pushedPreds = type1 ++ type2

    // Types 2 & 3 need to be evaluated after scanning
    (type2 ++ type3).toArray
  }

  override def pushedPredicates(): Array[Predicate] = {
    pushedPreds.toArray
  }

  override def pruneColumns(requiredSchema: sparktypes.StructType): Unit = {
    if (requiredSchema.isEmpty) {
      val pkf = sparktypes.StructField(table.primaryKeyField, SparkTypes.coreToSpark(pkField.`type`))
      cols = sparktypes.DataTypes.createStructType(Array(pkf))
    } else {
      cols = requiredSchema
    }
  }

  override def supportCompletePushDown(aggregation: Aggregation): Boolean = pushAggregation(aggregation)

  override def pushAggregation(aggregation: Aggregation): Boolean = {
    // TODO make agg pushdown work when the GROUP BY is the shard key, and perhaps a primary timestamp derivation?
    val funcs = SparkPushdown.pushableAggs(aggregation, table.primaryKeyField)
    if (funcs.nonEmpty && funcs.size == aggregation.aggregateExpressions().length) {
      pushedAggs = funcs.map(_._1)
      cols = sparktypes.StructType(funcs.map(_._2))
      true
    } else {
      false
    }
  }

  override def build(): Scan = {
    new SparkScan(info, table, cols, pushedPreds, pushedAggs)
  }
}
