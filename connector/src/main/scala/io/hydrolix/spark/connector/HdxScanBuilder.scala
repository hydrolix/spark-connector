package io.hydrolix.spark.connector

import io.hydrolix.spark.model.HdxConnectionInfo

import org.apache.spark.internal.Logging
import org.apache.spark.sql.HdxPushdown
import org.apache.spark.sql.connector.expressions.aggregate.{AggregateFunc, Aggregation}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types.{StructField, StructType}

class HdxScanBuilder(info: HdxConnectionInfo, table: HdxTable)
  extends ScanBuilder
    with SupportsPushDownV2Filters
    with SupportsPushDownRequiredColumns
    with SupportsPushDownAggregates
    with Logging
{
  private var pushedPreds: List[Predicate] = List()
  private var pushedAggs: List[AggregateFunc] = Nil
  private var cols: StructType = _
  private val pkField = table.hdxCols.getOrElse(table.primaryKeyField, sys.error("No PK field"))

  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    val pushable = predicates.toList.groupBy(HdxPushdown.pushable(table.primaryKeyField, table.shardKeyField, _, table.hdxCols))

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

  override def pruneColumns(requiredSchema: StructType): Unit = {
    if (requiredSchema.isEmpty) {
      cols = StructType(List(StructField(table.primaryKeyField, pkField.sparkType)))
    } else {
      cols = requiredSchema
    }
  }

  override def supportCompletePushDown(aggregation: Aggregation): Boolean = pushAggregation(aggregation)

  override def pushAggregation(aggregation: Aggregation): Boolean = {
    // TODO make agg pushdown work when the GROUP BY is the shard key, and perhaps a primary timestamp derivation?
    val funcs = HdxPushdown.pushableAggs(aggregation, table.primaryKeyField)
    if (funcs.nonEmpty && funcs.size == aggregation.aggregateExpressions().length) {
      pushedAggs = funcs.map(_._1)
      cols = StructType(funcs.map(_._2))
      true
    } else {
      false
    }
  }

  override def build(): Scan = {
    new HdxScan(info, table, cols, pushedPreds, pushedAggs)
  }
}
