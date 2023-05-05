package io.hydrolix.spark.connector

import io.hydrolix.spark.model.HdxConnectionInfo

import org.apache.spark.internal.Logging
import org.apache.spark.sql.HdxPredicatePushdown
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownRequiredColumns, SupportsPushDownV2Filters}
import org.apache.spark.sql.types.{StructField, StructType}

class HdxScanBuilder(info: HdxConnectionInfo, table: HdxTable)
  extends ScanBuilder
    with SupportsPushDownV2Filters
    with SupportsPushDownRequiredColumns
    with Logging
{
  private var pushed: List[Predicate] = List()
  private var cols: StructType = _
  private val pkField = table.hdxCols.getOrElse(table.primaryKeyField, sys.error("No PK field"))

  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    val pushable = predicates.toList.groupBy(HdxPredicatePushdown.pushable(table.primaryKeyField, table.shardKeyField, _, table.hdxCols))

    val type1 = pushable.getOrElse(1, Nil)
    val type2 = pushable.getOrElse(2, Nil)
    val type3 = pushable.getOrElse(3, Nil)

    if (type1.nonEmpty || type2.nonEmpty) log.info(s"These predicates are pushable: 1:[$type1], 2:[$type2]")
    if (type3.nonEmpty) log.info(s"These predicates are NOT pushable: 3:[$type3]")

    // Types 1 & 2 will be pushed
    pushed = type1 ++ type2

    // Types 2 & 3 need to be evaluated after scanning
    (type2 ++ type3).toArray
  }

  override def pushedPredicates(): Array[Predicate] = {
    pushed.toArray
  }

  override def pruneColumns(requiredSchema: StructType): Unit = {
    if (requiredSchema.isEmpty) {
      cols = StructType(List(StructField(table.primaryKeyField, pkField.sparkType)))
    } else {
      cols = requiredSchema
    }
  }

  override def build(): Scan = {
    new HdxScan(info, table, cols, pushed)
  }
}
