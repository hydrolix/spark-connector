package io.hydrolix.spark.connector

import io.hydrolix.spark.model.HdxColumnInfo

import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.types.StructType

case class HdxScanPartition(db: String,
                         table: String,
                          path: String,
                        schema: StructType,
                        pushed: List[Predicate],
                       hdxCols: Map[String, HdxColumnInfo])
  extends InputPartition
