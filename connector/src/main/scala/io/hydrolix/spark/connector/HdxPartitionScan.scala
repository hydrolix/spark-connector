package io.hydrolix.spark.connector

import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.types.StructType

case class HdxPartitionScan(db: String,
                            table: String,
                            path: String,
                            schema: StructType,
                            pushed: List[Predicate])
  extends InputPartition
