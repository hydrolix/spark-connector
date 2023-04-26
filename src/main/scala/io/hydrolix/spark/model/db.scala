package io.hydrolix.spark
package model

import org.apache.spark.sql.types.DataType

import java.time.Instant

/*
 * These are Scala representations of the metadata visible from a Hydrolix JDBC connection.
 */

/**
 * @param indexed
 *                - 0: not indexed in any partition
 *                - 1: indexed in some partitions
 *                - 2: indexed in all partitions
 */
case class HdxColumnInfo(name: String,
               clickhouseType: String,
                     nullable: Boolean,
                    sparkType: DataType,
                      indexed: Int)

case class HdxDbPartition(partition: String,
                       minTimestamp: Instant,
                       maxTimestamp: Instant,
                       manifestSize: Long,
                           dataSize: Long,
                          indexSize: Long,
                               rows: Long,
                            memSize: Long,
                           rootPath: String,
                           shardKey: String,
                             active: Boolean)
