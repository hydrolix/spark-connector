package io.hydrolix.spark
package model

import org.apache.spark.sql.types.DataType

import java.time.Instant

/*
 * These are Scala representations of the metadata visible from a Hydrolix JDBC connection.
 */

case class HdxColumnInfo(name: String,
                      colType: Int,
                     typeName: String,
                     nullable: Boolean,
                    sparkType: DataType)

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
