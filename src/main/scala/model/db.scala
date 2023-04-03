package io.hydrolix.spark
package model

import org.apache.spark.sql.types.DataType

import java.net.URI
import java.time.Instant
import java.util.UUID

case class HdxConnectionInfo(orgId: UUID,
                             jdbcUrl: String,
                             user: String,
                             password: String,
                             apiUrl: URI,
                             bucketPrefix: String,
                             cloud: String,
                             cloudCred1: String,
                             cloudCred2: String)

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

