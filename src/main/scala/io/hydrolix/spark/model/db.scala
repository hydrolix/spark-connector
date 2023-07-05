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
package io.hydrolix.spark.model

import com.fasterxml.jackson.annotation.{JsonFormat, OptBoolean}
import com.fasterxml.jackson.databind.PropertyNamingStrategies.SnakeCaseStrategy
import com.fasterxml.jackson.databind.annotation.JsonNaming
import org.apache.spark.sql.types.DataType

import java.time.Instant
import java.util.UUID

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
                      hdxType: HdxColumnDatatype,
                     nullable: Boolean,
                    sparkType: DataType,
                      indexed: Int)

@JsonNaming(classOf[SnakeCaseStrategy])
case class HdxDbPartition(
  partition: String,
  @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "UTC", lenient = OptBoolean.TRUE)
  minTimestamp: Instant,
  @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "UTC", lenient = OptBoolean.TRUE)
  maxTimestamp: Instant,
  manifestSize: Long,
  dataSize: Long,
  indexSize: Long,
  rows: Long,
  memSize: Long,
  rootPath: String,
  shardKey: String,
  active: Boolean,
  storageId: Option[UUID]
)
