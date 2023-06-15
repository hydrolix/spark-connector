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

import com.clickhouse.jdbc.ClickHouseDataSource
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.slf4j.LoggerFactory

import java.time.{Instant, ZoneOffset}
import java.util.{Properties, UUID}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.{Try, Using}

object HdxJdbcSession {
  private val cache = mutable.Map[HdxConnectionInfo, HdxJdbcSession]()

  def apply(info: HdxConnectionInfo): HdxJdbcSession = {
    cache.getOrElse(info, {
      val fresh = new HdxJdbcSession(info)
      cache += (info -> fresh)
      fresh
    })
  }
}

/**
 * TODO this uses a single connection for metadata about all databases; maybe there should be one of these per DB
 */
class HdxJdbcSession private (info: HdxConnectionInfo) {
  private val log = LoggerFactory.getLogger(getClass)

  private lazy val pool = {
    val ds = {
      val props = new Properties()
      props.put("web_context", "/query")
      props.put("path", "/query")
      props.put("user", info.user)
      props.put("username", info.user)
      props.put("password", info.password)
      props.put("compress", "false")
      props.put("ssl", "true")

      new ClickHouseDataSource(info.jdbcUrl, props)
    }

    val props = new Properties()
    props.put("jdbcUrl", info.jdbcUrl)
    props.put("dataSource", ds)
    new HikariDataSource(new HikariConfig(props))
  }

  def collectColumns(db: String, table: String): List[HdxColumnInfo] = {
    Using.Manager { use =>
      val conn = use(pool.getConnection)
      val stmt = use(conn.createStatement())
      val rs = use(stmt.executeQuery(
        s"""SELECT
           |column_name,
           |count(*)                    AS num_partitions, -- how many partitions this column appears in
           |groupUniqArray(column_type) AS column_types,   -- all distinct types this column ever had
           |sum(column_index)           AS sum_indexed     -- number of partitions in which this column was indexed
           |FROM `$db`.`$table#.metadata`
           |GROUP BY column_name""".stripMargin))

      val cols = ListBuffer[HdxColumnInfo]()
      while (rs.next()) {
        val name = rs.getString("column_name")
        val occurs = rs.getInt("num_partitions")
        val types = rs.getArray("column_types").getArray.asInstanceOf[Array[String]].toSet
        val sumIndexed = rs.getInt("sum_indexed")

        if (types.size > 1) {
          log.warn(s"Column $db.$table.$name had multiple types ($types); arbitrarily picking ${types.head} and hoping for the best!")
        }

        val (sparkType, hdxType, nullable) = Types.decodeClickhouseType(types.head)
        val indexed = if (sumIndexed == occurs) 2 else if (sumIndexed == 0) 0 else 1

        cols += HdxColumnInfo(name, types.head, hdxType, nullable, sparkType, indexed)
      }
      cols.toList
    }.get
  }

  /**
   * Get the sum(rows), min(primary) and max(primary) of ALL partitions
   */
  def collectPartitionAggs(db: String, table: String): (Long, Instant, Instant) = {
    Using.Manager { use =>
      val conn = use(pool.getConnection)
      val stmt = use(conn.createStatement())
      val rs = use(stmt.executeQuery(
        s"""SELECT
           |  sum(rows) as rows,
           |  min(min_timestamp) as min_primary,
           |  max(max_timestamp) as max_primary
           |FROM `$db`.`$table#.catalog`""".stripMargin))

      rs.next()

      (
        rs.getLong("rows"),
        rs.getTimestamp("min_primary").toLocalDateTime.toInstant(ZoneOffset.UTC),
        rs.getTimestamp("max_primary").toLocalDateTime.toInstant(ZoneOffset.UTC)
      )
    }.get
  }

  def collectPartitions(db: String, table: String): List[HdxDbPartition] = {
    Using.Manager { use =>
      val conn = use(pool.getConnection)
      val stmt = use(conn.createStatement())
      val rs = use(stmt.executeQuery(s"SELECT * FROM `$db`.`$table#.catalog`"))

      val partitions = ListBuffer[HdxDbPartition]()

      val hasStorageId = Try(rs.findColumn("storage_id")).isSuccess

      while (rs.next()) {
        partitions += HdxDbPartition(
          rs.getString("partition"),
          rs.getTimestamp("min_timestamp").toLocalDateTime.toInstant(ZoneOffset.UTC),
          rs.getTimestamp("max_timestamp").toLocalDateTime.toInstant(ZoneOffset.UTC),
          rs.getLong("manifest_size"),
          rs.getLong("data_size"),
          rs.getLong("index_size"),
          rs.getLong("rows"),
          rs.getLong("mem_size"),
          rs.getString("root_path"),
          rs.getString("shard_key"),
          rs.getByte("active") == 1,
          if (hasStorageId) {
            rs.getString("storage_id").noneIfEmpty.map(UUID.fromString)
          } else {
            None
          }
        )
      }
      partitions.toList
    }.get
  }
}
