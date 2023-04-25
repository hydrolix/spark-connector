package io.hydrolix.spark

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import io.hydrolix.spark.model.{HdxColumnInfo, HdxDbPartition}
import ru.yandex.clickhouse.ClickHouseDataSource

import java.util.Properties
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._
import scala.util.Using

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
  private lazy val pool = {
    val ds = {
      val props = new Properties()
      props.put("web_context", "/query")
      props.put("path", "/query")
      props.put("user", info.user)
      props.put("password", info.password)
      props.put("compress", "false")
      props.put("ssl", "true")

      new ClickHouseDataSource(info.jdbcUrl, props)
    }

    val props = new Properties()
    props.putAll(Map(
      "jdbcUrl" -> info.jdbcUrl,
      "dataSource" -> ds
    ).asJava)
    new HikariDataSource(new HikariConfig(props))
  }

  def collectColumns(db: String, table: String): List[HdxColumnInfo] = {
    Using.Manager { use =>
      val conn = use(pool.getConnection)
      val rs = use(conn.getMetaData.getColumns(null, db, table, null))

      val cols = ListBuffer[HdxColumnInfo]()
      while (rs.next()) {
        val name = rs.getString("COLUMN_NAME")
        val colType = rs.getInt("DATA_TYPE")
        val typeName = rs.getString("TYPE_NAME")
        val nullable = rs.getBoolean("NULLABLE")
        val sparkType = Types.clickhouseToSpark(typeName)._1

        cols += HdxColumnInfo(name, colType, typeName, nullable, sparkType)
      }
      cols.toList
    }.get
  }

  def collectPartitions(db: String, table: String): List[HdxDbPartition] = {
    Using.Manager { use =>
      val conn = use(pool.getConnection)
      val stmt = use(conn.createStatement())
      val rs = use(stmt.executeQuery(s"SELECT * FROM `$db`.`$table#.catalog`"))

      val partitions = ListBuffer[HdxDbPartition]()

      while (rs.next()) {
        partitions += HdxDbPartition(
          rs.getString("partition"),
          rs.getTimestamp("min_timestamp").toInstant,
          rs.getTimestamp("max_timestamp").toInstant,
          rs.getLong("manifest_size"),
          rs.getLong("data_size"),
          rs.getLong("index_size"),
          rs.getLong("rows"),
          rs.getLong("mem_size"),
          rs.getString("root_path"),
          rs.getString("shard_key"),
          rs.getByte("active") == 1
        )
      }
      partitions.toList
    }.get
  }

}
