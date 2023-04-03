package io.hydrolix.spark

import model.{HdxColumnInfo, HdxConnectionInfo, HdxDbPartition}

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.apache.spark.sql.types.{DataType, DataTypes}
import ru.yandex.clickhouse.ClickHouseDataSource

import java.util.Properties
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._
import scala.util.Using

/**
 * TODO this uses a single connection for metadata about all databases; maybe there should be one of these per DB
 */
class HdxJdbcSession(info: HdxConnectionInfo) {
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
        val sparkType = decodeClickhouseTypeName(typeName)._1

        cols += HdxColumnInfo(name, colType, typeName, nullable, sparkType)
      }
      cols.toList
    }.get
  }

  private val arrayR = """array\((.*?)\)""".r
  private val mapR = """array\((.*?),\s*(.*?)\)""".r
  private val nullableR = """nullable\((.*?)\)""".r
  private val datetime64R = """datetime64\((.*?)\)""".r
  private val datetimeR = """datetime\((.*?)\)""".r

  /**
   * TODO this is conservative about making space for rare, high-magnitude values, e.g. uint64 -> decimal ... should
   *  probably be optional
   *
   * @return (sparkType, nullable?)
   */
  private def decodeClickhouseTypeName(s: String): (DataType, Boolean) = {
    s.toLowerCase match {
      case "uint8" | "int8" | "int32" | "int16" | "uint16" => DataTypes.IntegerType -> false
      case "uint32" | "int64" => DataTypes.LongType -> false
      case "uint64" => DataTypes.createDecimalType(19, 0) -> false
      case "int128" | "uint128" => DataTypes.createDecimalType(39, 0) -> false
      case "int256" | "uint256" => DataTypes.createDecimalType(78, 0) -> false
      case "float32" => DataTypes.FloatType -> false
      case "float64" => DataTypes.DoubleType -> false
      case "string" => DataTypes.StringType -> false
      case datetime64R(_) => DataTypes.TimestampType -> false // TODO OK to discard precision here?
      case datetimeR(_) => DataTypes.TimestampType -> false // TODO OK to discard precision here?

      case arrayR(elementTypeName) =>
        val (typ, nullable) = decodeClickhouseTypeName(elementTypeName)
        DataTypes.createArrayType(typ) -> nullable

      case mapR(keyTypeName, valueTypeName) =>
        val (keyType, _) = decodeClickhouseTypeName(keyTypeName)
        val (valueType, valueNull) = decodeClickhouseTypeName(valueTypeName)
        DataTypes.createMapType(keyType, valueType, valueNull) -> false

      case nullableR(typeName) =>
        val (typ, _) = decodeClickhouseTypeName(typeName)
        typ -> true
    }
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
