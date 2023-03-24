package io.hydrolix.spark

import model._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.connector.catalog.{Identifier, SupportsCatalogOptions, Table}
import org.apache.spark.sql.connector.expressions.{Expressions, SortDirection, Transform}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.net.URI
import java.util
import java.util.UUID
import scala.collection.mutable
import scala.jdk.CollectionConverters._

object HdxDataSource {
  def main(args: Array[String]): Unit = {
    val opts = new CaseInsensitiveStringMap(Map(
      "io.hydrolix.spark.org_id" -> args(0),
      "io.hydrolix.spark.project_name" -> args(1),
      "io.hydrolix.spark.table_name" -> args(2),
      "io.hydrolix.spark.pkey_field_name" -> args(3), // TODO get this from API/hdx instead of a command-line arg; https://hydrolix.atlassian.net/browse/HDX-3648
      "io.hydrolix.spark.jdbc_url" -> args(4),
      "io.hydrolix.spark.jdbc_username" -> args(5),
      "io.hydrolix.spark.jdbc_password" -> args(6),
      "io.hydrolix.spark.api_url" -> args(7),
    ).asJava)

    val info = connectionInfo(opts)
    val ds = new HdxDataSource(info)

    println(ds.inferSchema(opts))
  }

  private def connectionInfo(options: CaseInsensitiveStringMap) = {
    val orgId = UUID.fromString(options.get("io.hydrolix.spark.org_id"))
    val url = options.get("io.hydrolix.spark.jdbc_url")
    val user = options.get("io.hydrolix.spark.jdbc_username")
    val pass = options.get("io.hydrolix.spark.jdbc_password")
    val apiUrl = new URI(options.get("io.hydrolix.spark.api_url"))

    HdxConnectionInfo(orgId, url, user, pass, apiUrl)
  }
}

class HdxDataSource(info: HdxConnectionInfo) extends DataSourceRegister with SupportsCatalogOptions with Logging {
  private val api = new HdxApiSession(info)
  private val jdbc = new HdxJdbcSession(info)

  override def shortName(): String = "hydrolix"

  private val columnsCache = mutable.HashMap[(String, String), List[HdxColumnInfo]]()

  private def columns(options: CaseInsensitiveStringMap, db: String, table: String): List[HdxColumnInfo] = {
    columnsCache.getOrElseUpdate((db, table), {
      jdbc.collectColumns(db, table)
    })
  }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val db = options.get("io.hydrolix.spark.project_name")
    val table = options.get("io.hydrolix.spark.table_name")

    val cols = columns(options, db, table)

    StructType(cols.map(col => StructField(col.name, col.sparkType, col.nullable)))
  }

  override def inferPartitioning(options: CaseInsensitiveStringMap): Array[Transform] = {
    val db = options.get("io.hydrolix.spark.project_name")
    val table = options.get("io.hydrolix.spark.table_name")
    val pkey = options.get("io.hydrolix.spark.pkey_field_name") // TODO get this from API/hdx instead of a command-line arg

    val hdxTable = api.table(db, table).getOrElse(throw NoSuchTableException(s"$db.$table"))
    val shardKey = hdxTable.settings.shardKey
    val sortKeys = hdxTable.settings.sortKeys

    (
      List(Expressions.apply(s"primary_key_$pkey", Expressions.column(pkey))) ++
      shardKey.map(sk => Expressions.apply(s"shard_key_$sk", Expressions.column(sk))).toList ++
      sortKeys.map(sk => Expressions.apply(s"sort_key_$sk", Expressions.sort(Expressions.column(sk), SortDirection.ASCENDING)))
    ).toArray
  }


  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table = {
    ???
  }


  override def extractIdentifier(options: CaseInsensitiveStringMap): Identifier = {
    ???
  }
}
