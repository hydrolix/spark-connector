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
  private val OPT_ORG_ID = "io.hydrolix.spark.org_id"
  private val OPT_PROJECT_NAME = "io.hydrolix.spark.project_name"
  private val OPT_TABLE_NAME = "io.hydrolix.spark.table_name"
  private val OPT_JDBC_URL = "io.hydrolix.spark.jdbc_url"
  private val OPT_USERNAME = "io.hydrolix.spark.username"
  private val OPT_PASSWORD = "io.hydrolix.spark.password"
  private val OPT_API_URL = "io.hydrolix.spark.api_url"

  def main(args: Array[String]): Unit = {
    val opts = new CaseInsensitiveStringMap(Map(
      OPT_ORG_ID -> args(0),
      OPT_PROJECT_NAME -> args(1),
      OPT_TABLE_NAME -> args(2),
      OPT_JDBC_URL -> args(3),
      OPT_USERNAME -> args(4),
      OPT_PASSWORD -> args(5),
      OPT_API_URL -> args(6),
    ).asJava)

    val info = connectionInfo(opts)
    val ds = new HdxDataSource(info)

    println(ds.inferSchema(opts))
    println(ds.inferPartitioning(opts))
  }

  private def connectionInfo(options: CaseInsensitiveStringMap) = {
    val orgId = UUID.fromString(options.get(OPT_ORG_ID))
    val url = options.get(OPT_JDBC_URL)
    val user = options.get(OPT_USERNAME)
    val pass = options.get(OPT_PASSWORD)
    val apiUrl = new URI(options.get(OPT_API_URL))

    HdxConnectionInfo(orgId, url, user, pass, apiUrl)
  }
}

class HdxDataSource(info: HdxConnectionInfo) extends DataSourceRegister with SupportsCatalogOptions with Logging {
  import HdxDataSource._

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
    val db = options.get(OPT_PROJECT_NAME)
    val table = options.get(OPT_TABLE_NAME)

    val cols = columns(options, db, table)

    StructType(cols.map(col => StructField(col.name, col.sparkType, col.nullable)))
  }

  override def inferPartitioning(options: CaseInsensitiveStringMap): Array[Transform] = {
    val db = options.get(OPT_PROJECT_NAME)
    val table = options.get(OPT_TABLE_NAME)

    val pk = api.pk(db, table)

    val hdxTable = api.table(db, table).getOrElse(throw NoSuchTableException(s"$db.$table"))
    val shardKey = hdxTable.settings.shardKey
    val sortKeys = hdxTable.settings.sortKeys

    (
      List(Expressions.apply(s"primary_key_${pk.name}", Expressions.column(pk.name))) ++
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
