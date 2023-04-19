package io.hydrolix.spark

import model.HdxConnectionInfo.{OPT_PROJECT_NAME, OPT_TABLE_NAME}
import model.{HdxColumnInfo, HdxConnectionInfo}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.CatalogUtils
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableChange}
import org.apache.spark.sql.connector.expressions.{Expressions, Transform}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits._

import java.{util => ju}
import scala.collection.mutable

abstract class Util extends Logging {
  var name: String = _
  protected var info: HdxConnectionInfo = _
  protected var api: HdxApiSession = _
  protected var jdbc: HdxJdbcSession = _

  private val columnsCache = mutable.HashMap[(String, String), List[HdxColumnInfo]]()

  protected def columns(db: String, table: String): List[HdxColumnInfo] = {
    columnsCache.getOrElseUpdate((db, table), {
      jdbc.collectColumns(db, table)
    })
  }

  def initialize(name: String, opts: CaseInsensitiveStringMap): Unit = {
    this.name = name
    this.info = HdxConnectionInfo.fromOpts(opts, log)
    this.api = new HdxApiSession(info)
    this.jdbc = new HdxJdbcSession(info)
  }

  def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    initialize("hydrolix", options)

    val db = options.get(OPT_PROJECT_NAME)
    val table = options.get(OPT_TABLE_NAME)

    val cols = columns(db, table)

    StructType(cols.map(col => StructField(col.name, col.sparkType, col.nullable)))
  }

  def inferPartitioning(options: CaseInsensitiveStringMap): Array[Transform] = {
    val db = options.get(OPT_PROJECT_NAME)
    val table = options.get(OPT_TABLE_NAME)

    val hdxTable = api.table(db, table).getOrElse(throw new NoSuchTableException(s"$db.$table"))
    val shardKey = hdxTable.settings.shardKey

    shardKey.map(sk => Expressions.apply(s"shard_key_$sk", Expressions.column(sk))).toArray
  }

  def getTable(schema: StructType, partitioning: Array[Transform], properties: ju.Map[String, String]): Table = {
    val db = properties.get(OPT_PROJECT_NAME)
    val table = properties.get(OPT_TABLE_NAME)

    val apiTable = api.table(db, table).getOrElse(throw new NoSuchTableException(s"$db.$table"))
    val primaryKey = api.pk(db, table)

    HdxTable(
      info,
      api,
      jdbc,
      Identifier.of(Array(db), table),
      schema,
      CaseInsensitiveStringMap.empty(),
      primaryKey.name,
      apiTable.settings.shardKey,
      apiTable.settings.sortKeys
    )
  }

  def listTables(namespace: Array[String]): Array[Identifier] = {
    assert(namespace.length == 1, "Namespace paths must have exactly one element (DB name)")
    api.tables(namespace.head).map { ht =>
      Identifier.of(namespace, ht.name)
    }.toArray
  }

  def loadTable(ident: Identifier): Table = {
    assert(ident.namespace().length == 1, "Namespace paths must have exactly one element (DB name)")

    val opts = info.asMap +
      (OPT_PROJECT_NAME -> ident.namespace().head) +
      (OPT_TABLE_NAME -> ident.name())

    val schema = inferSchema(opts.asOptions)

    getTable(
      schema,
      Array(),
      Map(
        OPT_PROJECT_NAME -> ident.namespace().head,
        OPT_TABLE_NAME -> ident.name()
      ).asOptions
    )
  }

  def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: ju.Map[String, String]): Table = nope()

  def alterTable(ident: Identifier, changes: TableChange*): Table = nope()

  def dropTable(ident: Identifier): Boolean = nope()

  def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = nope()
}
