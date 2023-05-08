package io.hydrolix.spark.connector

import io.hydrolix.spark.model.HdxConnectionInfo.{OPT_PROJECT_NAME, OPT_TABLE_NAME}
import io.hydrolix.spark.model.{HdxColumnInfo, HdxConnectionInfo, HdxJdbcSession}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.connector.catalog.{Identifier, NamespaceChange, SupportsNamespaces, Table, TableCatalog, TableChange}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits._

import java.util.Collections
import java.{util => ju}
import scala.collection.mutable

//noinspection ScalaUnusedSymbol: This is referenced as a classname on the Spark command line (`-c spark.sql.catalog.hydrolix=io.hydrolix.spark.connector.HdxTableCatalog`)
class HdxTableCatalog extends TableCatalog
                         with SupportsNamespaces
                         with Logging
{
  var name: String = _
  private var info: HdxConnectionInfo = _
  private var api: HdxApiSession = _
  private var jdbc: HdxJdbcSession = _

  private val columnsCache = mutable.HashMap[(String, String), List[HdxColumnInfo]]()

  private def columns(db: String, table: String): List[HdxColumnInfo] = {
    columnsCache.getOrElseUpdate((db, table), {
      jdbc.collectColumns(db, table)
    })
  }

  def initialize(name: String, opts: CaseInsensitiveStringMap): Unit = {
    this.name = name
    this.info = HdxConnectionInfo.fromOpts(opts, log)
    this.api = new HdxApiSession(info)
    this.jdbc = HdxJdbcSession(info)
  }

  private def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val db = options.get(OPT_PROJECT_NAME)
    val table = options.get(OPT_TABLE_NAME)

    val cols = columns(db, table)

    StructType(cols.map(col => StructField(col.name, col.sparkType, col.nullable)))
  }

  private def getTable(schema: StructType, properties: ju.Map[String, String]): Table = {
    val db = properties.get(OPT_PROJECT_NAME)
    val table = properties.get(OPT_TABLE_NAME)

    val apiTable = api.table(db, table).getOrElse(throw NoSuchTableException(s"$db.$table"))
    val primaryKey = api.pk(db, table)

    HdxTable(
      info,
      Identifier.of(Array(db), table),
      schema,
      CaseInsensitiveStringMap.empty(),
      primaryKey.name,
      apiTable.settings.shardKey,
      apiTable.settings.sortKeys,
      columns(db, table).map(col => col.name -> col).toMap
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
      Map(
        OPT_PROJECT_NAME -> ident.namespace().head,
        OPT_TABLE_NAME -> ident.name()
      ).asOptions
    )
  }

  override def listNamespaces(): Array[Array[String]] = {
    (for {
      db <- api.databases()
      table <- api.tables(db.name)
    } yield List(db.name, table.name).toArray).toArray
  }

  // TODO implement if needed
  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = ???

  // TODO implement if needed
  override def loadNamespaceMetadata(namespace: Array[String]): ju.Map[String, String] = Collections.emptyMap()

  //noinspection ScalaDeprecation
  override def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: ju.Map[String, String]): Table = nope()
  override def alterTable(ident: Identifier, changes: TableChange*): Table = nope()
  override def dropTable(ident: Identifier): Boolean = nope()
  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = nope()
  override def createNamespace(namespace: Array[String], metadata: ju.Map[String, String]): Unit = nope()
  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = nope()
  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean = nope()
}
