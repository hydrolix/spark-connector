package io.hydrolix.spark

import model._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchDatabaseException, NoSuchTableException}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.types.{StructField, StructType}

import java.net.URI
import java.util.UUID

object HdxCatalog {
  def main(args: Array[String]): Unit = {
    val info = HdxConnectionInfo(
      orgId = UUID.fromString(args(0)),
      jdbcUrl = args(1),
      user = args(2),
      password = args(3),
      apiUrl = new URI(args(4)),
      bucketPrefix = args(5),
      cloud = args(6),
      cloudCred1 = args(7),
      cloudCred2 = args(8)
    )
    val dbName = args(5)
    val tableName = args(6)

    val cat = new HdxCatalog(info)
    val db = cat.getDatabase(dbName)
    println(db)

    val table = cat.getTable(dbName, tableName)

    println(table)
  }
}

final class HdxCatalog(val info: HdxConnectionInfo) extends ReadOnlyExternalCatalog with Logging {
  private val jdbc = new HdxJdbcSession(info)
  private val api = new HdxApiSession(info)

  private var currentDb: (HdxProject, CatalogDatabase) = null

  override def listDatabases(): Seq[String] = {
    api.databases().map(_.name)
  }

  override def databaseExists(db: String): Boolean = {
    api.databases().exists(_.name == db)
  }

  override def getDatabase(db: String): CatalogDatabase = {
    val hdb = api.database(db).getOrElse(throw NoSuchDatabaseException(db))

    CatalogDatabase(
      hdb.name,
      hdb.description.orNull,
      hdb.url,
      Map(
        "projectId" -> hdb.uuid.toString,
      )
    )
  }

  override def listDatabases(pattern: String): Seq[String] = {
    StringUtils.filterPattern(listDatabases(), pattern)
  }

  override def setCurrentDatabase(db: String): Unit = {
    val hdb = api.database(db).getOrElse(throw NoSuchDatabaseException(db))
    currentDb = hdb -> getDatabase(db)
  }

  override def getTable(db: String, table: String): CatalogTable = {
    _table(db, table)
  }

  private def _table(db: String, table: String): CatalogTable = {
    val ht = api.table(db, table).getOrElse(throw NoSuchTableException(s"$db.$table"))
    val cols = jdbc.collectColumns(db, table)
    val pk = api.pk(db, table)

    CatalogTable(
      identifier = TableIdentifier(table, Some(db)),
      tableType = CatalogTableType.EXTERNAL,
      storage = CatalogStorageFormat(Some(ht.url.toURI), None, None, None, true, Map()),
      schema = StructType(cols.map(col => StructField(col.name, col.sparkType, col.nullable))),
      provider = Some("hydrolix"),
      createTime = ht.created.toEpochMilli,
      partitionColumnNames = List(pk.name) ++ ht.settings.shardKey.toList,
      tracksPartitionsInCatalog = true,
      schemaPreservesCase = true
    )
  }

  override def getTablesByName(db: String, tables: Seq[String]): Seq[CatalogTable] = {
    api.tables(db).map(table => _table(db, table.name))
  }

  override def tableExists(db: String, table: String): Boolean = {
    api.table(db, table).isDefined
  }

  override def listTables(db: String): Seq[String] = {
    api.tables(db).map(_.name)
  }

  override def listTables(db: String, pattern: String): Seq[String] = {
    StringUtils.filterPattern(api.tables(db).map(_.name), pattern)
  }

  override def getPartition(db: String, table: String, spec: TablePartitionSpec): CatalogTablePartition = ???
  override def getPartitionOption(db: String, table: String, spec: TablePartitionSpec): Option[CatalogTablePartition] = ???
  override def listPartitionNames(db: String, table: String, partialSpec: Option[TablePartitionSpec]): Seq[String] = ???
  override def listPartitions(db: String, table: String, partialSpec: Option[TablePartitionSpec]): Seq[CatalogTablePartition] = ???
  override def listPartitionsByFilter(db: String, table: String, predicates: Seq[Expression], defaultTimeZoneId: String): Seq[CatalogTablePartition] = ???

  override def getFunction(db: String, funcName: String): CatalogFunction = throw new IllegalArgumentException(s"Unknown function '$funcName'")
  override def functionExists(db: String, funcName: String): Boolean = false
  override def listFunctions(db: String, pattern: String): Seq[String] = Nil
  override def listViews(db: String, pattern: String): Seq[String] = Nil

}

trait ReadOnlyExternalCatalog extends ExternalCatalog {
  private def nope(): Nothing = throw new UnsupportedOperationException("unsupported")

  override def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit = nope()
  override def dropDatabase(db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit = nope()
  override def alterDatabase(dbDefinition: CatalogDatabase): Unit = nope()

  override def createTable(tableDefinition: CatalogTable, ignoreIfExists: Boolean): Unit = nope()
  override def dropTable(db: String, table: String, ignoreIfNotExists: Boolean, purge: Boolean): Unit = nope()
  override def renameTable(db: String, oldName: String, newName: String): Unit = nope()
  override def alterTable(tableDefinition: CatalogTable): Unit = nope()
  override def alterTableDataSchema(db: String, table: String, newDataSchema: StructType): Unit = nope()
  override def alterTableStats(db: String, table: String, stats: Option[CatalogStatistics]): Unit = nope()

  override def createPartitions(db: String, table: String, parts: Seq[CatalogTablePartition], ignoreIfExists: Boolean): Unit = nope()
  override def dropPartitions(db: String, table: String, parts: Seq[TablePartitionSpec], ignoreIfNotExists: Boolean, purge: Boolean, retainData: Boolean): Unit = nope()
  override def renamePartitions(db: String, table: String, specs: Seq[TablePartitionSpec], newSpecs: Seq[TablePartitionSpec]): Unit = nope()
  override def alterPartitions(db: String, table: String, parts: Seq[CatalogTablePartition]): Unit = nope()

  override def createFunction(db: String, funcDefinition: CatalogFunction): Unit = nope()
  override def dropFunction(db: String, funcName: String): Unit = nope()
  override def alterFunction(db: String, funcDefinition: CatalogFunction): Unit = nope()
  override def renameFunction(db: String, oldName: String, newName: String): Unit = nope()

  override def loadTable(db: String, table: String, loadPath: String, isOverwrite: Boolean, isSrcLocal: Boolean): Unit = nope()
  override def loadPartition(db: String, table: String, loadPath: String, partition: TablePartitionSpec, isOverwrite: Boolean, inheritTableSpecs: Boolean, isSrcLocal: Boolean): Unit = nope()
  override def loadDynamicPartitions(db: String, table: String, loadPath: String, partition: TablePartitionSpec, replace: Boolean, numDP: Int): Unit = nope()
}