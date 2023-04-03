package io.hydrolix.spark

import model._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.index.{SupportsIndex, TableIndex}
import org.apache.spark.sql.connector.expressions.{Expressions, NamedReference, Transform}
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.net.URI
import java.util
import java.util.{Properties, UUID}
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
  private val OPT_CLOUD = "io.hydrolix.spark.cloud"
  private val OPT_BUCKET_PREFIX = "io.hydrolix.spark.bucket_prefix"
  private val OPT_CLOUD_CRED1 = "io.hydrolix.spark.cloud_credential_1"
  private val OPT_CLOUD_CRED2 = "io.hydrolix.spark.cloud_credential_2"

  def main(args: Array[String]): Unit = {
    val opts = new CaseInsensitiveStringMap(Map(
      OPT_ORG_ID -> args(0),
      OPT_PROJECT_NAME -> args(1),
      OPT_TABLE_NAME -> args(2),
      OPT_JDBC_URL -> args(3),
      OPT_USERNAME -> args(4),
      OPT_PASSWORD -> args(5),
      OPT_API_URL -> args(6),
      OPT_CLOUD -> args(7),
      OPT_BUCKET_PREFIX -> args(8),
      OPT_CLOUD_CRED1 -> args(9),
      OPT_CLOUD_CRED2 -> args(10)
    ).asJava)

    val info = connectionInfo(opts)
    val ds = new HdxDataSource(info)

    val schema = ds.inferSchema(opts)
    val txs = ds.inferPartitioning(opts)
    val table = ds.getTable(schema, txs, opts).asInstanceOf[HdxTable]
    val sb = table.newScanBuilder(opts)
    val scan = sb.build()
    val batch = scan.toBatch
    val prf = batch.createReaderFactory()
    val parts = batch.planInputPartitions()
    val reader = prf.createReader(parts.head)
    val row = reader.get()
    println(row)
  }

  private def connectionInfo(options: CaseInsensitiveStringMap) = {
    val orgId = UUID.fromString(options.get(OPT_ORG_ID))
    val url = options.get(OPT_JDBC_URL)
    val user = options.get(OPT_USERNAME)
    val pass = options.get(OPT_PASSWORD)
    val apiUrl = new URI(options.get(OPT_API_URL))
    val bucketPrefix = options.get(OPT_BUCKET_PREFIX)
    val cloud = options.get(OPT_CLOUD)
    val cred1 = options.get(OPT_CLOUD_CRED1)
    val cred2 = options.get(OPT_CLOUD_CRED2)

    HdxConnectionInfo(orgId, url, user, pass, apiUrl, bucketPrefix, cloud, cred1, cred2)
  }
}

class HdxDataSource(info: HdxConnectionInfo)
  extends DataSourceRegister
     with SupportsCatalogOptions
     with TableCatalog
     with Logging
{
  import HdxDataSource._

  private val api = new HdxApiSession(info)
  private val jdbc = new HdxJdbcSession(info)

  override def shortName(): String = "hydrolix"

  private val columnsCache = mutable.HashMap[(String, String), List[HdxColumnInfo]]()

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
  }

  override def name(): String = shortName()

  private def columns(db: String, table: String): List[HdxColumnInfo] = {
    columnsCache.getOrElseUpdate((db, table), {
      jdbc.collectColumns(db, table)
    })
  }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val db = options.get(OPT_PROJECT_NAME)
    val table = options.get(OPT_TABLE_NAME)

    val cols = columns(db, table)

    StructType(cols.map(col => StructField(col.name, col.sparkType, col.nullable)))
  }

  override def inferPartitioning(options: CaseInsensitiveStringMap): Array[Transform] = {
    val db = options.get(OPT_PROJECT_NAME)
    val table = options.get(OPT_TABLE_NAME)

    val hdxTable = api.table(db, table).getOrElse(throw NoSuchTableException(s"$db.$table"))
    val shardKey = hdxTable.settings.shardKey

    shardKey.map(sk => Expressions.apply(s"shard_key_$sk", Expressions.column(sk))).toArray
  }

  override def getTable(schema: StructType,
                  partitioning: Array[Transform],
                    properties: util.Map[String, String])
                              : Table =
  {
    val db = properties.get(OPT_PROJECT_NAME)
    val table = properties.get(OPT_TABLE_NAME)

    val apiTable = api.table(db, table).getOrElse(throw NoSuchTableException(s"$db.$table"))
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

  override def extractIdentifier(options: CaseInsensitiveStringMap): Identifier = {
    val db = options.get(OPT_PROJECT_NAME)
    val table = options.get(OPT_TABLE_NAME)

    Identifier.of(Array(db), table)
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    assert(namespace.length == 1, "Namespace paths must have exactly one element (DB name)")
    api.tables(namespace.head).map { ht =>
      Identifier.of(namespace, ht.name)
    }.toArray
  }

  override def loadTable(ident: Identifier): Table = {
    assert(ident.namespace().length == 1, "Namespace paths must have exactly one element (DB name)")

    val map = CaseInsensitiveStringMap.empty()
    map.put(OPT_PROJECT_NAME, ident.namespace().head)
    map.put(OPT_TABLE_NAME, ident.name())
    val schema = inferSchema(map)

    getTable(
      schema,
      Array(),
      map
    )
  }

  override def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: util.Map[String, String]): Table = nope()
  override def alterTable(ident: Identifier, changes: TableChange*): Table = nope()
  override def dropTable(ident: Identifier): Boolean = nope()
  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = nope()

}

case class HdxTable(info: HdxConnectionInfo,
                     api: HdxApiSession,
                    jdbc: HdxJdbcSession,
                   ident: Identifier,
                  schema: StructType,
                 options: CaseInsensitiveStringMap,
         primaryKeyField: String,
           shardKeyField: Option[String],
           sortKeyFields: List[String])
  extends Table
     with SupportsRead
     with SupportsIndex
{
  private val indices = Array(s"primary_$primaryKeyField") ++
    sortKeyFields.map(sk => s"sort_$sk").toArray ++
    shardKeyField.map(sk => s"shard_$sk").toArray

  override def name(): String = ident.toString

  override def capabilities(): util.Set[TableCapability] = util.EnumSet.of(TableCapability.BATCH_READ)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new HdxScanBuilder(info, api, jdbc, this)
  }

  override def createIndex(indexName: String,
                             columns: Array[NamedReference],
                   columnsProperties: util.Map[NamedReference, util.Map[String, String]],
                          properties: util.Map[String, String])
                                    : Unit = nope()

  override def dropIndex(indexName: String): Unit = nope()

  override def indexExists(indexName: String): Boolean = {
    indices.contains(indexName)
  }

  override def listIndexes(): Array[TableIndex] = {
    indices.map { idxName =>
      val Array(idxType, fieldName) = idxName.split('_')

      new TableIndex(
        idxName,
        idxType,
        Array(Expressions.column(fieldName)),
        util.Map.of(),
        new Properties()
      )
    }
  }
}

class HdxScanBuilder(info: HdxConnectionInfo,
                      api: HdxApiSession,
                     jdbc: HdxJdbcSession,
                    table: HdxTable)
  extends ScanBuilder
     with Logging
{
  override def build(): Scan = {
    new HdxScan(info, api, jdbc, table, Set())
  }
}

class HdxScan(info: HdxConnectionInfo,
               api: HdxApiSession,
              jdbc: HdxJdbcSession,
             table: HdxTable,
              cols: Set[String])
  extends Scan
{
  override def toBatch: Batch = {
    new HdxBatch(info, api, jdbc, table, cols)
  }

  override def description(): String = super.description()

  override def readSchema(): StructType = {
    table.schema.copy(
      fields = table.schema.fields.filter(f => cols.contains(f.name))
    )
  }
}

class HdxBatch(info: HdxConnectionInfo,
                api: HdxApiSession,
               jdbc: HdxJdbcSession,
              table: HdxTable,
               cols: Set[String])
  extends Batch
{
  override def planInputPartitions(): Array[InputPartition] = {
    val db = table.ident.namespace().head
    val t = table.ident.name()

    val pk = api.pk(db, t)
    val parts = jdbc.collectPartitions(db, t)

    parts.map { hp =>
      HdxPartition(db, t, hp.partition, pk.name, cols)
    }.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = new HdxPartitionReaderFactory(info)
}
