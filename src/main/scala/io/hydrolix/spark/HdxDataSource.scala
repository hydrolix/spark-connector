package io.hydrolix.spark

import io.hydrolix.spark.HdxConnectionInfo._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.HdxPredicatePushdown
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.index.{SupportsIndex, TableIndex}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.expressions.{Expressions, NamedReference, Transform}
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.Properties
import java.{util => ju}
import scala.jdk.CollectionConverters._

object HdxDataSource extends Logging {
  def main(args: Array[String]): Unit = {
    val opts = new CaseInsensitiveStringMap(Map(
      OPT_ORG_ID -> args(0),
      OPT_PROJECT_NAME -> args(1),
      OPT_TABLE_NAME -> args(2),
      OPT_JDBC_URL -> args(3),
      OPT_USERNAME -> args(4),
      OPT_PASSWORD -> args(5),
      OPT_API_URL -> args(6),
      OPT_TURBINE_INI_PATH -> args(7),
      OPT_TURBINE_CMD_PATH -> args(8),
    ).asJava)

    val ds = new HdxTableCatalog()
    ds.initialize("hydrolix", opts)


    val schema = ds.inferSchema(opts)
    val txs = ds.inferPartitioning(opts)
    val table = ds.getTable(schema, txs, opts).asInstanceOf[HdxTable]
    val sb = table.newScanBuilder(opts)
    val scan = sb.build()
    val batch = scan.toBatch
    val prf = batch.createReaderFactory()
    val parts = batch.planInputPartitions()
    for (part <- parts) {
      val reader = prf.createReader(part)
      while (reader.next()) {
        log.info(reader.get().toString)
      }
      reader.close()
    }
  }
}

class HdxTableCatalog extends Util with TableCatalog with SupportsNamespaces with Logging {
  override def listNamespaces(): Array[Array[String]] = {
    (for {
      db <- api.databases()
      table <- api.tables(db.name)
    } yield List(db.name, table.name).toArray).toArray
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = ???

  override def loadNamespaceMetadata(namespace: Array[String]): ju.Map[String, String] = ju.Map.of()

  override def createNamespace(namespace: Array[String], metadata: ju.Map[String, String]): Unit = nope()
  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = nope()
  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean = nope()
  override def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: ju.Map[String, String]): Table = nope()
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

  override def capabilities(): ju.Set[TableCapability] = ju.EnumSet.of(TableCapability.BATCH_READ)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new HdxScanBuilder(info, api, jdbc, this)
  }

  override def createIndex(indexName: String,
                             columns: Array[NamedReference],
                   columnsProperties: ju.Map[NamedReference, ju.Map[String, String]],
                          properties: ju.Map[String, String])
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
        ju.Map.of(),
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
     with SupportsPushDownV2Filters
     with Logging
{
  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    val pushable = predicates.filter(HdxPredicatePushdown.pushable(table.primaryKeyField, table.shardKeyField, _))
    log.warn("These predicates may be pushable: {}", pushable.mkString("Array(", ", ", ")"))
    Array()
  }

  override def pushedPredicates(): Array[Predicate] = Array()

  override def build(): Scan = {
    new HdxScan(info, api, jdbc, table, table.schema)
  }
}

class HdxScan(info: HdxConnectionInfo,
               api: HdxApiSession,
              jdbc: HdxJdbcSession,
             table: HdxTable,
              cols: StructType)
  extends Scan
{
  override def toBatch: Batch = {
    new HdxBatch(info, api, jdbc, table)
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
              table: HdxTable)
  extends Batch
{
  override def planInputPartitions(): Array[InputPartition] = {
    val db = table.ident.namespace().head
    val t = table.ident.name()

    val pk = api.pk(db, t)
    val parts = jdbc.collectPartitions(db, t)

    parts.map { hp =>
      HdxPartition(db, t, hp.partition, pk.name, table.schema)
    }.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = new HdxPartitionReaderFactory(info)
}
