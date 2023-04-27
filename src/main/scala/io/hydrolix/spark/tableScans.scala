package io.hydrolix.spark

import org.apache.spark.internal.Logging
import org.apache.spark.sql.HdxPredicatePushdown
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.index.{SupportsIndex, TableIndex}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.expressions.{Expressions, NamedReference}
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.Properties
import java.{util => ju}

case class HdxTable(info: HdxConnectionInfo,
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
  private val indices = List(s"primary_$primaryKeyField") ++
    sortKeyFields.map(sk => s"sort_$sk") ++
    shardKeyField.map(sk => s"shard_$sk")

  override def name(): String = ident.toString

  override def capabilities(): ju.Set[TableCapability] = ju.EnumSet.of(TableCapability.BATCH_READ)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new HdxScanBuilder(info, this)
  }

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
    }.toArray
  }

  override def createIndex(indexName: String,
                             columns: Array[NamedReference],
                   columnsProperties: ju.Map[NamedReference, ju.Map[String, String]],
                          properties: ju.Map[String, String])
                                    : Unit = nope()

  override def dropIndex(indexName: String): Unit = nope()
}

class HdxScanBuilder(info: HdxConnectionInfo, table: HdxTable)
  extends ScanBuilder
     with SupportsPushDownV2Filters
     with SupportsPushDownRequiredColumns
     with Logging
{
  private var pushed: List[Predicate] = List()
  private var cols: StructType = _
  private val hcols = HdxJdbcSession(info)
    .collectColumns(table.ident.namespace().head, table.ident.name())
    .map(col => col.name -> col)
    .toMap

  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    val pushable = predicates.toList.groupBy(HdxPredicatePushdown.pushable(table.primaryKeyField, table.shardKeyField, _, hcols))

    val type1 = pushable.getOrElse(1, Nil)
    val type2 = pushable.getOrElse(2, Nil)
    val type3 = pushable.getOrElse(3, Nil)

    if (type1.nonEmpty || type2.nonEmpty) log.warn(s"These predicates are pushable: 1:[$type1], 2:[$type2]")
    if (type3.nonEmpty) log.warn(s"These predicates are NOT pushable: 3:[$type3]")

    // Types 1 & 2 will be pushed
    pushed = type1 ++ type2

    // Types 2 & 3 need to be evaluated after scanning
    (type2 ++ type3).toArray
  }

  override def pushedPredicates(): Array[Predicate] = {
    pushed.toArray
  }

  override def pruneColumns(requiredSchema: StructType): Unit = {
    cols = requiredSchema
  }

  override def build(): Scan = {
    new HdxScan(info, table, cols, pushed)
  }
}

class HdxScan(info: HdxConnectionInfo,
             table: HdxTable,
              cols: StructType,
            pushed: List[Predicate])
  extends Scan
{
  override def toBatch: Batch = {
    new HdxBatch(info, table, cols, pushed)
  }

  override def description(): String = super.description()

  override def readSchema(): StructType = cols
}

class HdxBatch(info: HdxConnectionInfo,
              table: HdxTable,
               cols: StructType,
             pushed: List[Predicate])
  extends Batch
     with Logging
{
  private val jdbc = HdxJdbcSession(info)

  override def planInputPartitions(): Array[InputPartition] = {
    val parts = jdbc.collectPartitions(table.ident.namespace().head, table.ident.name())

    parts.flatMap { hp =>
      val max = hp.maxTimestamp
      val min = hp.minTimestamp
      val sk = hp.shardKey

      if (pushed.nonEmpty && pushed.forall(HdxPredicatePushdown.prunePartition(table.primaryKeyField, table.shardKeyField, _, min, max, sk))) {
        // All pushed predicates found this partition can be pruned; skip it
        None
      } else {
        // Either nothing was pushed, or at least one predicate didn't want to prune this partition; scan it
        Some(HdxPartitionScan(
          table.ident.namespace().head,
          table.ident.name(),
          hp.partition,
          cols,
          pushed))
      }
    }.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = new HdxPartitionReaderFactory(info, table.primaryKeyField)
}
