package io.hydrolix.spark.connector

import io.hydrolix.spark.model.{HdxColumnInfo, HdxConnectionInfo}

import org.apache.spark.sql.connector.catalog.index.{SupportsIndex, TableIndex}
import org.apache.spark.sql.connector.catalog.{Identifier, SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.expressions.{Expressions, NamedReference}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.{Collections, Properties}
import java.{util => ju}

case class HdxTable(info: HdxConnectionInfo,
                   ident: Identifier,
                  schema: StructType,
                 options: CaseInsensitiveStringMap,
         primaryKeyField: String,
           shardKeyField: Option[String],
           sortKeyFields: List[String],
                 hdxCols: Map[String, HdxColumnInfo])
  extends Table
    with SupportsRead
    with SupportsIndex {
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
        Collections.emptyMap(),
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
