package io.hydrolix.spark.connector

import io.hydrolix.spark.model.{HdxColumnInfo, HdxConnectionInfo, HdxStorage}

import org.apache.spark.sql.connector.catalog.{Identifier, SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.{util => ju}

case class HdxTable(info: HdxConnectionInfo,
                 storage: HdxStorage,
                   ident: Identifier,
                  schema: StructType,
                 options: CaseInsensitiveStringMap,
         primaryKeyField: String,
           shardKeyField: Option[String],
           sortKeyFields: List[String],
                 hdxCols: Map[String, HdxColumnInfo])
  extends Table
    with SupportsRead
{
  override def name(): String = ident.toString

  override def capabilities(): ju.Set[TableCapability] = ju.EnumSet.of(TableCapability.BATCH_READ)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new HdxScanBuilder(info, storage, this)
  }
}
