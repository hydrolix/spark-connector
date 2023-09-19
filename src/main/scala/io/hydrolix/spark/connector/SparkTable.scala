/*
 * Copyright (c) 2023 Hydrolix Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hydrolix.spark.connector

import org.apache.spark.sql.catalyst.util.quoteIfNeeded
import org.apache.spark.sql.connector.catalog.{Identifier, SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.UUID
import java.{util => ju}

import io.hydrolix.connectors.HdxTable
import io.hydrolix.spark.model.{HdxColumnInfo, HdxConnectionInfo, HdxQueryMode, HdxStorageSettings}

case class SparkTable(val coreTable: HdxTable)
  extends Table
     with SupportsRead
{
  override def name(): String = coreTable.ident.map(quoteIfNeeded).mkString(".")

  override def capabilities(): ju.Set[TableCapability] = ju.EnumSet.of(TableCapability.BATCH_READ)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new HdxScanBuilder(coreTable.info, this)
  }
}
