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
package io.hydrolix.connectors.spark

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.Collections
import java.{util => ju}
import scala.collection.JavaConverters._

import io.hydrolix.connectors.HdxTableCatalog

//noinspection ScalaUnusedSymbol: This is referenced as a classname on the Spark command line (`-c spark.sql.catalog.hydrolix=io.hydrolix.spark.connector.SparkTableCatalog`)
final class SparkTableCatalog
    extends TableCatalog
       with SupportsNamespaces
       with Logging
{
  private var coreCatalog: HdxTableCatalog = _

  override def name(): String = coreCatalog.name

  override def initialize(name: String, opts: CaseInsensitiveStringMap): Unit = {
    val coreCatalog = new HdxTableCatalog()
    coreCatalog.initialize(name, opts.asScala.toMap)
    this.coreCatalog = coreCatalog
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    coreCatalog.listTables(namespace.toList).map { name =>
      Identifier.of(name.init.toArray, name.last)
    }.toArray
  }

  override def loadTable(ident: Identifier): Table = {
    val coreTable = coreCatalog.loadTable((ident.namespace() :+ ident.name()).toList)

    SparkTable(coreTable)
  }

  override def listNamespaces(): Array[Array[String]] = {
    coreCatalog.listNamespaces().map(_.toArray).toArray
  }

  // TODO implement if needed
  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = Array()

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
