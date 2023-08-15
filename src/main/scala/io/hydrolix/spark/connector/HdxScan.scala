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

import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Batch, Scan}
import org.apache.spark.sql.types.StructType

import io.hydrolix.spark.model.HdxConnectionInfo

final class HdxScan(info: HdxConnectionInfo,
                   table: HdxTable,
                    cols: StructType,
             pushedPreds: List[Predicate],
              pushedAggs: List[AggregateFunc])
  extends Scan
{
  // TODO consider adding SupportsRuntimeFiltering
  private val batch = new HdxBatch(info, table, cols, pushedPreds, pushedAggs)

  override def toBatch: Batch = batch

  override def description(): String = super.description()

  override def readSchema(): StructType = cols
}
