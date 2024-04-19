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
// NOTE: this is in the spark.sql package because we make use of a few private[sql] members in there 😐
package org.apache.spark.sql

import org.apache.spark.sql.connector.expressions.aggregate._
import org.apache.spark.sql.connector.expressions.{Expression, FieldReference}
import org.apache.spark.sql.types.{DataTypes, StructField}

/**
 * TODO:
 *  - see if multi-part names will ever show up (e.g. in a join?); that would break GetField but hopefully only
 *    in a way that would allow fewer pushdown opportunities rather than incorrect results.
 */
object SparkPushdown {
  // TODO port this to connectors-core
  def pushableAggs(aggregation: Aggregation, primaryKeyField: String): List[(AggregateFunc, StructField)] = {
    if (aggregation.groupByExpressions().nonEmpty) return Nil

    aggregation.aggregateExpressions().flatMap {
      case cs: CountStar => Some(cs -> StructField("COUNT(*)", DataTypes.LongType))
      case mf @ MinField(`primaryKeyField`) => Some(mf -> StructField(s"MIN($primaryKeyField)", DataTypes.TimestampType))
      case mf @ MaxField(`primaryKeyField`) => Some(mf -> StructField(s"MAX($primaryKeyField)", DataTypes.TimestampType))
      case _ => None
    }.toList
  }

  /**
   * Looks at an expression, and if it's a Min(FieldReference(`field`)) where `field` is single-valued, returns `field`.
   */
  private object MinField {
    def unapply(expr: Expression): Option[String] = {
      expr match {
        case min: Min =>
          min.children() match {
            case Array(FieldReference(Seq(fieldName))) => Some(fieldName)
            case _ => None
          }
        case _ => None
      }
    }
  }

  /**
   * Looks at an expression, and if it's a Max(FieldReference(`field`)) where `field` is single-valued, returns `field`.
   */
  private object MaxField {
    def unapply(expr: Expression): Option[String] = {
      expr match {
        case max: Max =>
          max.children() match {
            case Array(FieldReference(Seq(fieldName))) => Some(fieldName)
            case _ => None
          }
        case _ => None
      }
    }
  }
}
