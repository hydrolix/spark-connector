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

import io.hydrolix.spark.model.{HdxDbPartition, JSON}

import org.apache.spark.sql.HdxPushdown
import org.apache.spark.sql.HdxPushdown.{Comparison, GetField, Literal}
import org.apache.spark.sql.connector.expressions.filter.{And, Predicate}
import org.junit.Assert.assertTrue
import org.junit.{Ignore, Test}

import java.io.FileInputStream
import java.time.Instant
import scala.collection.JavaConverters._

class PartitionPruningTest {
  private val low = Instant.parse("2023-05-01T12:00:00.000Z")
  private val high = Instant.parse("2023-05-01T12:01:00.000Z")
  private val eq = Comparison(GetField("timestamp"), "=", Literal(low))
  private val ne = Comparison(GetField("timestamp"), "<>", Literal(low))
  private val lt = Comparison(GetField("timestamp"), "<", Literal(high))
  private val le = Comparison(GetField("timestamp"), "<=", Literal(high))
  private val gt = Comparison(GetField("timestamp"), ">", Literal(low))
  private val ge = Comparison(GetField("timestamp"), ">=", Literal(low))
  private val between = new And(gt, lt)

  @Ignore("relies on data outside the source tree") // TODO fix this
  @Test
  def `do stuff`(): Unit = {
    val it = JSON.objectMapper.readerFor[HdxDbPartition].readValues[HdxDbPartition](new FileInputStream("parts.json"))
    val parts = it.asScala.toList
    println(parts.size)

    val toBeScanned = parts.filterNot { p =>
      HdxPushdown.prunePartition("timestamp", None, between, p.minTimestamp, p.maxTimestamp, "42bc986dc5eec4d3")
    }

    assertTrue(s"At least one partition was supposed to be pruned (${toBeScanned.size} >= ${parts.size})", toBeScanned.size < parts.size)

    val pruned = HdxPushdown.prunePartition(
      "timestamp",
      None,
      eq,
      Instant.EPOCH,
      Instant.EPOCH,
      "42bc986dc5eec4d3"
    )

    assertTrue("Exact match out of range", pruned)
  }
}
