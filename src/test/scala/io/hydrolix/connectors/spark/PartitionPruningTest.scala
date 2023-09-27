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

import org.junit.Assert.assertTrue
import org.junit.{Ignore, Test}

import java.io.FileInputStream
import java.time.Instant
import scala.collection.JavaConverters._

import io.hydrolix.connectors
import io.hydrolix.connectors.expr
import io.hydrolix.connectors.expr.{ComparisonOp, TimestampLiteral}
import io.hydrolix.connectors.types.TimestampType

// TODO move this to connectors-core, it can live there now
class PartitionPruningTest {
  private val low = Instant.parse("2023-05-01T12:00:00.000Z")
  private val high = Instant.parse("2023-05-01T12:01:00.000Z")
  private val eq = expr.Comparison(expr.GetField("timestamp", TimestampType(3)), ComparisonOp.EQ, TimestampLiteral(low))
  private val ne = expr.Comparison(expr.GetField("timestamp", TimestampType(3)), ComparisonOp.NE, TimestampLiteral(low))
  private val lt = expr.Comparison(expr.GetField("timestamp", TimestampType(3)), ComparisonOp.LT, TimestampLiteral(high))
  private val le = expr.Comparison(expr.GetField("timestamp", TimestampType(3)), ComparisonOp.LE, TimestampLiteral(high))
  private val gt = expr.Comparison(expr.GetField("timestamp", TimestampType(3)), ComparisonOp.GT, TimestampLiteral(low))
  private val ge = expr.Comparison(expr.GetField("timestamp", TimestampType(3)), ComparisonOp.GE, TimestampLiteral(low))
  private val between = expr.And(List(gt, lt))

  @Ignore("relies on data outside the source tree") // TODO fix this
  @Test
  def `do stuff`(): Unit = {
    val it = connectors.JSON.objectMapper.readerFor[connectors.HdxDbPartition].readValues[connectors.HdxDbPartition](new FileInputStream("parts.json"))
    val parts = it.asScala.toList
    println(parts.size)

    val toBeScanned = parts.filterNot { p =>
      connectors.HdxPushdown.prunePartition("timestamp", None, between, p.minTimestamp, p.maxTimestamp, "42bc986dc5eec4d3")
    }

    assertTrue(s"At least one partition was supposed to be pruned (${toBeScanned.size} >= ${parts.size})", toBeScanned.size < parts.size)

    val pruned = connectors.HdxPushdown.prunePartition(
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
