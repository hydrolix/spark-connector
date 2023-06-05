package io.hydrolix.spark.connector

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.junit.Assert.{assertEquals, assertNotNull, fail}
import org.junit.Test

import java.io.ByteArrayInputStream
import scala.collection.mutable.ArrayBuffer

class ColumnarJsonParsingSimpleTest {
  private val simpleSchema = StructType(List(
    StructField("col1", DataTypes.IntegerType),
    StructField("col2", DataTypes.StringType)
  ))

  private val simpleBad = List(
    """{}""",                                                          // Empty object
    """[]""",                                                          // Empty array
    """"hello"""",                                                     // Bare string
    """true""",                                                        // Bare boolean
    """null""",                                                        // Bare null
    """123""",                                                         // Bare int
    """{"rowz":null,"colz":null}""",                                   // Rows and cols both misspelled
    """{"rows":null,"cols":null}""",                                   // Rows and cols are both required
    """{"rows":1,"cols":null}""",                                      // Cols is required
    """{"rows":null,"cols":{"col1":[1,2,3]}}""",                       // Rows is required
    """{"rows":3,"cols":{"col1":[]}""",                                // Not enough values: expected 3, got 0
    """{"rows":4,"cols":{"col1":null}""",                              // column array must be present
    """{"rows":5,"cols":{"col1":[1]}""",                               // Not enough values: expected 5, got 1
    """{"rows":5,"cols":{"colZZ":[1,2,3,4,5]}""",                      // No such column
    """{"rows":5,"cols":{"col1":["hello","there",null,null,null"]}""", // Wrong type
  )

  private val simpleGood = List(
    """{"rows":2,"cols":{}}""",
    """{"rows":3,"cols":{"col1":[1,2,3]}}""",
    """{"rows":3,"cols":{"col1":[1,null,3]}}""",
    """{"rows":3,"cols":{"col1":[null,null,null]}}""",
    """{"rows":3,"cols":{"col1":[1,null,3],"col2":["hello","there",null]}}""",
  )

  @Test
  def badLinesAllFailIndividually(): Unit = {
    for (line <- simpleBad) {
      try {
        HdxReaderColumnarJson.batches(
          simpleSchema,
          new ByteArrayInputStream(line.getBytes("UTF-8")),
          { batch =>
            fail(s"Expected no batches from $line but got $batch")
          },
          ()
        )
        fail(s"Expected failure for $line")
      } catch {
        case e: Throwable =>
          println(s"Expected error in $line: ${e.toString}")
      }
    }
  }

  @Test
  def goodLinesAllSucceedIndividually(): Unit = {
    for (line <- simpleGood) {
      var got: ColumnarBatch = null
      HdxReaderColumnarJson.batches(
        simpleSchema,
        new ByteArrayInputStream(line.getBytes("UTF-8")),
        { got = _ },
        { () } // OK!
      )

      assertNotNull(s"Expected $line to parse", got)
    }
  }

  @Test
  def goodLinesMultiline(): Unit = {
    val lines = simpleGood.mkString("\n")
    val got = ArrayBuffer[ColumnarBatch]()
    HdxReaderColumnarJson.batches(
      simpleSchema,
      new ByteArrayInputStream(lines.getBytes("UTF-8")),
      { got += _ },
      { () }
    )

    assertEquals(got.size, simpleGood.size)
  }

  @Test
  def emptyStream(): Unit = {
    val empties = List(
      "",
      "  ",
      "\n",
      "\r\n",
      " \n \n "
    )

    for (empty <- empties) {
      HdxReaderColumnarJson.batches(
        simpleSchema,
        new ByteArrayInputStream(empty.getBytes("UTF-8")),
        { _ => fail("Got unexpected batch") },
        { () }
      )
    }
  }
}
