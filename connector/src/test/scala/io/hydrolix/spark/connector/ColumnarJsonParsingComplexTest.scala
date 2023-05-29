package io.hydrolix.spark.connector

import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.execution.vectorized.{OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String
import org.junit.Assert.{assertArrayEquals, assertEquals, assertNotNull, fail}
import org.junit.Test

import java.io.ByteArrayInputStream
import scala.collection.mutable.ArrayBuffer
import scala.reflect.{ClassTag, classTag}

class ColumnarJsonParsingComplexTest {
  private val complexSchema = StructType(List(
    StructField("int", DataTypes.IntegerType),
    StructField("str", DataTypes.StringType),
    StructField("arr1", DataTypes.createArrayType(DataTypes.IntegerType)),
    StructField("arr2", DataTypes.createArrayType(DataTypes.StringType)),
    StructField("map1", DataTypes.createMapType(DataTypes.StringType, DataTypes.LongType)),
    StructField("map2", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType)),
    StructField("map3", DataTypes.createMapType(DataTypes.StringType, DataTypes.createArrayType(DataTypes.IntegerType)))
  ))

  private val complexBad = List(
    """{
      |  "rows":2,
      |  "cols":{
      |    "int":[],
      |    "str":[],
      |    "arr1":[],
      |    "arr2":[],
      |    "map1":[],
      |    "map2":[],
      |    "map3":[]
      |  }
      |}""".stripMargin, // not enough values: expected 2, got 0

    """{
      |  "rows":2,
      |  "cols":{
      |    "int":[1],
      |    "str":["one"],
      |    "arr1":[[1]],
      |    "arr2":[["one"]],
      |    "map1":[{"foo":123}],
      |    "map2":[{"foo":"bar"}],
      |    "map3":[{"foo":["bar"]}]
      |  }
      |}""".stripMargin, // not enough values: expected 2, got 1
  )

  private val complexGood = List(
    """{
      |  "rows":2,
      |  "cols":{
      |    "int":[1,2],
      |    "str":["one","two"],
      |    "arr1":[[1,2],[3,4]],
      |    "arr2":[["one","two"],["three","four"]],
      |    "map1":[{"k11l":11,"k12l":12},{"k21l":21,"k22l":22,"k23l":23}],
      |    "map2":[{"k11s":"v11","k12s":"v12"},{"k21s":"v21","k22s":"v22","k23s":"v23"}],
      |    "map3":[{"k11a":[110,111],"k12a":[120,121]]},{"k21a":[210,211]],"k22a":[220,221]],"k23a":[230,231]]}]
      |  }
      |}""".stripMargin,
  )

  @Test
  def badLinesAllFailIndividually(): Unit = {
    for (line <- complexBad) {
      try {
        HdxReaderColumnarJson.batches(
          complexSchema,
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
    for (line <- complexGood) {
      var got: ColumnarBatch = null
      HdxReaderColumnarJson.batches(
        complexSchema,
        new ByteArrayInputStream(line.getBytes("UTF-8")),
        { got = _ },
        { () } // OK!
      )

      assertNotNull(s"Expected $line to parse", got)
    }
  }

  @Test
  def goodLinesMultiline(): Unit = {
    val lines = complexGood.mkString("\n")
    val got = ArrayBuffer[ColumnarBatch]()
    HdxReaderColumnarJson.batches(
      complexSchema,
      new ByteArrayInputStream(lines.getBytes("UTF-8")),
      { got += _ },
      { () }
    )

    assertEquals(got.size, complexGood.size)

    assertArrayEquals(
      Array(1,2),
      got(0).column(0).getInts(0, 2)
    )

    val rows = got(0).rowIterator()
    checkRow(
      rows.next(),
      1,
      "one",
      Array(1, 2),
      Array("one", "two"),
      Map("k11l" -> 11L, "k12l" -> 12L),
      Map("k11s" -> "v11", "k12s" -> "v12"),
      Map("k11a" -> List(110,111), "k12a" -> List(120,121))
    )
    checkRow(
      rows.next(),
      2,
      "two",
      Array(3, 4),
      Array("three", "four"),
      Map("k21l" -> 21L, "k22l" -> 22L, "k23l" -> 23L),
      Map("k21s" -> "v21", "k22s" -> "v22", "k23s" -> "v23"),
      Map("k21a" -> List(210, 211), "k22a" -> List(220, 221), "k23a" -> List(230, 231)),
    )
  }

  private def checkRow(row: InternalRow,
                       int: Int,
                       str: String,
                      arr1: Array[Int],
                      arr2: Array[String],
                      map1: Map[String, Long],
                      map2: Map[String, String],
                      map3: Map[String, List[Int]])
                          : Unit =
  {
    assertEquals(int, row.get(0, complexSchema.fields(0).dataType))
    assertEquals(UTF8String.fromString(str), row.get(1, complexSchema.fields(1).dataType))
    assertArrayEquals(arr1, row.getArray(2).toIntArray())
    assertArrayEquals(arr2.map(UTF8String.fromString(_): AnyRef), row.getArray(3).toObjectArray(DataTypes.StringType))
    assertEquals(map1, CatalystTypeConverters.convertToScala(row.getMap(4), complexSchema.fields(4).dataType))
    assertEquals(map2, CatalystTypeConverters.convertToScala(row.getMap(5), complexSchema.fields(5).dataType))
    assertEquals(map3, CatalystTypeConverters.convertToScala(row.getMap(6), complexSchema.fields(6).dataType))
  }

  @Test
  def tryArraysAndMaps(): Unit = {
    /*
    StructField("int", DataTypes.IntegerType),
    StructField("str", DataTypes.StringType),
    StructField("arr1", DataTypes.createArrayType(DataTypes.IntegerType)),
    StructField("arr2", DataTypes.createArrayType(DataTypes.StringType)),
    StructField("map", DataTypes.createMapType(DataTypes.StringType, DataTypes.LongType))
    */

    val cols = OnHeapColumnVector.allocateColumns(5, complexSchema)

    val ints = Array(1,2,3,4,5)

    val strings = Array("hello 1", "hello 2", "hello 3", "hello 4", "hello 5")

    val intArrays = Array(
      Array(1),
      Array(11, 12),
      Array(21, 22, 23),
      Array(31, 32, 33, 34),
      Array(41, 42, 43, 44, 45),
    )

    val stringArrays = Array(
      Array("1"),
      Array("11", "12"),
      Array("21", "22", "23"),
      Array("31", "32", "33", "34"),
      Array("41", "42", "43", "44", "45"),
    )

    val maps1 = Array(
      Map("k1" -> 1L),
      Map("k11" -> 11L, "k12" -> 12L),
      Map("k21" -> 21L, "k22" -> 22L, "k23" -> 23L),
      Map("k31" -> 31L, "k32" -> 32L, "k33" -> 33L, "k34" -> 34L),
      Map("k41" -> 41L, "k42" -> 42L, "k43" -> 43L, "k44" -> 44L, "k45" -> 45L),
    )

    val maps2 = Array(
      Map("k1" -> "v1"),
      Map("k11" -> "v11", "k12" -> "v12"),
      Map("k21" -> "v21", "k22" -> "v22", "k23" -> "v23"),
      Map("k31" -> "v31", "k32" -> "v32", "k33" -> "v33", "k34" -> "v34"),
      Map("k41" -> "v41", "k42" -> "v42", "k43" -> "v43", "k44" -> "v44", "k45" -> "v45"),
    )

    for ((int, i) <- ints.zipWithIndex) {
      cols(0).putInt(i, int)
    }

    for ((string, i) <- strings.zipWithIndex) {
      cols(1).putByteArray(i, string.getBytes("UTF-8"))
    }

    for ((arr, i) <- intArrays.zipWithIndex) {
      appendArray(cols(2), i, arr)
    }

    for ((arr, i) <- stringArrays.zipWithIndex) {
      appendArray(cols(3), i, arr)
    }

    for ((map, i) <- maps1.zipWithIndex) {
      appendMap(cols(4), i, map)
    }

    for ((map, i) <- maps2.zipWithIndex) {
      appendMap(cols(5), i, map)
    }

    assertEquals(
      ints.toList,
      cols(0).getInts(0, ints.length).toList
    )

    assertEquals(
      strings.toList,
      strings.indices.map { i =>
        new String(cols(1).getArray(i).toByteArray, "UTF-8")
      }.toList
    )

    assertEquals(
      intArrays.toList.map(_.toList),
      intArrays.indices.map { i =>
        cols(2).getArray(i).toIntArray.toList
      }.toList
    )

    assertEquals(
      stringArrays.map(_.toList).toList,
      stringArrays.indices.map { i =>
        val arr = cols(3).getArray(i)
        arr.array().toList.asInstanceOf[List[UTF8String]].map(_.toString)
      }.toList
    )

    assertEquals(
      maps1.toList,
      maps1.indices.map { i =>
        val m = cols(4).getMap(i)
        val keys = m.keyArray().array().toList.asInstanceOf[List[UTF8String]].map(_.toString)
        val values = m.valueArray().array().toList

        keys.zip(values).toMap
      }.toList
    )

    assertEquals(
      maps2.toList,
      maps2.indices.map { i =>
        val m = cols(5).getMap(i)
        val keys = m.keyArray().array().toList.asInstanceOf[List[UTF8String]].map(_.toString)
        val values = m.valueArray().array().toList.asInstanceOf[List[UTF8String]].map(_.toString)

        keys.zip(values).toMap
      }.toList
    )
  }

  private def appendArray[T : ClassTag](col: WritableColumnVector, rowId: Int, values: Array[_ <: T]): Unit = {
    val child = col.getChild(0)

    classTag[T] match {
      case ClassTag.Int =>
        val ints = values.asInstanceOf[Array[Int]]
        val offset = child.appendInts(ints.length, ints, 0)
        col.putArray(rowId, offset, ints.length)

      case ct if ct.runtimeClass == classOf[String] =>
        val strings = values.asInstanceOf[Array[String]]

        var firstOffset = -1
        for (s <- strings) {
          val bytes = s.getBytes("UTF-8")
          val offset = child.appendByteArray(bytes, 0, bytes.length)
          if (firstOffset == -1) firstOffset = offset
        }
        col.putArray(rowId, firstOffset, strings.length)
    }
  }

  private def appendMap[V : ClassTag](col: WritableColumnVector, rowId: Int, values: Map[String, V]): Unit = {
    val kcol = col.getChild(0)
    val vcol = col.getChild(1)

    classTag[V] match {
      case ClassTag.Long =>
        var firstOffset = -1
        for ((k, v) <- values.asInstanceOf[Map[String, Long]]) {
          val bytes = k.getBytes("UTF-8")
          kcol.appendByteArray(bytes, 0, bytes.length) // TODO ignoring the key offset here...
          val voffset = vcol.appendLong(v)
          if (firstOffset == -1) firstOffset = voffset
        }
        col.putArray(rowId, firstOffset, values.size)
      case ct if ct.runtimeClass == classOf[String] =>
        var firstOffset = -1
        for ((k, v) <- values.asInstanceOf[Map[String, String]]) {
          val keyBytes = k.getBytes("UTF-8")
          val valueBytes = v.getBytes("UTF-8")
          kcol.appendByteArray(keyBytes, 0, keyBytes.length) // TODO ignoring the key offset here...
          val voffset = vcol.appendByteArray(valueBytes, 0, valueBytes.length)
          if (firstOffset == -1) firstOffset = voffset
        }
        col.putArray(rowId, firstOffset, values.size)
    }
  }
}
