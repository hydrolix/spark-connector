package io.hydrolix.spark.connector

import com.brein.time.timeintervals.collections.SetIntervalCollection
import com.brein.time.timeintervals.indexes.IntervalTreeBuilder
import com.brein.time.timeintervals.indexes.IntervalTreeBuilder.IntervalType
import com.brein.time.timeintervals.intervals.{IdInterval, LongInterval}

import scala.collection.JavaConverters._
import java.lang.{Long => jLong}

object IntervalPlayground {
  private type IDI = IdInterval[String, jLong]

  private object IDI {
    def apply(s: String, lo: Long, hi: Long, openStart: Boolean = false, openEnd: Boolean = false): IDI = {
      new IdInterval[String, jLong](s, new LongInterval(lo, hi, openStart, openEnd))
    }
  }

  def main(args: Array[String]): Unit = {
    val tree = IntervalTreeBuilder.newBuilder()
      .usePredefinedType(IntervalType.TIMESTAMP)
      .collectIntervals(_ => new SetIntervalCollection())
      .build()

    tree.add(IDI("one to one", 1, 1))
    tree.add(IDI("one to three", 1, 3))
    tree.add(IDI("two to five", 2, 5))
    tree.add(IDI("five to seven", 5, 7))

    println(tree)

    val ids = tree.overlap(new LongInterval(2, 4)).asScala.map(_.asInstanceOf[IDI].getId)

    println(ids)
  }
}
