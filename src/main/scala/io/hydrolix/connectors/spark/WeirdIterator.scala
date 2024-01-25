package io.hydrolix.connectors.spark

import java.{util => ju}

/** See [[org.apache.spark.sql.connector.read.PartitionReader]] for `next()`/`get()` contract */
//noinspection ConvertNullInitializerToUnderscore
final class WeirdIterator[T >: Null <: AnyRef](iterator: ju.Iterator[T], poisonPill: T) {
  @volatile private var value: T = null

  def next(): Boolean = {
    if (iterator.hasNext) {
      val got = iterator.next()
      if (got eq poisonPill) {
        false
      } else {
        value = got
        true
      }
    } else {
      false
    }
  }

  def get(): T = {
    if (value == null) throw new NoSuchElementException("get() on empty WeirdIterator")
    value
  }
}
