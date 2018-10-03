package com.datastax.spark.connector.rdd

import com.datastax.spark.connector.rdd.partitioner.CqlTokenRange

case class TokenRangeIterator[T](iterator: Iterator[T],
                                ranges: Iterable[CqlTokenRange[_, _]]) extends Iterator[T] {
  override def hasNext: Boolean = iterator.hasNext

  override def next(): T = iterator.next()
}
