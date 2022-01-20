package com.github
package minispark.algebra

import org.apache.spark.sql.Dataset

/**
 * Represents caches extract data.
 *
 * @param e Extract instance to cache.
 * @tparam T Type of input data.
 */
class Split[T](e: Extract[T]) extends Extract[T] {
  /** Lazy variable to store the data. */
  lazy val d: Dataset[T] = e().cache()

  /**
   * Produces cached dataset.
   *
   * @return Returns cached dataset.
   */
  override def apply(): Dataset[T] = d
}

/** Companion object. */
object Split {
  /**
   * Constructor of split instance.
   *
   * @param e Extract instance to cache.
   * @tparam T Typeof input data.
   * @return Return split instance.
   */
  def apply[T](e: Extract[T]): Split[T] = new Split[T](e)
}
