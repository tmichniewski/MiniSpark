package com.github
package minispark

import org.apache.spark.sql.Dataset

/**
 * Represents caching extract.
 *
 * @param e Extract to cache.
 * @tparam T Type of input data.
 */
class Split[T](e: Extract[T]) extends Extract[T] {
  /** Lazy variable to store the data. */
  lazy val d: Dataset[T] = e().cache()

  /**
   * Produces cached Dataset.
   *
   * @return Returns cached Dataset.
   */
  override def apply(): Dataset[T] = d
}

/** Companion object. */
object Split {
  /**
   * Constructor of split.
   *
   * @param e Extract to cache.
   * @tparam T Typeof input data.
   * @return Return split.
   */
  def apply[T](e: Extract[T]): Split[T] = new Split[T](e)
}
