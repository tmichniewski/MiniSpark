package com.github
package minispark.algebra

import org.apache.spark.sql.Dataset

/**
 * Represents a function from one Dataset into another.
 * It is the second part of ETL - the transform phase.
 *
 * @tparam T Type of input data.
 * @tparam U Type of output data.
 */
trait Transform[T, U] extends (Dataset[T] => Dataset[U]) {
  /**
   * Composes this transform instance with the other one.
   *
   * @param t Transform instance to apply next.
   * @tparam V Resulting type of data.
   * @return Returns composed transform instance.
   */
  def +[V](t: Transform[U, V]): Transform[T, V] = (d: Dataset[T]) => d.transform(this andThen t) // T + T => T

  /**
   * Composes this transform instance with the load instance.
   * @param l Load instance to use and produce final results.
   * @return Returns composed load instance.
   */
  def +(l: Load[U]): Load[T] = (d: Dataset[T]) => (this andThen l)(d) // T + L => L
}
