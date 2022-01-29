package com.github
package minispark

import org.apache.spark.sql.Dataset

/**
 * Represents a function from one Dataset into another.
 * This is a type alias for standard Scala Function1 trait and its composition andThen method.
 * Such functions are present in any Spark notebook, but here we define a type for them.
 * It is the second part of ETL - the transform phase.
 *
 * @tparam T Type of input data.
 * @tparam U Type of output data.
 */
trait Transform[T, U] extends (Dataset[T] => Dataset[U]) {
  /**
   * Composes this transform with the other one.
   * An alias to andThen to andThen method.
   *
   * @param t Transform to apply next.
   * @tparam V Resulting type of data.
   * @return Returns composed transform.
   */
  def +[V](t: Transform[U, V]): Transform[T, V] = (d: Dataset[T]) => d.transform(this andThen t) // T + T => T
  // <=> (this andThen t)(d) or andThen(t)(d) or t(apply(d))

  /**
   * Composes this transform with load.
   * An alias to andThen to andThen method.
   *
   * @param l Load to use and produce final results.
   * @return Returns composed load.
   */
  def +(l: Load[U]): Load[T] = (d: Dataset[T]) => (this andThen l)(d) // T + L => L
  // <=> (this andThen l)(d) or andThen(l)(d) or l(apply(d))
}
