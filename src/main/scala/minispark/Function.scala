package com.github
package minispark

import org.apache.spark.sql.{Dataset, Row}

/**
 * The function represents any kind of transformation of one Dataset into another.
 * Such functions are present in any Spark notebook,
 * but here we define a type for such functions.
 * We call this type the Function.
 *
 * @tparam T Type of input data.
 * @tparam U Type of output data.
 */
trait Function[T, U] extends (Dataset[T] => Dataset[U]) with F1[T, U] with Serializable {
  /**
   * Sequential application of two functions. An alias to andThen method.
   *
   * @param f The function which will be applied next.
   * @tparam V Type of resulting data.
   * @return Returns the composed function.
   */
  def +[V](f: Function[U, V]): Function[T, V] = (d: Dataset[T]) => d.transform(this andThen f)
  // <=> (this andThen f)(d) or andThen(f)(d) or f(apply(d))
  // Please note that there is also a similar method on superclass F1 which returns F1
}
