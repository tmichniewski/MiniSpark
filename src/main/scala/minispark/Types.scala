package com.github
package minispark

import org.apache.spark.sql.Dataset

// Below types constitute Scala story about function composition.
// As we may see, always holds relation X + F1 = X, where X stands for F0, F1, F2 or FN.

/**
 * Zero parameter function. Producer of data.
 *
 * @tparam T Type of output data.
 */
trait F0[T] extends (() => Dataset[T]) {
  /**
   * Composition of F0 and F1.
   *
   * @param f1tu F1 function to apply next.
   * @tparam U Type of output data.
   * @return Returns composed function.
   */
  def +[U](f1tu: F1[T, U]): F0[U] = () => f1tu(apply()) // F0 + F1 = F0
}

/**
 * One parameter function. Transformer of data.
 *
 * @tparam T Type of input data.
 * @tparam U Type of output data.
 */
trait F1[T, U] extends (Dataset[T] => Dataset[U]) {
  /**
   * Composition of F1 and F1.
   *
   * @param f1uv F1 function to apply next.
   * @tparam V Type of output data.
   * @return Returns composed function.
   */
  def +[V](f1uv: F1[U, V]): F1[T, V] = (d: Dataset[T]) => f1uv(apply(d)) // F1 + F1 = F1
}

/**
 * Two parameter function. Combiner of data.
 *
 * @tparam T Type of input data.
 * @tparam U Type of input data.
 * @tparam V Type of output data.
 */
trait F2[T, U, V] extends ((Dataset[T], Dataset[U]) => Dataset[V]) {
  /**
   * Composition of F2 and F1.
   *
   * @param f1vw F1 function to apply next.
   * @tparam W Type of output data.
   * @return Returns composed function.
   */
  def +[W](f1vw: F1[V, W]): F2[T, U, W] = (d1: Dataset[T], d2: Dataset[U]) => f1vw(apply(d1, d2)) // F2 + F1 = F2
}

/**
 * N parameter function. Reducer of data. For example using union.
 *
 * @tparam T Type of input data.
 * @tparam U Type of output data.
 */
trait FN[T, U] extends (Seq[Dataset[T]] => Dataset[U]) {
  /**
   * Composition of FN and F1.
   *
   * @param f1uw F1 function to apply next.
   * @tparam V Type of output data.
   * @return Returns composed function.
   */
  def +[V](f1uw: F1[U, V]): FN[T, V] = (ds: Seq[Dataset[T]]) => f1uw(apply(ds)) // FN + F1 = FN
}
