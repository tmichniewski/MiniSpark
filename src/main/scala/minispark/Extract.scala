package com.github
package minispark

import org.apache.spark.sql.Dataset

/**
 * Represents a parameterless function which produces a Dataset.
 * It is the first part of ETL - the extract phase.
 *
 * @tparam T Type of input data.
 */
trait Extract[T] extends (() => Dataset[T]) {
  /**
   * Composes this extract instance with another one.
   *
   * @param e Extract instance to compose with.
   * @tparam U Type of another extract instance.
   * @return Return pair of extracts.
   */
  def +[U](e: Extract[U]): ExtractPair[T, U] = ExtractPair[T, U](this, e) // E + E => E2

  /**
   * Composes this extract instance with another transform instance.
   *
   * @param t Transform instance to compose with.
   * @tparam U Type of output data.
   * @return Returns composed extract instance.
   */
  def +[U](t: Transform[T, U]): Extract[U] = () => t(apply()) // E + T => E

  /**
   * Composes this extract instance with another load instance.
   *
   * @param l Load instance to compose with.
   * @return Returns composed ETL instance.
   */
  def +(l: Load[T]): ETL = () => l(apply()) // E + L => ETL

  /**
   * Caches the given Dataset.
   *
   * @return Returns split version of the given instance.
   */
  def split: Split[T] = Split(this) // E => cached E
}
