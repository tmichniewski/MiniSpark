package com.github
package minispark

import org.apache.spark.sql.Dataset

/**
 * Represents a parameterless function which produces a Dataset.
 * It is the first part of ETL - the extract phase.
 *
 * @tparam T Type of output data.
 */
trait Extract[T] extends (() => Dataset[T]) {
  /**
   * Composes this extract with another one.
   *
   * @param e Extract to compose with.
   * @tparam U Type of another extract.
   * @return Return pair of extracts.
   */
  def +[U](e: Extract[U]): ExtractPair[T, U] = ExtractPair[T, U](this, e) // E + E => E2

  /**
   * Composes this extract with another transform.
   *
   * @param t Transform to compose with.
   * @tparam U Type of output data.
   * @return Returns composed extract.
   */
  def +[U](t: Transform[T, U]): Extract[U] = () => t(apply()) // E + T => E

  /**
   * Composes this extract with load.
   *
   * @param l Load to compose with.
   * @return Returns composed ETL.
   */
  def +(l: Load[T]): ETL = () => l(apply()) // E + L => ETL

  /**
   * Caches the given Dataset.
   *
   * @return Returns split version of the given extract.
   */
  def split: Extract[T] = { // E => cached E
    lazy val d: Dataset[T] = apply().cache()
    () => d
  }
}
