package com.github
package minispark

// $COVERAGE-OFF$
/**
 * Represent a pair of extracts.
 *
 * @param e1 First extract.
 * @param e2 Second extract.
 * @tparam T Type of first data.
 * @tparam U Type of second data.
 */
class ExtractPair[T, U](e1: Extract[T], e2: Extract[U]) {
  /**
   * Composes extract pair with combine.
   *
   * @param c Combine to compose with.
   * @tparam V Type of output data.
   * @return Returns composed extract.
   */
  def +[V](c: Combine[T, U, V]): Extract[V] = () => c(e1(), e2()) // E2 + C => E
}

/** Companion object. */
object ExtractPair {
  /**
   * Constructor of the extract pair.
   *
   * @param e1 First extract.
   * @param e2 Second extract.
   * @tparam T Type of first data.
   * @tparam U Type of second data.
   * @return Return the extract pair.
   */
  def apply[T, U](e1: Extract[T], e2: Extract[U]): ExtractPair[T, U] = new ExtractPair[T, U](e1, e2)
}
// $COVERAGE-ON$
