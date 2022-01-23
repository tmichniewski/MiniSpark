package com.github
package minispark

// $COVERAGE-OFF$
/**
 * Represent a pair of extract instances.
 *
 * @param e1 First extract instance.
 * @param e2 Second extract instance.
 * @tparam T Type of first data.
 * @tparam U Type of second data.
 */
class ExtractPair[T, U](e1: Extract[T], e2: Extract[U]) {
  /**
   * Composes this extract instance pair with the combine instance.
   *
   * @param c Combine instance to compose with.
   * @tparam V Type of output data.
   * @return Returns composed extract instance.
   */
  def +[V](c: Combine[T, U, V]): Extract[V] = () => c(e1(), e2()) // E2 + C => E
}

/** Companion object. */
object ExtractPair {
  /**
   * Constructor of the extract pair instance.
   *
   * @param e1 First extract instance.
   * @param e2 Second extract instance.
   * @tparam T Type of first data.
   * @tparam U Type of second data.
   * @return Return the extract pair instance.
   */
  def apply[T, U](e1: Extract[T], e2: Extract[U]): ExtractPair[T, U] = new ExtractPair[T, U](e1, e2)
}
// $COVERAGE-ON$
