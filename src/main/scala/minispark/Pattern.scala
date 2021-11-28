package com.github
package minispark

import minispark.Functions.map

import org.apache.spark.sql.Encoder

/**
 * Generic pattern. It uses the concept of getter which converts T into Input.
 * So, in general, we replace inheritance relation of T being subtype of Input
 * into necessity to provide more general higher order function called getter.
 * This way we no longer have to extend input type T with the type of Input.
 *
 * It is recommend to define Input and Output as a concrete types or containers for data.
 * We no longer need additional types (traits) for them, as it is the getter to provide Input value.
 * This way we do not need to extend T type with type of Input, as this relation is hidden inside the getter.
 * There is also no need to extend U with type of Output, as this relation is hidden inside the constructor.
 */
trait Pattern {
  /** Type of input data container. */
  type Input
  /** Type of output data container. */
  type Output
  /** Type of parameters container. */
  type Params

  /**
   * Higher order method which returns the mapping function to convert input type into output type.
   *
   * @param params Parameters to construct the resulting mapping function.
   * @return Returns the mapping function to convert input into output.
   */
  def build(params: Params): Input => Output

  /**
   * Factory method which produces the map function.
   *
   * @param params Parameters to construct the resulting mapping function.
   * @param getter Function to convert T into Input.
   * @param constructor Function to convert T and Output into U.
   * @tparam T Type of input data.
   * @tparam U Type of output data.
   * @return Returns the map function.
   */
  def apply[T, U: Encoder](params: Params, getter: T => Input, constructor: (T, Output) => U): Function[T, U] = {
    map[T, U] {
      val mapper: Input => Output = build(params)
      (r: T) => constructor(r, mapper(getter(r)))
    }
  }
}
