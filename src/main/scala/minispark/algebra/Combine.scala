package com.github
package minispark.algebra

import org.apache.spark.sql.Dataset

/**
 * Represents a function which combines two Datasets and produces another one.
 *
 * @tparam T Type of first input.
 * @tparam U Type of second input.
 * @tparam V Type of output.
 */
trait Combine[T, U, V] extends ((Dataset[T], Dataset[U]) => Dataset[V])
