package com.github
package minispark.algebra

import org.apache.spark.sql.Dataset

/**
 * Represents a function a Dataset with no return value.
 * It is the third part of ETL - the load phase.
 *
 * @tparam T Type of input data.
 */
trait Load[T] extends (Dataset[T] => Unit)
