package com.github
package minispark

import org.apache.spark.sql.Dataset

/**
 * Represents a function of Dataset with no return value.
 * It is the third part of ETL - the load phase.
 *
 * @tparam T Type of input data.
 */
trait Load[T] extends (Dataset[T] => Unit)
