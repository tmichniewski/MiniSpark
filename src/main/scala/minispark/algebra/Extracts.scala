package com.github
package minispark.algebra

import minispark.Spark.spark

import org.apache.spark.sql.{Encoder, Row}

// $COVERAGE-OFF$Left for integration tests in Databricks
/** Contains typical extracts. */
object Extracts {
  /**
   * Produces extract instance.
   *
   * @param filename Filename to read.
   * @return Returns extract instance.
   */
  def extractRowParquet(filename: String): Extract[Row] = () => spark.read.parquet(filename)

  /**
   * Produces extract instance.
   *
   * @param filename Filename to read.
   * @tparam T Type of input data.
   * @return Returns extract instance.
   */
  def extractParquet[T: Encoder](filename: String): Extract[T] = () => spark.read.parquet(filename).as[T]
}
// $COVERAGE-ON$
