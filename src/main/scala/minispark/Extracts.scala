package com.github
package minispark

import minispark.Spark.spark

import org.apache.spark.sql.{Encoder, Row}

/** Contains typical extracts. */
object Extracts {
  /**
   * Produces extract.
   *
   * @param filename Filename to read.
   * @return Returns extract.
   */
  def extractRowParquet(filename: String): Extract[Row] = () => spark.read.parquet(filename)

  /**
   * Produces extract.
   *
   * @param filename Filename to read.
   * @tparam T Type of input data.
   * @return Returns extract.
   */
  def extractParquet[T: Encoder](filename: String): Extract[T] = () => spark.read.parquet(filename).as[T]
}
