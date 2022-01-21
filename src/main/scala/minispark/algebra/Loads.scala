package com.github
package minispark.algebra

import org.apache.spark.sql.Dataset

// $COVERAGE-OFF$
/** Contains typical loads. */
object Loads {
  /**
   * Produces load instance.
   *
   * @param filename Filename to save.
   * @tparam T Type of output data.
   * @return Returns load instance.
   */
  def loadParquet[T](filename: String): Load[T] = (d: Dataset[T]) => d.write.parquet(filename)
}
// $COVERAGE-ON$
