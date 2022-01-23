package com.github
package minispark

import org.apache.spark.sql.Dataset

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
