package com.github
package minispark

/** Contains typical loads. */
object Loads {
  /**
   * Produces load.
   *
   * @param filename Filename to save.
   * @tparam T Type of output data.
   * @return Load.
   */
  def loadParquet[T](filename: String): Load[T] = _.write.parquet(filename)
}
