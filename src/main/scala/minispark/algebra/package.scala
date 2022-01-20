package com.github
package minispark

import org.apache.spark.sql.Dataset

import scala.language.implicitConversions

/** Contains ETL types and operations which constitute the complete algebra. */
package object algebra {
  /**
   * Implicitly converts Function into Transform.
   *
   * @param f Function to convert.
   * @tparam T Type of input data.
   * @tparam U Type of output data.
   * @return Returns a Transform instance.
   */
  implicit def functionToTransform[T, U](f: Function[T, U]): Transform[T, U] = (d: Dataset[T]) => f(d)
}
