package com.github
package minispark

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row}

/**
 * Spark ML transformer which uses the Function.
 * This gives plenty of possibilities to create new ML Transformers.
 *
 * @param uid Transformer id.
 */
abstract class FunctionTransformer(override val uid: String)
  extends Transformer with DefaultParamsWritable {
  /** Additional, default constructor. */
  def this() = this(Identifiable.randomUID("MiniSpark_FunctionTransformer"))

  /**
   * Function to perform the transformation.
   *
   * @return Returns the function to perform the transformation.
   */
  def func: Function[Row, Row]

  /**
   * Check schema validity and produce the output schema from the input schema.
   * Raise an exception if something is invalid.
   *
   * @param schema Input schema.
   * @return Return output schema. Raises an exception if schemas are inappropriate.
   */
  override def transformSchema(schema: StructType): StructType

  /**
   * Transforms the input dataset.
   *
   * @param dataset Dataset to be transformed.
   * @return Returns transformed dataset.
   */
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema)
    func(dataset.toDF())
  }

  /**
   * Creates a copy of this instance with the same UID and some extra params.
   *
   * @param extra Extra parameters.
   * @return Returns copy of this transformer.
   */
  override def copy(extra: ParamMap): Transformer = this // defaultCopy(extra)
}
