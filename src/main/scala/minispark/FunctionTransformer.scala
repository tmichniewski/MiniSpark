package com.github
package minispark

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row}

/**
 * Spark ML transformer which uses the Function.
 * This gives plenty of possibilities to create new ML Transformers.
 *
 * @param func Function to perform the transformation.
 * @param validator Schema validator.
 * @param uid Transformer id.
 */
class FunctionTransformer(
  func: Function[Row, Row],
  validator: StructType => StructType,
  override val uid: String = Identifiable.randomUID("MiniSpark")
) extends Transformer with DefaultParamsWritable {
  /**
   * Creates a copy of this instance with the same UID and some extra params.
   *
   * @param extra Extra parameters.
   * @return Returns copy of this transformer.
   */
  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  /**
   * Check schema validity and produce the output schema from the input schema.
   * Raise an exception if something is invalid.
   *
   * @param schema Input schema.
   * @return Return output schema. Raises an exception if schemas are inappropriate.
   */
  override def transformSchema(schema: StructType): StructType = validator(schema)

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
}

/** Companion object. */
object FunctionTransformer extends DefaultParamsReadable[FunctionTransformer] {
  /**
   * Reads an ML instance from the input path, a shortcut of read.load(path).
   * Note: Implementing classes should override this to be Java-friendly.
   *
   * @param path Path to transformer to be loaded.
   * @return Returns the loaded transformer.
   */
  override def load(path: String): FunctionTransformer = super.load(path)

  /**
   * Constructs the instance.
   *
   * @param func Function to perform the transformation.
   * @param validator Schema validator.
   * @return Returns the new instance.
   */
  def apply(func: Function[Row, Row], validator: StructType => StructType): FunctionTransformer =
    new FunctionTransformer(func, validator)
}
