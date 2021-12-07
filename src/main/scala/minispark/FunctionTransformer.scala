package com.github
package minispark

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

/**
 * Spark ML transformer which uses the Function.
 * This gives plenty of possibilities to create new ML Transformers.
 *
 * @param uid Transformer id.
 */
class FunctionTransformer(override val uid: String) extends Transformer with DefaultParamsWritable {
  /** Additional, default constructor. */
  def this() = this(Identifiable.randomUID("FunctionTransformer2"))

  /** Schema parameter. The function is provided in Seq[(column, type)] form. */
  final val schema: Param[Seq[(String, DataType)]] =
    new Param[Seq[(String, DataType)]](this, "schema", "Schema")

  /**
   * Setter for the parameter.
   *
   * @param value New value of the parameter.
   * @return Returns this transformer.
   */
  def setSchema(value: Seq[(String, DataType)]): this.type = set(schema, value)

  /**
   * Getter for the parameter.
   *
   * @return Returns value of the parameter.
   */
  def getSchema: Seq[(String, DataType)] = $(schema)

  /** Function parameter. The function is provided in lambda form. */
  final val function: Param[Function[Row, Row]] =
    new Param[Function[Row, Row]](this, "function", "Function")

  /**
   * Setter for the parameter.
   *
   * @param value New value of the parameter.
   * @return Returns this transformer.
   */
  def setFunction(value: Function[Row, Row]): this.type = set(function, value)

  /**
   * Getter for the parameter.
   *
   * @return Returns value of the parameter.
   */
  def getFunction: Function[Row, Row] = $(function)

  /**
   * Check schema validity and produce the output schema from the input schema.
   * Raise an exception if something is invalid.
   *
   * @param schema Input schema.
   * @return Return output schema. Raises an exception if input schema is inappropriate.
   */
  override def transformSchema(schema: StructType): StructType = {
    val valid: Boolean = getSchema.forall { (p: (String, DataType)) =>
      val index: Int = schema.fields.indexWhere { (sf: StructField) =>
        sf.name == p._1 && sf.dataType == p._2
      }
      if (index == -1)
        throw new RuntimeException(s"Incorrect input schema, no column: $p._1 of type $p._2.toString()")
      index >= 0
    }
    require(valid)

    schema
  }

  /**
   * Transforms the input dataset.
   *
   * @param dataset Dataset to be transformed.
   * @return Returns transformed dataset.
   */
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema)
    getFunction(dataset.toDF())
  }

  /**
   * Creates a copy of this instance with the same UID and some extra params.
   *
   * @param extra Extra parameters.
   * @return Returns copy of this transformer.
   */
  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}

/** Companion object with load and applied methods. */
object FunctionTransformer extends DefaultParamsReadable[FunctionTransformer] {
  /**
   * Loads the model.
   * @param path Path to saved model.
   * @return Returns loaded model.
   */
  override def load(path: String): FunctionTransformer = super.load(path)

  /**
   * Constructs the instance.
   *
   * @return Returns the new instance.
   */
  def apply(): FunctionTransformer = new FunctionTransformer()
}
