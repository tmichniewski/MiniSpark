package com.github
package minispark

import org.apache.spark.ml.Transformer
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, Row}

/** Contains typical operations. */
object Functions {
  // basic

  /**
   * Filters the Dataset.
   *
   * @param condition Filter condition.
   * @tparam T Type of input and output data.
   * @return Returns the function to filter the Dataset.
   */
  def filter[T](condition: String): Function[T, T] = (d: Dataset[T]) => d filter condition

  /**
   * Filters the Dataset.
   *
   * @param condition Filter condition.
   * @tparam T Type of input and output data.
   * @return Returns the function to filter the Dataset.
   */
  def filter[T](condition: Column): Function[T, T] = (d: Dataset[T]) => d filter condition

  /**
   * Selects the given columns from the Dataset.
   *
   * @param column First column to be retrieved.
   * @param columns Rest of the columns.
   * @tparam T Type of input data.
   * @return Returns the function to select the given columns.
   */
  def select[T](column: String, columns: String*): Function[T, Row] = (d: Dataset[T]) => d.select(column, columns: _*)

  /**
   * Selects the given columns from the Dataset.
   *
   * @param columns Columns to be retrieved.
   * @tparam T Type of input data.
   * @return Returns the function to select the given columns.
   */
  def select[T](columns: Column*): Function[T, Row] = (d: Dataset[T]) => d.select(columns: _*)

  /**
   * Adds the given column to the Dataset.
   *
   * @param column Column name.
   * @param value Expression to carry out the value.
   * @tparam T Type of input data.
   * @return Returns the function to add the given column.
   */
  def add[T](column: String, value: Column): Function[T, Row] = (d: Dataset[T]) => d.withColumn(column, value)

  /**
   * Adds the given columns to the Dataset.
   *
   * @param columns Tuples with column name and the expression to carry out the value.
   * @tparam T Type of input data.
   * @return Returns the function to add the given columns.
   */
  def add[T](columns: (String, Column)*): Function[T, Row] = (d: Dataset[T]) =>
    columns.foldLeft(d.toDF()) { case (a: DataFrame, e: (String, Column)) => add(e._1, e._2)(a) }

  /**
   * Drops the given columns from the Dataset.
   * If some column does not exist, then this is no operation.
   *
   * @param columns Columns to drop.
   * @tparam T Type of input data.
   * @return Returns the function to drop the given columns.
   */
  def drop[T](columns: String*): Function[T, Row] = (d: Dataset[T]) => d.drop(columns: _*)

  /**
   * Renames the given column.
   * If the column does not exist, then this is no operation.
   *
   * @param oldColumn Column to rename.
   * @param newColumn New name.
   * @tparam T Type of input data.
   * @return Returns the function to rename the given column.
   */
  def rename[T](oldColumn: String, newColumn: String): Function[T, Row] =
    (d: Dataset[T]) => d.withColumnRenamed(oldColumn, newColumn)

  /**
   * Renames the given columns.
   * If the column does not exist, then this is no operation.
   *
   * @param renameExpr Tuples in the form of old name and new name.
   * @tparam T Type of input data.
   * @return Returns the function to rename the given columns.
   */
  def rename[T](renameExpr: (String, String)*): Function[T, Row] = (d: Dataset[T]) =>
    renameExpr.foldLeft(d.toDF()) { case (a: DataFrame, e: (String, String)) => rename(e._1, e._2)(a) }

  /**
   * Casts the given column to the given data type.
   *
   * @param column Column to cast.
   * @param newType New data type.
   * @tparam T Type of input data.
   * @return Returns the function to cast the given column.
   */
  def cast[T](column: String, newType: DataType): Function[T, Row] =
    (d: Dataset[T]) => d.withColumn(column, d(column).cast(newType))

  /**
   * Casts the given columns to the given data types.
   *
   * @param typesExpr Tuples in the form of column name and the destination type.
   * @tparam T Type of input data.
   * @return Returns the function to cast the given columns.
   */
  def cast[T](typesExpr: (String, DataType)*): Function[T, Row] = (d: Dataset[T]) =>
    typesExpr.foldLeft(d.toDF()) { case (a: DataFrame, e: (String, DataType)) => cast(e._1, e._2)(a) }

  /**
   * Maps using the given function.
   *
   * @param f Function to convert from input to output.
   * @tparam T Type of input data.
   * @tparam U Type of output data.
   * @return Returns the function to map the Dataset.
   */
  def map[T, U: Encoder](f: T => U): Function[T, U] = (d: Dataset[T]) => d map f

  /**
   * Flat maps using the given function.
   *
   * @param f Function to convert from input to output.
   * @tparam T Type of input data.
   * @tparam U Type of output data.
   * @return Returns the function to flatMap the Dataset.
   */
  def flatMap[T, U: Encoder](f: T => TraversableOnce[U]): Function[T, U] = (d: Dataset[T]) => d flatMap f

  /**
   * Aggregates using the given specification.
   *
   * @param groupBy Collection of columns to group by.
   * @param aggregations Aggregation expressions consisting of column and aggregation function.
   * Aggregation function might be one of: "avg", "count", "max", "min", "sum".
   * @tparam T Type of input data.
   * @return Returns the function to aggregate the Dataset.
   */
  @SuppressWarnings(Array("UnsafeTraversableMethods")) // added require to protect head and tail
  def agg[T](groupBy: Seq[String], aggregations: Seq[(String, String)]): Function[T, Row] = {
    require(groupBy.nonEmpty)
    require(aggregations.nonEmpty)

    (d: Dataset[T]) =>
      d
        .groupBy(groupBy.head, groupBy.tail: _*)
        .agg(aggregations.head, aggregations.tail: _*)
  }

  // set

  /**
   * Unions the other Dataset[T].
   *
   * @param other The other Dataset to be merged with.
   * @tparam T Type of input and output data.
   * @return Returns the function to union the given Datasets.
   */
  def union[T](other: Dataset[T]): Function[T, T] = (d: Dataset[T]) => d union other

  /**
   * Subtracts the other Dataset[T].
   *
   * @param other The other Dataset to be subtracted.
   * @tparam T Type of input and output data.
   * @return Returns the function to subtract the given Datasets.
   */
  def subtract[T](other: Dataset[T]): Function[T, T] = (d: Dataset[T]) => d except other

  /**
   * Intersects the other Dataset[T].
   *
   * @param other The other Dataset to be intersected with.
   * @tparam T Type of input and output data.
   * @return Returns the function to intersect the given Datasets.
   */
  def intersect[T](other: Dataset[T]): Function[T, T] = (d: Dataset[T]) => d intersect other

  /**
   * Delta with the other Dataset[T].
   *
   * @param other The other Dataset to be checked with.
   * @tparam T Type of input and output data.
   * @return Returns the function to find delta of the given Datasets.
   */
  def delta[T](other: Dataset[T]): Function[T, T] = (d: Dataset[T]) => (d except other) union (other except d)

  // joins

  /**
   * Cross joins with the other Dataset.
   *
   * @param other The other Dataset to be cross joined.
   * @tparam T Type of input data.
   * @return Returns the function to cross join the given Datasets.
   */
  def cross[T](other: Dataset[_]): Function[T, Row] = (d: Dataset[T]) => d crossJoin other

  /**
   * Cross joins with the other Dataset.
   *
   * @param other The other Dataset to be cross joined.
   * @tparam T Type of input data.
   * @tparam U Type of input data.
   * @return Returns the function to cross join the given Datasets.
   */
  def crossTyped[T, U](other: Dataset[U]): Function[T, (T, U)] =
    (d: Dataset[T]) => d.joinWith(other, lit(true), "cross")

  /**
   * Inner joins with the other Dataset.
   *
   * @param other The other Dataset to be inner joined.
   * @param columns Columns to make the equijoin on.
   * @tparam T Type of input data.
   * @return Returns the function to inner join the given Datasets.
   */
  def inner[T](other: Dataset[_], columns: Seq[String]): Function[T, Row] =
    (d: Dataset[T]) => d.join(other, columns, "inner")

  /**
   * Inner joins with the other Dataset.
   *
   * @param other The other Dataset to be inner joined.
   * @param joinExpr Join expression.
   * @tparam T Type of input data.
   * @return Returns the function to inner join the given Datasets.
   */
  def inner[T](other: Dataset[_], joinExpr: Column): Function[T, Row] =
    (d: Dataset[T]) => d.join(other, joinExpr, "inner")

  /**
   * Inner joins with the other Dataset.
   *
   * @param other The other Dataset to be inner joined.
   * @param joinExpr Join expression.
   * @tparam T Type of input data.
   * @tparam U Type of input data.
   * @return Returns the function to inner join the given Datasets.
   */
  def innerTyped[T, U](other: Dataset[U], joinExpr: Column): Function[T, (T, U)] =
    (d: Dataset[T]) => d.joinWith(other, joinExpr, "inner")

  /**
   * Left outer joins with the other Dataset.
   *
   * @param other The other Dataset to be left outer joined.
   * @param columns Columns to make the equijoin on.
   * @tparam T Type of input data.
   * @return Returns the function to left outer join the given Datasets.
   */
  def left[T](other: Dataset[_], columns: Seq[String]): Function[T, Row] =
    (d: Dataset[T]) => d.join(other, columns, "left")

  /**
   * Left outer joins with the other Dataset.
   *
   * @param other The other Dataset to be left outer joined.
   * @param joinExpr Join expression.
   * @tparam T Type of input data.
   * @return Returns the function to left outer join the given Datasets.
   */
  def left[T](other: Dataset[_], joinExpr: Column): Function[T, Row] =
    (d: Dataset[T]) => d.join(other, joinExpr, "left")

  /**
   * Left outer joins with the other Dataset.
   *
   * @param other The other Dataset to be left outer joined.
   * @param joinExpr Join expression.
   * @tparam T Type of input data.
   * @tparam U Type of input data.
   * @return Returns the function to left outer join the given Datasets.
   */
  def leftTyped[T, U](other: Dataset[U], joinExpr: Column): Function[T, (T,U)] =
    (d: Dataset[T]) => d.joinWith(other, joinExpr, "left")

  /**
   * Right outer joins with the other Dataset.
   *
   * @param other The other Dataset to be right outer joined.
   * @param columns Columns to make the equijoin on.
   * @tparam T Type of input data.
   * @return Returns the function to right outer join the given Datasets.
   */
  def right[T](other: Dataset[_], columns: Seq[String]): Function[T, Row] =
    (d: Dataset[T]) => d.join(other, columns, "right")

  /**
   * Right outer joins with the other Dataset.
   *
   * @param other The other Dataset to be right outer joined.
   * @param joinExpr Join expression.
   * @tparam T Type of input data.
   * @return Returns the function to right outer join the given Datasets.
   */
  def right[T](other: Dataset[_], joinExpr: Column): Function[T, Row] =
    (d: Dataset[T]) => d.join(other, joinExpr, "right")

  /**
   * Right outer joins with the other Dataset.
   *
   * @param other The other Dataset to be right outer joined.
   * @param joinExpr Join expression.
   * @tparam T Type of input data.
   * @tparam U Type of input data.
   * @return Returns the function to right outer join the given Datasets.
   */
  def rightTyped[T, U](other: Dataset[U], joinExpr: Column): Function[T, (T, U)] =
    (d: Dataset[T]) => d.joinWith(other, joinExpr, "right")

  /**
   * Full outer joins with the other Dataset.
   *
   * @param other The other Dataset to be full outer joined.
   * @param columns Columns to make the equijoin on.
   * @tparam T Type of input data.
   * @return Returns the function to full outer join the given Datasets.
   */
  def full[T](other: Dataset[_], columns: Seq[String]): Function[T, Row] =
    (d: Dataset[T]) => d.join(other, columns, "full")

  /**
   * Full outer joins with the other Dataset.
   *
   * @param other The other Dataset to be full outer joined.
   * @param joinExpr Join expression.
   * @tparam T Type of input data.
   * @return Returns the function to full outer join the given Datasets.
   */
  def full[T](other: Dataset[_], joinExpr: Column): Function[T, Row] =
    (d: Dataset[T]) => d.join(other, joinExpr, "full")

  /**
   * Full outer joins with the other Dataset.
   *
   * @param other The other Dataset to be full outer joined.
   * @param joinExpr Join expression.
   * @tparam T Type of input data.
   * @tparam U Type of input data.
   * @return Returns the function to full outer join the given Datasets.
   */
  def fullTyped[T, U](other: Dataset[U], joinExpr: Column): Function[T, (T, U)] =
    (d: Dataset[T]) => d.joinWith(other, joinExpr, "full")

  // utility

  /**
   * Casts the given Dataset[Row] to Dataset[T].
   *
   * @tparam T Type of output data.
   * @return Returns the function to cast the given Dataset.
   */
  def as[T: Encoder](): Function[Row, T] = (d: Dataset[Row]) => d.as[T]

  /**
   * Casts the given Dataset[T] to Dataset[Row].
   *
   * @tparam T Type of input data.
   * @return Returns the function to cast the given Dataset.
   */
  def row[T](): Function[T, Row] = (d: Dataset[T]) => d.toDF()

  /**
   * Caches the given Dataset.
   *
   * @tparam T Type of input data.
   * @return Returns the function to cache the given Dataset.
   */
  def cache[T](): Function[T, T] = (d: Dataset[T]) => d.cache()

  /**
   * Sorts the given Dataset.
   *
   * @param column First column to sort on.
   * @param columns Rest of the columns.
   * @tparam T Type of input and output data.
   * @return Returns the function to sort the given Dataset.
   */
  def sort[T](column: String, columns: String*): Function[T, T] = (d: Dataset[T]) => d.orderBy(column, columns: _*)

  /**
   * Sorts the given Dataset.
   *
   * @param columns Columns to sort on.
   * @tparam T Type of input and output data.
   * @return Returns the function to sort the given Dataset.
   */
  def sort[T](columns: Column*): Function[T, T] = (d: Dataset[T]) => d.orderBy(columns: _*)

  /**
   * Connects several functions into one composed function.
   *
   * @param f First function to compose.
   * @param fs Rest of functions to compose.
   * @tparam T Type of input and output.
   * @return Returns composition of input functions.
   */
  @SuppressWarnings(Array("UnsafeTraversableMethods")) // reduce is safe here, as we call it on non empty collection
  def pipeline[T](f: Function[T, T], fs: Function[T, T]*): Function[T, T] = (f +: fs).reduce(_ + _)

  // ML

  /**
   * Transforms the given Dataset using given transformer.
   *
   * @param transformer The ML transformer tobe used.
   * @return Returns the function to transform the given Dataset.
   */
  def trans(transformer: Transformer): Function[Row, Row] = (d: Dataset[_]) => transformer.transform(d)

  // types

  /**
   * Reduces the given Datasets.
   *
   * @tparam T Type of input data.
   * @return Returns the function to reduce the given Dataset.
   */
  @deprecated("Use plain Scala instead")
  def reduce[T](): FN[T, T] = (ds: Seq[Dataset[T]]) => ds.reduce(_ union _)
}
