package com.github
package minispark

import org.apache.spark.sql._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.DataType

/** Contains typical operations. */
object Transforms {
  // basic

  /**
   * Filters the Dataset.
   *
   * Typed API.
   *
   * @param condition Filter condition.
   * @tparam T Type of input and output data.
   * @return Returns transform to filter the Dataset.
   */
  def filter[T](condition: String): Transform[T, T] = _ filter condition

  /**
   * Filters the Dataset.
   *
   * Typed API.
   *
   * @param condition Filter condition.
   * @tparam T Type of input and output data.
   * @return Returns transform to filter the Dataset.
   */
  def filter[T](condition: Column): Transform[T, T] = _ filter condition

  /**
   * Selects the given columns from the Dataset.
   *
   * Untyped API. For typed API use map or flatMap instead.
   *
   * @param column First column to be retrieved.
   * @param columns Rest of the columns.
   * @tparam T Type of input data.
   * @return Returns transform to select the given columns.
   */
  def select[T](column: String, columns: String*): Transform[T, Row] = _.select(column, columns: _*)

  /**
   * Selects the given columns from the Dataset.
   *
   * Untyped API. For typed API use map or flatMap instead.
   *
   * @param columns Columns to be retrieved.
   * @tparam T Type of input data.
   * @return Returns transform to select the given columns.
   */
  def select[T](columns: Column*): Transform[T, Row] = _.select(columns: _*)

  /**
   * Adds the given column to the Dataset.
   *
   * Untyped API. For typed API use map or flatMap instead.
   *
   * @param column Column name.
   * @param value Expression to carry out the value.
   * @tparam T Type of input data.
   * @return Returns transform to add the given column.
   */
  def add[T](column: String, value: Column): Transform[T, Row] = _.withColumn(column, value)

  /**
   * Adds the given columns to the Dataset.
   * Untyped API. For typed API use map or flatMap instead.
   *
   * @param columns Tuples with column name and the expression to carry out the value.
   * @tparam T Type of input data.
   * @return Returns transform to add the given columns.
   */
  def add[T](columns: (String, Column)*): Transform[T, Row] = (d: Dataset[T]) =>
    columns.foldLeft(d.toDF()) { case (a: DataFrame, e: (String, Column)) => add(e._1, e._2)(a) }

  /**
   * Drops the given columns from the Dataset.
   * If some column does not exist, then this is no operation.
   *
   * Untyped API. For typed API use map or flatMap instead.
   *
   * @param columns Columns to drop.
   * @tparam T Type of input data.
   * @return Returns transform to drop the given columns.
   */
  def drop[T](columns: String*): Transform[T, Row] = _.drop(columns: _*)

  /**
   * Renames the given column.
   *
   * If the column does not exist, then this is no operation.
   *
   * Untyped API. For typed API use map or flatMap instead.
   *
   * @param oldColumn Column to rename.
   * @param newColumn New name.
   * @tparam T Type of input data.
   * @return Returns transform to rename the given column.
   */
  def rename[T](oldColumn: String, newColumn: String): Transform[T, Row] =
    _.withColumnRenamed(oldColumn, newColumn)

  /**
   * Renames the given columns.
   * If the column does not exist, then this is no operation.
   *
   * Untyped API. For typed API use map or flatMap instead.
   *
   * @param renameExpr Tuples in the form of old name and new name.
   * @tparam T Type of input data.
   * @return Returns transform to rename the given columns.
   */
  def rename[T](renameExpr: (String, String)*): Transform[T, Row] = (d: Dataset[T]) =>
    renameExpr.foldLeft(d.toDF()) { case (a: DataFrame, e: (String, String)) => rename(e._1, e._2)(a) }

  /**
   * Casts the given column to the given data type.
   *
   * Untyped API. For typed API use map or flatMap instead.
   *
   * @param column Column to cast.
   * @param newType New data type.
   * @tparam T Type of input data.
   * @return Returns transform to cast the given column.
   */
  def cast[T](column: String, newType: DataType): Transform[T, Row] =
    (d: Dataset[T]) => d.withColumn(column, d(column).cast(newType))

  /**
   * Casts the given columns to the given data types.
   *
   * Untyped API. For typed API use map or flatMap instead.
   *
   * @param typesExpr Tuples in the form of column name and the destination type.
   * @tparam T Type of input data.
   * @return Returns transform to cast the given columns.
   */
  def cast[T](typesExpr: (String, DataType)*): Transform[T, Row] = (d: Dataset[T]) =>
    typesExpr.foldLeft(d.toDF()) { case (a: DataFrame, e: (String, DataType)) => cast(e._1, e._2)(a) }

  /**
   * Maps using the given function.
   *
   * Typed API.
   *
   * @param f Function to convert from input to output.
   * @tparam T Type of input data.
   * @tparam U Type of output data.
   * @return Returns the function to map the Dataset.
   */
  def map[T, U: Encoder](f: T => U): Transform[T, U] = _ map f

  /**
   * Flat maps using the given function.
   *
   * Typed API.
   *
   * @param f Function to convert from input to output.
   * @tparam T Type of input data.
   * @tparam U Type of output data.
   * @return Returns transform to flatMap the Dataset.
   */
  def flatMap[T, U: Encoder](f: T => TraversableOnce[U]): Transform[T, U] = _ flatMap f

  /**
   * Aggregates using the given specification.
   *
   * Untyped API.
   *
   * @param groupBy Collection of columns to group by.
   * @param aggregations Aggregation expressions consisting of column and aggregation function.
   * Aggregation function might be one of: "avg", "count", "max", "min", "sum".
   * @tparam T Type of input data.
   * @return Returns transform to aggregate the Dataset.
   */
  @SuppressWarnings(Array("UnsafeTraversableMethods")) // added require to protect head and tail
  def agg[T](groupBy: Seq[String], aggregations: Seq[(String, String)]): Transform[T, Row] = {
    require(groupBy.nonEmpty)
    require(aggregations.nonEmpty)

    _.groupBy(groupBy.head, groupBy.tail: _*).agg(aggregations.head, aggregations.tail: _*)
  }

  /**
   * Aggregates using the given specification.
   *
   * Untyped API.
   *
   * @param groupBy Collection of columns to group by.
   * @param expr First aggregation expression.
   * @param exprs Rest of aggregation expressions.
   * @tparam T Type of input data.
   * @return Returns transform to aggregate the Dataset.
   */
  @SuppressWarnings(Array("UnsafeTraversableMethods")) // added require to protect head and tail
  def agg[T](groupBy: Seq[String], expr: Column, exprs: Column*): Transform[T, Row] = {
    require(groupBy.nonEmpty)
    _.groupBy(groupBy.head, groupBy.tail: _*).agg(expr, exprs: _*)
  }

  // set

  /**
   * Unions the other Dataset[T].
   *
   * Typed API.
   *
   * @param other The other Dataset to be merged with.
   * @tparam T Type of input and output data.
   * @return Returns transform to union the given Datasets.
   */
  def union[T](other: Dataset[T]): Transform[T, T] = _ union other

  /**
   * Subtracts the other Dataset[T].
   *
   * Typed API.
   *
   * @param other The other Dataset to be subtracted.
   * @tparam T Type of input and output data.
   * @return Returns transform to subtract the given Datasets.
   */
  def subtract[T](other: Dataset[T]): Transform[T, T] = _ except other

  /**
   * Intersects the other Dataset[T].
   *
   * Typed API.
   *
   * @param other The other Dataset to be intersected with.
   * @tparam T Type of input and output data.
   * @return Returns transform to intersect the given Datasets.
   */
  def intersect[T](other: Dataset[T]): Transform[T, T] = _ intersect other

  /**
   * Delta with the other Dataset[T].
   *
   * Typed API.
   *
   * @param other The other Dataset to be checked with.
   * @tparam T Type of input and output data.
   * @return Returns transform to find delta of the given Datasets.
   */
  def delta[T](other: Dataset[T]): Transform[T, T] =
    (d: Dataset[T]) => (d except other) union (other except d)

  // joins

  /**
   * Cross joins with the other Dataset.
   *
   * Untyped API. For typed use crossTyped instead.
   *
   * @param other The other Dataset to be cross joined.
   * @tparam T Type of input data.
   * @return Returns transform to cross join the given Datasets.
   */
  def cross[T](other: Dataset[_]): Transform[T, Row] = _ crossJoin other

  /**
   * Cross joins with the other Dataset.
   *
   * Typed API.
   *
   * @param other The other Dataset to be cross joined.
   * @tparam T Type of input data.
   * @tparam U Type of input data.
   * @return Returns transform to cross join the given Datasets.
   */
  def crossTyped[T, U](other: Dataset[U]): Transform[T, (T, U)] =
    _.joinWith(other, lit(true), "cross")

  /**
   * Inner joins with the other Dataset.
   *
   * Untyped API. For typed use innerTyped instead.
   *
   * @param other The other Dataset to be inner joined.
   * @param columns Columns to make the equijoin on.
   * @tparam T Type of input data.
   * @return Returns transform to inner join the given Datasets.
   */
  def inner[T](other: Dataset[_], columns: Seq[String]): Transform[T, Row] =
    _.join(other, columns, "inner")

  /**
   * Inner joins with the other Dataset.
   *
   * Untyped API. For typed use innerTyped instead.
   *
   * @param other The other Dataset to be inner joined.
   * @param joinExpr Join expression.
   * @tparam T Type of input data.
   * @return Returns transform to inner join the given Datasets.
   */
  def inner[T](other: Dataset[_], joinExpr: Column): Transform[T, Row] =
    _.join(other, joinExpr, "inner")

  /**
   * Inner joins with the other Dataset.
   *
   * Typed API.
   *
   * @param other The other Dataset to be inner joined.
   * @param joinExpr Join expression.
   * @tparam T Type of input data.
   * @tparam U Type of input data.
   * @return Returns transform to inner join the given Datasets.
   */
  def innerTyped[T, U](other: Dataset[U], joinExpr: Column): Transform[T, (T, U)] =
    _.joinWith(other, joinExpr, "inner")

  /**
   * Left outer joins with the other Dataset.
   *
   * Untyped API. For typed use leftTyped instead.
   *
   * @param other The other Dataset to be left outer joined.
   * @param columns Columns to make the equijoin on.
   * @tparam T Type of input data.
   * @return Returns transform to left outer join the given Datasets.
   */
  def left[T](other: Dataset[_], columns: Seq[String]): Transform[T, Row] =
    _.join(other, columns, "left")

  /**
   * Left outer joins with the other Dataset.
   *
   * Untyped API. For typed use leftTyped instead.
   *
   * @param other The other Dataset to be left outer joined.
   * @param joinExpr Join expression.
   * @tparam T Type of input data.
   * @return Returns transform to left outer join the given Datasets.
   */
  def left[T](other: Dataset[_], joinExpr: Column): Transform[T, Row] =
    _.join(other, joinExpr, "left")

  /**
   * Left outer joins with the other Dataset.
   *
   * Typed API.
   *
   * @param other The other Dataset to be left outer joined.
   * @param joinExpr Join expression.
   * @tparam T Type of input data.
   * @tparam U Type of input data.
   * @return Returns transform to left outer join the given Datasets.
   */
  def leftTyped[T, U](other: Dataset[U], joinExpr: Column): Transform[T, (T,U)] =
    _.joinWith(other, joinExpr, "left")

  /**
   * Right outer joins with the other Dataset.
   *
   * Untyped API. For typed use rightTyped instead.
   *
   * @param other The other Dataset to be right outer joined.
   * @param columns Columns to make the equijoin on.
   * @tparam T Type of input data.
   * @return Returns transform to right outer join the given Datasets.
   */
  def right[T](other: Dataset[_], columns: Seq[String]): Transform[T, Row] =
    _.join(other, columns, "right")

  /**
   * Right outer joins with the other Dataset.
   *
   * Untyped API. For typed use rightTyped instead.
   *
   * @param other The other Dataset to be right outer joined.
   * @param joinExpr Join expression.
   * @tparam T Type of input data.
   * @return Returns transform to right outer join the given Datasets.
   */
  def right[T](other: Dataset[_], joinExpr: Column): Transform[T, Row] =
    _.join(other, joinExpr, "right")

  /**
   * Right outer joins with the other Dataset.
   *
   * Typed API.
   *
   * @param other The other Dataset to be right outer joined.
   * @param joinExpr Join expression.
   * @tparam T Type of input data.
   * @tparam U Type of input data.
   * @return Returns transform to right outer join the given Datasets.
   */
  def rightTyped[T, U](other: Dataset[U], joinExpr: Column): Transform[T, (T, U)] =
    _.joinWith(other, joinExpr, "right")

  /**
   * Full outer joins with the other Dataset.
   *
   * Untyped API. For typed use fullTyped instead.
   *
   * @param other The other Dataset to be full outer joined.
   * @param columns Columns to make the equijoin on.
   * @tparam T Type of input data.
   * @return Returns transform to full outer join the given Datasets.
   */
  def full[T](other: Dataset[_], columns: Seq[String]): Transform[T, Row] =
    _.join(other, columns, "full")

  /**
   * Full outer joins with the other Dataset.
   *
   * Untyped API. For typed use fullTyped instead.
   *
   * @param other The other Dataset to be full outer joined.
   * @param joinExpr Join expression.
   * @tparam T Type of input data.
   * @return Returns transform to full outer join the given Datasets.
   */
  def full[T](other: Dataset[_], joinExpr: Column): Transform[T, Row] =
    _.join(other, joinExpr, "full")

  /**
   * Full outer joins with the other Dataset.
   *
   * Typed API.
   *
   * @param other The other Dataset to be full outer joined.
   * @param joinExpr Join expression.
   * @tparam T Type of input data.
   * @tparam U Type of input data.
   * @return Returns transform to full outer join the given Datasets.
   */
  def fullTyped[T, U](other: Dataset[U], joinExpr: Column): Transform[T, (T, U)] =
    _.joinWith(other, joinExpr, "full")

  // utility

  /**
   * Casts the given Dataset[Row] to Dataset[T].
   *
   * Typed API.
   *
   * @tparam T Type of output data.
   * @return Returns transform to cast the given Dataset.
   */
  def as[T: Encoder](): Transform[Row, T] = _.as[T]

  /**
   * Casts the given Dataset[T] to Dataset[Row].
   *
   * Untyped API.
   *
   * @tparam T Type of input data.
   * @return Returns transform to cast the given Dataset.
   */
  def row[T](): Transform[T, Row] = _.toDF()

  /**
   * Caches the given Dataset.
   *
   * Typed API.
   *
   * @tparam T Type of input data.
   * @return Returns transform to cache the given Dataset.
   */
  def cache[T](): Transform[T, T] = _.cache()

  /**
   * Sorts the given Dataset.
   *
   * Typed API.
   *
   * @param column First column to sort on.
   * @param columns Rest of the columns.
   * @tparam T Type of input and output data.
   * @return Returns transform to sort the given Dataset.
   */
  def sort[T](column: String, columns: String*): Transform[T, T] = _.orderBy(column, columns: _*)

  /**
   * Sorts the given Dataset.
   *
   * Typed API.
   *
   * @param columns Columns to sort on.
   * @tparam T Type of input and output data.
   * @return Returns transform to sort the given Dataset.
   */
  def sort[T](columns: Column*): Transform[T, T] = _.orderBy(columns: _*)

  /**
   * Connects several transforms into a composed one.
   *
   * Typed API.
   *
   * @param t First transform to compose.
   * @param ts Rest of transforms to compose.
   * @tparam T Type of input and output.
   * @return Returns composition of input transforms.
   */
  @SuppressWarnings(Array("UnsafeTraversableMethods")) // reduce is safe here, as we call it on non empty collection
  def pipeline[T](t: Transform[T, T], ts: Transform[T, T]*): Transform[T, T] = (t +: ts).reduce(_ + _)
}
