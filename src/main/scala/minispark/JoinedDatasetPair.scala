package com.github
package minispark

import org.apache.spark.sql.{Column, Dataset, Row}

/**
 * Represents a pair of Datasets to be joined.
 *
 * @param d1 First Dataset.
 * @param d2 Second Dataset.
 * @param joinType Join type.
 * @tparam T Type of first data.
 * @tparam U Type of second data.
 */
class JoinedDatasetPair[T, U](d1: Dataset[T], d2: Dataset[U], joinType: String) {
  /**
   * Finishes the join transformation.
   *
   * Untyped API.
   *
   * @param columns Collection of columns to join on.
   * @return Returns the joined Datasets.
   */
  def on(columns: Seq[String]): Dataset[Row] = joinType match {
    case "inner" | "left" | "right" | "full" => d1.join(d2, columns, joinType)
  }

  /**
   * Finishes the join transformation.
   *
   * Untyped API.
   *
   * @param joinExpr Join expression.
   * @return Returns the joined Datasets.
   */
  def on(joinExpr: Column): Dataset[Row] = joinType match {
    case "inner" | "left" | "right" | "full" => d1.join(d2, joinExpr, joinType)
  }

  /**
   * Finishes the join transformation.
   *
   * Typed API.
   *
   * @param joinExpr Join expression.
   * @return Returns the joined Datasets which contains tuple of rows from both Datasets.
   */
  def onTyped(joinExpr: Column): Dataset[(T, U)] = joinType match {
    case "inner" | "left" | "right" | "full" => d1.joinWith(d2, joinExpr, joinType)
  }
}

/** Companion object with apply. */
object JoinedDatasetPair {
  /**
   * Constructs the instance.
   *
   * @param d1 First Dataset.
   * @param d2 Second Dataset.
   * @param joinType Join type.
   * @tparam T Type of first data.
   * @tparam U Type of second data.
   * @return Returns the joined Datasets.
   */
  def apply[T, U](d1: Dataset[T], d2: Dataset[U], joinType: String): JoinedDatasetPair[T, U] =
    new JoinedDatasetPair[T, U](d1, d2, joinType)
}
