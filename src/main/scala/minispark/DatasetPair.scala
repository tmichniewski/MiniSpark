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
final case class DatasetPair[T, U](d1: Dataset[T], d2: Dataset[U], joinType: String) {
  /**
   * Finishes the join transformation.
   *
   * Untyped API.
   *
   * @param columns Collection of columns to join on.
   * @return Joined Datasets.
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
   * @return Joined Datasets.
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
   * @return Joined Datasets which contains tuple of rows from both Datasets.
   */
  def onTyped(joinExpr: Column): Dataset[(T, U)] = joinType match {
    case "inner" | "left" | "right" | "full" => d1.joinWith(d2, joinExpr, joinType)
  }
}
