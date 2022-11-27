package com.github
package minispark

import org.apache.spark.sql.{Dataset, Row}

/** Contains implicit Dataset operators. */
object Implicits {
  /**
   * Implicit class to perform additional operations on Dataset.
   *
   * @param d The object to perform some operations on.
   * @tparam T Type of data.
   */
  @SuppressWarnings(Array("AvoidOperatorOverload")) // we deliberately use operators as we construct Dataset operators
  implicit class ExtendedDataset[T](private val d: Dataset[T]) extends AnyVal {
    /**
     * Applies the given transform function to the Dataset.
     *
     * Typed API.
     *
     * @param t The transform function which will be applied.
     * @tparam U Type of resulting data.
     * @return Returns the produced Dataset.
     */
    def ++[U](t: Transform[T, U]): Dataset[U] = d transform t // <=> t(d)

    /**
     * Unions the other Dataset[T].
     *
     * Typed API.
     *
     * @param other The other Dataset to be merged with.
     * @return Returns the united Datasets.
     */
    def +(other: Dataset[T]): Dataset[T] = d union other

    /**
     * Subtracts the other Dataset[T].
     *
     * Typed API.
     *
     * @param other The other Dataset to be subtracted.
     * @return Returns the subtracted Datasets.
     */
    def -(other: Dataset[T]): Dataset[T] = d except other

    /**
     * Intersects the other Dataset[T].
     *
     * Typed API.
     *
     * @param other The other Dataset to be intersected with.
     * @return Returns the intersected Datasets.
     */
    def *(other: Dataset[T]): Dataset[T] = d intersect other

    /**
     * Delta with the other Dataset[T].
     *
     * Typed API.
     *
     * @param other The other Dataset to be checked with.
     * @return Returns the delta of the given Datasets.
     */
    def -+-(other: Dataset[T]): Dataset[T] = (d - other) + (other - d)

    /**
     * Cross joins with the other Dataset.
     *
     * Untyped API.
     *
     * @param other The other Dataset to be cross joined.
     * @return Returns the cross join of the given Datasets.
     */
    def |*|(other: Dataset[_]): Dataset[Row] = d crossJoin other
    // alternatively this could be called |+| from the "cross" word (meaning plus),
    // but this operator would suggest that there is some union under the neath,
    // while it is not the case

    /**
     * Inner joins with the other Dataset.
     *
     * For typed API follow with onTyped, otherwise untyped API.
     *
     * @param other The other Dataset to be inner joined.
     * @tparam U Type of other input data.
     * @return Returns JoinedDatasetPair to perform the real join on.
     */
    def |=|[U](other: Dataset[U]): DatasetPair[T, U] = DatasetPair(d, other, "inner")

    /**
     * Left outer joins with the other Dataset.
     *
     * For typed API follow with onTyped, otherwise untyped API.
     *
     * @param other The other Dataset to be left outer joined.
     * @tparam U Type of other input data.
     * @return Returns JoinedDatasetPair to perform the real join on.
     */
    def |=+|[U](other: Dataset[U]): DatasetPair[T, U] = DatasetPair(d, other, "left")

    /**
     * Right outer joins with the other Dataset.
     *
     * For typed API follow with onTyped, otherwise untyped API.
     *
     * @param other The other Dataset to be left outer joined.
     * @tparam U Type of other input data.
     * @return Returns JoinedDatasetPair to perform the real join on.
     */
    def |+=|[U](other: Dataset[U]): DatasetPair[T, U] = DatasetPair(d, other, "right")

    /**
     * Full outer joins with the other Dataset.
     *
     * For typed API follow with onTyped, otherwise untyped API.
     *
     * @param other The other Dataset to be left outer joined.
     * @tparam U Type of other input data.
     * @return Returns JoinedDatasetPair to perform the real join on.
     */
    def |+=+|[U](other: Dataset[U]): DatasetPair[T, U] = DatasetPair(d, other, "full")
  }
}
