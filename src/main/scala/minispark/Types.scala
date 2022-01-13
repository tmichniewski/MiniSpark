package com.github
package minispark

import org.apache.spark.sql.Dataset

trait ETL extends (() => Unit)

trait Extract[T] extends (() => Dataset[T]) {
  def +[U](transform: Transform[T, U]): Extract[U] = () => transform(apply()) // E + T = E
  def +(load: Load[T]): ETL = () => load(apply()) // E + L = ETL
  def +[U, V](combine: Combine[U, T, V]): Transform[U, V] = combine(_, apply()) // E + C = T
}

trait Transform[T, U] extends (Dataset[T] => Dataset[U]) {
  def +[V](transform: Transform[U, V]): Transform[T, V] = (d: Dataset[T]) => transform(apply(d)) // T + T = T
  def +(load: Load[U]): Load[T] = (d: Dataset[T]) => load(apply(d)) // T + L = L
  def +[V, W](combine: Combine[U, V, W]): Combine[T, V, W] = (d1: Dataset[T], d2: Dataset[V]) => combine(apply(d1), d2) // T + C = C
}

trait Load[T] extends (Dataset[T] => Unit)

trait Combine[T, U, V] extends ((Dataset[T], Dataset[U]) => Dataset[V]) {
  def +[W](transform: Transform[V, W]): Combine[T, U, W] = (d1: Dataset[T], d2: Dataset[U]) => transform(apply(d1, d2)) // C + T = C
}
