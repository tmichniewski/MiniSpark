package com.github
package minispark

import org.apache.spark.sql.Dataset

trait ETL extends (() => Unit)

trait Extract[T] extends (() => Dataset[T]) {
  def +[U](extract: Extract[U]): ExtractPair[T, U] = ExtractPair[T, U](this, extract) // E + E = E2
  def +[U](transform: Transform[T, U]): Extract[U] = () => transform(apply()) // E + T = E
  def +(load: Load[T]): ETL = () => load(apply()) // E + L = ETL
  def *[U, V](combine: Combine[T, U, V]): Transform[U, V] = combine(apply(), _) // E + C = T
  def *[U, V](combine: Combine[U, T, V]): Transform[U, V] = combine(_, apply()) // E + C = T
}
// E + E * C = E + T = E

case class ExtractPair[T, U](e1: Extract[T], e2: Extract[U]) {
  def +[V](combine: Combine[T, U, V]): Extract[V] = () => combine(e1(), e2()) // E + E + C = E
}

trait Transform[T, U] extends (Dataset[T] => Dataset[U]) {
  def +[V](transform: Transform[U, V]): Transform[T, V] = (d: Dataset[T]) => transform(apply(d)) // T + T = T
  def +(load: Load[U]): Load[T] = (d: Dataset[T]) => load(apply(d)) // T + L = L
  def ++[V, W](combine: Combine[U, V, W]): Combine[T, V, W] = (d1: Dataset[T], d2: Dataset[V]) => combine(apply(d1), d2) // T + C = C
  def ++[V, W](combine: Combine[V, U, W]): Combine[V, T, W] = (d1: Dataset[V], d2: Dataset[T]) => combine(d1, apply(d2)) // T + C = C
}
// (T + T) ++ C = T + (T ++ C) ?

trait Load[T] extends (Dataset[T] => Unit)

trait Combine[T, U, V] extends ((Dataset[T], Dataset[U]) => Dataset[V]) {
  def +[W](transform: Transform[V, W]): Combine[T, U, W] = (d1: Dataset[T], d2: Dataset[U]) => transform(apply(d1, d2)) // C + T = C
  def ++(load: Load[V]): (Dataset[T], Dataset[U]) => Unit = (d1: Dataset[T], d2: Dataset[U]) => load(apply(d1, d2)) // C + L = L2
}

object types {
  def join[T, U, V](e1: Extract[T], e2: Extract[U], combine: Combine[T, U, V]): Extract[V] = () => combine(e1(), e2())
}
