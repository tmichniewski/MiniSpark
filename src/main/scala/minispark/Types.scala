package com.github
package minispark

import org.apache.spark.sql.Dataset

trait ETL extends (() => Unit)

trait Extract[T] extends (() => Dataset[T]) {
  def +[U](extract: Extract[U]): ExtractPair[T, U] = ExtractPair[T, U](this, extract) // E + E => E2
  def +[U](transform: Transform[T, U]): Extract[U] = () => transform(apply()) // E + T => E
  def +(load: Load[T]): ETL = () => load(apply()) // E + L => ETL
  def split: Split[T] = Split(this) // E => cached E
}

case class ExtractPair[T, U](extract1: Extract[T], extract2: Extract[U]) {
  def +[V](combine: Combine[T, U, V]): Extract[V] = () => combine(extract1(), extract2()) // E2 + C => E
}

case class Split[T](extract: Extract[T]) extends Extract[T] {
  lazy val d: Dataset[T] = extract().cache()
  override def apply(): Dataset[T] = d
}

trait Transform[T, U] extends (Dataset[T] => Dataset[U]) {
  def +[V](transform: Transform[U, V]): Transform[T, V] = (d: Dataset[T]) => transform(apply(d)) // T + T => T
  def +(load: Load[U]): Load[T] = (d: Dataset[T]) => load(apply(d)) // T + L => L
}

trait Load[T] extends (Dataset[T] => Unit)

trait Combine[T, U, V] extends ((Dataset[T], Dataset[U]) => Dataset[V])
