package com.github
package minispark

import org.apache.spark.sql.Dataset

trait ETL extends (() => Unit)

trait Extract[T] extends (() => Dataset[T]) {
  def +[U](transform: Transform[T, U]): Extract[U] = () => transform(apply()) // E + T = E
  def +(load: Load[T]): ETL = () => load(apply()) // E + L = ETL
}

trait Transform[T, U] extends (Dataset[T] => Dataset[U]) {
  def +[V](transform: Transform[U, V]): Transform[T, V] = (d: Dataset[T]) => transform(apply(d)) // T + T = T
  def +(load: Load[U]): Load[T] = (d: Dataset[T]) => load(apply(d)) // T + L = L
}

trait Load[T] extends (Dataset[T] => Unit)

trait Combine[T, U, V] extends ((Dataset[T], Dataset[U]) => Dataset[V])
