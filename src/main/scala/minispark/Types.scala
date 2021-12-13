package com.github
package minispark

import org.apache.spark.sql.Dataset

trait F0[T] extends (() => Dataset[T]) {
  def +[U](f1tu: F1[T, U]): F0[U] = () => f1tu(apply()) // F0 + F1 = F0
}

trait F1[T, U] extends (Dataset[T] => Dataset[U]) {
  def +[V](f1uv: F1[U, V]): F1[T, V] = (d: Dataset[T]) => f1uv(apply(d)) // F1 + F1 = F1
}

trait F2[T, U, V] extends ((Dataset[T], Dataset[U]) => Dataset[V]) {
  def +[W](f1vw: F1[V, W]): F2[T, U, W] = (d1: Dataset[T], d2: Dataset[U]) => f1vw(apply(d1, d2)) // F2 + F1 = F2
}

trait FN[T, U] extends (Seq[Dataset[T]] => Dataset[U]) {
  def +[V](f1uw: F1[U, V]): FN[T, V] = (ds: Seq[Dataset[T]]) => f1uw(apply(ds)) // FN + F1 = FN
}

// X + F1 = X
