package com.github
package minispark

import org.apache.spark.sql.Dataset

trait F0[T] extends (() => Dataset[T]) { outer =>
  def +[U](f0: F0[U]): F0F0[T, U] = new F0F0[T, U](this, f0) // F0 + F0 = (F0, F0)
  def +[U](f1: F1[T, U]): F0[U] = () => f1(apply()) // F0 + F1 = F0
  def +[U, V](f2: F2[T, U, V]): F1[U, V] = f2(outer.apply(), _) // F0 + F2 = F1
}

class F0F0[T, U](f1: F0[T], f2: F0[U]) {
  def+[V](f: F2[T, U, V]): F0[V] = () => f(f1(), f2()) // (F0, F0) + F2 = F0
}

trait F1[T, U] extends (Dataset[T] => Dataset[U]) {
  def +[V](f: F1[U, V]): F1[T, V] = (d: Dataset[T]) => f(apply(d)) // F1 + F1 = F1
}

trait F2[T, U, V] extends ((Dataset[T], Dataset[U]) => Dataset[V]) {
  def +[W](f: F1[V, W]): F2[T, U, W] = (d1: Dataset[T], d2: Dataset[U]) => f(apply(d1, d2)) // F2 + F1 = F2
}

trait FN[T, U] extends (Seq[Dataset[T]] => Dataset[U]) {
  def +[V](f: F1[U, V]): FN[T, V] = (ds: Seq[Dataset[T]]) => f(apply(ds)) // FN + F1 = FN
}

trait Reducer[T, U] {
  def apply(ds: Dataset[T]*): Dataset[U]
  def +[V](f: F1[U, V]): Reducer[T, V] = (ds: Dataset[T]*) => f(apply(ds: _*)) // Reduce + F1 = Reduce
}

// X + F1 = X
