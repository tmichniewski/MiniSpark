package com.github
package minispark

import org.apache.spark.sql.Dataset

// TODO Add coverage

trait F0[T] extends (() => Dataset[T]) {
  def +[U](f0u: F0[U]): F0F0[T, U] = F0F0[T, U](this, f0u) // F0 + F0 = (F0, F0)
  def +[U](f1tu: F1[T, U]): F0[U] = () => f1tu(apply()) // F0 + F1 = F0
  def +[U, V](f2tuv: F2[T, U, V]): F1[U, V] = f2tuv(apply(), _) // F0 + F2 = F1
}

class F0F0[T, U](val f0t: F0[T], val f0u: F0[U]) {
  def +[V](f2tuv: F2[T, U, V]): F0[V] = () => f2tuv(f0t(), f0u()) // (F0, F0) + F2 = F0
}
@deprecated("Not necessary, use it through f2 on the class")
object F0F0 {
  def apply[T, U](f0t: F0[T], f0u: F0[U]) = new F0F0[T, U](f0t, f0u)
}

trait F1[T, U] extends (Dataset[T] => Dataset[U]) {
  def +[V](f1uv: F1[U, V]): F1[T, V] = (d: Dataset[T]) => f1uv(apply(d)) // F1 + F1 = F1
}

trait F2[T, U, V] extends ((Dataset[T], Dataset[U]) => Dataset[V]) {
  def +[W](f1vw: F1[V, W]): F2[T, U, W] = (d1: Dataset[T], d2: Dataset[U]) => f1vw(apply(d1, d2)) // F2 + F1 = F2
}

trait FN[T, U] extends (Seq[Dataset[T]] => Dataset[U]) {
  def +[W](f1uw: F1[U, W]): FN[T, W] = (ds: Seq[Dataset[T]]) => f1uw(apply(ds)) // FN + F1 = FN
}

// X + F1 = X
