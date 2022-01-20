package com.github
package minispark

import org.apache.spark.sql.Dataset

import scala.language.implicitConversions

trait ETL extends (() => Unit)

trait Extract[T] extends (() => Dataset[T]) {
  def +[U](e: Extract[U]): ExtractPair[T, U] = ExtractPair[T, U](this, e) // E + E => E2
  def +[U](t: Transform[T, U]): Extract[U] = () => t(apply()) // E + T => E
  def +(l: Load[T]): ETL = () => l(apply()) // E + L => ETL
  def split: Split[T] = Split(this) // E => cached E
}

trait Transform[T, U] extends Function[T, U] {
  def +[V](t: Transform[U, V]): Transform[T, V] = (d: Dataset[T]) => d.transform(this andThen t) // T + T => T
  def +(l: Load[U]): Load[T] = (d: Dataset[T]) => (this andThen l)(d) // T + L => L
}

trait Load[T] extends (Dataset[T] => Unit)

trait Combine[T, U, V] extends ((Dataset[T], Dataset[U]) => Dataset[V])

object ExtractPair {
  def apply[T, U](e1: Extract[T], e2: Extract[U]): ExtractPair[T, U] = new ExtractPair[T, U](e1, e2)
}
class ExtractPair[T, U](e1: Extract[T], e2: Extract[U]) {
  def +[V](c: Combine[T, U, V]): Extract[V] = () => c(e1(), e2()) // E2 + C => E
}

object Split {
  def apply[T](e: Extract[T]): Split[T] = new Split[T](e)
}
class Split[T](e: Extract[T]) extends Extract[T] {
  lazy val d: Dataset[T] = e().cache()
  override def apply(): Dataset[T] = d
}

object Types {
  implicit def functionToTransform[T, U](f: Function[T, U]): T Transform U = (d: Dataset[T]) => f(d)
}
