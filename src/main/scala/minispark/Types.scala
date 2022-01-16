package com.github
package minispark

import org.apache.spark.sql.Dataset

trait ETL extends (() => Unit)

trait Extract[X] extends (() => Dataset[X]) {
  def +[Y](e: Extract[Y]): ExtractPair[X, Y] = ExtractPair[X, Y](this, e) // E + E = E2
  def +[Y](t: Transform[X, Y]): Extract[Y] = () => t(apply()) // E + T = E
  def +(l: Load[X]): ETL = () => l(apply()) // E + L = ETL
  def split: Split[X] = Split(this)
}
// E + E * C = E + T = E

case class ExtractPair[X, Y](e1: Extract[X], e2: Extract[Y]) {
  def +[Z](c: Combine[X, Y, Z]): Extract[Z] = () => c(e1(), e2()) // E + E + C = E
}

case class Split[X](e: Extract[X]) extends Extract[X] {
  lazy val d: Dataset[X] = e().cache()
  override def apply(): Dataset[X] = d
}

trait Transform[X, Y] extends (Dataset[X] => Dataset[Y]) {
  def +[Z](t: Transform[Y, Z]): Transform[X, Z] = (d: Dataset[X]) => t(apply(d)) // T + T = T
  def +(l: Load[Y]): Load[X] = (d: Dataset[X]) => l(apply(d)) // T + L = L
}
// (T + T) ++ C = T + (T ++ C) ?

trait Load[X] extends (Dataset[X] => Unit)

trait Combine[X, Y, Z] extends ((Dataset[X], Dataset[Y]) => Dataset[Z])
