package com.github
package minispark

import org.apache.spark.sql.Dataset

trait ETL extends (() => Unit)

trait Extract[X] extends (() => Dataset[X]) {
  def +[Y](extract: Extract[Y]): ExtractPair[X, Y] = ExtractPair[X, Y](this, extract) // E + E = E2
  def +[Y](transform: Transform[X, Y]): Extract[Y] = () => transform(apply()) // E + T = E
  def +(load: Load[X]): ETL = () => load(apply()) // E + L = ETL
  def split: Split[X] = Split(this)
}
// E + E * C = E + T = E

case class ExtractPair[X, Y](extract1: Extract[X], extract2: Extract[Y]) {
  def +[Z](combine: Combine[X, Y, Z]): Extract[Z] = () => combine(extract1(), extract2()) // E + E + C = E
}

case class Split[X](extract: Extract[X]) extends Extract[X] {
  lazy val d: Dataset[X] = extract().cache()
  override def apply(): Dataset[X] = d
}

trait Transform[X, Y] extends (Dataset[X] => Dataset[Y]) {
  def +[Z](transform: Transform[Y, Z]): Transform[X, Z] = (d: Dataset[X]) => transform(apply(d)) // T + T = T
  def +(load: Load[Y]): Load[X] = (d: Dataset[X]) => load(apply(d)) // T + L = L
}
// (T + T) ++ C = T + (T ++ C) ?

trait Load[X] extends (Dataset[X] => Unit)

trait Combine[X, Y, Z] extends ((Dataset[X], Dataset[Y]) => Dataset[Z])
