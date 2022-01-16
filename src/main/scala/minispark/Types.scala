package com.github
package minispark

import org.apache.spark.sql.Dataset

trait ETL extends (() => Unit)

trait E[X] extends (() => Dataset[X]) {
  def +[Y](e: E[Y]): E2[X, Y] = E2[X, Y](this, e) // E + E = E2
  def +[Y](t: T[X, Y]): E[Y] = () => t(apply()) // E + T = E
  def +(l: L[X]): ETL = () => l(apply()) // E + L = ETL
  def split: S[X] = S(this)
}
// E + E * C = E + T = E

case class E2[X, Y](e1: E[X], e2: E[Y]) {
  def +[Z](c: C[X, Y, Z]): E[Z] = () => c(e1(), e2()) // E + E + C = E
}

case class S[X](e: E[X]) extends E[X] {
  lazy val d: Dataset[X] = e().cache()
  override def apply(): Dataset[X] = d
}

trait T[X, Y] extends (Dataset[X] => Dataset[Y]) {
  def +[Z](t: T[Y, Z]): T[X, Z] = (d: Dataset[X]) => t(apply(d)) // T + T = T
  def +(l: L[Y]): L[X] = (d: Dataset[X]) => l(apply(d)) // T + L = L
}
// (T + T) ++ C = T + (T ++ C) ?

trait L[X] extends (Dataset[X] => Unit)

trait C[X, Y, Z] extends ((Dataset[X], Dataset[Y]) => Dataset[Z])
