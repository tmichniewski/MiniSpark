package com.github
package minispark

import org.apache.spark.sql.Dataset

trait ETL extends (() => Unit)

trait E[X] extends (() => Dataset[X]) {
  def +[Y](e: E[Y]): E2[X, Y] = E2[X, Y](this, e) // E + E = E2
  def +[Y](t: T[X, Y]): E[Y] = () => t(apply()) // E + T = E
  def +(l: L[X]): ETL = () => l(apply()) // E + L = ETL
  def split: S[X] = S(this)
  def *[Y, Z](c: C[X, Y, Z]): T[Y, Z] = c(apply(), _) // E + C = T
  def *[Y, Z](c: C[Y, X, Z]): T[Y, Z] = c(_, apply()) // E + C = T
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
  def ++[Z, W](c: C[Y, Z, W]): C[X, Z, W] = (d1: Dataset[X], d2: Dataset[Z]) => c(apply(d1), d2) // T + C = C
  def ++[Z, W](c: C[Z, Y, W]): C[Z, X, W] = (d1: Dataset[Z], d2: Dataset[X]) => c(d1, apply(d2)) // T + C = C
}
// (T + T) ++ C = T + (T ++ C) ?

trait L[X] extends (Dataset[X] => Unit)

trait C[X, Y, Z] extends ((Dataset[X], Dataset[Y]) => Dataset[Z]) {
  def +[W](t: T[Z, W]): C[X, Y, W] = (d1: Dataset[X], d2: Dataset[Y]) => t(apply(d1, d2)) // C + T = C
  def ++(l: L[Z]): (Dataset[X], Dataset[Y]) => Unit = (d1: Dataset[X], d2: Dataset[Y]) => l(apply(d1, d2)) // C + L = L2
}

@deprecated
object types {
  def join[X, Y, Z](e1: E[X], e2: E[Y], combine: C[X, Y, Z]): E[Z] = () => combine(e1(), e2())
}
