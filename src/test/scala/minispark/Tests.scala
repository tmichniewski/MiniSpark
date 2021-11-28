package com.github
package minispark

import minispark.Adder.{AdderInputType, AdderOutputType}
import minispark.Functions._
import minispark.Implicits.ExtendedDataset
import minispark.Incrementer.IncrementerParams
import minispark.Pattern._

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

import java.sql.{Date, Timestamp}

case class Record(id: Int, amount: Double, name: String, date: Date, time: Timestamp)

object Adder extends EnterprisePattern {
  trait AdderInputType { val a: Int }
  case class AdderInputValue(a: Int) extends AdderInputType
  trait AdderOutputType { val b: Int }
  case class AdderOutputValue(b: Int) extends AdderOutputType
  override type Input = AdderInputType
  override type Output = AdderOutputType
  override type Params = Option[Nothing]
  override def build(params: Option[Nothing] = None): AdderInputType => AdderOutputType =
    (input: AdderInputType) => AdderOutputValue(input.a + 1)
}

object Multiply extends ElasticPattern {
  trait MultiplyInputType { val a: Int }
  case class MultiplyInputValue(a: Int) extends MultiplyInputType
  trait MultiplyOutputType { val b: Int }
  case class MultiplyOutputValue(b: Int) extends MultiplyOutputType
  override type Input = MultiplyInputType
  override type Output = MultiplyOutputType
  override type Params = Option[Nothing]

  override def build(params: Params = None): Input => Output =
    (input: MultiplyInputType) => MultiplyOutputValue(2 * input.a)
}

object Incrementer extends GenericPattern {
  override type Input = Int
  override type Output = Int
  case class IncrementerParams(delta: Int)
  override type Params = IncrementerParams
  override def build(params: Params): Input => Output = _ + params.delta
}

trait AType { val a: Int }
case class A(name: String, a: Int) extends AType with AdderInputType
trait BType { val b: Int }
case class B(b: Int) extends BType
case class C(name: String, a: Int, b: Int) extends BType with AdderOutputType

class Tests extends AnyFunSuite {
  val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
  import spark.implicits._

  val d: Date = Date.valueOf("2021-10-01")
  val t: Timestamp = Timestamp.valueOf("2021-10-01 16:24:16")

  val rows: Seq[Record] = Seq(Record(1, 1.23, "Name1", d, t), Record(2, 2.46, "Name2", d, t))
  val df: DataFrame = rows.toDF()
  val ds: Dataset[Record] = rows.toDS()

  test("Test Spark") {
    assert(spark.range(1).count() == 1L)
  }

  test("Test ExtendedDataset ++") {
    val f: Function[Row, Row] = (d: Dataset[Row]) => d.withColumn("id", lit(1))
    assert((ds.drop("id") ++ f).count() == 2L)
  }

  test("Test ExtendedDataset multi ++") {
    val cacher: Function[Record, Record] = cache()
    val mapper: Function[Record, Record] = map((r: Record) => r.copy(id = -r.id))
    val sorter: Function[Record, Record] = sort("id")
    assert((ds ++ cacher ++ mapper ++ sorter).collect()(0).id == -2)

    val composer: Function[Record, Record] = cacher + mapper + sorter
    assert((ds ++ composer).collect()(0).id == -2)
  }

  test("Test ExtendedDataset +") {
    assert((ds + ds).count() == 4L)
  }

  test("Test ExtendedDataset -") {
    assert((ds - ds).count() == 0L)
  }

  test("Test ExtendedDataset *") {
    assert((ds * ds).count() == 2L)
  }

  test("Test ExtendedDataset -+-") {
    assert((ds -+- ds).count() == 0L)
  }

  test("Test ExtendedDataset |*|") {
    assert((ds |*| ds).count() == 4L)
  }

  test("Test ExtendedDataset |=|") {
    assert((ds |=| ds.limit(1) on Seq("id")).count() == 1L)
  }

  test("Test ExtendedDataset |=+|") {
    assert((ds |=+| ds.limit(1) on Seq("id")).count() == 2L)
  }

  test("Test ExtendedDataset |+=|") {
    assert((ds.limit(1) |+=| ds on Seq("id")).count() == 2L)
  }

  test("Test ExtendedDataset |+=+|") {
    assert((ds.filter("id = 1") |+=+| ds.filter("id = 2") on Seq("id")).count() == 2L)
  }

  test("Test Functions filter on String") {
    assert((ds ++ filter("id = 1")).count() == 1L)
  }

  test("Test Functions filter on Column") {
    assert((ds ++ filter(ds("id") === 1)).count() == 1L)
  }

  test("Test Functions select on Strings") {
    assert((ds ++ select("id", "name")).columns sameElements Array("id", "name"))
  }

  test("Test Functions select on Columns") {
    assert((ds ++ select($"id", ds("name"))).columns sameElements Array("id", "name"))
  }

  test("Test Functions add one column") {
    assert((ds ++ add("total", lit(1))).columns sameElements (ds.columns ++ Array("total")))
  }

  test("Test Functions add many columns") {
    assert((ds ++ add(("total", lit(1)), ("text", lit("Text")))).columns sameElements
      (ds.columns ++ Array("total", "text")))
  }

  test("Test Functions drop") {
    assert((ds ++ drop("id", "amount", "name")).columns sameElements Array("date", "time"))
  }

  test("Test Functions rename one column") {
    assert((ds ++ rename("id", "ident")).columns(0) == "ident")
  }

  test("Test Functions rename many columns") {
    assert((ds ++ rename(("id", "ident"), ("amount", "total"))).columns(1) == "total")
  }

  test("Test Functions cast one column") {
    assert((ds ++ cast("id", LongType)).dtypes(0)._2 == "LongType")
  }

  test("Test Functions cast many columns") {
    assert((ds ++ cast(("id", LongType), ("amount", LongType))).dtypes(1)._2 == "LongType")
  }

  test("Test Functions map") {
    assert((ds ++ map((r: Record) => r.copy(id = r.id + 1))).collect()(0).id == 2)
  }

  test("Test Functions flatMap") {
    assert((ds ++ flatMap((r: Record) => Seq(r.copy(id = r.id + 1)))).collect()(0).id == 2)
  }

  test("Test Functions agg") {
    assert((ds ++ agg(Seq("id"), Seq(("name", "count")))).collect()(0).getAs[Long]("count(name)") == 1L)
  }

  test("Test Functions union") {
    assert((ds ++ union(ds)).count() == 4L)
  }

  test("Test Functions subtract") {
    assert((ds ++ subtract(ds.limit(1))).count() == 1L)
  }

  test("Test Functions intersect") {
    assert((ds ++ intersect(ds.limit(1))).count() == 1L)
  }

  test("Test Functions delta") {
    assert((ds ++ delta(ds)).count() == 0L)
  }

  test("Test Functions cross") {
    assert((ds ++ cross(ds)).count() == 4L)
  }

  test("Test Functions inner") {
    assert((ds ++ inner(ds.limit(1), Seq("id"))).count() == 1L)
  }

  test("Test Functions left") {
    assert((ds ++ left(ds.limit(1), Seq("id"))).count() == 2L)
  }

  test("Test Functions right") {
    assert((ds.limit(1) ++ right(ds, Seq("id"))).count() == 2L)
  }

  test("TestFunctions full") {
    assert((ds.filter("id = 1") ++ full(ds.filter("id = 2"), Seq("id"))).count() == 2L)
  }

  test("Test Functions as") {
    assert((df ++ as[Record]()).collect()(0).id == 1)
  }

  test("Test Functions row") {
    assert((ds ++ row()).collect()(0).getAs[Int]("id") == 1)
  }

  test("Test Functions cache") {
    assert((ds ++ cache()).count() == 2L)
  }

  test("Test Functions sort on Strings") {
    assert((ds ++ sort("id", "name")).collect()(0).id == 1)
  }

  test("Test Functions sort on Columns") {
    assert((ds ++ sort(ds("id"), ds("name"))).collect()(0).id == 1)
  }

  test("Test Functions pipeline") {
    val cacher: Function[Record, Record] = cache()
    val mapper: Function[Record, Record] = map((r: Record) => r.copy(id = -r.id))
    val sorter: Function[Record, Record] = sort("id")
    assert((ds ++ pipeline(cacher, mapper, sorter)).collect()(0).id == -2)
  }

  test("Test Pattern SimplePattern") {
    object Adder extends SimplePattern[AType, B, Option[Nothing]] {
      override def build(params: Option[Nothing] = None): AType => B =
        (input: AType) => B(input.a + 1)
    }
    val ds: Dataset[A] = Seq(A("dummy", 1)).toDS()
    assert((ds ++ Adder[A](None)).collect()(0) == B(2))
  }

  test("Test Pattern TuplePattern") {
    object Adder extends TuplePattern[AType, B, Option[Nothing]] {
      override def build(params: Option[Nothing] = None): AType => B =
        (input: AType) => B(input.a + 1)
    }
    val ds: Dataset[A] = Seq(A("dummy", 1)).toDS()
    assert((ds ++ Adder[A](None)).collect()(0) == (A("dummy", 1), B(2)))
  }

  test("Test Pattern ConstructorPattern") {
    object Adder extends ConstructorPattern[AType, BType, Option[Nothing]] {
      override def build(params: Option[Nothing] = None): AType => BType =
        (input: AType) => B(input.a + 1)
    }
    val ds: Dataset[A] = Seq(A("dummy", 1)).toDS()
    implicit val constructor: (A, BType) => C = (r: A, output: BType) => C(r.name, r.a, output.b)
    assert((ds ++ Adder[A, C](None)).collect()(0) == C("dummy", 1, 2))
  }

  test("Test Pattern EnterprisePattern") {
    val ds: Dataset[A] = Seq(A("dummy", 1)).toDS()
    implicit val constructor: (A, AdderOutputType) => C = (r: A, output: AdderOutputType) => C(r.name, r.a, output.b)
    assert((ds ++ Adder[A, C](None)).collect()(0) == C("dummy", 1, 2))
  }

  test("Test Pattern ElasticPattern") {
    val ds: Dataset[A] = Seq(A("dummy", 1)).toDS()
    // implicit val constructor: (A, AdderOutputType) => C = (r: A, output: AdderOutputType) => C(r.name, r.a, output.b)
    // assert((ds ++ Adder[A, C](None)).collect()(0) == C("dummy", 1, 2))
  }

  test("Test Pattern GenericPattern") {
    val result: Dataset[Record] = ds ++ Incrementer[Record, Record](IncrementerParams(7), _.id,
      (r: Record, output: Incrementer.Output) => r.copy(id = output))
    assert(result.map(_.id).collect() sameElements Array(8, 9))
  }

  test("Test Pattern ThePattern") {
//    val result: Dataset[Record] = ds ++ Incrementer[Record, Record](IncrementerParams(7), _.id,
//      (r: Record, output: Incrementer.Output) => r.copy(id = output))
//    assert(result.map(_.id).collect() sameElements Array(8, 9))
  }
}