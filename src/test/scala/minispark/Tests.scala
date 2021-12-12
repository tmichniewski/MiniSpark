package com.github
package minispark

import minispark.Adder.{AdderInput, AdderOutput, AdderParams}
import minispark.Functions._
import minispark.Implicits.ExtendedDataset
import minispark.Serialize.{deserialize, serialize}

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

import java.sql.{Date, Timestamp}

case class Record(id: Int, amount: Double, name: String, date: Date, time: Timestamp)

object Adder extends MapPattern {
  case class AdderInput(value: Int)
  case class AdderOutput(value: Int)
  override type Input = AdderInput
  override type Output = AdderOutput
  case class AdderParams(delta: Int)
  override type Params = AdderParams
  override def build(params: Params): Input => Output =
    (r: AdderInput) => AdderOutput(r.value + params.delta)
}

class Tests extends AnyFunSuite {
  val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
  import spark.implicits._

  val d: Date = Date.valueOf("2021-11-28")
  val t: Timestamp = Timestamp.valueOf("2021-11-28 12:34:56")

  val rows: Seq[Record] = Seq(Record(1, 1.23, "Name1", d, t), Record(2, 2.46, "Name2", d, t))
  val df: DataFrame = rows.toDF().alias("df")
  val ds: Dataset[Record] = rows.toDS().alias("ds")

  test("Test Spark") {
    assert(spark.range(1).count() == 1L)
  }

  test("Test ExtendedDataset: ++") {
    val f: Function[Row, Row] = (d: Dataset[Row]) => d.withColumn("id", lit(1))
    assert((ds.drop("id") ++ f).count() == 2L)
  }

  test("Test ExtendedDataset: multi ++") {
    val cacher: Function[Record, Record] = cache()
    val mapper: Function[Record, Record] = map((r: Record) => r.copy(id = -r.id))
    val sorter: Function[Record, Record] = sort("id")
    assert((ds ++ cacher ++ mapper ++ sorter).collect()(0).id == -2)

    val composer: Function[Record, Record] = cacher + mapper + sorter
    assert((ds ++ composer).collect()(0).id == -2)
  }

  test("Test ExtendedDataset: +") {
    assert((ds + ds).count() == 4L)
  }

  test("Test ExtendedDataset: -") {
    assert((ds - ds).count() == 0L)
  }

  test("Test ExtendedDataset: *") {
    assert((ds * ds).count() == 2L)
  }

  test("Test ExtendedDataset: -+-") {
    assert((ds -+- ds).count() == 0L)
  }

  test("Test ExtendedDataset: |*|") {
    assert((ds |*| ds).count() == 4L)
  }

  test("Test ExtendedDataset: |=| on Strings") {
    assert((ds |=| ds.limit(1) on Seq("id")).count() == 1L)
  }

  test("Test ExtendedDataset: |=| on Column") {
    val ds1: Dataset[Record] = ds.limit(1).alias("ds1")
    assert((ds |=| ds1 on col("ds.id") === col("ds1.id")).count() == 1L)
  }

  test("Test ExtendedDataset: |=| onTyped Column") {
    val ds1: Dataset[Record] = ds.limit(1).alias("ds1")
    assert((ds |=| ds1 onTyped col("ds.id") === col("ds1.id")).count() == 1L)
  }

  test("Test ExtendedDataset: |=+| on Strings") {
    assert((ds |=+| ds.limit(1) on Seq("id")).count() == 2L)
  }

  test("Test ExtendedDataset: |=+| on Column") {
    val ds1: Dataset[Record] = ds.limit(1).alias("ds1")
    assert((ds |=+| ds1 on col("ds.id") === col("ds1.id")).count() == 2L)
  }

  test("Test ExtendedDataset: |=+| onTyped Column") {
    val ds1: Dataset[Record] = ds.limit(1).alias("ds1")
    assert((ds |=+| ds1 onTyped col("ds.id") === col("ds1.id")).count() == 2L)
  }

  test("Test ExtendedDataset: |+=| on Strings") {
    assert((ds.limit(1) |+=| ds on Seq("id")).count() == 2L)
  }

  test("Test ExtendedDataset: |+=| on Column") {
    val ds1: Dataset[Record] = ds.limit(1).alias("ds1")
    assert((ds1 |+=| ds on col("ds1.id") === col("ds.id")).count() == 2L)
  }

  test("Test ExtendedDataset: |+=| onTyped Column") {
    val ds1: Dataset[Record] = ds.limit(1).alias("ds1")
    assert((ds1 |+=| ds onTyped col("ds1.id") === col("ds.id")).count() == 2L)
  }

  test("Test ExtendedDataset: |+=+| on Strings") {
    assert((ds.filter("id = 1") |+=+| ds.filter("id = 2") on Seq("id")).count() == 2L)
  }

  test("Test ExtendedDataset: |+=+| on Column") {
    val ds1: Dataset[Record] = ds.filter("id = 1").alias("ds1")
    val ds2: Dataset[Record] = ds.filter("id = 2").alias("ds2")
    assert((ds1 |+=+| ds2 on col("ds1.id") === col("ds2.id")).count() == 2L)
  }

  test("Test ExtendedDataset: |+=+| onTyped Column") {
    val ds1: Dataset[Record] = ds.filter("id = 1").alias("ds1")
    val ds2: Dataset[Record] = ds.filter("id = 2").alias("ds2")
    assert((ds1 |+=+| ds2 onTyped col("ds1.id") === col("ds2.id")).count() == 2L)
  }

  test("Test Functions: filter on String") {
    assert((ds ++ filter[Record]("id = 1")).count() == 1L)
  }

  test("Test Functions: filter on Column") {
    assert((ds ++ filter[Record](ds("id") === 1)).count() == 1L)
  }

  test("Test Functions: select on Strings") {
    assert((ds ++ select[Record]("id", "name")).columns sameElements Array("id", "name"))
  }

  test("Test Functions: select on Columns") {
    assert((ds ++ select[Record]($"id", ds("name"))).columns sameElements Array("id", "name"))
  }

  test("Test Functions: add one column") {
    assert((ds ++ add[Record]("total", lit(1))).columns sameElements (ds.columns ++ Array("total")))
  }

  test("Test Functions: add many columns") {
    assert((ds ++ add[Record](("total", lit(1)), ("text", lit("Text")))).columns sameElements
      (ds.columns ++ Array("total", "text")))
  }

  test("Test Functions: drop") {
    assert((ds ++ drop[Record]("id", "amount", "name")).columns sameElements Array("date", "time"))
  }

  test("Test Functions: rename one column") {
    assert((ds ++ rename[Record]("id", "ident")).columns(0) == "ident")
  }

  test("Test Functions: rename many columns") {
    assert((ds ++ rename[Record](("id", "ident"), ("amount", "total"))).columns(1) == "total")
  }

  test("Test Functions: cast one column") {
    assert((ds ++ cast[Record]("id", LongType)).dtypes(0)._2 == "LongType")
  }

  test("Test Functions: cast many columns") {
    assert((ds ++ cast[Record](("id", LongType), ("amount", LongType))).dtypes(1)._2 == "LongType")
  }

  test("Test Functions: map") {
    assert((ds ++ map[Record, Record]((r: Record) => r.copy(id = r.id + 1))).collect()(0).id == 2)
  }

  test("Test Functions: flatMap") {
    assert((ds ++ flatMap[Record, Record]((r: Record) => Seq(r.copy(id = r.id + 1)))).collect()(0).id == 2)
  }

  test("Test Functions: agg") {
    assert((ds ++ agg[Record](Seq("id"), Seq(("name", "count")))).collect()(0).getAs[Long]("count(name)") == 1L)
  }

  test("Test Functions: union") {
    assert((ds ++ union[Record](ds)).count() == 4L)
  }

  test("Test Functions: subtract") {
    assert((ds ++ subtract[Record](ds.limit(1))).count() == 1L)
  }

  test("Test Functions: intersect") {
    assert((ds ++ intersect[Record](ds.limit(1))).count() == 1L)
  }

  test("Test Functions: delta") {
    assert((ds ++ delta[Record](ds)).count() == 0L)
  }

  test("Test Functions: cross") {
    assert((ds ++ cross[Record](ds)).count() == 4L)
  }

  test("Test Functions: crossTyped") {
    assert((ds ++ crossTyped[Record, Record](ds)).count() == 4L)
  }

  test("Test Functions: inner on String") {
    assert((ds ++ inner[Record](ds.limit(1), Seq("id"))).count() == 1L)
  }

  test("Test Functions: inner on Column") {
    val ds1: Dataset[Record] = ds.limit(1).alias("ds1")
    assert((ds ++ inner[Record](ds1, col("ds.id") === col("ds1.id"))).count() == 1L)
  }

  test("Test Functions: innerTyped on Column") {
    val ds1: Dataset[Record] = ds.limit(1).alias("ds1")
    assert((ds ++ innerTyped[Record, Record](ds1, col("ds.id") === col("ds1.id"))).count() == 1L)
  }

  test("Test Functions: left on String") {
    assert((ds ++ left[Record](ds.limit(1), Seq("id"))).count() == 2L)
  }

  test("Test Functions: left on Column") {
    val ds1: Dataset[Record] = ds.limit(1).alias("ds1")
    assert((ds ++ left[Record](ds1, col("ds.id") === col("ds1.id"))).count() == 2L)
  }

  test("Test Functions: leftTyped on Column") {
    val ds1: Dataset[Record] = ds.limit(1).alias("ds1")
    assert((ds ++ leftTyped[Record, Record](ds1, col("ds.id") === col("ds1.id"))).count() == 2L)
  }

  test("Test Functions: right on String") {
    assert((ds.limit(1) ++ right[Record](ds, Seq("id"))).count() == 2L)
  }

  test("Test Functions: right on Column") {
    val ds1: Dataset[Record] = ds.limit(1).alias("ds1")
    assert((ds1 ++ right[Record](ds, col("ds1.id") === col("ds.id"))).count() == 2L)
  }

  test("Test Functions: rightTyped on Column") {
    val ds1: Dataset[Record] = ds.limit(1).alias("ds1")
    assert((ds1 ++ rightTyped[Record, Record](ds, col("ds1.id") === col("ds.id"))).count() == 2L)
  }

  test("Test Functions: full on String") {
    assert((ds.filter("id = 1") ++ full[Record](ds.filter("id = 2"), Seq("id"))).count() == 2L)
  }

  test("Test Functions: full on Column") {
    val ds1: Dataset[Record] = ds.filter("id = 1").alias("ds1")
    val ds2: Dataset[Record] = ds.filter("id = 2").alias("ds2")
    assert((ds1 ++ full[Record](ds2, col("ds1.id") === col("ds2.id"))).count() == 2L)
  }

  test("Test Functions: fullTyped on Column") {
    val ds1: Dataset[Record] = ds.filter("id = 1").alias("ds1")
    val ds2: Dataset[Record] = ds.filter("id = 2").alias("ds2")
    assert((ds1 ++ fullTyped[Record, Record](ds2, col("ds1.id") === col("ds2.id"))).count() == 2L)
  }

  test("Test Functions: as") {
    assert((df ++ as[Record]()).collect()(0).id == 1)
  }

  test("Test Functions: row") {
    assert((ds ++ row[Record]()).collect()(0).getAs[Int]("id") == 1)
  }

  test("Test Functions: cache") {
    assert((ds ++ cache[Record]()).count() == 2L)
  }

  test("Test Functions: sort on Strings") {
    assert((ds ++ sort[Record]("id", "name")).collect()(0).id == 1)
  }

  test("Test Functions: sort on Columns") {
    assert((ds ++ sort[Record](ds("id"), ds("name"))).collect()(0).id == 1)
  }

  test("Test Functions: pipeline") {
    val cacher: Function[Record, Record] = cache[Record]()
    val mapper: Function[Record, Record] = map[Record, Record]((r: Record) => r.copy(id = -r.id))
    val sorter: Function[Record, Record] = sort[Record]("id")
    assert((ds ++ pipeline[Record](cacher, mapper, sorter)).collect()(0).id == -2)
  }

  test("Test Functions: trans") {
    val ft: FunctionTransformer = FunctionTransformer()
    ft.setSchema(Seq(("id", IntegerType)))
    ft.setFunction(filter[Row]("id = 1"))
    val func: Function[Row, Row] = trans(ft)
    val result: Dataset[Row] = df ++ func
    assert(result.count() == 1L)
  }

  test("Test Functions: trans with copy") {
    val ft: FunctionTransformer = FunctionTransformer()
    ft.setSchema(Seq(("id", IntegerType)))
    ft.setFunction(filter[Row]("id = 1"))
    val func: Function[Row, Row] = trans(ft.copy(ParamMap()))
    val result: Dataset[Row] = df ++ func
    assert(result.count() == 1L)
  }

  test("Test FunctionTransformer") {
    val ft: FunctionTransformer = FunctionTransformer()
    ft.setSchema(Seq(("id", IntegerType)))
    ft.setFunction(filter[Row]("id = 1"))
    val result: DataFrame = ft.transform(ds)
    assert(result.count() == 1L)
  }

  test("Test FunctionTransformer - incorrect schema") {
    val ft: FunctionTransformer = FunctionTransformer()
    ft.setSchema(Seq(("dummy", IntegerType)))
    ft.setFunction(filter[Row]("id = 1"))
    assertThrows[RuntimeException](ft.transform(ds))
  }

  //test("Test FunctionTransformer - save and load") {
  //  import java.io.File
  //  import scala.reflect.io.Directory
  //  def deleteDirectory(path: String): Unit = new Directory(new File(path)).deleteRecursively()
  //  deleteDirectory("C:/TEMP/transformer.json")
  //  val ft: FunctionTransformer = FunctionTransformer()
  //  ft.setSchema(Seq(("id", IntegerType)))
  //  ft.setFunction(filter[Row]("id = 1"))
  //  ft.save("C:/TEMP/transformer.json")
  //  val transformer: FunctionTransformer = FunctionTransformer.load("C:/TEMP/transformer.json")
  //  val result: DataFrame = transformer.transform(ds)
  //  assert(result.count() == 1L)
  //}

  test("Test the Pattern") {
    val result: Dataset[Record] = ds ++ Adder[Record, Record](AdderParams(7),
      (r: Record) => AdderInput(r.id),
      (r: Record, output: AdderOutput) => r.copy(id = output.value))
    assert(result.map(_.id).collect() sameElements Array(8, 9))
  }

  test("Test the Pattern with intermediate variables") {
    val getter: Record => AdderInput = (r: Record) => AdderInput(r.id)
    val constructor: (Record, AdderOutput) => Record =
      (r: Record, output: AdderOutput) => r.copy(id = output.value)
    val adder: Function[Record, Record] = Adder[Record, Record](AdderParams(7), getter, constructor)
    val result: Dataset[Record] = ds ++ adder
    assert(result.map(_.id).collect() sameElements Array(8, 9))
  }

  test("Test schema serialize and deserialize") {
    val schema: Seq[(String, DataType)] = Seq(("id", IntegerType), ("amount", DoubleType), ("name", StringType),
      ("date", DateType), ("time", TimestampType))
    val serializedSchema: String = serialize(schema)
    val deserializedSchema: Seq[(String, DataType)] =
      deserialize(serializedSchema).asInstanceOf[Seq[(String, DataType)]]
    assert(schema == deserializedSchema)
  }

  test("Test function serialize and deserialize") {
    val function: Int => Int = (i: Int) => i + 1
    val serializedFunction: String = serialize(function)
    val deserializedFunction: Int => Int = deserialize(serializedFunction).asInstanceOf[Int => Int]
    assert(function(1) == 2)
    assert(deserializedFunction(1) == 2)
  }

  test("Types: F0 + F1") {
    val f0: F0[Record] = () => ds
    val f1: F1[Record, Record] = (d: Dataset[Record]) => d.map((r: Record) => r.copy(id = 2 * r.id))
    val result: Dataset[Record] = (f0 + f1)()
    assert(result.collect()(0).id == 2)
  }

  test("Types: F0 + F2") {
    val f0: F0[Record] = () => ds
    val f2: F2[Record, Record, (Record, Record)] =
      (d1: Dataset[Record], d2:Dataset[Record]) => d1.joinWith[Record](d2, d1("id") === d2("id"))
    val f0f2: F1[Record, (Record, Record)] = f0 + f2
    val result: Dataset[(Record, Record)] = (f0 + f0f2)()
    assert(result.count() == 2)
  }

  test("Types: F0 + F0 + F2") {
    val f0: F0[Record] = () => ds
    val f2: F2[Record, Record, (Record, Record)] =
      (d1: Dataset[Record], d2:Dataset[Record]) => d1.joinWith[Record](d2, d1("id") === d2("id"))
    val f0f0f2: F0[(Record, Record)] = f0 + f0 + f2
    val result: Dataset[(Record, Record)] = f0f0f2()
    assert(result.count() == 2L)
  }

  test("Types: F1 + F1") {
    val f1: F1[Record, Record] = (d: Dataset[Record]) => d.map((r: Record) => r.copy(id = 2 * r.id))
    val f1f1: F1[Record, Record] = f1 + f1
    val result: Dataset[Record] = f1f1(ds)
    assert(result.count() == 2L)
  }

  test("Types: F2 + F1") {
    val f2: F2[Record, Record, (Record, Record)] =
      (d1: Dataset[Record], d2:Dataset[Record]) => d1.joinWith[Record](d2, d1("id") === d2("id"))
    val f1: F1[(Record, Record), Record] = (d: Dataset[(Record, Record)]) => d.map(_._1)
    val f2f1: F2[Record, Record, Record] = f2 + f1
    val result: Dataset[Record] = f2f1(ds, ds)
    assert(result.count() == 2L)
    assert(result.collect()(0).id == 1L)
  }

  test("Types: FN + F1") {
    val fn: FN[Record, Record] = (ds: Seq[Dataset[Record]]) => ds.reduce(_ union _)
    val f1: F1[Record, Record] = (d: Dataset[Record]) => d.map((r: Record) => r.copy(id = 2 * r.id))
    val fnf1: FN[Record, Record] = fn + f1
    val result: Dataset[Record] = fnf1(Seq(ds, ds))
    assert(result.count() == 4L)
  }

  test("Test Functions: reduce") {
    assert(reduce[Record]()(Seq(ds, ds)).count() == 4L)
  }
}
