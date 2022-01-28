# MiniSpark

This is a Scala library for ETL processing to be used on top of Spark. It is simple in design but quite useful. Most of
it is implemented in pure Scala functions.

## Transform

The main concept of this library is an observation that every Spark query might be expressed as some sequence of
functions converting one `Dataset` into another.

In general, such functions converts `Dataset[T]` into `Dataset[U]`. In Scala, this might be expressed as
`Dataset[T] => Dataset[U]` and is equivalent to Scala one-argument function type `Function1[Dataset[T], Dataset[U]]`.

In fact, this is the second part - `Transform` - of classical `ETL` approach - `Extract`, `Transform`, `Load`.

One of standard methods in this trait is a method called `andThen` to sequentially apply two functions. In our case, for
such kind of function composition we prefer to use the `+` operator, but this is just an alias to `andThen` method.

Summing up, we define the following type, called `Transform`:

```scala
trait Transform[T, U] extends (Dataset[T] => Dataset[U]) {
  def +[V](t: Transform[U, V]): Transform[T, V] = (d: Dataset[T]) => (this andThen t)(d)
}
```

Please note that so far we used only an alias to standard `Function1` Scala trait and its `andThen` method.

Surprisingly, this very simple concept is present commonly in many Spark notebooks in the shape of methods like:

```scala
def func(d: DataFrame): DataFrame
```

However, if we have a few such methods, for example:

```scala
def a(d: DataFrame): DataFrame
def b(d: DataFrame): DataFrame
def c(d: DataFrame): DataFrame
```

and we want to use them in sequence, we would have to write them in the reverted order:

```scala
val result: DataFrame = c(b(a(df)))
```

while instead, we would like to use them in a more natural way, like in the pseudocode below:

```
df firstApply a thenApply b nextApply c
```

or even:

```
composedABC = a + b + c
df apply composedABC
```

where, instead of sequential application of methods `a`, `b` and `c` to `df`
we may want to construct one composed function `a + b + c` and apply it on `df` only once.

How to achieve this? Instead of such methods we prefer to instantiate lambda expressions of the `Transform` type, while
to illustrate this we additionally define a few schemas as case classes:

```scala
// input schema
final case class Person(
  firstName: String,
  lastName: String
)

// intermediate result schema
final case class PersonWithFullName(
  firstName: String,
  lastName: String,
  fullName: String
)

// output schema
final case class PersonWithGreeting(
  firstName: String,
  lastName: String,
  fullName: String,
  greeting: String
)

// first function
val addFullName: Transform[Person, PersonWithFullName] = { (d: Dataset[Person]) =>
  d.map { (p: Person) =>
    PersonWithFullName(
      firstName = p.firstName,
      lastName = p.lastName,
      fullName = p.firstName + ' ' + p.lastName
    )
  }
}

// second function
val addGreeting: Transform[PersonWithFullName, PersonWithGreeting] = { (d: Dataset[PersonWithFullName]) =>
  d.map { (p: PersonWithFullName) =>
    PersonWithGreeting(
      firstName = p.firstName,
      lastName = p.lastName,
      fullName = p.fullName,
      greeting = "Hello " + p.firstName
    )
  }
}
```

Then, having defined two such functions we may compose them, to achieve one function only:

```scala
val addFullNameAndGreeting: Transform[Person, PersonWithGreeting] = addFullName + addGreeting
```

and use it the classical function-call way:

```scala
val result: Dataset[PersonWithGreeting] = addFullNameAndGreeting(df)
```

or using the implicit `ExtendedDataset.++` composition operator defined later:

```scala
val result: Dataset[PersonWithGreeting] = df ++ addFullNameAndGreeting
```

All of this is equivalent to:

```scala
val result: Dataset[PersonWithGreeting] = addGreeting(addFullName(df))
```

but as you may see the classical function-call usage requires not only the reverted order of methods, but also
the `df` `DataFrame`
instance to apply the methods on. In our approach, we may separate the composition of functions from their application,
and this improves application decomposition, code reusability and readability.

With such a simple concept we may start building more and more useful functions using smaller ones as building blocks.

Please note that such a function might be also used in Spark `Dataset.transform` method, because our function is exactly
equal to the type of parameter to the `Dataset.transform` method.

In fact, our function might be perceived to be both - an alias to Scala `Function1` and a type of parameter for
Spark `Dataset.transform` method.

## Dataset composition with Transform

This library provides also a set of additional operators on Spark `Dataset`. They mainly provide operator-like names for
other Spark `Dataset` methods and let to use infix notation. One important extension to this set of operators is
the `++`
method, implemented - next to other operators - in the implicit class `ExtendedDataset`
(BTW - this could be expressed as an extension method in Scala 3, but so far in Spark we are in Scala 2):

```scala
implicit class ExtendedDataset[T](val d: Dataset[T]) extends AnyVal {
  def ++[U](t: Transform[T, U]): Dataset[U] = d transform t
```

The purpose of this method, which is yet another alias to standard - this time Spark - method called `Dataset.transform`
is to be able to compose expressions consisting of a `Dataset` with subsequent `Transform`, when such an expression will
produce another `Dataset`.

```scala
val person: Dataset[Person] = spark.read.parquet("person.parquet").as[Person]
val newPerson1: Dataset[PersonWithGreeting] = person ++ addFullName ++ addGreeting
// or
val newPerson2: Dataset[PersonWithGreeting] = person ++ (addFullName + addGreeting)
// or
val newPerson3: Dataset[PersonWithGreeting] = person ++ addFullNameAndGreeting
```

This is the core concept to shape Spark applications and express them as composition of such `Transform`s. Please note
that such `Transform`s are self-existing entities which might be stored as values and passed within the application,
while standard `Dataset` methods have always to be connected to the `Dataset` they are called on, and as a consequence
they cannot be reused or stored.

## Dataset implicit operators

In addition to the composition method on `Dataset`, there is a bunch of mentioned earlier operators, which are supposed
to shorten and simplify set operators on `Datasets` as well as joins.

### Dataset set operators

|Operator |Signature                             |
|---------|--------------------------------------|
|Union    |def +(other: Dataset[T]): Dataset[T]  |
|Subtract |def -(other: Dataset[T]): Dataset[T]  |
|Intersect|def *(other: Dataset[T]): Dataset[T]  |
|Delta    |def -+-(other: Dataset[T]): Dataset[T]|

### Dataset joins operators

|Join type       |Signature|
|----------------|------------------------------------------------------------|
|Cross join      |def &#124;*&#124;(other: Dataset[_]): Dataset[Row]          |
|Inner join      |def &#124;=&#124;[U](other: Dataset[U]): DatasetPair[T, U]  |
|Left outer join |def &#124;=+&#124;[U](other: Dataset[U]): DatasetPair[T, U] |
|Right outer join|def &#124;+=&#124;[U](other: Dataset[U]): DatasetPair[T, U] |
|Full outer join |def &#124;+=+&#124;[U](other: Dataset[U]): DatasetPair[T, U]|

## Sample typical Transforms

In addition to implemented `Dataset` operators there are also predefined `Transform`s. Basically, they only mimic
Spark `Dataset` methods, but the `Transform` type may set an interface to larger ones and due to the composition
operator the functions might be bigger and bigger and this way constitute the whole modules or subsystems.

|Operation       |Signature                                                                               |
|----------------|----------------------------------------------------------------------------------------|
|Filter rows     |def filter[T](condition: String): Transform[T, T]                                       |
|Filter rows     |def filter[T](condition: Column): Transform[T, T]                                       |
|Select columns  |def select[T](column: String, columns: String*): Transform[T, Row]                      |
|Select columns  |def select[T](columns: Column*): Transform[T, Row]                                      |
|Add column      |def add[T](column: String, value: Column): Transform[T, Row]                            |
|Add columns     |def add[T](columns: (String, Column)*): Transform[T, Row]                               |
|Drop columns    |def drop[T](columns: String*): Transform[T, Row]                                        |
|Rename column   |def rename[T](oldColumn: String, newColumn: String): Transform[T, Row]                  |
|Rename columns  |def rename[T](renameExpr: (String, String)*): Transform[T, Row]                         |
|Cast column     |def cast[T](column: String, newType: DataType): Transform[T, Row]                       |
|Cast columns    |def cast[T](typesExpr: (String, DataType)*): Transform[T, Row]                          |
|Map rows        |def map[T, U: Encoder](f: T => U): Transform[T, U]                                      |
|FlatMap rows    |def flatMap[T, U: Encoder](f: T => TraversableOnce[U]): Transform[T, U]                 |
|Aggregate       |def agg[T](groupBy: Seq[String], aggregations: Seq[(String, String)]): Transform[T, Row]|
|Aggregate       |def agg[T](groupBy: Seq[String], expr: Column, exprs: Column*): Transform[T, Row]       |
|Union           |def union[T](other: Dataset[T]): Transform[T, T]                                        |
|Subtract        |def subtract[T](other: Dataset[T]): Transform[T, T]                                     |
|Intersect       |def intersect[T](other: Dataset[T]): Transform[T, T]                                    |
|Delta           |def delta[T](other: Dataset[T]): Transform[T, T]                                        |
|Cross join      |def cross[T](other: Dataset[_]): Transform[T, Row]                                      |
|Cross join      |def crossTyped[T, U](other: Dataset[U]): Transform[T, (T, U)]                           |
|Inner join      |def inner[T](other: Dataset[_], columns: Seq[String]): Transform[T, Row]                |
|Inner join      |def inner[T](other: Dataset[_], joinExpr: Column): Transform[T, Row]                    |
|Inner join      |def innerTyped[T, U](other: Dataset[U], joinExpr: Column): Transform[T, (T, U)]         |
|Left outer join |def left[T](other: Dataset[_], columns: Seq[String]): Transform[T, Row]                 |
|Left outer join |def left[T](other: Dataset[_], joinExpr: Column): Transform[T, Row]                     |
|Left outer join |def leftTyped[T, U](other: Dataset[U], joinExpr: Column): Transform[T, (T, U)]          |
|Right outer join|def right[T](other: Dataset[_], columns: Seq[String]): Transform[T, Row]                |
|Right outer join|def right[T](other: Dataset[_], joinExpr: Column): Transform[T, Row]                    |
|Right outer join|def rightTyped[T, U](other: Dataset[U], joinExpr: Column): Transform[T, (T, U)]         |
|Full outer join |def full[T](other: Dataset[_], columns: Seq[String]): Transform[T, Row]                 |
|Full outer join |def full[T](other: Dataset[_], joinExpr: Column): Transform[T, Row]                     |
|Full outer join |def fullTyped[T, U](other: Dataset[U], joinExpr: Column): Transform[T, (T, U)]          |
|Cast Dataset    |def as[T: Encoder] (): Transform[Row, T]                                                |
|Cast Dataset    |def row[T] (): Transform[T, Row]                                                        |
|Cache           |def cache[T] (): Transform[T, T]                                                        |
|Sort            |def sort[T](column: String, columns: String*): Transform[T, T]                          |
|Sort            |def sort[T](columns: Column*): Transform[T, T]                                          |
|Pipeline        |def pipeline[T](t: Transform[T, T], ts: Transform[T, T]*): Transform[T, T]              |

## The map pattern

The most typical operation being performed on a `Dataset` is the map function. It expects a function to convert input
record into output record. This may be enough to perform some not so complex transformations, and such a map operation
may work with one input schema and produce another one.

What to do, if we need something more complex, or we would like to be able to use it on a broader range of input or
output schemas. The answer to such a challenge is the map pattern which is an extension to the map function.

```scala
trait MapPattern {
  type Input
  type Output
  type Params

  def build(params: Params): Input => Output

  def apply[T, U: Encoder](params: Params, getter: T => Input, constructor: (T, Output) => U): Transform[T, U] = {
    map[T, U] {
      val mapper: Input => Output = build(params)
      (r: T) => constructor(r, mapper(getter(r)))
    }
  }
}
```

The map pattern is a type which defines a common interface to handle such use cases. It specifies the containers
for `Input`
and `Output` types, the container for parameters, and it expects to provide the function which will build the mapper
function for the given parameters and `Input` and `Output` types.

Then, it also provides the `apply` method which will give in return the map `Transform`, provided that it will get a
specific `getter` to convert the input record to the `Input` type and a `constructor` which will convert all the
produced data to the output record.

```scala
case class Record(id: Int, amount: Double, name: String, date: Date, time: Timestamp)

object Adder extends MapPattern {
  override type Input = Int // or any case class defined within the Adder object
  override type Output = Int // or any case class defined within the Adder object
  case class AdderParams(delta: Int)
  override type Params = AdderParams // or e.g. Option[Nothing] and then None in case of no parameters
  override def build(params: Params): Input => Output = _ + params.delta
}

val getter: Record => Int = (r: Record) => r.id
val constructor: (Record, Int) => Record = (r: Record, output: Adder.Output) => r.copy(id = output)
val adder: Transform[Record, Record] = Adder[Record, Record](AdderParams(7), getter, constructor)
val result: Dataset[Record] = ds ++ adder
// or in shorter version:
val result: Dataset[Record] = ds ++ Adder[Record, Record](AdderParams(7), _.id,
  (r: Record, output: Adder.Output) => r.copy(id = output))
// see the tests for more examples
```

Moreover, to make it work, input schema of `Record` does not have to contain an `id` column. Instead, it is the `getter`
responsibility to provide a value of `Adder.Input` type, even if this would require to carry this out through some not
so complex processing. In other words, if the input schema had no `id` column, then the `getter` method may derive it.

Consequently, the same with the `constructor` method, it is not required that output schema, here of type `Record`, is a
subclass of `Adder.Output` type, as it is the `constructor` responsibility to perform conversion from input record and
output of the mapper function
(produced by `build` factory method) to the output schema.

Finally, the most important is that the real logic is inside the mapper function which deals with `Input` and `Output`
types and these things are implemented inside the function of this pattern type. This is the logic of this
transformation.

Please note that this logic might be arbitrarily complex and be implemented using pure functions, while the `getter` and
the `constructor` are the only interfaces to input and output schemas, and they are provided not during the function
implementation, but during the real usage in a given context.

In other words, the `Transform` implemented according to such a pattern follows the typical pattern of ETL:

- Extract is the `getter`,
- Transform is the mapper function produced by factory method `build`,
- Load is the `constructor`.

The emphasis is that it is the mapper function - the middle one - that is the most complex. The other two are just
interfaces to input and output schemas - the outside world.

Please note that such a pattern might be applied to a wide range of use cases, starting from a plain function, through a
bigger function covering some business related use cases and ending on the big functions representing whole submodules.
So, this is the decision of the user where and how to use it. Moreover, this is also possible to encapsulate functions
implemented with this pattern inside other functions also implemented with this pattern, and so on.

## Example of using Transform type

As an example toy application we implement word count query which in Spark is a "hello world" application.

Let us start from the original simple solution:

```scala
val df: DataFrame = spark.read.text("<path>")
df.as[String].flatMap(_.split(" ")).groupBy("value").count()
```

Then, we do the same using `Transform`s:

```scala
val df: DataFrame = spark.read.text("<path>")
val castToDataset: Transform[Row, String] = as[String]()
val splitter: Transform[String, String] = flatMap[String, String](_.split(" "))
val aggregator: Transform[String, Row] = agg[String](Seq("value"), Seq(("value", "count")))
df ++ castToDataset ++ splitter ++ aggregator
```

Please note that in this example we explicitly defined the types of particular `Transform`s, while there is no such
possibility in the standard Spark approach. Alternatively, we could also skip the types and use Scala type inference
mechanism:

```scala
val df: DataFrame = spark.read.text("<path>")
val castToDataset = as[String]()
val splitter = flatMap[String, String](_.split(" "))
val aggregator = agg[String](Seq("value"), Seq(("value", "count")))
df ++ castToDataset ++ splitter ++ aggregator
```

or even in a more compact way:

```scala
val df: DataFrame = spark.read.text("<path>")
val aggregator = flatMap[String, String](_.split(" ")) +
  agg[String](Seq("value"), Seq(("value", "count")))
df ++ as[String]() ++ aggregator
```

which gives plenty of possibilities including reusing of the aggregator `Transform` in any place, not only on this `df`
`DataFrame` instance.

So, having such API we have more freedom in reusing pieces of implementation as well as possibilities to encapsulate
series of Spark calls within reusable `Transform`s. And these are the building blocks of enterprise class systems which
might be composed of such `Transform`s.

## Algebra

So far we described the `Transform` and how it interacts with the `Dataset`. Now, we try to go a step further and try to
define a complete set of operations covering the classical approach of `ETL` which stands for:

- Extract,
- Transform,
- Load.

First, we define the `Extract` part which stands for producing the data. In our case we model this as a parameterless
function which returns a `Dataset`.

```scala
trait Extract[T] extends (() => Dataset[T])
```

Then, we define the `Transform` part which stands for transforming the data. In our case we model this as a function
from one `Dataset` into another one.

```scala
trait Transform[T, U] extends (Dataset[T] => Dataset[U])
```

Next, we define the `Load` part which stands for consuming the data. In our case we also model this as a function, this
time from a `Dataset` into `Unit`, as in this case we are not interested in any result, but call it only to perform the
side effects of saving the data somewhere.

```scala
trait Load[T] extends (Dataset[T] => Unit)
```

Additionally, we define a type for composition of functions `Extract`,`Transform` and `Load` called `ETL`:

```scala
trait ETL extends (() => Unit)
```

and a type for the function to combine two `Dataset`s:

```scala
trait Combine[T, U, V] extends ((Dataset[T], Dataset[U]) => Dataset[V])
```

Finally, we define the type for modeling a pair of `Extract`s:

```scala
class ExtractPair[T, U](e1: Extract[T], e2: Extract[U])
```

At the end, we define a set of operations on those types which in general serve as composition operators. Their purpose
is to let compose those types together in the following scenarios:

- `Extract[T]` + `Extract[U]` gives `ExtractPair[T, U]`,
- `Extract[T]` + `Transform[T, U]` gives `Extract[U]`,
- `Extract[T]` + `Load[T]` gives `ETL`,
- `Transform[T, U]` + `Transform[U, V]` gives `Transform[T, V]`,
- `Transform[T, U]` + `Load[U]` gives `Load[T]`,
- `ExtractPair[T, U]` + `Combine[T, U, V]` gives `Extract[V]`.

and some helper methods, for example to get split of an `Extract`.

Below is the structure of those types together with the methods they automatically provide.

```scala
trait ETL extends (() => Unit) {
  def run(): Unit = apply()
}

trait Extract[T] extends (() => Dataset[T]) {
  def +[U](e: Extract[U]): ExtractPair[T, U] = ExtractPair[T, U](this, e) // E + E => E2
  def +[U](t: Transform[T, U]): Extract[U] = () => t(apply()) // E + T => E
  def +(l: Load[T]): ETL = () => l(apply()) // E + L => ETL
  def split: Extract[T] = { // E => cached E
    lazy val d: Dataset[T] = apply().cache()
    () => d
  }
}

trait Transform[T, U] extends (Dataset[T] => Dataset[U]) {
  def +[V](t: Transform[U, V]): Transform[T, V] = (d: Dataset[T]) => d.transform(this andThen t) // T + T => T
  def +(l: Load[U]): Load[T] = (d: Dataset[T]) => (this andThen l)(d) // T + L => L
}

trait Load[T] extends (Dataset[T] => Unit)

trait Combine[T, U, V] extends ((Dataset[T], Dataset[U]) => Dataset[V])

class ExtractPair[T, U](e1: Extract[T], e2: Extract[U]) {
  def +[V](c: Combine[T, U, V]): Extract[V] = () => c(e1(), e2()) // E2 + C => E
}
object ExtractPair {
  def apply[T, U](e1: Extract[T], e2: Extract[U]): ExtractPair[T, U] = new ExtractPair[T, U](e1, e2)
}
```

## Example of using algebra types

Having defined such operations we may start building higher level pipelines of operations, for example (assuming that
`Ei` stands for `Extract`s, `Ti` stands for `Transform`s, `Li` stands for `Load`s and so forth):

```scala
val e1: Extract[Row] = e0 + t1 + t2 + t3
val e: Extract[Row] = e0 + e1 + c1
val t: Transform[Row, Row] = t5 + t6
val l: Load[Row] = t7 + l1
val etl: ETL = e + t + l
etl()
// or
etl.run()
// or simply
e + t + l run() // to avoid instantiating of etl variable
```

## How to use the library

If you implement a relatively small plain processing in a notebook, you may use the sole concept of Transform composed
with `Dataset` via `++` operator and use it together with standard `Dataset` IO operations like:

```scala
val dsEmployee: Dataset[Employee] = spark.read.parquet("<path>")
val tr: Transform[Employee, Department] = map((e: Employee) => Department(e.deptNo))
val dsDepartment: Dataset[Department] = dsEmployee ++ tr
```

However, if you implement an enterprise class system, you may want to consider the full flavour of this API including
abstractions for extracting and loading like this:

```scala
val e: Extract[Employee] = extractParquet("<input_path>")
val t: Transform[Employee, Department] = map((e: Employee) => Department(e.deptNo))
val l: Load[Department] = loadParquet("<output_path>")
e + t + l run()
```

where you have more options to compose larger extractors, transformers and loaders, and thus have more freedom to
elegantly compose them to build large application.

## Summary

To sum up, this library consists of:

- usage of the `Dataset` type as the main data representation,
- the `ExtendedDataset.++` operator which is an alias to `Dataset.transform` method,
- set of implicits which provide aliases to typical `Dataset` operations,
- the `Transform[T, U]` type which is an alias to Scala `Function1[Dataset[T], Dataset[U]]` type,
- the `Transform.+` composition operator which is an alias to Scala `Function1.andThen`,
- set of methods producing typical Spark `Transform`s as one-liner aliases to Spark counterparts,
- set of types extending `Transform` type to cover the whole `ETL` process: `Extract`, `Load`, `Combine`,
- set of composition `+` operators to glue together different types of the algebra,
- and finally the `MapPattern` which seems to cover the most typical use cases.

In turn, the map pattern has the following features:

- provides the contract or an interface to build the map `Transform`,
- specifies `Input`, `Output` and `Params` containers for data, typically in the form of case class only or some
  standard type,
- no necessity to create any traits,
- common API for creating the mapper function in the form of abstract `build` method,
- standard `apply` method which returns the map `Transform`,
- the `apply` method which is type parameterized, but those types `[T, U]` do not have to be subclasses of `Input`
  and `Output` respectively,
- instead, the `apply` method uses the generic and functional interface to input and output records in the form
  of `getter` and `constructor` functions.

## Final word

Concluding, this library is not about the API, which hardly brings anything new.

Instead, it is about the thinking. The thinking about building enterprise class systems and their decomposition into
smaller parts. Thinking about how to shape the small pieces of the system, and then, how to glue them together.

Nowadays, if we ask ourselves what is the biggest challenge of modern software engineering, it may turn out that this is
a complexity, because the systems are becoming bigger and bigger. So, how we address this challenge? We give a
programming model to decompose the system into smaller parts and express them via the `Transform`s, which then might be
composed back to constitute the whole application.

Summing up, this library is a Scala story about systems decomposition, or in other words, about functions composition,
while it turns out that the core element of this puzzle is the plain function which is the most fundamental part of any
system implementation.

## Versions

|Version|Date      |Description                                             |
|-------|----------|--------------------------------------------------------|
|3.0.0  |2022-01-23|Merge Function into Transform. Remove algebra package.  |
|2.2.0  |2022-01-20|Add algebra of ETL operations.                          |
|2.1.1  |2021-12-27|Update readme.                                          |
|2.1.0  |2021-12-26|Added Column version of agg function.                   |
|2.0.0  |2021-12-25|Remove FunctionTransformer, FX Types and trans Function.|
|1.5.6  |2021-12-23|Update comments.                                        |
|1.5.5  |2021-12-23|Update comments and refactor F1.+ method.               |
|1.5.4  |2021-12-21|Refactor.                                               |
|1.5.3  |2021-12-18|Simplify Function definition.                           |
|1.5.2  |2021-12-18|Use Dataset.transform in composition methods.           |
|1.5.1  |2021-12-15|Add more tests.                                         |
|1.5.0  |2021-12-12|Add prototype of core types.                            |
|1.4.1  |2021-12-11|Clean the code.                                         |
|1.4.0  |2021-12-08|Update saving and loading of FunctionTransformer.       |
|1.3.0  |2021-12-07|Replace FunctionTransformer with parameterized version. |
|1.2.1  |2021-12-06|Amend FunctionTransformer to use extendable class.      |
|1.2.0  |2021-12-05|Add integration with Spark ML.                          |
|1.1.0  |2021-12-04|Add joins on Column and typed joins.                    |
|1.0.0  |2021-11-28|First version.                                          |
