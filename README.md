# MiniSpark

This is Scala library to be used on top of Spark.
It is simple in design but quite useful in usage.
It is expressed in pure Scala functions.

## The Function

The main concept of this library is an observation that every Spark query
might be expressed as some sequence of functions
converting one Dataset into another Dataset.

In general, such a function converts `Dataset[T]` into `Dataset[U]`.
In Scala this might be expressed as `Dataset[T] => Dataset[U]`
and this is equivalent to Scala type (trait) `Function1[Dataset[T], Dataset[U]]`.

One of standard methods in this trait is a method called `andThen` to sequentially apply two functions.
In our case we prefer to use a name `+` for this kind of function composition.

Summing up, we define the following type called *the function*:

```
/**
 * The function represents any kind of transformation from one Dataset into another.
 *
 * @tparam T Type of input data.
 * @tparam U Type of output data.
 */
trait Function[T, U] extends (Dataset[T] => Dataset[U]) {
  /**
   * Sequential application of two functions. An alias to andThen method.
   *
   * @param f The function which will be applied next.
   * @tparam V Type of resulting data.
   * @return Returns the composed function.
   */
  def +[V](f: Function[U, V]): Function[T, V] = (d: Dataset[T]) => (this andThen f)(d) // <=> this.andThen(f)(d)
}
```

Please notice that so far we used only an alias to standard `Function1` Scala trait and one of its methods.

Surprisingly, this very simple concept is present commonly in any Spark notebook in the shape of methods like:

```def func(d: DataFrame): DataFrame```

But instead of methods we prefer to instantiate lambda expressions of the given type:

```
final case class Person(
  firstName: String,
  lastName: String
)
final case class PersonWithFullName(
  firstName: String,
  lastName: String,
  fullName: String
)
final case class PersonWithGreeting(
  firstName: String,
  lastName: String,
  fullName: String,
  greeting: String
)

val addFullName: Function[Person, PersonWithFullName] = { (d: Dataset[Person]) =>
  d.map { (p: Person) =>
    PersonWithFullName(
      firstName = p.firstName,
      lastName = p.lastName,
      fullName = p.firstName + ' ' + p.lastName
    )
  }
}

val addGreeting: Function[PersonWithFullName, PersonWithGreeting] = { (d: Dataset[PersonWithFullName]) =>
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

```
val addFullNameAndGreeting: Function[Person, PersonWithGreeting] = addFullName + addGreeting
```

Having such a simple concept we may start building more and more useful functions from some smaller ones.

Please also notice, that such a function might be also used in Spark `Dataset.transform` method,
because our function is exactly equal to the type of parameter to the transform method.

So, in fact, our function might be perceived to be both -
an alias to Scala Function1 and a type of Spark transform method.

## Dataset composition with the function

This library provides also a set of additional operators on Spark Dataset.
They mainly provide operator like names for other Spark Dataset methods.
One important extension to this set of operators is the `++` method,
implemented - next to other operators - in the implicit class
(BTW - this could be expressed as an extension method in Scala 3,
but so far in Spark we are in Scala 2):

```
implicit class ExtendedDataset[T](val d: Dataset[T]) extends AnyVal {
  /**
   * Applies the given function to the Dataset.
   *
   * @param f The function which will be applied.
   * @tparam U Type of resulting data.
   * @return Returns the produced Dataset.
   */
  def ++[U](f: Function[T, U]): Dataset[U] = f(d) // d transform f
```

The purpose of this method,
which is yet another alias to standard - this time Spark - method called `Dataset.transform`,
is to be able to compose expressions consisting of Dataset with subsequent function,
when such an expression will produce another Dataset.

```
val person: Dataset[Person] = spark.read.parquet("person.parquet").as[Person]
val newPerson1: Dataset[PersonWithGreeting] = person ++ addFullName ++ addGreeting
// or
val newPerson2: Dataset[PersonWithGreeting] = person ++ (addFullName + addGreeting)
// or
val newPerson3: Dataset[PersonWithGreeting] = person ++ addFullNameAndGreeting
```

This is the core concept to shape Spark applications and express them as composition of such functions.

## Dataset implicit operators

In addition to the composition method on Dataset, there is a bunch of mentioned earlier operators,
which are supposed to shorten and simplify some set operators on Datasets as well as joins.

### Dataset set operators

|Operator |Signature                             |
|---------|--------------------------------------|
|Union    |def +(other: Dataset[T]): Dataset[T]  |
|Subtract |def -(other: Dataset[T]): Dataset[T]  |
|Intersect|def *(other: Dataset[T]): Dataset[T]  |
|Delta    |def -+-(other: Dataset[T]): Dataset[T]|

### Dataset joins operators

|Join type       |Signature|
|----------------|------------------------------------------------------------------|
|Cross join      |def &#124;*&#124;(other: Dataset[_]): Dataset[Row]                |
|Inner join      |def &#124;=&#124;[U](other: Dataset[U]): JoinedDatasetPair[T, U]  |
|Left outer join |def &#124;=+&#124;[U](other: Dataset[U]): JoinedDatasetPair[T, U] |
|Right outer join|def &#124;+=&#124;[U](other: Dataset[U]): JoinedDatasetPair[T, U] |
|Full outer join |def &#124;+=+&#124;[U](other: Dataset[U]): JoinedDatasetPair[T, U]|

## Sample typical functions

In addition to implemented Dataset operators there are also predefined functions.

|Operation       |Signature                                                                              |
|----------------|---------------------------------------------------------------------------------------|
|Filter rows     |def filter[T](condition: String): Function[T, T]                                       |
|Filter rows     |def filter[T](condition: Column): Function[T, T]                                       |
|Select columns  |def select[T](column: String, columns: String*): Function[T, Row]                      |
|Select columns  |def select[T](columns: Column*): Function[T, Row]                                      |
|Add column      |def add[T](column: String, value: Column): Function[T, Row]                            |
|Add columns     |def add[T](columns: (String, Column)*): Function[T, Row]                               |
|Drop columns    |def drop[T](columns: String*): Function[T, Row]                                        |
|Rename column   |def rename[T](oldColumn: String, newColumn: String): Function[T, Row]                  |
|Rename columns  |def rename[T](renameExpr: (String, String)*): Function[T, Row]                         |
|Cast column     |def cast[T](column: String, newType: DataType): Function[T, Row]                       |
|Cast columns    |def cast[T](typesExpr: (String, DataType)*): Function[T, Row]                          |
|Map rows        |def map[T, U: Encoder](f: T => U): Function[T, U]                                      |
|FlatMap rows    |def flatMap[T, U: Encoder](f: T => TraversableOnce[U]): Function[T, U]                 |
|Aggregate       |def agg[T](groupBy: Seq[String], aggregations: Seq[(String, String)]): Function[T, Row]|
|Union           |def union[T](other: Dataset[T]): Function[T, T]                                        |
|Subtract        |def subtract[T](other: Dataset[T]): Function[T, T]                                     |
|Intersect       |def intersect[T](other: Dataset[T]): Function[T, T]                                    |
|Delta           |def delta[T](other: Dataset[T]): Function[T, T]                                        |
|Cross join      |def cross[T](other: Dataset[_]): Function[T, Row]                                      |
|Inner join      |def inner[T](other: Dataset[_], columns: Seq[String]): Function[T, Row]                |
|Left outer join |def left[T](other: Dataset[_], columns: Seq[String]): Function[T, Row]                 |
|Right outer join|def right[T](other: Dataset[_], columns: Seq[String]): Function[T, Row]                |
|Full outer join |def full[T](other: Dataset[_], columns: Seq[String]): Function[T, Row]                 |
|Cast Dataset    |def as[T: Encoder] (): Function[Row, T]                                                |
|Cast Dataset    |def row[T] (): Function[T, Row]                                                        |
|Cache           |def cache[T] (): Function[T, T]                                                        |
|Sort            |def sort[T](column: String, columns: String*): Function[T, T]                          |
|Sort            |def sort[T](columns: Column*): Function[T, T]                                          |
|Pipeline        |def pipeline[T](f: Function[T, T], fs: Function[T, T]*): Function[T, T]                |

## The map pattern

The most typical operation being performed on a Dataset is the map function.
It expects a function to convert input record into output record.
This may be enough to perform some not so complex transformation.
Moreover, such a map operation may work with one input schema and produce another one.

What to do, if we need something more complex, or we would like
to be able to use it on broader range of input or output schemas.
The answer to such a challenge is the map pattern which is an extension to the map function.

The pattern is a type which defines a common interface to handle such use case.
It specifies the containers for Input and Output types, the container for parameters,
and it expects to provide the function which will build the mapper function for the given parameters
and Input and Output types.

Then, it also provides the apply function which will produce in return the map function,
provided that it will get a specific getter to convert the input record to the Input type
and a constructor, which will convert all the produced data to the output record.

```
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
val adder: Function[Record, Record] = Adder[Record, Record](AdderParams(7), getter, constructor)
val result: Dataset[Record] = ds ++ adder
// or in shorter version:
val result: Dataset[Record] = ds ++ Adder[Record, Record](AdderParams(7), _.id,
  (r: Record, output: Adder.Output) => r.copy(id = output))
```

Moreover, to make it work, it does not have to be like that, that input schema of Record contains
an `id` column. It is the getter responsibility to provide value of Adder.Input type,
even if this would require to carry this out through some not so complex processing.
In other words, if the input schema had no `id` column, then the getter method may produce it.

Consequently, the same with the constructor method, it is not required that output schema,
here also of type Record, is a subclass of Adder.Output type, as it is the constructor
responsibility to perform conversion from input record and output of the mapper function
produced by build factory method to the output schema.

Finally, the most important is that the real logic is inside the mapper function
which deals with Input and Output types and these things are implemented inside
the function of this pattern type. This is the logic of this transformation.
Please notice that this logic might be arbitrarily complicated
and be implemented using pure functions, while getter and constructor are only interfaces
to input and output schemas, and they are provided not during the function implementation,
but during real usage in a given context.

In other words, the function implemented according to such a pattern follows typical pattern of ETL:
- Extract is the getter,
- Transform is the mapper function produced by factory method build,
- Load is the constructor.

The emphasis is that it is the mapper function - the middle one - that is the most complex.
The other two are just interfaces to input and output schemas.
