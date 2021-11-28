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
 * The function represents any kind of transformation of one Dataset into another.
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

val addFullName: Dataset[Person] => Dataset[PersonWithFullName] = { (d: Dataset[Person]) =>
  d.map { (p: Person) =>
    PersonWithFullName(
      firstName = p.firstName,
      lastName = p.lastName,
      fullName = p.firstName + ' ' + p.lastName
    )
  }
}

val addGreeting: Dataset[PersonWithFullName] => Dataset[PersonWithGreeting] = { (d: Dataset[PersonWithFullName]) =>
  d.map { (p: Person) =>
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
val addFullNameAndGreeting: Dataset[Person] => Dataset[PersonWithGreeting] = addFullName + addGreeting
```

Having such a simple concept we may start building more and more useful functions from some smaller ones.

Please also notice, that such a function might be also used in Spark Dataset.transform method,
because our function is exactly equal to the type of parameter to the transform method.

So, in fact our function might be perceived to be both -
an alias to Scala Function1 and a type of Spark transform method.

## Dataset composition with the function

This library provides also a set of additional operators of Spark Dataset.
They mainly provide operator like names for other Spark Dataset methods.
One important extension to those set of operators is the method `++`
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
which is yet another alias to standard - this time Spark - method called Dataset.transform,
is to be able to compose expressions consisting of Dataset with subsequent function,
when such an expression will produce another Dataset.

```
val person: Dataset[Person] = spark.read.parquet("person.parquet")
val newPerson: Dataset[PersonWithGreeting] = person ++ addFullName ++ addGreeting
// or
val newPerson: Dataset[PersonWithGreeting] = person ++ (addFullName + addGreeting)
// or
val newPerson: Dataset[PersonWithGreeting] = person ++ addFullNameAndGreeting // the function defined earlier
```

This is the core concept to shape Spark applications and express them as composition of such functions.

## Dataset implicit operators

In addition to the composition method on Dataset, there are bunch of mentioned earlier operators,
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

## Sample functions

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

## The pattern

TODO
