# Part I - MiniSpark

This is Scala library for ETL processing to be used on top of Spark.
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
|Cross join      |def crossTyped[T, U](other: Dataset[U]): Function[T, (T, U)]                           |
|Inner join      |def inner[T](other: Dataset[_], columns: Seq[String]): Function[T, Row]                |
|Inner join      |def inner[T](other: Dataset[_], joinExpr: Column): Function[T, Row]                    |
|Inner join      |def inner[T, U](other: Dataset[U], joinExpr: Column): Function[T, (T, U)]              |
|Left outer join |def left[T](other: Dataset[_], columns: Seq[String]): Function[T, Row]                 |
|Left outer join |def left[T](other: Dataset[_], joinExpr: Column): Function[T, Row]                     |
|Left outer join |def leftTyped[T, U](other: Dataset[U], joinExpr: Column): Function[T, (T, U)]          |
|Right outer join|def right[T](other: Dataset[_], columns: Seq[String]): Function[T, Row]                |
|Right outer join|def right[T](other: Dataset[_], joinExpr: Column): Function[T, Row]                    |
|Right outer join|def rightTyped[T, U](other: Dataset[U], joinExpr: Column): Function[T, (T, U)]         |
|Full outer join |def full[T](other: Dataset[_], columns: Seq[String]): Function[T, Row]                 |
|Full outer join |def full[T](other: Dataset[_], joinExpr: Column): Function[T, Row]                     |
|Full outer join |def fullTyped[T, U](other: Dataset[U], joinExpr: Column): Function[T, (T, U)]          |
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
and a constructor which will convert all the produced data to the output record.

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
// see the tests for more examples
```

Moreover, to make it work, it does not have to be like that, that input schema of Record contains
an `id` column. It is the getter responsibility to provide value of Adder.Input type,
even if this would require to carry this out through some not so complex processing.
In other words, if the input schema had no `id` column, then the getter method may produce it.

Consequently, the same with the constructor method, it is not required that output schema,
here of type Record, is a subclass of Adder.Output type, as it is the constructor
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

Please notice that such a pattern might be applied to wide range of use cases,
starting from a plain function, through bigger function covering some business related use cases
and ending on the big functions representing whole submodules. So, this is the decision of the user
where to use it. Moreover, this is also possible to encapsulate functions implemented with this pattern
inside other functions also implemented with this pattern, and so on. 

## Summary

Summing up, this library consists of:
- usage of the `Dataset` type as the main data representation,
- the `Dataset.++` operator which is an alias to `Dataset.transform` method,
- set of implicits which provide aliases to typical Dataset operations,
- the `Function[T, U]` type which is an alias to Scala `Function1[Dataset[T], Dataset[U]]` type,
- the `Function.+` composition operator which is an alias to Scala `Function1.andThen`,
- set of methods producing typical Spark functions as one-liner aliases to Spark counterparts,
- and finally the map pattern.

In turn, the map pattern has the following features:
- provides the contract or an interface to build the map function,
- specifies `Input`, `Output` and `Params` containers for data, typically in the form of case class only or some standard type,
- no necessity to create any traits,
- common API for creating the mapper function in the form of abstract `build` method,
- standard `apply` method which returns the map function,
- the apply method is type parameterized, but those types `[T, U]` do not have to be subclasses of `Input` and `Output` respectively,
- instead, the apply method uses generic and functional interface to input and output records in the form of getter and constructor functions.

## Final word

Concluding, this library is not about the API, which hardly brings anything new.

Instead, it is about the thinking.
The thinking about building enterprise class systems and their decomposition into smaller parts.
Thinking about how to shape the small pieces of the system and then, how to glue them together.

# Part II - Integration with Spark ML

So far we covered typical ETL processing on top of Spark.
Now, we will focus on how to integrate with Spark ML.
To achieve this we try to provide two very simple constructs.

## ML Transformer on Function

Having defined some `Function` we may use it as an ML `Transformer`.
To do so we use the `FunctionTransformer` class
which below is presented without implementation.

```
/**
 * Spark ML transformer which uses the Function.
 * This gives plenty of possibilities to create new ML Transformers.
 *
 * @param uid Transformer id.
 */
class FunctionTransformer(override val uid: String) extends Transformer with DefaultParamsWritable {
  /** Additional, default constructor. */
  def this()

  /**
   * Schema parameter. The function is provided in Seq[(column, type)] form,
   * but stored in serialized form as String, due to limitations of Param.jsonEncode.
   */
  final val schema: Param[String]

  /**
   * Setter for the parameter.
   *
   * @param value New value of the parameter.
   * @return Returns this transformer.
   */
  def setSchema(value: Seq[(String, DataType)]): this.type

  /**
   * Getter for the parameter.
   *
   * @return Returns value of the parameter.
   */
  def getSchema: Seq[(String, DataType)] = deserialize($(schema)).asInstanceOf[Seq[(String, DataType)]]

  /**
   * Function parameter. The function is provided in lambda form,
   * but stored in serialized form as String, due to limitations of Param.jsonEncode.
   */
  final val function: Param[String] = new Param[String](this, "function", "Function")

  /**
   * Setter for the parameter.
   *
   * @param value New value of the parameter.
   * @return Returns this transformer.
   */
  def setFunction(value: Function[Row, Row]): this.type

  /**
   * Getter for the parameter.
   *
   * @return Returns value of the parameter.
   */
  def getFunction: Function[Row, Row]

  /**
   * Check schema validity and produce the output schema from the input schema.
   * Raise an exception if something is invalid.
   *
   * @param inputSchema Input schema.
   * @return Return output schema. Raises an exception if input schema is inappropriate.
   */
  override def transformSchema(inputSchema: StructType): StructType

  /**
   * Transforms the input dataset.
   *
   * @param dataset Dataset to be transformed.
   * @return Returns transformed dataset.
   */
  override def transform(dataset: Dataset[_]): DataFrame

  /**
   * Creates a copy of this instance with the same UID and some extra params.
   *
   * @param extra Extra parameters.
   * @return Returns copy of this transformer.
   */
  override def copy(extra: ParamMap): Transformer
}
```

This way we may produce an ML `Transformer`.

```
val ft: FunctionTransformer = FunctionTransformer()
ft.setSchema(Seq(("id", IntegerType)))
ft.setFunction(filter[Row]("id = 1"))
val result: DataFrame = ft.transform(ds)
```

## Function using ML Transformer

We may also use any Spark ML `Transformer` as our `Function`.
To do so we use the `trans` function which needs an ML `Transformer`.
As a result it returns a function.

```
val func: Function[Row, Row] = trans(ft)
val result: Dataset[Row] = df ++ func
```

The `trans` function has the following signature.

|Operation |Signature                                              |
|----------|-------------------------------------------------------|
|Trans     |def trans(transformer: Transformer): Function[Row, Row]|

Please notice that here we have to stay within untyped API,
as in general Spark ML works only on DataFrames.

# Composition of functions

So far we defined plain functions which together with set of implicits let build
any Spark application. Now we go a step further and define types which may:
- produce data - F0,
- process data - F1 (which is equivalent to Function),
- combine data - F2,
- reduce data - FN.

Those types are plain aliases to Scala functions of specific number of parameters.
Then we supplement them with additional method (operator) to compose them with F1 function
which in general might co next after any of them, as F1 will simply modify the result of all of those types.
As a result we received nice set of operations with a few rules of composing them. 

# Complete example

As an example toy application we implement hello world query which in Spark is word count.

## First approach using the Functions

TODO

## Second approach using the Types

TODO

# Versions

|Version|Date      |Description                                             |
|-------|----------|--------------------------------------------------------|
|1.5.0  |2021-12-12|Add prototype of core types.                            |
|1.4.1  |2021-12-11|Clean the code.                                         |
|1.4.0  |2021-12-08|Update saving and loading of FunctionTransformer.       |
|1.3.0  |2021-12-07|Replace FunctionTransformer with parameterized version. |
|1.2.1  |2021-12-06|Amend FunctionTransformer to use extendable class.      |
|1.2.0  |2021-12-05|Add integration with Spark ML.                          |
|1.1.0  |2021-12-04|Add joins on Column and typed joins.                    |
|1.0.0  |2021-11-28|First version.                                          |
