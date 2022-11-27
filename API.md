# API

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

### Dataset join operators

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
