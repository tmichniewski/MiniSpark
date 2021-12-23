import sbt.Keys.classLoaderLayeringStrategy

name := "MiniSpark"

version := "1.5.6"

scalaVersion := "2.12.14"
val sparkVersion = "3.2.0"

idePackagePrefix := Some("com.github")
Global / excludeLintKeys += idePackagePrefix

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.10" % Test, // Unit tests
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided", // Spark
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided", // Spark SQL
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided" // Spark MLLib
)

// To avoid java.lang.ClassNotFoundException
Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat

// force full test coverage
coverageMinimumStmtTotal := 100
coverageFailOnMinimum := true

// quality checks
scapegoatVersion in ThisBuild := "1.4.10"
