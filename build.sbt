name := "MiniSpark"

version := "3.1.0"

scalaVersion := "2.12.14"
val sparkVersion = "3.3.1"

idePackagePrefix := Some("com.github")
Global / excludeLintKeys += idePackagePrefix

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.14" % Test, // Unit tests
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided", // Spark
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided" // Spark SQL
)

// force full test coverage
coverageFailOnMinimum := true
coverageMinimumStmtTotal := 100
coverageMinimumBranchTotal := 100
coverageMinimumStmtPerPackage := 100
coverageMinimumBranchPerPackage := 100
coverageMinimumStmtPerFile := 100
coverageMinimumBranchPerFile := 100

// quality checks
ThisBuild / scapegoatVersion := "1.4.14"
