package com.github
package minispark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.lang.System.getenv
import java.util.Properties
import scala.io.Source

// $COVERAGE-OFF$Left for integration tests in Databricks
/** Contains helper functions connected with Spark. */
object Spark {
  /** Global spark session. */
  @transient lazy val spark: SparkSession = getSpark

  /**
   * Initializes spark session object.
   *
   * @return Returns Spark Session.
   */
  def getSpark: SparkSession = SparkSession.builder().config(getSparkConf).getOrCreate()

  /**
   * Initializes Spark configuration from a properties file.
   *
   * @return Returns Spark configuration object.
   */
  def getSparkConf: SparkConf = {
    val sparkConf: SparkConf = new SparkConf()
    val props: Properties = new Properties
    props.load(Source.fromFile(getenv("MINISPARK_CONF") + "/spark.conf").bufferedReader())
    props.forEach((k, v) => sparkConf.set(k.toString, v.toString))
    sparkConf
  }
}
// $COVERAGE-ON
