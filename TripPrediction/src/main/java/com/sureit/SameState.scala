package com.sureit

import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import scala.collection.immutable.TreeSet
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions._
import scala.collection.immutable.TreeSet
import scala.util.Try
import scala.collection.mutable.ArrayBuffer
import java.time.{ LocalDate, Period }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.DataFrameStatFunctions
import scala.collection.immutable.TreeSet
import org.apache.spark.sql.SQLImplicits
import java.time.{ LocalDate, LocalDateTime, Period, Duration }
import java.time.format.DateTimeFormatter
object SameState extends App {
  def apply(inputData: RDD[(String, String, String, String, String, String, String)], inputVariables: Array[String]) = {
    val spark = getSparkSession()
    import spark.implicits._
    val inputPlaza = inputVariables(0)
    val performanceDate = inputVariables(1)

    val inputDataFiltered = inputData.filter(x => x._2 == inputPlaza)
      .filter(x => (x._3.substring(0, 10) != performanceDate))
      .map(x => (x._1, x._4))
      .toDF("tag", "same_state")
    // .persist(StorageLevel.MEMORY_AND_DISK)

    val trips = inputDataFiltered.groupBy("tag").agg(sum(col("tag"))).withColumnRenamed("sum(tag)", "tag_COUNT")
    val trips_count1 = trips.select($"tag", $"tag_COUNT").withColumn("TRIPS>1", when(col("tag_COUNT") > 1, lit(1)).otherwise(lit(0)))
    val TRIPS_COUNT = trips_count1.select("tag", "TRIPS>1")

    val distinctTag = inputDataFiltered.distinct
    val same_state = distinctTag.join(TRIPS_COUNT, Seq("tag"), "outer")

    same_state

  }
  def getSparkSession(): SparkSession = {
    SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "hdfs://192.168.70.7:9000/vivek/temp")
      .getOrCreate()
  }
}