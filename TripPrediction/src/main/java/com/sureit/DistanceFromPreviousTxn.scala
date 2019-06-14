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
object DistanceFromPreviousTxn extends App {

  case class Record(tag: String, plaza: String, date: String, time: String)

  def apply(inputData: RDD[(String, String, String, String, String, String, String)], inputVariables: Array[String]) = {
    val spark = getSparkSession()
    import spark.implicits._
    val inputPlaza = inputVariables(0)
    val performanceDate = inputVariables(1)

    val prevDay = LocalDate.parse(performanceDate).minusDays(1).toString()

    val customizedInputData = inputData

      .map(x => (x._1, x._2, x._3.substring(0, 10), x._3.substring(10)))
      .filter(x => (x._3 == prevDay))
      .distinct
      .toDF("tag", "plaza", "date", "time")
    // .persist(StorageLevel.DISK_ONLY)

    val tagWithLastPlaza = customizedInputData.as[Record]
      .groupByKey(x => (x.tag))
      .reduceGroups((x, y) => if (x.time > y.time) x else y)
      .map(x => (x._1, x._2.plaza)).toDF("tag", "plaza")
    // .persist(StorageLevel.DISK_ONLY)

    val plazaDistance = getplazaDistance
      .filter(x => x._1 == inputPlaza).map(x => (x._2, x._3))
      .toDF("plaza", "distance")
    //.persist(StorageLevel.DISK_ONLY)

    val plazaWithDistance = tagWithLastPlaza.
      join(plazaDistance, Seq("plaza"))
    //.persist(StorageLevel.DISK_ONLY)
    val plazaDistanceVariables = plazaWithDistance
      .select(
        $"tag",
        lit($"distance" < 500).cast(IntegerType) as "nearer",
        lit($"distance" > 1000).cast(IntegerType) as "distant")

    plazaDistanceVariables

  }
  def getSparkSession(): SparkSession = {
    SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "hdfs://192.168.70.7:9000/vivek/temp")
      .getOrCreate()
  }
  def getplazaDistance = {
    val spark = getSparkSession()
    val distanceRDD = spark.sparkContext.textFile("hdfs://192.168.70.7:9000/vivek/INSIGHT/CSV/PlazaCodeDistance.txt").map(_.split(",")).map(x => (x(0), x(1), x(2).toFloat))
    //  distanceRDD.groupByKey.collectAsMap().map(x => (x._1, x._2.toMap))
    distanceRDD
  }
}