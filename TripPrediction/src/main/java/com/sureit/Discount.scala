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
object Discount extends App {

  case class Record(tag: String, time: String, discount: String)

  def apply(inputData: RDD[(String, String, String, String, String, String, String)], inputVariables: Array[String]) = {
    val spark = getSparkSession()
    import spark.implicits._
    val inputPlaza = inputVariables(0)
    val performanceDate = inputVariables(1)

    val inputDataFiltered = inputData.filter(x => x._2 == inputPlaza)
      .filter(x => (x._3.substring(0, 10) != performanceDate))
      .filter(x => (x._5 != "0"))
      .map(x => (x._1, x._3, x._5))
    // .persist(StorageLevel.DISK_ONLY)

    val inputDF = inputDataFiltered.toDF("tag", "time", "discount").persist(StorageLevel.DISK_ONLY)
    /*println("sql count**********")
    val t0 = System.currentTimeMillis()
    val maxTime = inputDF.groupBy($"tag").agg($"tag", max($"time").alias("time"))
    val req = inputDF.join(maxTime, Seq("tag", "time")).select($"tag", $"discount", $"time").persist
    //req.filter($"discount"===1).show(10,false)
    println(req.filter($"discount" === 1).count)
    val t1 = System.currentTimeMillis()
    println("sql:" + (t1 - t0).toFloat / 1000)*/

    val tagWithDiscount = inputDF.as[Record]
      .groupByKey(x => (x.tag))
      .reduceGroups((x, y) => if (x.time > y.time) { x } else { y })
      .map(x => (x._2.tag, x._2.discount, x._2.time)) //.toDF("tag", "discount", "time").persist
      .persist(StorageLevel.DISK_ONLY)
    val tagWithDailyAndMonthly = tagWithDiscount.filter(x => x._2 != 3)
      // .map{case(x)=>if(x._3==0) x}
      .map(x => (x,
        (if (x._2 == "1") (1, 0) else if (x._2 == "2") (0, 1) else (0, 0))))
      .map(x => (x._1._1, x._2._1, x._2._2))
      .toDF("tag", "daily_pass", "monthly_pass")
      .persist(StorageLevel.DISK_ONLY)

    val local = tagWithDiscount.filter(x => x._2 == "3").map(x => (x._1, 1)).distinct.toDF("tag", "local")
    val discountVariables = tagWithDailyAndMonthly.join(local, Seq("tag"), "outer")

    discountVariables
    //.filter($"monthly_pass"===1)

    // val x=tagWithDiscount.filter($"discount"===1).except(req.filter($"discount"===1))

    //x.show(false)

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

