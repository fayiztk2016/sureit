package com.sureit

import org.apache.spark._
import scala.math
import org.apache.spark.storage.StorageLevel
import java.util.Scanner
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import scala.collection.immutable.TreeSet
import scala.util.Try
import scala.collection.mutable.ArrayBuffer
import java.time.{ LocalDate, Period }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import scala.collection.immutable.TreeSet
import org.apache.spark.sql.SQLImplicits
import java.time.{ LocalDate, LocalDateTime, Period, Duration }
import java.time.format.DateTimeFormatter
import java.sql.Timestamp
import org.apache.spark.SparkException

object VariableCreation extends App {
  //      Logger.getLogger("org").setLevel(Level.ERROR)
  def apply(inputData: RDD[(String, String, String, String, String, String, String)], inputVariables: Array[String]) = {

    val spark = getSparkSession()

    //    val S = new Scanner(System.in)
    //    print("Enter Plaza Code : ")
    //    val In1 = S.next()
    //    print("Enter performanceDate Code(Format : 2018-01-01) : ")
    //    val In2 = S.next()
    //    //  print("Enter MaxDate Code : ")
    //    //  val In3 = S.next()
    //
    //    val t0 = System.currentTimeMillis()
    //    println("Running Variable Creation")
    //    val inputVariables = //Array("54002", "2017-06-19", "2017-04-01")
    //      Array(In1, In2)
    val inputPlaza = inputVariables(0)
    val performanceDate = inputVariables(1)
    val j = inputVariables(2)
    val inputVariables1 = Array(inputPlaza, performanceDate, j)
    //  val inputDate = LocalDate.parse(inputVariables(1))
    //  val startDate = inputVariables(2)

    //    val inputData = getInputData
    //      // .filter(x => x._1 == "9189070480100001CD5C")
    //      .persist(StorageLevel.MEMORY_AND_DISK)

    val inputDataFiltered = inputData
      .filter(x => (x._3.substring(0, 10) <= performanceDate))
    //    .filter(x => (x._3.substring(0, 10) >= startDate))
    //.persist(StorageLevel.MEMORY_AND_DISK)
 
    val daysOfProportion = DaysOfProporion(inputDataFiltered, inputVariables1)
    val prev1to7 = Prev1to7(inputDataFiltered, inputVariables1)
    val txnOnPerformanceDate = TxnOnPerformanceDate(inputDataFiltered, inputVariables1)
    val distanceFromPreviousTxn = DistanceFromPreviousTxn(inputDataFiltered, inputVariables1)
    val same_state = SameState(inputDataFiltered, inputVariables1)
    val discount = Discount(inputDataFiltered, inputVariables1)

    //    val clubbed_class = ClubbedClass(inputDataFiltered, inputVariables1)
    //    val txn = Txn(inputDataFiltered, inputVariables1)

    val variables = txnOnPerformanceDate
      .join(distanceFromPreviousTxn, Seq("tag"), "outer")
      .join(daysOfProportion, Seq("tag"), "outer")
      .join(prev1to7, Seq("tag"), "outer")
      .join(same_state, Seq("tag"), "outer")
      .join(discount, Seq("tag"), "outer")
    //      .join(clubbed_class, Seq("tag"), "outer")
    //      .join(txn, Seq("tag"), "outer")
    // .persist(StorageLevel.DISK_ONLY)

    val distinctTag = DistinctTag(inputDataFiltered, inputVariables)

    val variable = distinctTag.join(variables, Seq("tag"), "left_outer")
      .na.fill(0)
    //.persist(StorageLevel.DISK_ONLY)
    //  println(variables.count)
    //    writeToCSV(variable)
    //    println((t1 - t0).toFloat / 1000)
    variable
    //  variable.write.mode(SaveMode.Overwrite).csv("hdfs://192.168.70.7:9000/vivek/INSIGHT/CSV")
  }
  def getSparkSession(): SparkSession = {
    SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "hdfs://192.168.70.7:9000/vivek/temp")
      .getOrCreate()
  }

  //  def getInputData = {
  //
  //    val spark = getSparkSession()
  //    //spark.sparkContext.textFile("file:///D:/task/data/TagPlazaCodeEntryExitLaneTime.txt")
  //    spark.sparkContext.textFile("hdfs://192.168.70.7:9000/vivek/INSIGHT/CSV/TagPlazaTimeStateDiscountClassTxn.txt")
  //
  //      .map(_.split(","))
  //
  //      .map(x => (x(0), x(1), x(2), x(3), x(4), x(5), x(6)))
  //
  //  }
  //  def writeToCSV(df: DataFrame, file: String):
  //  def writeToCSV(df: DataFrame): Unit = {
  //
  //    //      print("Enter Variable Creation Version Code : ")
  //    val Version = 1
  //
  //    val folder = "hdfs://192.168.70.7:9000/vivek/VariableCreation/" + Version + "/"
  //    //    val folder2 = "file:///192.168.70.15/Share_Folder/Variable_Creation"
  //    df.repartition(1).write.format("csv").mode("overwrite").option("header", "true").save(folder)
  //    //    df.repartition(1).write.format("csv").mode("overwrite").option("header", "true").save(folder2)
  //
  //  }

  //  val t1 = System.currentTimeMillis()

  //9189070480100001CD5C 45 104

}