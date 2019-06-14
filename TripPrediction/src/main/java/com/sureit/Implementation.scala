package com.sureit

import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import java.util.Scanner
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import scala.collection.immutable.TreeSet
import scala.util.Try
import scala.Array
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

object Implementation extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val t0 = System.currentTimeMillis()
  val In2 = "2018-10-01"
  val spark: SparkSession = getSparkSession()
  import spark.implicits._
  val z = getInputPlaza
  val y = z.mapPartitionsWithIndex {
    (idx, iter) => if (idx == 0) iter.drop(1) else iter
  }
  val plazalist = y.collect().toList
  val inputData = getInputData.persist(StorageLevel.DISK_ONLY)
  //  var i = 0
  //   val plaza = "8001"
  //  for (plaza <- plazalist if plaza != null if i < 2) {
  //    print("Started running for Plaza : " + plaza)
  //    val inputVariables = Array(plaza, In2)
  //    val Variable = VariableCreation(inputData, inputVariables)
  //    println("count of rows :"+ Variable.count())
  //    writeToCSV(Variable, plaza)
  //    print("Plaza " + plaza + " Done")

  //    i += 1
  //  }

  val plazaWithBeta = getInputPlaza.map(_.split(";")).map(x => (x(0), x(1), x(2), x(3)))
  val In = spark.createDataFrame(plazaWithBeta).toDF("PLAZA", "DATE", "BETA", "CUTOFF")
  In.show(10)
  var i = 0

  In.foreach { x =>

//    val plaza = In.select("PLAZA")
//    val date = In.select("DATE")
//    val beta = In.select("BETA")
//    val cutoff = In.select("CUTOFF")

  }

  //  In.map { x =>
  //    //    print("begin")
  //    val j = i.toString()
  //    val plazaWithBetaArray = x.split(";")
  //    val plaza = x.select("PLAZA")
  //    val date = plazaWithBetaArray(1)
  //    val beta = plazaWithBetaArray(2).split(",")
  //    val cutoff = plazaWithBetaArray(3)
  //    println("Started running for Plaza : " + plaza)
  //    val inputVariables = Array(plaza, date, j)
  //    val variables = VariableCreation(inputData, inputVariables)

  //    val implementationOut = Probability(variables, beta, cutoff)

  //    val out = implementationOut.collect().map(x => (x(0), x(1), x(17)))

  //    writeToCSV(implementationOut, plaza, date)
  //    println("Plaza " + plaza + " Done")
  //    i += 1
  //
  //  }

  def getSparkSession(): SparkSession = {
    SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "hdfs://192.168.70.7:9000/vivek/temp")
      .getOrCreate()
  }

  def getInputPlaza = {

    val spark = getSparkSession()
    spark.sparkContext.textFile("hdfs://192.168.70.7:9000/vivek/INSIGHT/CSV/Plaza2.txt")

  }

  def getInputData = {

    val spark = getSparkSession()
    spark.sparkContext.textFile("hdfs://192.168.70.7:9000/vivek/INSIGHT/CSV/TagPlazaTimeStateDiscountClassTxn.txt")
      .map(_.split(","))
      //    val y = (t.filter(x => x.size != 7))
      //    println(y.count)
      //    val z = y.map(x => x.mkString(","))
      //    z.take(10).foreach(println)
      .map(x => (x(0), x(1), x(2), x(3), x(4), x(5), x(6)))

  }

  def writeToCSV(df: DataFrame, plaza: String, date: String): Unit = {

    //      print("Enter Variable Creation Version Code : ")

    val folder = "hdfs://192.168.70.7:9000/vivek/Implementation/" + plaza + "/" + date + "/"
    //    val folder2 = "file:///192.168.70.15/Share_Folder/Variable_Creation"
    df.repartition(1).write.format("csv").mode("overwrite").option("header", "true").save(folder)
    //    df.repartition(1).write.format("csv").mode("overwrite").option("header", "true").save(folder2)

    val t1 = System.currentTimeMillis()
    print((t1 - t0).toFloat / 1000)

  }

}