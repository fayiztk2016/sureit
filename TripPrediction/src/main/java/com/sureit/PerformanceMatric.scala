package com.sureit

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import java.util.Properties
import org.apache.spark.sql.SQLContext
import org.apache.calcite.sql.advise.SqlSimpleParser.Query
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
import org.apache.spark.SparkContext
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.functions

object PerformanceMatric extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark: SparkSession = getSparkSession()
  val plazalist1 = getInputPlaza.mapPartitionsWithIndex {
    (idx, iter) => if (idx == 0) iter.drop(1) else iter
  }
  val plazalist = plazalist1.collect().toList
  val url = "jdbc:sqlserver://192.168.70.15:1433; database=SUREIT"
  val properties = new Properties()
  properties.put("user", "vivek")
  properties.put("password", "welcome123")
  properties.put("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
  //    val format = new SimpleDateFormat("yyyy-MM-dd")
  //  val performanceDate = format.format(Calendar.getInstance().getTime())
  //  val TestDate = LocalDate.parse(performanceDate).minusDays(1).toString()

  val x = plazalist.map { x =>

    val plazaWithBetaArray = x.split(";")
    val plaza = plazaWithBetaArray(0)
    val date = plazaWithBetaArray(1)
//    println(date)

    val Input = getInputData(plaza, date)
    //    println(Input.count())
    val InputFiltered = Input.mapPartitionsWithIndex {
      (idx, iter) => if (idx == 0) iter.drop(1) else iter
    }
    //    InputFiltered.take(5).foreach(println)
    val query = "(select distinct TAGID from SUREIT.CTP.INSIGHT where PLAZACODE = " + plaza + " and cast(EXITTXNDATE as date) = '" + date + "') Event"
    val Event = spark.read.jdbc(url = url, table = query, properties).count()
    val Predict = InputFiltered.filter(x => x._3 == "1").count()
    val FalseNegitive = InputFiltered.filter(x => x._2 == "0").filter(x => x._3 == "0").count()
    val TruePositive = InputFiltered.filter(x => x._2 == "1").filter(x => x._3 == "1").count()
    val Type1 = InputFiltered.filter(x => x._2 == "0").filter(x => x._3 == "1").count()
    val Type2 = InputFiltered.filter(x => x._2 == "1").filter(x => x._3 == "0").count()
    val Final = (plaza, date, Event, Predict, FalseNegitive, TruePositive, Type1, Type2)

    Final
    //    println(Final)

    //    val In1 = InputFiltered.toDF("Tag")
    //    InputFiltered.toDF("Tag", "Actual", "Predict").createOrReplaceTempView("temp")

    //    In1.show(10)

    //    val FalseNegitive = spark.sql("select count(*) as FalseNegitive from temp where Actual = 0 and Predict = 0")
    //    val TruePositive = spark.sql("select count(*) as TruePositive from temp where Actual = 1 and Predict = 1")
    //    val Type1 = spark.sql("select count(*) as Type1 from temp where Actual = 0 and Predict = 1")
    //    val Type2 = spark.sql("select count(*) as Type2 from temp where Actual = 1 and Predict = 0")

    //    val query = "(select TAGID, case when PLAZACODE = " + plaza + " and cast(EXITTXNDATE as date) = " + date + " then 1 else 0 end as event from SUREIT.CTP.INSIGHT) Event"
    //    val event = spark.read.jdbc(url=url, table=query, properties)
    //    event.show(10)

    //actual input
    //v1,v2,v3,v4
    //total

  }
  import spark.implicits._
  val Output = spark.sparkContext.parallelize(x).toDF("Plaza", "Date", "Event", "Predict", "FalseNegitive", "TruePositive", "Type1", "Type2")
      Output.show(50)
  val format = new SimpleDateFormat("yyyy-MM-dd")
  val Date = format.format(Calendar.getInstance().getTime())
  write(Output, Date)
  //println(x.mkString(","))

  def getSparkSession(): SparkSession = {
    SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "hdfs://192.168.70.7:9000/vivek/temp")
      .getOrCreate()
  }

  def getInputData(plaza: String, date: String) = {
    val spark = getSparkSession()
    spark.sparkContext.textFile("hdfs://192.168.70.7:9000/vivek/Implementation/" + plaza + "/" + date + "/")
      .map(_.split(","))
      .map(x => (x(0), x(1), x(19)))
  }

  def getInputPlaza = {

    val spark = getSparkSession()
    spark.sparkContext.textFile("hdfs://192.168.70.7:9000/vivek/INSIGHT/CSV/Plaza2.txt")

  }

  def write(df: DataFrame, date: String) = {
    val folder = "hdfs://192.168.70.7:9000/vivek/PerformanceMatrix/" + date + "/"
    df.repartition(1).write.format("csv").mode("overwrite").option("header", "true").save(folder)

  }

}