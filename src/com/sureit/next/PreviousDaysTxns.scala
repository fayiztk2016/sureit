package com.sureit.next
import org.apache.spark._
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

import java.time.{ LocalDate, LocalDateTime, Period, Duration }
import java.time.format.DateTimeFormatter

object PreviousDaysTxns {

  case class txn(plaza: String, time: LocalDate) extends Ordered[txn] {

    override def compare(that: txn): Int =
      that.time.compareTo(this.time)

  }
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val t0 = System.currentTimeMillis()
    println(t0)
    val spark = getSparkSession()
    val inputVariables = Array("54002", "2017-06-20")
    val inputDate = LocalDate.parse(inputVariables(1))
    val inputPlaza = inputVariables(0)

    val eighthDay = inputDate.minusDays(8)

    val input = getInputData.filter(x => x._2.equals(inputPlaza)).filter(x => x._5.isBefore(inputDate))
    // .filter(x=>x._1=="9189070480100001CD7E")
    //  println("input*****************************************");
    // println(input.take(5).foreach(println))

    val tagDetails = input.map(x => (x._1, TreeSet(txn(x._2, x._5))))
      .reduceByKey((x, y) => (x ++ y))

    val tagWithLatestTxn = tagDetails.map(x => (x._1, x._2.head.time, x._2))

    val filteredTags = tagWithLatestTxn.filter(x => (x._2.isAfter(eighthDay)))
    //  println("filteredTagss*****************************************");
    //println(filteredTags.take(5).foreach(println))

    val tagWithDiff =
      filteredTags.map(x =>
        (x._1, for (tn <- x._3 if tn.time.isAfter(eighthDay)) yield {
          //(Duration.between(inputDate, tn.time).toDays().toInt)
          Period.between(tn.time, inputDate).getDays

        }))
    //println("tagWithDiff*****************************************");
    //println(tagWithDiff.take(5).foreach(println))
        
    implicit def bool2int(b: Boolean) = if (b) 1 else 0
    val variables = tagWithDiff.map(x => (x._1, x._2.contains(1).toInt, x._2.contains(2).toInt,
      x._2.contains(3).toInt, x._2.contains(4).toInt,
      x._2.contains(5).toInt, x._2.contains(6).toInt,
      x._2.contains(7).toInt))
    //println("variables*****************************************");
    //variables.take(5).foreach(println)

    // val LastPlaza = analayzedByPlaza.reduceByKey(getLastPla    za)
      
        val variablesDF = spark.createDataFrame(variables).toDF("TAG","PRE_1","PRE_2","PRE_3","PRE_4","PRE_5","PRE_6","PRE_7")
   writeToCSV(variablesDF, "variablesDF.csv")
   
      
    val t1 = System.currentTimeMillis()
    println((t1 - t0).toFloat / 60000)
  }
  def getSparkSession(): SparkSession = {
    SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp")
      .getOrCreate()
  }

  def getInputData = {

    val spark = getSparkSession()
    spark.sparkContext.textFile("file:///D:/task/data/TagPlazaCodeEntryExitLaneTime.txt")

      .map(_.split(","))
      .map(x => (x(0), x(1), x(2), x(3), LocalDate.parse(x(4).substring(0, 10))))

  }

  def writeToCSV(df: DataFrame, file: String): Unit = {
    val folder = "file:///D:/task/PreviousDaysTxn/1/"
    df.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(folder + file)
  }

}