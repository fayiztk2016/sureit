package com.sureit.next

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import scala.collection.immutable.TreeSet
import scala.util.Sorting
import scala.collection.mutable.ArrayBuffer

import java.time.{ LocalDate, Period }

object DataCreation1 {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

  val t0 = System.currentTimeMillis()
   

    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp")
      .getOrCreate()

    val input = spark.sparkContext.textFile("file:///D:/task/data/TagExitPlaza.txt")
    val inputRDD = input.map(_.split(",")).map(x => ((x(0), x(1)), (x(3), Set(txn(x(1), x(3), x(2))))))
    val analayzedByPlaza = inputRDD.reduceByKey(analysisByPlaza).map(x => (x._1._1, (x._1._2, x._2._1, x._2._2)))
    val LastPlaza = analayzedByPlaza.reduceByKey(getLastPlaza)

    val modelOutput = LastPlaza.mapValues(model).map(x => (x._1, x._2._1, x._2._2, x._2._3, x._2._4, x._2._5, x._2._6, x._2._7, x._2._8, x._2._9))

    val modelOutputDF = spark.createDataFrame(modelOutput)
      .toDF("TAG", "LastPlaza", "Time", "Pre1", "Pre2", "Pre3", "Pre4", "Pre5", "Pre6", "Pre7")
    writeToCSV(modelOutputDF, "output")
    
     val t1 = System.currentTimeMillis()
  println("Elapsed time: " + (t1 - t0).toFloat / 60000 + "mnts")
  }

  case class txn(plaza: String, time: String, lane: String) extends Ordered[txn] {

    override def compare(that: txn): Int =
      that.time.compareTo(this.time)

  }
 
  type dataByPlaza = (String, Set[txn])
  def analysisByPlaza(data1: dataByPlaza, data2: dataByPlaza): dataByPlaza = {
    if (data1._1 > data2._1) {
      (data1._1, data1._2 ++ data2._2)
    } else {
      (data2._1, data1._2 ++ data2._2)
    }
  }

  type dataByTag = (String, String, Set[txn])
  def getLastPlaza(data1: dataByTag, data2: dataByTag): dataByTag = {
    if (data1._2 > data2._2) {
      (data1._1, data1._2, data1._3)
    } else {
      (data2._1, data2._2, data2._3)
    }
  }

  type out = (String, String, Int, Int, Int, Int, Int, Int, Int)
  type in = (String, String, Set[txn])
  def model(input: in): out = {
    val plaza = input._1
    val time = input._2
    val details = input._3
    val event = ArrayBuffer(0, 0, 0, 0, 0, 0, 0)

    val today = LocalDate.parse(time.substring(0, 10))

    val it = details.iterator
    it.next
    var break = false
    while (it.hasNext) {

      val curr = it.next.time.substring(0, 10)
      val currDate = LocalDate.parse(curr)
      val diff = Period.between(currDate, today).getDays
      // println(currDate+" "+today+" "+diff)
      if (diff <= 7 && diff > 0) {
        event(diff - 1) = 1
      } else {
        break = true
      }

      
    }
    val (p1, p2, p3, p4, p5, p6, p7) = (event(0), event(1), event(2), event(3), event(4), event(5), event(6))
    (plaza, time, p1, p2, p3, p4, p5, p6, p7)
  }
  def writeToCSV(df: DataFrame, file: String): Unit = {
    val folder = "file:///D:/task/ModelCreation/2/"
    df.coalesce(1).write.format("csv").option("header", "true").save(folder + file)
  }
}