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

import java.time.{ LocalDateTime, Period, Duration }
import java.time.format.DateTimeFormatter

object RouteTagging3 {

  case class txn(plaza: String, entry: String, exit: String, time: LocalDateTime) extends Ordered[txn] {

    override def compare(that: txn): Int =
      this.time.compareTo(that.time)

  }
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val t0 = System.currentTimeMillis()
    println(t0)
    val spark = getSparkSession()
    import spark.implicits._

    val input = getInputData
    println("input*****************************************");
  //  println(input.take(5).foreach(println))

    val ordered = input.reduceByKey((x, y) => (x ++ y)) //.map(x => (x._1, (x._1._2, x._2._1, x._2._2)))
    val tagWithTxn = ordered.map(x => (x._1, x._2.toArray))
    tagWithTxn.map(x=>(x._1,x._2.mkString(","))).take(10).foreach(println)
    val pairsArray =
      for (
        tag <- tagWithTxn
      ) yield {
        for (i <- 1 to tag._2.length - 1) yield {
          (tag._2(i).plaza, tag._2(i).entry, tag._2(i - 1).plaza, tag._2(i - 1).exit, Duration.between(tag._2(i).time, tag._2(i - 1).time).toHours())
        
        
        }

      }

    val pairs = pairsArray.flatMap(x => x)
    
      //{ case ( arr) => arr.map(x=>((x._1,x._2,x._3),1)) }
      .filter(x => (x._1 != x._3))
    //.filter(x=>x._1=="21003")
    //  println("----------"+pairs.filter(x=>(x._3!==x._4)).count)
    val pairswithSet = pairs.map(x => ((x._1, x._2, x._3,x._4), (1.toLong, x._5)))

   // println("pairs*****************************************");
    //println(pairswithSet.foreach(println))

    val plazaBycount = pairswithSet.reduceByKey((x, y) => ((x._1 + y._1), (x._2 + y._2)))
      .map(x => ((x._1._1, x._1._2), (x._1._3,x._1._4, x._2._1.toFloat, x._2._1.toFloat, x._2._2.toFloat)))

    /* println("plazaBycount*****************************************");
    println(plazaBycount.take(5).foreach(println))
    println("plazaBycount*****************************************");
    println(plazaBycount.take(5).foreach(println))*/
    val prevPlazaRatio = plazaBycount
      .reduceByKey((x, y) => if (x._3 > y._3) (x._1, x._2, x._3,x._4 + y._4, x._5) else (y._1, y._2,y._3, x._4 + y._4,  y._5))
      .map(x => (x._1._1, x._1._2, x._2._1, x._2._2, x._2._3,x._2._4,(x._2._3 * 100 / x._2._4), x._2._5 / x._2._3))

    println("prevPlazaRatio*****************************************");

    //  prevPlazaRatio.filter(x=>x._7>24).take(5).foreach(println)

    val prevPlazaRatioDF = prevPlazaRatio.toDF("plaza", "lane", "prev_plaza","prev_lane", "txn_to_this_prev_plaza","total_prev_txn", "perc", "avg time")
// println(prevPlazaRatioDF.count)
  // val goodprob=prevPlazaRatioDF.filter($"perc">50)
  //println(goodprob.count)
  
    writeToCSV(prevPlazaRatioDF, "prevPlazaRatio")

    // val LastPlaza = analayzedByPlaza.reduceByKey(getLastPla    za)

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
     // .filter(x=>x(5).substring(0,10)>"2018-01-01")
      .map(x => (x(0), x(1), x(2), x(3), LocalDateTime.parse(x(4), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))))
      
      .map(x => (x._1, TreeSet(txn(x._2, x._3, x._4, x._5))))

  }

  def writeToCSV(df: DataFrame, file: String): Unit = {
    val folder = "file:///D:/task/RouteTagging/7-prev/"
    df.repartition(1).write.format("csv").mode("overwrite").option("header", "true").save(folder + file)
  }

}