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

object AverageTimeForTravel {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val t0 = System.currentTimeMillis()
    println(t0)
    val spark = getSparkSession()
    
  //  check
    

    import spark.implicits._
    val data = getAllData
    
    //data.take(110).foreach(println)
    println("-----------")
    val dataWithSpeed=data.map(x=>(x,if (x._7!=0) (x._6/x._7)
    else (x._6/0.5)))
    //dataWithSpeed.take(110).foreach(println)
    val corrupted=dataWithSpeed.filter(x=>x._2>150).persist
    .map(x=>(x._1._1,x._1._3,x._1._4,x._1._2,x._1._5,x._1._6,x._1._7,x._2))
    
    
     val corrputedDF = spark.createDataFrame(corrupted).
     toDF("TAG","PLAZA","PREV PLAZA","TIME","PREV TIME","DISTANCE","TIME DIFF","SPEED")
  // writeToCSV(corrputedDF, "tagDistanceVariableDF.csv")
 // println(corrupted.map(x=>x._1).distinct.count)
    corrupted.take(10).foreach(println)
/*
    val tagDF = spark.createDataFrame(tagPlazaDate).toDF("TAG", "PLAZA", "DATE")

    //.sortByKey()
    val dd1 = tagDF.withColumn(
      "prev",
      lag("PLAZA", offset = 1, defaultValue = -2)
        .over(Window.partitionBy("TAG")
          .orderBy("DATE")))

    dd1.show()
    writeToCSV(dd1, "tagWithLastPlaza")*/
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

  def getAllData = {
    val spark = getSparkSession()

    /* spark.read.format("CSV").option("header","true").load("file:///D:/task/data/INSIGHT.csv")
      .rdd.filter(x=>x(0).equals("34161FA8223286F8020C8DA0"))
   .map(x=>Try{(x(0).toString(),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9).toString(),x(9).toString.length)}.getOrElse(-1,-1))
  */ 
//    spark.sparkContext.textFile("file:///D:/task/data/qwe.csv").map(_.split(",")).map(x => (x(0), x(1), x(2),x(4)))
     spark.read.format("CSV").option("header","true").load("file:///D:/task/data/AverageTimeBetweenPlaza-Fal-Acc-Icici-2.0.csv").rdd
     .filter( x=> !(x(5)==null)).filter(x=>(!(x(5).toString().equals("0"))))
    
     //.filter(x=>x(0).equals("34161FA8202100002B000001"))
     .map(x => (x(0).toString, x(1).toString, x(2).toString,x(3).toString,x(4).toString,x(5).toString().toFloat, 
         x(6).toString().toFloat))
  //    .map(x=>(if (x._7==0.toFloat) 0.5.toFloat else x._7))
  }
 
  def writeToCSV(df: DataFrame, file: String): Unit = {
    val folder = "file:///D:/task/AverageTime/2-falcon/"
    df.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(folder + file)
  }
  def check={
    val spark = getSparkSession()
    val multitag= spark.read.format("CSV").option("header","true").load("file:///D:/task/VehicleNoMultiTag/Vehicle Number with Tag of latest Txn.csv").rdd
     .map(x=>x(1).toString())
     val invalid=spark.read.format("CSV").option("header","true").load("file:///D:/task/AverageTime/CorruptedTxns_Speed.csv").rdd
     .map(x=>x(0).toString)
     multitag.take(30).foreach(println)
     println("-----------------------")
      invalid.take(30).foreach(println)
     val out=multitag.intersection(invalid);
     println("multitag:"+multitag.distinct.count)
     println("invalid:"+invalid.distinct.count)
    println(out.distinct.count)
  }
}