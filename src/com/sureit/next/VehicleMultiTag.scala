package com.sureit.next

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import scala.collection.immutable.TreeSet
import scala.util.Sorting
import scala.collection.mutable.ArrayBuffer

object VehicleMultiTag {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val t0 = System.currentTimeMillis()
    val spark = getSparkSession()

    val input = getVehicleNoTagTime();
    /*   import spark.implicits._
    val inputGrouped = getVehicleNoTagTime().map(x=>((x._1,x._2))).countByValue().filter(x=>(x._2>1))
    .map(x=>(x._1._1,x._1._2,x._2))
  // inputGrouped.take(30).foreach(println)
   val groupByVehicleDF=inputGrouped.toSeq.toDF("VehicleNo", "Tag", "Count")

      writeToCSV(groupByVehicleDF, "groupByVehicleDF")*/
    val VehicleNoMultiTag = getGroupedVehicleNoTag().mapValues(x => x._1).filter(x => x._1.equals("HR55T526"))
      .groupByKey().map(x => (x, x._2.size))
      .filter(x => x._2 > 1)
      .map(x => x._1._1).collect().toSet
    // val x=VehicleNoMultiTag.
    val out = input.filter(x => VehicleNoMultiTag.contains(x._1)).map(x=>(x._1,(x._2,x._3)))
    //.countByKey()
  //  VehicleNoMultiTag.foreach(println)
   // out.foreach(println)
    val latestTxnForVehicleNo=out.reduceByKey((x,y)=>{
      if(x._2>y._2){
        x
      }else{
        y
      }
    }
    ).map(x=>(x._1,x._2._1,x._2._2))
    
    out.foreach(println)
    
  //   val latestTxnDf = spark.createDataFrame(latestTxnForVehicleNo).toDF("VEHICLENUMBER","TAG","TIME")
   //writeToCSV(latestTxnDf, "latestTxnForVehicleNo.csv")

    

    //filter(x=>x._3>1)

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
  def getVehicleNoTagTime() = {
    val spark = getSparkSession()
    spark.sparkContext.textFile("file:///D:/task/data/VehicleNoTagTime.txt").map(_.split(","))
      .map(x => (x(0), x(1), x(2)))

    //spark.sparkContext.textFile("file:///D:/task/data/VehicleClassPlazaAmountfor6Months.csv").map(_.split(",")).filter(x=>(!x(0).equals(""))).map(x=>((x(0),x(1)),(x(2).toFloat,x(3).toInt)))
  }

  def writeToCSV(df: DataFrame, file: String): Unit = {
    val folder = "file:///D:/task/VehicleNoMultiTag/2/"
    df.coalesce(1).write.format("csv").option("header", "true").save(folder + file)
  }
  def getGroupedVehicleNoTag() = {
    val spark = getSparkSession()
    spark.sparkContext.textFile("file:///D:/task/VehicleNoMultiTag/data/groupByVehicleDF/groupedVehicleNoTag.csv").map(_.split(","))
      .map(x => (x(0), (x(1), x(2).toInt)))

    //spark.sparkContext.textFile("file:///D:/task/data/VehicleClassPlazaAmountfor6Months.csv").map(_.split(",")).filter(x=>(!x(0).equals(""))).map(x=>((x(0),x(1)),(x(2).toFloat,x(3).toInt)))
  }
}