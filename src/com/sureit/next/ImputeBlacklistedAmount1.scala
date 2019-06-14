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

object ImputeBlacklistedAmount1 {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val t0 = System.currentTimeMillis()

    val spark = getSparkSession()

    val blackTxn_PlazaVehicleclassTag = getBlacklistedTxnWithVehicleClass() 

    val VehiclePlazaAmount = getVehicleClassPlazaAmountFor3Months()

    val imputed_1 = impute(VehiclePlazaAmount, blackTxn_PlazaVehicleclassTag)

    val remainingBlackTxn_PlazaVehicleclassTag = (blackTxn_PlazaVehicleclassTag.subtractByKey(imputed_1.map(x => (x._1, x._2._1))))
   
    val VehiclePlazaAmount_2 = getVehicleClassPlazaAmountFor6Months()
    val imputed_2 = impute(VehiclePlazaAmount_2, remainingBlackTxn_PlazaVehicleclassTag)
   
   
    val output = imputed_1 ++ imputed_2
   
    
     
    val formattedOutput = output.map(x => (x._2._1, x._1._1, x._2._2))
    val formattedOutputDF = spark.createDataFrame(formattedOutput)
      .toDF("TAG", "Plaza", "Amount")
    writeToCSV(formattedOutputDF, "output")
    
     val txnsRemaining=(blackTxn_PlazaVehicleclassTag.subtractByKey(output.map(x => (x._1, x._2._1)))).map(x=>(x._2,x._1._1,x._1._2))
     
    val txnsRemainingDF=     
      spark.createDataFrame(txnsRemaining).toDF("TAG", "Plaza", "VehicleClass")
      writeToCSV(txnsRemainingDF, "remaining")

    println(blackTxn_PlazaVehicleclassTag.count)
     println(imputed_1.count)
     println(output.count)
     println(txnsRemaining.count)
     
    val t1 = System.currentTimeMillis()
    println("Elapsed time: " + (t1 - t0).toFloat / 60000 + "mnts")

    // blackTxns.subtractByKey(tagVehicleClaass).take(30).foreach(println)
  }
  def getBlacklistedTxnWithVehicleClass(): RDD[((String, String), String)] = {
    val blackTxns = getBlacklistedTxns().distinct() //tag,plaza
    println("c" + getBlacklistedTxns.count)
    val tagVehicleClaass = getTagwithVehicleClass() //tag,vehicle
    val blackTxnsWithVehicleClass = blackTxns.join(tagVehicleClaass) //tag,plaza,vehicle
    val blackTxn_PlazaVehicle_Tag = blackTxnsWithVehicleClass.map(x => (x._2, x._1))
    blackTxn_PlazaVehicle_Tag
  }

  def impute(
    VehiclePlazaAmount:            RDD[((String, String), (Float, Int))],
    blackTxn_PlazaVehicleclassTag: RDD[((String, String), String)]): RDD[((String, String), (String, Float))] = {

    val spark = getSparkSession()
    val PlazaVehicle_FrequentAmount = VehiclePlazaAmount.filter(x=>x._2._1!=0.0).reduceByKey((x, y) => if (x._2 >= y._2) (x) else (y))
      .map(x => ((x._1._2, x._1._1), x._2._1))
    //VehiclePlaza_FrequentAmount.filter(x=>x._1._2=="17").foreach(println)
    println(PlazaVehicle_FrequentAmount.filter(x=>x._2==0.0).count)
    
    val imputed = blackTxn_PlazaVehicleclassTag.join(PlazaVehicle_FrequentAmount)
    // println(blackTxns.count +" "+blackTxnsWithVehicleClass.count)
    //  TagPlazaAmount.take(100).foreach(println)
    // println(blackTxns.count)
    // println(tagVehicleClaass.count)
    // println(blackTxn_PlazaVehicle_Tag.count)
    // println(TagPlazaAmount.count)

    /*  println(blackTxns.subtractByKey(tagVehicleClaass).count)

    println(PlazaVehicle_Tag.subtractByKey(PlazaVehicle_FrequentAmount).count)
  println(PlazaVehicle_FrequentAmount.subtractByKey(PlazaVehicle_Tag).count)

  PlazaVehicle_Tag.subtractByKey(PlazaVehicle_FrequentAmount).take(30).foreach(println)*/
    imputed
  }

  def getSparkSession(): SparkSession = {
    SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp")
      .getOrCreate()
  }
  def getVehicleClassPlazaAmountFor3Months() = {
    val spark = getSparkSession()
    spark.sparkContext.textFile("file:///D:/task/data/VehicleClassPlazaAmount.csv").map(_.split(",")).filter(x => (!x(0).equals(""))).map(x => ((x(0), x(1)), (x(2).toFloat, x(3).toInt)))
    //spark.sparkContext.textFile("file:///D:/task/data/VehicleClassPlazaAmountfor6Months.csv").map(_.split(",")).filter(x=>(!x(0).equals(""))).map(x=>((x(0),x(1)),(x(2).toFloat,x(3).toInt)))
  }
  def getVehicleClassPlazaAmountFor6Months() = {
    val spark = getSparkSession()
    spark.sparkContext.textFile("file:///D:/task/data/VehicleClassPlazaAmountfor6Months.csv").map(_.split(",")).filter(x => (!x(0).equals(""))).map(x => ((x(0), x(1)), (x(2).toFloat, x(3).toInt)))
  }

  def getBlacklistedTxns() = {
    val spark = getSparkSession()
    val in = spark.sparkContext.textFile("file:///D:/task/data/BlacklistedTxns.txt").map(_.split(","))
    //in.filter(_.length<=1).map(x=>x.mkString(" ")).foreach(println)
    in.filter(_.length > 1).map(x => (x(0), x(1)))

  }
  def getTagwithVehicleClass() = {
    val spark = getSparkSession()
    spark.sparkContext.textFile("file:///D:/task/data/TagVehicleClass.txt").map(_.split(",")).map(x => (x(0), x(1)))

  }
   def writeToCSV(df: DataFrame, file: String): Unit = {
    val folder = "file:///D:/task/ImputeBlacklistedTxn/2/"
    df.coalesce(1).write.format("csv").option("header", "true").save(folder + file)
  }
}