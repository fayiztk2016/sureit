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

object ImputeBlacklistedAmount {
  
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

  val t0 = System.currentTimeMillis()
   
  val spark =getSparkSession()
  
    val VehiclePlazaAmount = getVehicleClassPlazaAmount(spark)
  
    val PlazaVehicle_FrequentAmount=VehiclePlazaAmount.reduceByKey((x,y)=>if(x._2>=y._2) (x) else (y))
                                            .map(x=>((x._1._2,x._1._1),x._2._1))
   //VehiclePlaza_FrequentAmount.filter(x=>x._1._2=="17").foreach(println)
   
    
    val blackTxns=getBlacklistedTxns(spark) //tag,plaza
    val tagVehicleClaass=getTagwithVehicleClass(spark)  //tag,vehicle
    val blackTxnsWithVehicleClass=blackTxns.join(tagVehicleClaass) //tag,plaza,vehicle
    val PlazaVehicle_Tag=blackTxnsWithVehicleClass.map(x=>(x._2,x._1))
    
    
   val TagPlazaAmount=PlazaVehicle_Tag.join(PlazaVehicle_FrequentAmount)
   // println(blackTxns.count +" "+blackTxnsWithVehicleClass.count)
  //  TagPlazaAmount.take(100).foreach(println)
    println(blackTxns.count)
   // println(tagVehicleClaass.count)
      println(blackTxnsWithVehicleClass.count)
    println(TagPlazaAmount.count)
  
 /*  println(blackTxns.subtractByKey(tagVehicleClaass).count)
   
    println(PlazaVehicle_Tag.subtractByKey(PlazaVehicle_FrequentAmount).count)
  println(PlazaVehicle_FrequentAmount.subtractByKey(PlazaVehicle_Tag).count)
  
  PlazaVehicle_Tag.subtractByKey(PlazaVehicle_FrequentAmount).take(30).foreach(println)*/
   
   
   
   
  // blackTxns.subtractByKey(tagVehicleClaass).take(30).foreach(println)
  }
  type data= ((String, String), (Float, Int))
  def impute( spark:SparkSession,data:RDD[String]){
    
  }
  
  def getSparkSession():SparkSession={
    SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp")
      .getOrCreate()
  }
  def getVehicleClassPlazaAmount(spark:SparkSession)={
  //spark.sparkContext.textFile("file:///D:/task/data/VehicleClassPlazaAmount.csv").map(_.split(",")).filter(x=>(!x(0).equals(""))).map(x=>((x(0),x(1)),(x(2).toFloat,x(3).toInt)))
   spark.sparkContext.textFile("file:///D:/task/data/VehicleClassPlazaAmountfor6Months.csv").map(_.split(",")).filter(x=>(!x(0).equals(""))).map(x=>((x(0),x(1)),(x(2).toFloat,x(3).toInt)))
  }
  
   def getBlacklistedTxns(spark:SparkSession)={
  val in=spark.sparkContext.textFile("file:///D:/task/data/BlacklistedTxns.txt").map(_.split(","))
 //in.filter(_.length<=1).map(x=>x.mkString(" ")).foreach(println) 
  in.filter(_.length>1).map(x=>(x(0),x(1)))
  
  }
    def getTagwithVehicleClass(spark:SparkSession)={
  spark.sparkContext.textFile("file:///D:/task/data/TagVehicleClass.txt").map(_.split(",")).map(x=>(x(0),x(1)))
  
  }
}