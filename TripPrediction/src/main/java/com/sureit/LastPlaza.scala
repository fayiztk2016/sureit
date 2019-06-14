package com.sureit

import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import scala.collection.immutable.TreeSet
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions._
import scala.collection.immutable.TreeSet
import scala.util.Try
import scala.collection.mutable.ArrayBuffer
import java.time.{ LocalDate, Period }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.DataFrameStatFunctions
import scala.collection.immutable.TreeSet
import org.apache.spark.sql.SQLImplicits
import java.time.{ LocalDate, LocalDateTime, Period, Duration }
import java.time.format.DateTimeFormatter

object LastPlaza extends App {
  
    def apply(inputData: RDD[(String, String, String, String, String, String, String)], inputVariables: Array[String]) = {
      
    val spark = getSparkSession()
    import spark.implicits._
    val inputPlaza = inputVariables(0)
    val performanceDate = inputVariables(1)

    val inputDataFiltered = inputData.filter(x => x._2 == inputPlaza)
      .filter(x => (x._3.substring(0, 10) != performanceDate))
      .map(x => (x._1, x._2,x._3))
    .toDS()
    
     val lastPlazaTime=inputDataFiltered .groupByKey(x => x._1)
   .reduceGroups((x, y) => if (x._3 > y._3) { x } else { y })
   .map(x=>x._2)
  .toDF("tag","plaza","time")
     
  

    lastPlazaTime

  }
    
  def getSparkSession(): SparkSession = {
    SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "hdfs://192.168.70.7:9000/vivek/temp")
      .getOrCreate()
  }
  
}
  
