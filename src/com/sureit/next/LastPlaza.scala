package com.sureit.next

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object LastPlaza {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    /*  println("Enter estimation date : ")
    val estimationDate = scala.io.StdIn.readLine()*/

    val estimationDate = "20181020"

    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp")
      .getOrCreate()

    val input = spark.sparkContext.textFile("file:///D:/task/data/TagExitPlaza.txt")
    val inputRDD = input.map(_.split(",")).map(x => (x(0), x(1), x(2), x(3)))
    val inputDF = spark.createDataFrame(inputRDD).toDF("TAG", "PLAZA", "LANE", "DATE")

    val customizedInputRDD = inputDF.filter(date_format(col("DATE"), "YYYYMMdd") <= estimationDate)
      .rdd.map(x => (x(0).toString, (x(1).toString, x(2).toString, x(3).toString)))
    //  println(inputDF.select(col("DATE"),date_format(col("DATE"),"YYYYMMdd")).show(30))

    val tagWithLastPlaza = customizedInputRDD.reduceByKey(findLastPlaza).map(x=>(x._1,x._2._1,x._2._2,x._2._3))
    
    
     val tagWithLastPlazaDF = spark.createDataFrame(tagWithLastPlaza).toDF("TAG", "PLAZA", "LANE", "DATE")
    writeToCSV(tagWithLastPlazaDF,"tagWithLastPlaza")
  }
  
  type dataType = (String, String, String)
  def findLastPlaza(data1: dataType, data2: dataType): dataType = {

    if (data1._3 > data2._3) {
      data1
    } else {
      data2
    }
  }
   def writeToCSV(df: DataFrame,name:String): Unit = {
    val folder = "file:///D:/task/findLastPlaza/1/"
   
    df.coalesce(1).write.format("csv").option("header", "true").save(folder +name)
  }

}