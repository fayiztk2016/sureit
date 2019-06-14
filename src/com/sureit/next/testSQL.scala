package com.sureit.next

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import org.apache.spark.sql.functions._
import com.microsoft.sqlserver.jdbc.SQLServerDriver
import java.util.Calendar
object testSQL {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder
      .appName("testSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()

    val inputData = spark.read.format("csv").option("header", "true").load("file:///D:/task/out2.csv")
    //println(inputData.select(inputData("tagid")).count())
    println("inital distinct tags:" + inputData.select(inputData("tagid")).distinct().count())

    val temp = inputData.filter("VEHICLENUMBER is not null ")
    println("distinct tags after removing all  null vehicle numbers:" + temp.select(temp("tagid")).distinct().count())

    val temp1 = temp.filter("tagid is not null")
    println("distinct tags after removing all  null tagids:" + temp1.select(temp1("tagid")).distinct().count())

    /*val filterdSet=temp1.filter("VEHICLENUMBER !='XXXXXXXXXXXX'").cache()
println("distinct tags after removing vehicle number as 'XXX..':"+filterdSet.select(filterdSet("tagid")).distinct().count())
 */
    val newFilteredSet = temp1.filter("translate(VEHICLENUMBER,'X','') !=''")
    println("distinct tags after removing vehicle number as 'XXX..':" + newFilteredSet.select(newFilteredSet("tagid")).distinct().count())

    val filteredRdd = newFilteredSet.rdd.map(row => (row.get(0).toString(), (row.get(1).toString(), row.get(2).toString().toInt, row.get(2).toString().toInt)))
    val reducedRdd = filteredRdd.reduceByKey(getMax);
    val rddAfterModelling = reducedRdd.filter(x => (x._2._2.toFloat / x._2._3.toFloat) > 0.5)

    val outputRdd = rddAfterModelling.mapValues(x => x._1)
    println("Distinct tags with vehicle number after modelling:" + outputRdd.count())
    //rddAfterModelling.take(20).foreach(println)
    val pattern = "^[A-Z]{2}[0-9]{2}[A-Z]+[0-9]+$"
    val out1 = outputRdd.mapValues(x => (x, x.matches(pattern))).persist()
    val out2 = outputRdd.filter(x => x._2.matches(pattern))
    out1.take(20).foreach(println)
    println(out2.count());

  }

  type data = (String, Int, Int)

  def getMax(data1: data, data2: data): data = {
    val vehicleNumber1 = data1._1
     val vehicleNumber1_Count = data1._2
    val vehicleNumber1_total_Count = data1._3

    val vehicleNumber2 = data2._1
    val vehicleNumber2_Count = data2._2
    val vehicleNumber2_total_Count = data2._3

    if (vehicleNumber1_Count >= vehicleNumber2_Count) {
      (vehicleNumber1, vehicleNumber1_Count, vehicleNumber1_total_Count + vehicleNumber2_total_Count)
    } else {
      (vehicleNumber2, vehicleNumber2_Count, vehicleNumber1_total_Count + vehicleNumber2_total_Count)
    }
  }
}