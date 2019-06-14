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

object DataCreation {
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

    //tm.foreach(println)
/*
    val input = spark.sparkContext.textFile("file:///D:/task/data/TagExitPlazaSample.txt")
    val inputRDD = input.map(_.split(",")).map(x => (x(0), (x(1), x(3), Map(x(1) -> TreeSet(txn(x(1), x(3), x(2)))))))
    val analyzed = inputRDD.reduceByKey(plazaAnalysis)
    // analyzed.take(30).foreach(println)

    val lastPlazaTxns = analyzed.mapValues(x => (x._1, x._2, x._3.get(x._1).get))
    val output = lastPlazaTxns //.map(x=>(x._1,x._2._1,x._2._2,x._2._3))
      .mapValues(model)
    output.take(100).foreach(println)*/
      
        val input = spark.sparkContext.textFile("file:///D:/task/data/TagExitPlazaSample.txt")
    val inputRDD = input.map(_.split(",")).map(x => ((x(0), x(1)),( x(3), TreeSet(txn(x(1), x(3), x(2))))))
    val analayzedByPlaza=inputRDD.reduceByKey(analysisByPlaza).map(x=>(x._1._1,(x._1._2,x._2._1,x._2._2)))
    val LastPlaza=analayzedByPlaza.reduceByKey(getLastPlaza)
    //.filter(x=>(x._1.equals("918907048040000088E4")))
   
   // analayzedByPlaza.groupByKey.filter(x=>x._2.size>1).
    //.filter(x=>x._2._3.size>1).take(30)
//  LastPlaza  .filter(x=>(x._1.equals("9189070480400000B9E0")))    .foreach(println)
    
     val output = LastPlaza //.map(x=>(x._1,x._2._1,x._2._2,x._2._3))
      .mapValues(model)
      
    output .take(100)    .foreach(println)

  }

  case class txn(plaza: String, time: String, lane: String) extends Ordered[txn] {

    override def compare(that: txn): Int =
      that.time.compareTo(this.time)

  }
  type dataType = (String, String, Map[String, TreeSet[txn]])
  def plazaAnalysis(data1: dataType, data2: dataType): dataType = {
    if (data1._2 > data2._2) {
      (data1._1, data1._2, data1._3 ++ data2._3)
    } else {
      (data2._1, data2._2, data1._3 ++ data2._3)
    }
  }
  type dataByPlaza=(String,  TreeSet[txn])
  def analysisByPlaza(data1:dataByPlaza,data2:dataByPlaza):dataByPlaza={
    if (data1._1 > data2._1) {
      (data1._1,  data1._2 ++ data2._2)
    } else {
      (data2._1, data1._2 ++ data2._2)
    }
  }
  
  type dataByTag=    (String,String,  TreeSet[txn])
  def getLastPlaza(data1:dataByTag,data2:dataByTag):dataByTag={
    if (data1._2 > data2._2) {
      (data1._1, data1._2,data1._3)
    } else {
     (data2._1, data2._2,data2._3)
    }
  }

  type out = (String, String, ArrayBuffer[Int])
  type in = (String, String, TreeSet[txn])
  def model(input: in): out = {
    val plaza = input._1
    val time = input._2
    val details = input._3
    val event = ArrayBuffer(0, 0, 0, 0, 0, 0,0)

    val today = LocalDate.parse(time.substring(0, 10))

    val it = details.iterator
    it.next
    var break = false
   while (it.hasNext) {
     
     val curr=it.next.time.substring(0, 10)
      val currDate = LocalDate.parse(curr)
      val diff = Period.between(currDate, today).getDays
     // println(currDate+" "+today+" "+diff)
       if( diff<=7 && diff>0){
             event(diff-1)=1
         }else{
            break=true
         }
         
    }
 
    (plaza, time, event)
  }

}