package com.sureit.next

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object CustomerTime {
  
  def main(args:Array[String]){
    
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "CustomerTime")
    val lines=sc.textFile("file:///D:/Learning/Spark/udemy/source/SparkScala/customer-orders.csv")
    val words=lines.map(x=>x.split(","))
    val wordPair=words.map(x=>(x(0),x(2).toFloat))
    val out=wordPair.reduceByKey((x,y)=>(x+y)).sortBy(_._2)
  //  val sorted=out.toSeq.sortBy().reverese
    out.foreach(println)
    
   
  }
  
}