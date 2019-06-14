package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.max
//import java.io.StringReader
import au.com.bytecode.opencsv.CSVReader
import java.util.HashMap
import java.lang.Integer
import org.apache.spark.rdd.JdbcRDD
/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object RatingsCounter {
 
  /** Our main function where the action happens */
  def findRating(line:String)={
    val vals=line.split("\t" )
    val x=vals.flatMap(x=>x.split(" "))
    val movie=vals(1)
    val rating=Array(vals(2).toFloat,1.toFloat)
    (movie,rating)
  }
 
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "RatingsCounter")
   
    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("../ml-100k/u.data")
    val test=lines.map(x=>x.split("\t")).map(x=>(x(0).toFloat,(x(1).toFloat,x(2).toFloat)))
    val testKet=test.sortByKey();
    val mapp=testKet.mapValues(x=>1)
       mapp.foreach(println)
       
       
    // val jdbc=new JdbcRDD();  
     
  /* // Convert each line to a string, split it out by tabs, and extract the third field.
    // (The file format is userID, movieID, rating, timestamp)
    val ratings = lines.map(x => x.toString().split("\t")(2)))
    
    // Count up how many times each value (rating) occurs
    val results = ratings.countByValue()
    
    // Sort the resulting map of (rating, count) tuples
 //   val sortedResults = results.toSeq.sortBy(_._1)
    
    // Print each result on its own line.
    lines.foreach(print)*/
    val totalRating=lines.map(findRating).reduceByKey((x,y)=>Array(x(0)+y(0),x(1)+y(1)))
    val popular=totalRating.map(x=>(x._1,(x._2(0)/x._2(1)))).sortBy(_._2 )
   // popular.foreach(println)
    
     val moviesNratings = lines.map(x => x.toString().split("\t")).map(x=>(x(1).toFloat,x(2).toFloat))
     val total=moviesNratings.mapValues(x=>(x,1)).reduceByKey((x,y)=>( x._1+y._1,x._2+y._2))
   val out=total.mapValues((x=> (x._1/x._2)))
   val maxim=out.max
   //   out.foreach(println)
   //   
     
     
    
    
    
    
    
     
       
    
  }
}
