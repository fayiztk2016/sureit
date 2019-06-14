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
import scala.collection.immutable.TreeSet

object RouteTagging {
  
  case class txn(plaza: String, time: String, lane: String) extends Ordered[txn] {

    override def compare(that: txn): Int =
      that.time.compareTo(this.time)

  }
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val t0 = System.currentTimeMillis()
    println(t0)
    val spark = getSparkSession()
 
 //   val input =getInputData
    
    
    val input = spark.sparkContext.textFile("file:///D:/task/data/TagExitPlazaSample.txt")
    val inputRDD = input.map(_.split(",")).map(x => (x(0),  TreeSet(txn(x(1), x(3), x(2)))))
    val ordered = inputRDD.reduceByKey((x,y)=>(x++y))//.map(x => (x._1, (x._1._2, x._2._1, x._2._2)))
    val tagWithTxn=ordered.map(x=>(x._1,x._2.toArray))
    val pairsArray=
      for(
          tag<-tagWithTxn)yield{
        for (i<-0 to tag._2.length-3) yield {
          (tag._2(i).plaza,tag._2(i+1).plaza,tag._2(i).lane)
        }
              
       
      }
    val pairs=pairsArray.flatMap
    { case ( arr) => arr.map(x=>((x._1,x._2,x._3),1)) }
    .filter(x=>(x._1._1!=x._1._2))
    val plazaBycount=pairs.reduceByKey((x,y)=>(x+y))
    .map(x=>((x._1._1,x._1._3),(x._1._2,x._2.toFloat,x._2.toFloat)))
    val nextPlazaRatio=plazaBycount
    .reduceByKey((x,y)=> if(x._2>y._2) (x._1,x._2,x._3+y._3) else (y._1,y._2,x._3+y._3))
    .map(x=>(x._1,x._2._1,x._2._2,x._2._3,(x._2._2*100/x._2._3)))
        
        
    
   nextPlazaRatio.take(30).foreach(println)
    
    
     
   // val LastPlaza = analayzedByPlaza.reduceByKey(getLastPla    za)

 
    
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

  def getInputData = {
    val spark = getSparkSession()   
  //spark.sparkContext.textFile("file:///D:/task/data/TagPlazaCodeSample.csv").map(_.split(",")).map(x => (x(0), x(1), x(2)))   
  spark.sparkContext.textFile("file:///D:/task/data/TagExitPlazaSample.txt")
    .map(_.split(",")).map(x => ((x(0), x(1)), (x(3), Set(txn(x(1), x(3), x(2))))))
  }
  
  def writeToCSV(df: DataFrame, file: String): Unit = {
    val folder = "file:///D:/task/RouteTagging/1/"
    df.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(folder + file)
  }
 
}