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

object NearerTag1 {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val t0 = System.currentTimeMillis()
    println(t0)
    val spark = getSparkSession()
    val input = Array("6001", "2017-10-15")
    val inputDate = LocalDate.parse(input(1))
    val inputPlaza = input(0)
    val prevDay = inputDate.minusDays(1).toString()
//    println(inputDate)
  //  println(prevDay)

    val tagPlazaDate = getAllData.persist
    
    /*.filter(x=>x._1=="9.18907E+19").persist
    
    val currTxn=tagPlazaDate.filter(_._3.substring(0, 10).equals(input(1)))
       val prevTxn=tagPlazaDate.filter(_._3.substring(0, 10).equals(prevDay))
   
    
    println(""+currTxn.foreach(println))
     println("------------------")
 println(""+prevTxn.foreach(println))*/
   
    val filteredTxnOnDate = tagPlazaDate.filter(_._3.substring(0, 10).equals(prevDay))
   
    val tagLatestTxnOnPlaza = filteredTxnOnDate.map(x => ((x._1, (x._2, x._3.substring(10))))).reduceByKey((x, y) => if (x._2 > y._2) x else y)
    .map(x=>(x._1,x._2._1))
    //println("input Plaza:"+inputPlaza+" ")
    
   // tagLatestTxnOnPlaza.take(10).foreach(println)  
    println("---------------------------------------------------------------")
    val plazaDistance=getplazaDistance
    val distanceTravellorFromInputPlaza=
      tagLatestTxnOnPlaza.map(x=>(x,(plazaDistance.getOrElse(inputPlaza,Map("1"-> -1f)).getOrElse(x._2, -1f)))).persist
   
      val txnsOnInputPlaza = tagPlazaDate .filter(_._2.equals(input(0))).filter(_._3.substring(0, 10).equals(input(1)))
                      .map(_._1).distinct.collect.toSet
      
      val nearerTags=distanceTravellorFromInputPlaza . filter(x=> x._2>0f&&x._2<500).map(x=>(x._1._1,x._1._2,x._2))
      val nearerTagwithInputDayTxn=nearerTags.map(x=>(x._1,x._2,x._3,if (txnsOnInputPlaza.contains(x._1)) 1 else 0))
      
      val distantTags=distanceTravellorFromInputPlaza.filter(x=> x._2>1000).map(x=>(x._1._1,x._1._2,x._2))
      val distantTagswithInputDayTxn=distantTags.map(x=>(x._1,x._2,x._3,if (txnsOnInputPlaza.contains(x._1)) 1 else 0))
       
    //  distanceTravellorFromInputPlaza.take(5).foreach(println) 
       println("---------------------------------------------------------------")
    //nearerTagwithInputDayTxn.take(5).foreach(println) 
     //nearerTagwithInputDayTxn.take(5).foreach(println) 
     
 /*    val nearerTagsDF = spark.createDataFrame(nearerTagwithInputDayTxn).toDF("TAG","PLAZA","DISTANCE","TXN_ON_INPUT_DATE")
   writeToCSV(nearerTagsDF, "nearerTagsDF.csv")
     
      val distantTagsDF = spark.createDataFrame(distantTagswithInputDayTxn).toDF("TAG","PLAZA","DISTANCE","TXN_ON_INPUT_DATE")
   writeToCSV(distantTagsDF, "distantTagsDF.csv")*/
   
      val nearerTagVariable=nearerTagwithInputDayTxn.map(x=>(x._1,1,0))
      val distantTagVariable=distantTagswithInputDayTxn.map(x=>(x._1,0,1))
      val distanceVariables=distantTagVariable.union(nearerTagVariable).map(x=>(x._1,(x._2,x._3)));
    
    val distinctTags=tagPlazaDate.map(_._1).distinct.map(x=>(x,if (txnsOnInputPlaza.contains(x)) 1 else 0))
    val tagDistanceVariable=distinctTags.leftOuterJoin(distanceVariables).map(x=>(x._1,x._2._2,x._2._1))
    
  val tagDistanceVariableFormatted=tagDistanceVariable.map{
      case (x,None,z)=> (x,(0,0),z)
      case(x,Some(y),z)=>(x,y,z)
    }.map(x=>(x._1,x._2._1,x._2._2,x._3))
     val tagDistanceVariableDF = spark.createDataFrame(tagDistanceVariableFormatted).toDF("TAG","NEARER","DISTANT","TXN_ON_INPUT_DATE")
   writeToCSV(tagDistanceVariableDF, "tagDistanceVariableDF.csv")
   
   
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
  
  def getAllData = {
    val spark = getSparkSession()

    /* spark.read.format("CSV").option("header","true").load("file:///D:/task/data/INSIGHT.csv")
      .rdd.filter(x=>x(0).equals("34161FA8223286F8020C8DA0"))
   .map(x=>Try{(x(0).toString(),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9).toString(),x(9).toString.length)}.getOrElse(-1,-1))
   */
    spark.sparkContext.textFile("file:///D:/task/data/TagPlazaCode.txt").map(_.split(",")).map(x => (x(0), x(1), x(2)))
  }
  def getplazaDistance={
     val spark = getSparkSession()
      val distanceRDD=spark.sparkContext.textFile("file:///D:/task/data/PlazaCodeDistance.txt").map(_.split(",")).map(x => (x(0), (x(1), x(2).toFloat)))
      distanceRDD.groupByKey.collectAsMap().map(x=>(x._1,x._2.toMap))
  }
   def writeToCSV(df: DataFrame, file: String): Unit = {
    val folder = "file:///D:/task/NearerTag/sample/"
    df.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(folder + file)
  }
}