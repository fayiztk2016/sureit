package com.sureit.next


import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql._
//import  org.apache.spark.implicits._

object taskPartition {
   def main(args:Array[String]){
     Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
    val lines = spark.sparkContext.textFile("file:///D:/task/TagVehST.txt")
    val txnVals= 
      lines.map(x=>x.split(",")).filter(x=>x.length>1).map(x=>(x(0),x(1))).partitionBy(new HashPartitioner(200))
  /*  val pattern="^[TN|KA].*$"
val out1=txnVals.mapValues(x=>(x,x.matches(pattern))).persist()
out1.take(10).foreach(println)
//val out2=out1.filter(x=>x.*/
 
   
    
      val proper=spark.sparkContext.textFile("file:///D:/task/TagID_VehicleNumber_18_02.1.csv")
       val sourceVals= proper.map(x=>x.split(",")).filter(x=>x.length>1).map(x=>(x(0),(x(1))))
       
    /*   val out=txnVals.leftOuterJoin(sourceVals).persist()
       
        
        //println(txnVals.count())
      //txnVals.take(10).foreach(println)
      // out.saveAsTextFile("file:///D:/task/out.txt")
       val out1=out.filter(x=>x._2._2 .equals("None"))
       println(out1.count())
       val out2=out.filter(x=>(!x._2._2 .equals("None")))
  
       println(RDDProperFormat.count())
       println(out2.map(x=>x._1.distinct).count())*/
       val format="^[a-zA-Z]{2}[0-9]{1,2}[A-Z]+[0-9]+$"
    val RDDProperFormat=sourceVals.filter(x => (x._2.matches(format))).persist()
    RDDProperFormat.take(10).foreach(println)
       println(RDDProperFormat.count())
    
   
       
//   sourceVals.take(10).foreach(println)
        
        
   }
   
}