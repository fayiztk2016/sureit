package com.sureit.next

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql._
//import  org.apache.spark.implicits._


object task {
   case class TXN(tag:String, number:String,property:Integer)
   case class Source(tag:String, number:String,property:Integer)
  
  def mapper(line:String):TXN = {
     
     val fields = line.split(',')  
    if(fields.length>1){
     TXN(fields(0), fields(1),0)
     
     }
    else{
        TXN("", "",0)
    }
   
  }
    def sourceMapper(line:String):TXN = {
     
     val fields = line.split(',')  
    if(fields.length>1){
     TXN(fields(0), fields(1),1)
     
     }
    else{
        TXN("", "",1)
    }
   
  }
   
  def main(args:Array[String]){
     Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
    val lines = spark.sparkContext.textFile("file:///D:/task/TagVehST.txt")
  // println(lines.filter(x=>x.replace(",","")=="").count())
   //lines.take(10).foreach(println)
   
    import spark.implicits._
    val txns=lines.map(mapper).toDS()
  // txns.show(10)
   
   val temp1= txns.filter("tag is not null")
   
   val proper=spark.sparkContext.textFile("file:///D:/task/TagVehicle.txt")
   val sourceDS=proper.map(sourceMapper).toDS()
   
   val out=temp1.join(sourceDS,Seq("tag"),"left_outer")
   println(out.count())
   //out.show(10)
    
 // val ds=temp1.groupBy("tag",new Hashi).count().show()
   
   
   /*
    val temp=lines.map(x=>x.split(','))  
    val temp0=temp.filter(x=>x.length>=2)
   //te.take(10).toString().foreach(println)
    val temp1=temp0.map(x=>(x(0),x(1)))
    val temp11=temp1.filter(x=>x._1!="")
    val temp12=temp11.filter(x=>x._2!="")
   val temp2=temp12.groupByKey().count()
   println(temp2)*?*/
   
  }
  
}