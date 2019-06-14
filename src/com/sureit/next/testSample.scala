package com.sureit.next

import scala.collection.immutable.TreeSet
import java.time.{ LocalDate,LocalDateTime, Period,Duration }
import java.time.format.DateTimeFormatter
import scala.util.Try
import scala.collection.mutable.ArrayBuffer
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import java.util.{Calendar,Date=>date}
import java.text.SimpleDateFormat

import org.apache.spark.sql._
import org.apache.spark.sql.functions.{date_format}
import org.apache.spark.sql.catalyst.expressions._

import java.sql.{Date =>d}
import org.apache.spark.sql.functions
object testSample extends App {
   
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
 
      val date="2017-06-16 05:35:22.000"
      val oldPattern = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    
      val lc=LocalDateTime.parse(date,oldPattern)
      println(lc)
       val date1="2017-06-17 07:58:43.000"
       val lc1=LocalDateTime.parse(date1,oldPattern)
       val dr=Duration.between(lc, lc1).toHours()
      println("********************"+dr)
       
       val d1=LocalDate.parse("2017-12-17")
       val d2=LocalDate.parse("2017-10-16")
       val diff=Period.between(d1, d2)
       
      // val diff2=Duration.between(d1,d2).toHours()
       val diff3=Period.between(d1, d2).getMonths
      println("diff:"+diff+" "+diff3)
     
      
    val    data=spark.sparkContext.
    parallelize(Seq((1,0),(2,1),(3,0),(4,2)))
     
    val d=data. map(x=>
      if(x._1==0) (x,0,0) else (x._1,0)
    )
    
        val    data2=spark.sparkContext.
    parallelize(Seq(("sun",10),("mon",20)
       // ,("tue",3), ("wed",4),("thus",5),
     // ,  ("sun","11"),("mon","12"),("tue","13")
        //, ("tue",24),("thusd",5)
        ))
      
          val    test=
    spark.sparkContext.
    parallelize(Seq((1.toString(),null,3)))
    test.foreach(println)
 //  val tt=test.filter(x=>x._1.equals(null))
        
val dd=data.leftOuterJoin(data).map(x=>(x._1,x._2._1,x._2._2))
      
    val out1=dd.map{
      case (x,y,None)=> (x,y,0)
      case(x,y,Some(z))=>(x,y,z)
    }
    out1.foreach(println)

        val i=Map(1->1)
        val xy=Try{8}
        println(xy)
/*       case class p(id:String) extends Ordered[p]{
      
      override  def compare(that: p):Int =
    that.id.compareTo(this.id)
     
      
    }   
 val t=TreeSet(p("1"),p("2"),p("3"))
 val it=t.iterator
 //val tem=(it.next)
 while (it.hasNext){
   println(it.next.id)
 }*/
 /*
 val te="12-11-2010xys"
 println(te.substring(0,10))
    val dateFormat = new SimpleDateFormat("ddmmyyyy")
   
     val time=LocalDate.parse("2017-06-03 17:33:57.000")
     val time1=LocalDate.parse("2010-12-10")
   //  println("x:"+time.after(time1))
     val out=Period.between(time,time1).getDays
     println(out)*/
   val ar=ArrayBuffer(1,90)
   val out=ar.mkString(",")
   println(out)
   // out.e
   abstract class tes(x:String){
     val person:String
     
   }
   //val t1=new tes
    case class p(name:String){
       def equals(p1:p):Boolean={
         println("testss")
         this.name.equals(p1.name)
       }
    }
    val p1=p("a")
    val p2=new p("a")
    println(p1==p2)
    
    val sq=for(i <- 1 to 10)
      yield(
           
          )
    println(sq)
   val m=Map(1->Array(2,3),2->Array(1)).toSeq
   val x=spark.sparkContext.parallelize(m)
  
 val tup=spark.sparkContext.
    parallelize(Seq((1,2,3),(1,3,4),(2,4,5),(1,3,10))).map(x=>(x._1,(x._2->x._3))).groupByKey.collectAsMap()
 //   val mp1=tup.mapValues(x=>Map(x))
   // val initialSet = mutable.HashSet.empty[(Int, Int)]
   // val mp2=mp1.aggregateByKey.(seqOp, combOp).collect.toMap
    //.mapValues(x=>Set(x))
   // mp1.foreach(println)
 //   println(mp2(1)(2))
   // val  val1=mp1.get
 
 //val mp4=mp3.collect
    val mp1=tup.mapValues(x=>x.toMap)
 println("x")
 tup.foreach(println)
 mp1.foreach(println)
 println(mp1(1)(2))
/*import spark.sqlContext.implicits._
    val someDF = Seq(
  (8, d.valueOf("2016-09-30")),
  (64, d.valueOf("2016-09-30"))
 
).toDF("number", "word")

val for1=someDF.withColumn("day", date_format(col("word"),"EEEE"))
for1.show*/
    
 
  
  
}