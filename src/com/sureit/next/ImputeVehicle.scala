package com.sureit.next
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object ImputeVehicle {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
     
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
      
    val linesproper = spark.sparkContext.textFile("file:///D:/task/input_tag_vehicle.csv")
    val sourceValProper =
      linesproper.map(x => x.split(",")).filter(x => x.length > 0).map(x => (x(0))) .persist() 
      
      
    val lines = spark.sparkContext.textFile("file:///D:/task/VehicleNoNotAvailable.csv")
    val sourceVals =
      lines.map(x => x.split(",")).filter(x => x.length > 0).map(x => (x(0)))
      
      
    //.partitionBy(new HashPartitioner(200))
    val pattern = "^[34|91].*$"
    val filteredSourceVals = sourceVals.filter(x => (x.matches(pattern)))
                                       .filter(x => (x.length() > 10 && x.length() < 40))
                                     .map(x=>(x)).persist()
                                      .collect.toSet
     //  val common= sourceValProper.filter(x=>(filteredSourceVals.contains(x)) )
        
       //  val txnLines = spark.sparkContext.textFile("file:///D:/task/TagVehST.txt")//.persist()
   
         // println(txnLines.count())
    println(sourceVals.count())
         /*
        println(common.count())
           println(sourceVals.count())
           println(sourceValProper.count)
           sourceValProper.take(10).foreach(println)
           println()
           filteredSourceVals.take(10).foreach(println)
                       //.partitionBy(new HashPartitioner(50)) //.persist() //449521 !distincts
                                    
                                      
   
 
          
    
    val txnVals = txnLines .map(x => x.split(","))
                          .filter(x => x.length >=2)
                        .map( x=>
                             if(x.length==2)
                             (x(0),(x(1),null))
                             else
                             (x(0),(x(1),x(2)))
                             )
                        .partitionBy(new HashPartitioner(200))
                         
                               
                         // .map(x => (x(0), x(1),x(2))
                          //.partitionBy(new HashPartitioner(200))
  
    val joined1=   txnVals.join( filteredSourceVals)  
  //    println(txnVals.count())
       println(joined1.count())
       println(filteredSourceVals.count())
       
       
                           
    val joined= txnVals.filter(x=>(filteredSourceVals.contains(x._1)) )
                       .partitionBy(new HashPartitioner(50)) //.persist() //449521 !distincts
    
  //  println(joined.filter(x=>x._3==null).count())
   
    val format="^[a-zA-Z]{2}[0-9]{1,2}[A-Z]+[0-9]+$"
    val properFormatRDD=joined.mapValues(x=>(x._1)).filter(x => (x._2.matches(format))).distinct().persist()
  
    println(properFormatRDD.count)
    
    val properFormatGroup=properFormatRDD.groupByKey() .filter(x=>x._2.size>1).mapValues(x=>(x.toList))
        val properFormatDF=spark.createDataFrame(properFormatGroup).toDF("TAG","VEHICLENUMBER")
        .select(col("TAG"),concat_ws(" ",col("VEHICLENUMBER")) as "VEHICLENUMBER")
    properFormatDF.coalesce(1).write.format("csv").option("header", "true").save("file:///D:/task/ImputeVehicle/TagWithMultiVehicleNumber.csv")
 
 val multitagSet=properFormatGroup.map(x=>x._1).collect.toSet
    val uniqueVehicles=properFormatRDD.filter(x=>(!multitagSet.contains(x._1)))
    spark.createDataFrame(uniqueVehicles)
          .toDF("TAG","VEHICLENUMBER")
          .coalesce(1).write.format("csv").option("header", "true").save("file:///D:/task/ImputeVehicle/UniqueVehicleNumber.csv")

 //  val formatwith4= "^[a-zA-Z]{2}[0-9]{1,2}[A-Z]+[0-9]{4}$"
   //val filteredProper=
   
    val setProper=properFormatRDD.map(x=>x._1).collect.toSet
     val improper= joined.filter(x=>(!setProper.contains(x._1)) )//.persist() //449521 distincts   
   
 val groupByState=improper.map(x=>((x._1,x._2._2),1.toLong)).reduceByKey((x,y)=>(x+y))
                         
                                                                          
                                                                      
                                                              
 val tagWithMaxState=groupByState.map(x=>(x._1._1,(x._1._2, x._2, x._2))).persist()
                 // .partitionBy(new HashPartitioner(16))
                   .reduceByKey(getMax)//.persist()
   
/// val finalOne=groupByState.(filter(getMax))
     
  
  
 // val filteredOnPerc=idwithMaxState.filter(x=>((x._2._2.toFloat/x._2._3.toFloat)<=.75)) //.persist()
  
   // groupByState.coalesce(1).saveAsTextFile("file:///D:/task/ImputeVehicle/groupByState.txt")
 
  val groupByStateDF=spark.createDataFrame(groupByState.map(x=>(x._1._1,x._1._2,x._2))).toDF("TAG","STATE","COUNT")
   // groupByStateDF.coalesce(1).write.format("csv").option("header", "true").save("file:///D:/task/ImputeVehicle/groupByStateDF.csv")
   
 // println(joined.filter(x=>x._3==null).count())
  
   // val tagWithMaxStateDF=spark.createDataFrame(tagWithMaxState.map(x=>(x._1,x._2._1,x._2._2,x._2._3))).toDF("TAG","STATE","COUNT","TOTALCOUNT")
   // tagWithMaxStateDF.coalesce(1).write.format("csv").option("header", "true").save("file:///D:/task/ImputeVehicle/tagWithMaxStateDF.csv")
   
   //filteredVals.map(x=>x._1).take(10).foreach(println)
    // not matching 34 or 91 -->167
    //length==>24 20 18 19 23
  //DSwithProperFormat.distinct().take(50).foreach(println)
// println(improper.distinct().count())
 //   println(joined.map(x=>x._1).distinct().count())
 // println(RDDProperFormat.map(x=>x._1).distinct().count())
  //println(temp.map(x=>x._1).distinct().count())
 */
//println(properFormatGroup.count())
//println(properFormatRDD.map(x=>x._1).count())
//properFormatGroup.take(35).foreach(println)
  //val t1 = System.nanoTime()
    //println("Elapsed time: " + (t1 - t0) + "ns")
 //println(properFormatGroup.count())
 // println(filteredOnPerc.count())
 // println(joined.map(x=>(x._1)).distinct().count())
      
  }
    type data=(String,Long,Long)
  def getMax(data1:data,data2:data):data={

     val state1=data1._1
     val count1=data1._2
     val totalCount1=data1._3
     

     val state2=data2._1
     val count2=data2._2
      val totalCount2=data2._3
     
     if(count1>=count2){
       (state1,count1,count1+count2)
     }else{
       (state2,count2,count1+count2)
     }
   }
}