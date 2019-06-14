package com.sureit.next
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
object FindTxnState {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val t0 = System.currentTimeMillis()

    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp")
      .getOrCreate()
      
      
    val lines = spark.sparkContext.textFile("file:///D:/task/data/TagVehicle.txt")
    val sourceVals =
      lines.map(x => x.split(",")).filter(x => x.length > 2) .map(x => (x(0),x(2))) 
      .partitionBy(new HashPartitioner(50))
      
    //  println(sourceVals.count)
     
    val pattern = "^[34|91].*$"
    val filteredSourceVals = sourceVals.filter(x => (x._1.matches(pattern)))
      .filter(x => (x._1.length() >= 20 && x._1.length() <= 24))
     // .collect.toSet
  //    filteredSourceVals.take(10).foreach(println)
    
    
    

    val groupByState = filteredSourceVals.map(x => ((x._1, x._2), 1.toLong)).reduceByKey((x, y) => (x + y))
  //  joined.unpersist()

    val tagWithMaxState = groupByState.map(x => (x._1._1, (x._1._2, x._2, x._2))).reduceByKey(getMax).map(x => (x._1, x._2._1, x._2._2, x._2._3))
        .filter(x => ((x._3.toFloat / x._4.toFloat) >= .25)).persist()
    val tagWithMaxStateDF = spark.createDataFrame(tagWithMaxState).toDF("TAG", "STATE", "COUNT", "TOTALCOUNT")
    writeToCSV(tagWithMaxStateDF, "tagWithMaxStateDF.csv")

 
    
    val stateMap = spark.sparkContext.textFile("file:///D:/task/data/StateCodeName.txt").map(x => x.split(","))
      .map(x => (x(1), x(0))).collect().toMap
    val tagState = tagWithMaxState.map(x => (x._1, stateMap.getOrElse(x._2,x._1))).persist()
    val tagStateDF = spark.createDataFrame(tagState).toDF("TAG", "STATECODE")
    writeToCSV(tagStateDF, "tagStateDF")

    
    println(tagState.filter(x=>x._2.length>2).count)
    val t1 = System.currentTimeMillis()
    println("Elapsed time: " + (t1 - t0).toFloat / 60000 + "mnts")
    
    
  }

  def writeToCSV(df: DataFrame, file: String): Unit = {
    val folder = "file:///D:/task/TagState/2/"
    df.coalesce(1).write.format("csv").option("header", "true").save(folder + file)
  }
  type data = (String, Long, Long)
  def getMax(data1: data, data2: data): data = {

    val state1 = data1._1
    val count1 = data1._2
    val totalCount1 = data1._3

    val state2 = data2._1
    val count2 = data2._2
    val totalCount2 = data2._3

    if (count1 >= count2) {
      (state1, count1, count1 + count2)
    } else {
      (state2, count2, count1 + count2)
    }
  }
}