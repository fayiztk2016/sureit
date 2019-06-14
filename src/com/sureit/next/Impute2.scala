package com.sureit.next
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
object Impute2 {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val t0 = System.currentTimeMillis()

    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp")
      .getOrCreate()
      
      
    val lines = spark.sparkContext.textFile("file:///D:/task/TagID_VehicleNumber_18_02.1.csv")
    val sourceVals =
      lines.map(x => x.split(",")).filter(x => x.length > 0).map(x => (x(0)))    
    val pattern = "^[34|91].*$"
    val filteredSourceVals = sourceVals.filter(x => (x.matches(pattern)))
      .filter(x => (x.length() > 10 && x.length() < 40))
      .collect.toSet
      
    val txnLines = spark.sparkContext.textFile("file:///D:/task/TagVehST.txt")
    val txnVals = txnLines.map(x => x.split(",")).filter(x => x.length >= 2)
      .map(x =>
        if (x.length == 2)
          (x(0), (x(1), null))
        else
          (x(0), (x(1), x(2))))

    val joined = txnVals.filter(x => (filteredSourceVals.contains(x._1)))
       //.partitionBy(new HashPartitioner(50))
       .persist()
      
    

    val format = "^[a-zA-Z]{2}[0-9]{1,2}[A-Z]+[0-9]+$"
    val properFormatRDD = joined.mapValues(x => (x._1)).filter(x => (x._2.matches(format))).distinct().persist()

    val multiVehicleGroup = properFormatRDD.groupByKey().filter(x => x._2.size > 1).mapValues(x => (x.toList))
    val multiVehicleDF = spark.createDataFrame(multiVehicleGroup).toDF("TAG", "VEHICLENUMBER").select(col("TAG"), concat_ws(" ", col("VEHICLENUMBER")) as "VEHICLENUMBER")
    writeToCSV(multiVehicleDF, "TagWithMultiVehicleNumber.csv")

    val multiVehicleSet = multiVehicleGroup.map(x => x._1).collect.toSet
    val uniqueVehiclesRDD = properFormatRDD.filter(x => (!multiVehicleSet.contains(x._1)))
    val uniqueVehiclesDF = spark.createDataFrame(uniqueVehiclesRDD).toDF("TAG", "VEHICLENUMBER")
    writeToCSV(uniqueVehiclesDF, "UniqueVehicleNumber.csv")

    val setProper = properFormatRDD.map(x => x._1).collect.toSet
    val improper = joined.filter(x => (!setProper.contains(x._1)))  //449521 distincts

    val groupByState = improper.filter(x => x._2._2 != null).map(x => ((x._1, x._2._2), 1.toLong)).reduceByKey((x, y) => (x + y)).map(x => (x._1._1, x._1._2, x._2)).persist()
    val groupByStateDF = spark.createDataFrame(groupByState).toDF("TAG", "STATE", "COUNT")
    writeToCSV(groupByStateDF, "groupByState.csv")

    joined.unpersist()

    val tagWithMaxState = groupByState.map(x => (x._1, (x._2, x._3, x._3))).reduceByKey(getMax).map(x => (x._1, x._2._1, x._2._2, x._2._3))
        .filter(x => ((x._3.toFloat / x._4.toFloat) >= .25))
    val tagWithMaxStateDF = spark.createDataFrame(tagWithMaxState).toDF("TAG", "STATE", "COUNT", "TOTALCOUNT")
    writeToCSV(tagWithMaxStateDF, "tagWithMaxStateDF.csv")

 
    
    val stateMap = spark.sparkContext.textFile("file:///D:/task/StateCode.csv").map(x => x.split(","))
      .map(x => (x(1), x(0))).collect().toMap
    val imputedVehicleNumbers = tagWithMaxState.map(x => (x._1, stateMap.getOrElse(x._2,"default").toString() + "00")).persist()
    val imputedVehicleNumbersDF = spark.createDataFrame(imputedVehicleNumbers).toDF("TAG", "VEHICLENUMBERS")
    writeToCSV(imputedVehicleNumbersDF, "imputedVehicleNumbersDF.csv")

    
    println(imputedVehicleNumbers.filter(x=>x._2.equals("default")).count)
    val t1 = System.currentTimeMillis()
    println("Elapsed time: " + (t1 - t0).toFloat / 60000 + "mnts")
    
    
  }

  def writeToCSV(df: DataFrame, file: String): Unit = {
    val folder = "file:///D:/task/Impute/testCOnfirm/"
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