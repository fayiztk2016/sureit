package com.sureit
import org.apache.spark.storage.StorageLevel
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
object TxnOnPerformanceDate {
  def apply(inputData: RDD[(String, String, String, String, String, String, String)], inputVariables: Array[String]) = {
    val spark = getSparkSession()
    import spark.implicits._
    val inputPlaza = inputVariables(0)
    val performanceDate = inputVariables(1)
    val customizedInputData = inputData.filter(x => x._2 == inputPlaza)
      .map(x => (x._1, x._3.substring(0, 10)))
      .distinct

    val tagWithTxnOnPerformanceDate = customizedInputData.filter(x => (x._2 == performanceDate))
      .map(x => (x._1, 1))
    //     .persist(StorageLevel.DISK_ONLY)

    val tagWithTxnOnPerformanceDateDF = tagWithTxnOnPerformanceDate.toDF("tag", "txn_on_input_date")
    tagWithTxnOnPerformanceDateDF
  }
  def getSparkSession(): SparkSession = {
    SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "hdfs://192.168.70.7:9000/vivek/temp")
      .getOrCreate()
  }
}