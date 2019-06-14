package com.sureit
import org.apache.spark._
import scala.util.Try
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import scala.collection.immutable.TreeSet
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions._
import scala.collection.immutable.TreeSet
import scala.util.Try
import scala.collection.mutable.ArrayBuffer
import java.time.{ LocalDate, Period }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.DataFrameStatFunctions
import scala.collection.immutable.TreeSet
import org.apache.spark.sql.SQLImplicits
import java.time.{ LocalDate, LocalDateTime, Period, Duration }
import java.time.format.DateTimeFormatter

object Prev1to7 extends App {
  def apply(inputData: RDD[(String, String, String, String, String, String, String)], inputVariables: Array[String]) // : DataFrame 
  = {

    val spark = getSparkSession()
    import spark.implicits._
    val inputPlaza = inputVariables(0)
    val performanceDate = inputVariables(1)
    //    println("input:"+inputPlaza)
    val customizedInputData = inputData.filter(x => x._2 == inputPlaza)
      .map(x => (x._1, x._3.substring(0, 10)))
      .filter(x => x._2 != performanceDate)
    //      .filter(x=>x._1.length()>=9)
    //       .filter(x=>x._2.length()>=9)

    //   .persist(StorageLevel.MEMORY_AND_DISK)
    //.filter(x=>x._1=="34161FA8203286140203EC00")

    //customizedInputData.foreach(println)
    val customizedInputDataDF = customizedInputData.toDF("tag", "time")

      .withColumn("time", Try { to_date($"time") }.getOrElse(to_date(lit("2010-04-04"))))
      .withColumn("perf_date", to_date(lit(performanceDate)))

      .filter($"time" >= date_add($"perf_date", -7))
      .distinct
    //.persist(StorageLevel.DISK_ONLY)

    implicit def bool2int(b: Boolean) = if (b) 1 else 0
    val prev = customizedInputDataDF
      .select(
        $"tag",
        lit(datediff($"perf_date", $"time") === 1).cast(IntegerType) as "prev1",
        lit(datediff($"perf_date", $"time") === 2).cast(IntegerType) as "prev2",
        lit(datediff($"perf_date", $"time") === 3).cast(IntegerType) as "prev3",
        lit(datediff($"perf_date", $"time") === 4).cast(IntegerType) as "prev4",
        lit(datediff($"perf_date", $"time") === 5).cast(IntegerType) as "prev5",
        lit(datediff($"perf_date", $"time") === 6).cast(IntegerType) as "prev6",
        lit(datediff($"perf_date", $"time") === 7).cast(IntegerType) as "prev7")

    val prevGroup = prev.groupBy("tag").agg(sum($"prev1").alias("prev1"), sum($"prev2").alias("prev2"),
      sum($"prev3").alias("prev3"), sum($"prev4").alias("prev4"),
      sum($"prev5").alias("prev5"), sum($"prev6").alias("prev6"),
      sum($"prev7").alias("prev7"))
    // prev.show
    prevGroup
    // agg(collect_set("time"))
    //.withColumn("count",count($"collect_set(time)"))
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