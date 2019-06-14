package com.sureit
import java.util.Scanner
import org.apache.spark._
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

object Probability {

  def apply(variable: DataFrame, beta: Array[String], cutoff: String) = {
    val spark = getSparkSession()
    //    val S = new Scanner(System.in)
    import spark.implicits._

    //    print("Enter Parameter Value : ")
    val Parameter = beta(0)
    //    print("Enter nearer Value : ")
    val nearer = beta(1)
    //    print("Enter distant Value : ")
    val distant = beta(2)
    //    print("Enter dp Value : ")
    val dp = beta(3)
    //    print("Enter prev1 Value : ")
    val dplog = beta(4)
    val prev1 = beta(5)
    //    print("Enter prev2 Value : ")
    val prev2 = beta(6)
    //    print("Enter prev3 Value : ")
    val prev3 = beta(7)
    //    print("Enter prev4 Value : ")
    val prev4 = beta(8)
    //    print("Enter prev5 Value : ")
    val prev5 = beta(9)
    //    print("Enter prev6 Value : ")
    val prev6 = beta(10)
    //    print("Enter prev7 Value : ")
    val prev7 = beta(11)
    //    print("Enter same_state Value : ")
    val same_state = beta(12)
    //    print("Enter daily_pass Value : ")
    val daily_pass = beta(13)
    //    print("Enter monthly_pass Value : ")
    val monthly_pass = beta(14)
    //    print("Enter local Value : ")
    val local = beta(15)
    //    print("Enter clubbed_class Value : ")
    //    val clubbed_class = S.next()
    //    print("Enter txn Value : ")
    //    val txn = S.next()

    val beta1 = Array(
      Parameter, nearer, distant, dp, dplog, prev1, prev2, prev3, prev4, prev5, prev6, prev7, same_state, daily_pass, monthly_pass, local)
    val toDouble = udf[Double, String](_.toDouble)
    val variableFormatted = variable
      .withColumn("nearer", toDouble($"nearer"))
      .withColumn("distant", toDouble($"distant"))
      .withColumn("dp", toDouble($"dp"))
      .withColumn("dplog", toDouble($"dplog"))
      .withColumn("prev1", toDouble($"prev1"))
      .withColumn("prev2", toDouble($"prev2"))
      .withColumn("prev3", toDouble($"prev3"))
      .withColumn("prev4", toDouble($"prev4"))
      .withColumn("prev5", toDouble($"prev5"))
      .withColumn("prev6", toDouble($"prev6"))
      .withColumn("prev7", toDouble($"prev7"))
      .withColumn("same_state", toDouble($"same_state"))
      .withColumn("daily_pass", toDouble($"daily_pass"))
      .withColumn("monthly_pass", toDouble($"monthly_pass"))
      .withColumn("local", toDouble($"local"))
    //      .withColumn("clubbed_class", toDouble($"clubbed_class"))
    //      .withColumn("txn", toDouble($"txn"))

    val variableWithZ = variable
      .withColumn(
        "z",
        (lit(beta(0)) +
          lit(beta(1)) * $"nearer" +
          lit(beta(2)) * $"distant" +
          lit(beta(3)) * $"dp" +
          lit(beta(4)) * $"dplog" +
          lit(beta(5)) * $"prev1" +
          lit(beta(6)) * $"prev2" +
          lit(beta(7)) * $"prev3" +
          lit(beta(8)) * $"prev4" +
          lit(beta(9)) * $"prev5" +
          lit(beta(10)) * $"prev6" +
          lit(beta(11)) * $"prev7" +
          lit(beta(12)) * $"same_state" +
          lit(beta(13)) * $"daily_pass" +
          lit(beta(14)) * $"monthly_pass" +
          lit(beta(15)) * $"local"))
    //          lit(beta(15)) * $"clubbed_class" +
    //          lit(beta(16)) * $"txn"))

    //    print("Enter Cut-off : ")
    //    val In4 = S.next()
    val variableWithProb = variableWithZ
      .withColumn(
        "prob", bround((lit(1) / (lit(1) + (exp(lit(-1) * $"z")))), 4))

    val variableWithOutcome = variableWithProb.withColumn("event", (when($"prob" > cutoff, 1).otherwise(0)))
    variableWithOutcome

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