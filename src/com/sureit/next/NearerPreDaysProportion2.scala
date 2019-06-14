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
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import scala.collection.immutable.TreeSet
import org.apache.spark.sql.SQLImplicits
import java.time.{ LocalDate, LocalDateTime, Period, Duration }
import java.time.format.DateTimeFormatter
import java.sql.Timestamp

object NearerPreDaysProportion2 {

  case class txn(plaza: String, time: LocalDate) extends Ordered[txn] {

    override def compare(that: txn): Int =
      that.time.compareTo(this.time)

  }
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val t0 = System.currentTimeMillis()
    println(t0)
    val spark = getSparkSession()
   
    val inputVariables = Array("8001", "2018-10-13", "2018-07-01")
    val inputPlaza = inputVariables(0)
    val performanceDate = inputVariables(1)
    val inputDate = LocalDate.parse(inputVariables(1))
    val startDate = inputVariables(2)

    val inputData = getInputData
      //.filter(x => x._1 == "34161FA8203286140203EC00")
      .persist()
      
      val inputDataFiltered=inputData.map(x=>(x._1,x._5))
       .filter(x=>(x._2.substring(0,10) <= performanceDate))
    .filter(x=>(x._2.substring(0,10)>= startDate))
      
  //  val sevenDayVariables=get7DayVariables(inputDataFiltered,inputVariables)
  // val wholeDataVaraibles=getwholeDataVaraibles()
    //val peformanceDayVariables=getpeformanceDayVariables()
    val preDaysProp = getPrevDaysProportion(inputData, inputVariables)
    preDaysProp.show
  /*  val distance = getDistanceVariable(inputData, inputVariables)

    val variable1 =preDaysProp.join(distance, preDaysProp("TAG") === distance("TAG"))
        .select(preDaysProp("TAG"), distance("TXN_ON_INPUT_DATE"),
          distance("NEARER"), distance("DISTANT"),
          preDaysProp("DAYS_PROP"),
          preDaysProp("PRE_1"), preDaysProp("PRE_2"), preDaysProp("PRE_3"), preDaysProp("PRE_4"), preDaysProp("PRE_5"), preDaysProp("PRE_6"), preDaysProp("PRE_7")) //.na.fill(0)
    writeToCSV(variable1, "variable-1.csv")*/
  //  variable1.show
    val t1 = System.currentTimeMillis()
    println((t1 - t0).toFloat / 60000)
  }
 /*  def get7DayVariables(inputData: RDD[( String, String)], inputVariables: Array[String]) = {
     
     val inputPlaza = inputVariables(0)
    val performanceDate = inputVariables(1)
    val inputDate = LocalDate.parse(inputVariables(1))
    val startDate = inputVariables(2)
    
     val tagTimeDF = inputData.map(x => (x._1,x._2.substring(0, 10)))
      .toDF("tag", "time")
      .withColumn("time",to_date( $"time"))
   .filter($"time" === to_date(lit(performanceDate)))
    .filter($"time" >= to_date(lit(startDate)))
    
    
      val inputFiltered = inputData.map(x => (x._1, x._2, x._3, x._4, LocalDate.parse(x._5.substring(0, 10))))
      .filter(x => x._2.equals(inputPlaza)).filter(x => x._5.isBefore(inputDate))
    // .filter(x=>x._1=="9189070480100001CD7E")
    //  println("input*****************************************");
    // println(input.take(5).foreach(println))

    val tagDetails = inputFiltered.map(x => (x._1, TreeSet(txn(x._2, x._5))))
      .reduceByKey((x, y) => (x ++ y))

    val tagWithLatestTxn = tagDetails.map(x => (x._1, x._2.head.time, x._2))

    val filteredTags = tagWithLatestTxn.filter(x => (x._2.isAfter(eighthDay)))
    //  println("filteredTagss*****************************************");
    //println(filteredTags.take(5).foreach(println))

    val tagWithDiff =
      filteredTags.map(x =>
        (x._1, for (tn <- x._3 if tn.time.isAfter(eighthDay)) yield {
          //(Duration.between(inputDate, tn.time).toDays().toInt)
          Period.between(tn.time, inputDate).getDays

        }))
    //println("tagWithDiff*****************************************");
    //println(tagWithDiff.take(5).foreach(println))

    implicit def bool2int(b: Boolean) = if (b) 1 else 0
    val prev7Days = tagWithDiff.map(x => (x._1, x._2.contains(1).toInt, x._2.contains(2).toInt,
      x._2.contains(3).toInt, x._2.contains(4).toInt,
      x._2.contains(5).toInt, x._2.contains(6).toInt,
      x._2.contains(7).toInt))
    //println("variables*****************************************");
    //variables.take(5).foreach(println)

    val prev7DaysDF = spark.createDataFrame(prev7Days).toDF("TAG", "PRE_1", "PRE_2", "PRE_3", "PRE_4", "PRE_5", "PRE_6", "PRE_7")
    //writeToCSV(prev7DaysDF, "prev7DaysDF.csv")
   }*/
  def getPrevDaysProportion(inputData: RDD[(String, String, String, String, String)], inputVariables: Array[String]) = {
    val spark = getSparkSession()
  import spark.implicits._
    val inputPlaza = inputVariables(0)
    val performanceDate = inputVariables(1)
    val inputDate = LocalDate.parse(inputVariables(1))
    val startDate = inputVariables(2)

    val eighthDay = inputDate.minusDays(8)
   

    val tagTimeDF = inputData.map(x => (x._1,x._5.substring(0, 10)))
      .toDF("tag", "time")
      .withColumn("time",to_date( $"time"))
   .filter($"time" === to_date(lit(performanceDate)))
    .filter($"time" >= to_date(lit(startDate)))
     
      .distinct()

    tagTimeDF.createTempView("TagTime")
    tagTimeDF.show()
    val tagCountMinDF = spark.sql("select tag,count(tag) count,min(time) start_date from TagTime group by tag")
    //tagCountMinDF.printSchema()
    val tagCountDiff = tagCountMinDF.withColumn("diff", datediff(to_date(lit(performanceDate)), $"start_date"))
    //.map(x => (x._1, x._2.toFloat, Period.between(x._3, inputDate).getDays))

    val daysProportionDF = tagCountDiff.select($"tag", ($"count" / $"diff") as "dp")
    // daysProportionDF.show
    //tagCountDiff.take(5).foreach(println)

    val inputFiltered = inputData.map(x => (x._1, x._2, x._3, x._4, LocalDate.parse(x._5.substring(0, 10))))
      .filter(x => x._2.equals(inputPlaza)).filter(x => x._5.isBefore(inputDate))
    // .filter(x=>x._1=="9189070480100001CD7E")
    //  println("input*****************************************");
    // println(input.take(5).foreach(println))

    val tagDetails = inputFiltered.map(x => (x._1, TreeSet(txn(x._2, x._5))))
      .reduceByKey((x, y) => (x ++ y))

    val tagWithLatestTxn = tagDetails.map(x => (x._1, x._2.head.time, x._2))

    val filteredTags = tagWithLatestTxn.filter(x => (x._2.isAfter(eighthDay)))
    //  println("filteredTagss*****************************************");
    //println(filteredTags.take(5).foreach(println))

    val tagWithDiff =
      filteredTags.map(x =>
        (x._1, for (tn <- x._3 if tn.time.isAfter(eighthDay)) yield {
          //(Duration.between(inputDate, tn.time).toDays().toInt)
          Period.between(tn.time, inputDate).getDays

        }))
    //println("tagWithDiff*****************************************");
    //println(tagWithDiff.take(5).foreach(println))

    implicit def bool2int(b: Boolean) = if (b) 1 else 0
    val prev7Days = tagWithDiff.map(x => (x._1, x._2.contains(1).toInt, x._2.contains(2).toInt,
      x._2.contains(3).toInt, x._2.contains(4).toInt,
      x._2.contains(5).toInt, x._2.contains(6).toInt,
      x._2.contains(7).toInt))
    //println("variables*****************************************");
    //variables.take(5).foreach(println)

    val prev7DaysDF = spark.createDataFrame(prev7Days).toDF("TAG", "PRE_1", "PRE_2", "PRE_3", "PRE_4", "PRE_5", "PRE_6", "PRE_7")
    //writeToCSV(prev7DaysDF, "prev7DaysDF.csv")

    val prev7DaysFormatted = prev7Days.map((x => (x._1, (x._2, x._3, x._4, x._5, x._6, x._7, x._8))))

    val preDaysPropotionDF = //daysPropotion.leftOuterJoin(prev7DaysFormatted)
      daysProportionDF.join(prev7DaysDF, daysProportionDF("tag") === prev7DaysDF("TAG"), "left_outer")
        .select(daysProportionDF("TAG"), daysProportionDF("dp") as "DAYS_PROP",
          prev7DaysDF("PRE_1"), prev7DaysDF("PRE_2"), prev7DaysDF("PRE_3"), prev7DaysDF("PRE_4"), prev7DaysDF("PRE_5"), prev7DaysDF("PRE_6"), prev7DaysDF("PRE_7"))
        .na.fill(0)

    // writeToCSV(joinedDF, "joinedDF.csv")
    //  println("preDaysPropotionDF:"+preDaysPropotionDF.count())
    preDaysPropotionDF

  }

  def getDistanceVariable(inputData: RDD[(String, String, String, String, String)], inputVariables: Array[String]) = {
    val spark = getSparkSession()
    val inputPlaza = inputVariables(0)
    val inputDate = LocalDate.parse(inputVariables(1))
    val prevDay = inputDate.minusDays(1).toString()

    val tagPlazaDate = inputData.map(x => (x._1, x._2, x._5))

    val filteredTxnOnDate = tagPlazaDate.filter(_._3.substring(0, 10).equals(prevDay))

    val tagLatestTxnOnPlaza = filteredTxnOnDate.map(x => ((x._1, (x._2, x._3.substring(10))))).reduceByKey((x, y) => if (x._2 > y._2) x else y)
      .map(x => (x._1, x._2._1))
    //println("input Plaza:"+inputPlaza+" ")

    // tagLatestTxnOnPlaza.take(10).foreach(println)
    println("---------------------------------------------------------------")
    val plazaDistance = getplazaDistance
    val distanceTravellorFromInputPlaza =
      tagLatestTxnOnPlaza.map(x => (x, (plazaDistance.getOrElse(inputPlaza, Map("1" -> -1f)).getOrElse(x._2, -1f)))).persist

    val txnsOnInputPlaza = tagPlazaDate.filter(_._2.equals(inputPlaza)).filter(_._3.substring(0, 10).equals(inputVariables(1)))
      .map(_._1).distinct.collect.toSet

    val nearerTags = distanceTravellorFromInputPlaza.filter(x => x._2 > 0f && x._2 < 500).map(x => (x._1._1, x._1._2, x._2))

    val distantTags = distanceTravellorFromInputPlaza.filter(x => x._2 > 1000).map(x => (x._1._1, x._1._2, x._2))

    val nearerTagVariable = nearerTags.map(x => (x._1, 1, 0))
    val distantTagVariable = distantTags.map(x => (x._1, 0, 1))
    val distanceVariables = distantTagVariable.union(nearerTagVariable).map(x => (x._1, x._2, x._3));
    val distanceVariablesDF = spark.createDataFrame(distanceVariables).toDF("TAG", "NEARER", "DISTANT")

    val distinctTags = tagPlazaDate.map(_._1).distinct.map(x => (x, if (txnsOnInputPlaza.contains(x)) 1 else 0))
    val distinctTagsDF = spark.createDataFrame(distinctTags).toDF("TAG", "TXN_ON_INPUT_DATE")

    val distanceWithCurrentDayTxnDF = //daysPropotion.leftOuterJoin(prev7DaysFormatted)
      distinctTagsDF.join(distanceVariablesDF, distinctTagsDF("TAG") === distanceVariablesDF("TAG"), "left_outer")
        .select(distinctTagsDF("TAG"), distinctTagsDF("TXN_ON_INPUT_DATE"),
          distanceVariablesDF("NEARER"), distanceVariablesDF("DISTANT"))
        .na.fill(0)
    // println("distanceWithCurrentDayTxnDF:"+distanceWithCurrentDayTxnDF.count())

    distanceWithCurrentDayTxnDF
    //  val tagDistanceVariable=distinctTags.leftOuterJoin(distanceVariables).map(x=>(x._1,x._2._2,x._2._1))

  }
  def getSparkSession(): SparkSession = {
    SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp")
      .getOrCreate()
  }

  def getInputData = {

    val spark = getSparkSession()
    spark.sparkContext.textFile("file:///D:/task/data/TagPlazaCodeEntryExitLaneTime-sample.txt")

      .map(_.split(","))

      .map(x => (x(0), x(1), x(2), x(3), x(4)))

  }

  def writeToCSV(df: DataFrame, file: String): Unit = {
    val folder = "file:///D:/task/NearerPreDaysProportion/s2/"
    df.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(folder + file)
  }
  def getplazaDistance = {
    val spark = getSparkSession()
    val distanceRDD = spark.sparkContext.textFile("file:///D:/task/data/PlazaCodeDistance.txt").map(_.split(",")).map(x => (x(0), (x(1), x(2).toFloat)))
    distanceRDD.groupByKey.collectAsMap().map(x => (x._1, x._2.toMap))
  }

}