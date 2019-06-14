package com.sureit.next
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
object Impute5 {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val t0 = System.currentTimeMillis()

    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp")
      .getOrCreate()

    val improperRegNo = spark.sparkContext.textFile("file:///D:/task/data/ImproperRegNo.csv")
    val improperRegNoRDD =
      improperRegNo.map(x => x.split(",")).filter(x => x.length > 1).map(x => (x(0), x(1)))

    val format = "^[a-zA-Z]{2}[0-9]+[A-Z0-9]+$"
    val stateCode = spark.sparkContext.textFile("file:///D:/task/data/StateCodeName.txt").map(x => x.split(",")).map(x => (x(0), x(1)))
    val stateCodeMap = stateCode.collect.toMap
    val validRegNo = improperRegNoRDD.filter(x => (x._2.matches(format))) //314684,43937,29054
      .filter(x => stateCodeMap.contains(x._2.substring(0, 2))).persist()
    val validRegNoDF = spark.createDataFrame(validRegNo).toDF("TAG", "VEHICLENUMBER")
    writeToCSV(validRegNoDF, "1.1 validRegNoDF")

    val multiRegNoGroup = validRegNo.distinct.groupByKey().filter(x => x._2.size > 1).mapValues(x => (x.toList)).persist
    val multiRegNoGroupDF = spark.createDataFrame(multiRegNoGroup).toDF("TAG", "VEHICLENUMBER").select(col("TAG"), concat_ws(" ", col("VEHICLENUMBER")) as "VEHICLENUMBER")
    writeToCSV(multiRegNoGroupDF, "1.2 multiRegNoGroupDF")

    // val multiVehicleSet = multiVehicleGroup.map(x => x._1).collect.toSet
    val uniqueRegNoRDD = validRegNo.subtractByKey(multiRegNoGroup)
    val uniqueRegNoDF = spark.createDataFrame(uniqueRegNoRDD).toDF("TAG", "VEHICLENUMBER")
    writeToCSV(uniqueRegNoDF, "1.3 uniqueRegNoDF")

    val remainingRDD = improperRegNoRDD.subtractByKey(validRegNo)
    val remaining = remainingRDD.map(_._1).collect().toSet

    /*println(improperRegNoRDD.map(_._1).distinct.count)
        println(validRegNo.map(_._1).distinct.count)
         println(multiVehicleGroup.map(_._1).count)
      println(uniqueRegNoDF.count)
      println(improperVehicleNo.map(_._1).distinct.count)
      * 272779 26811  1824 25391 245968
      */

    //println(improperRegNoRDD.subtractByKey(validRegNo).count())

    val txnLines = spark.sparkContext.textFile("file:///D:/task/data/TagVehicle.txt")
    val txnVals = txnLines.map(x => x.split(",")).filter(x => x.length >= 2)
      .map(x =>
        if (x.length == 2)
          (x(0), (x(1), null))
        else
          (x(0), (x(1), x(2))))
    val joined = txnVals.filter(x => (remaining.contains(x._1)))
      //.partitionBy(new HashPartitioner(50))
      .persist()
      
      val mismatch=remainingRDD.subtractByKey(joined).persist()
       val mismatchDF = spark.createDataFrame(mismatch).toDF("TAG", "VEHICLE")
   writeToCSV(mismatchDF, "mismatch")
   println(mismatch.map(_._1).distinct.count)
      
/*
    // val format = "^[a-zA-Z]{2}[0-9]{1,2}[A-Z]+[0-9]+$"
    val validVehicleNo = joined.mapValues(x => (x._1)).filter(x => (x._2.matches(format)))
      .filter(x => stateCodeMap.contains(x._2.substring(0, 2))).distinct().persist()

    val multiVehicleGroup = validVehicleNo.groupByKey().filter(x => x._2.size > 1).mapValues(x => (x.toList)).persist()
    val multiVehicleDF = spark.createDataFrame(multiVehicleGroup).toDF("TAG", "VEHICLENUMBER").select(col("TAG"), concat_ws(" ", col("VEHICLENUMBER")) as "VEHICLENUMBER")
    writeToCSV(multiVehicleDF, "1.4 TagWithMultiVehicleNumber")

    val uniqueVehiclesRDD = validVehicleNo.subtractByKey(multiVehicleGroup)
    val uniqueVehiclesDF = spark.createDataFrame(uniqueVehiclesRDD).toDF("TAG", "VEHICLENUMBER")
    writeToCSV(uniqueVehiclesDF, "1.5 UniqueVehicleNumber")

    val txnAfterRegNoValidation = joined.subtractByKey(validVehicleNo).persist //449521 distincts

    val groupByState = txnAfterRegNoValidation.filter(x => x._2._2 != null).map(x => ((x._1, x._2._2), 1.toLong)).reduceByKey((x, y) => (x + y))
      .map(x => (x._1._1, x._1._2, x._2)) //.persist()
    // val groupByStateDF = spark.createDataFrame(groupByState).toDF("TAG", "STATE", "COUNT")
    //writeToCSV(groupByStateDF, "groupByState.csv")

    //  joined.unpersist()

    val tagWithMaxState = groupByState.map(x => (x._1, (x._2, x._3, x._3))).reduceByKey(getMax).map(x => (x._1, x._2._1, x._2._2, x._2._3))
      .filter(x => ((x._3.toFloat / x._4.toFloat) >= .25))
    val tagWithMaxStateDF = spark.createDataFrame(tagWithMaxState).toDF("TAG", "STATE", "COUNT", "TOTALCOUNT")
    writeToCSV(tagWithMaxStateDF, "1.6 tagWithMaxStateDF")

    val stateMap = stateCode.map(x => (x._2, x._1)).collect.toMap
    val imputedVehicleNumbers = tagWithMaxState.map(x => (x._1, stateMap.getOrElse(x._2, x._2.toString()).toString() + "00")).persist()
    val imputedVehicleNumbersDF = spark.createDataFrame(imputedVehicleNumbers).toDF("TAG", "VEHICLENUMBERS")
    writeToCSV(imputedVehicleNumbersDF, "1.7 imputedVehicleNumbersDF")

    println("count of def=" + imputedVehicleNumbers.filter(x => x._2.length() > 4).count)

    println("improperRegNoRDD: " + improperRegNoRDD.map(_._1).distinct.count)
    println("validRegNo: " + validRegNo.map(_._1).distinct.count)
    println("multiRegNoGroup: " + multiRegNoGroup.map(_._1).count())

    println("uniqueRegNoDF: " + uniqueRegNoDF.count)
    println("remaining: " + remaining.size)
    println("validVehicleNo: " + validVehicleNo.count)
    println("multiVehicleGroup: " + multiVehicleGroup.map(_._1).count)
    println("uniqueVehiclesRDD: " + uniqueVehiclesRDD.count())
    println("imputedVehicleNumbers: " + imputedVehicleNumbers.count)
    // 272779 26811  1824 25391 245968

    val imputedset = imputedVehicleNumbers.collect().toMap
    println("Txns  containing imputed vechicle numbers: " + joined.filter(x => imputedset.contains(x._1)).count)

    /*  val im1=txnAfterRegNoValidation.map(x=>(x._1,x._2._1,x._2._2))
      val improperDF = spark.createDataFrame(im1).toDF("TAG", "STATE", "COUNT")
    writeToCSV(improperDF, "1.8 improperDF")*/

    val t1 = System.currentTimeMillis()
    println("Elapsed time: " + (t1 - t0).toFloat / 60000 + "mnts")
*/
  }

  def writeToCSV(df: DataFrame, file: String): Unit = {
    val folder = "file:///D:/task/Impute/6.7_6_mismatch/"
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