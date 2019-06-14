package com.sureit.next
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.graphframes._
import java.time.{ LocalDate, Period }
import org.apache.log4j._
import scala.util.Try
object RouteTaggingGraph {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val t0 = System.currentTimeMillis()
    val spark = getSparkSession()
    import spark.implicits._
    val inputVariables = //Array("54002", "2017-06-19", "2017-04-01","0.08")
      Array("8001", "2018-10-03", "2018-07-01", "0.08")

    //   val plaza = "8001"
    val inputPlaza = inputVariables(0)
    val performanceDate = inputVariables(1)
    val prevDay = LocalDate.parse(performanceDate).minusDays(1).toString()
    val nextDay=LocalDate.parse(performanceDate).minusDays(-1).toString()
    println(nextDay)

    val v = getPlazaData.map(_._1).distinct().toDF("id")

    val e = getPlazaData.toDF("src", "dst", "time")

    println("*********************starting")

    val g = GraphFrame(v, e)
    println("complete creation")

    val paths = e.where(s"substring(src,0,4) = '$inputPlaza'")
      .select(
        e("src"),
        e("dst"),
        array(e("src"), e("dst")).alias("path"),
        e("time").alias("time"))

    val x = find(paths, e, "100", 0, Array())
    val x1 = spark.sparkContext.parallelize(x).toDF("plazalane")

    val dest = x1.distinct
    dest.show
    val input = getInputData.toDF("tag", "plazalane", "time", "date").cache
    val prevDayTxn = input.filter($"date" === lit(prevDay))
   // prevDayTxn.select("plazalane").distinct.show(400)
    println("prevDayTxn")
    //prevDayTxn.show(3)
    println("dest")
    //dest.show(3)
    val prevDayPlazaTags = dest.join(prevDayTxn, Seq("plazalane")).cache

    println("prevDayPlazaTags")
  //  prevDayPlazaTags.select("plazalane").distinct.show

    //prevDayPlazaTags.show(3,false)
    val performanceDayTxn = input.filter(s"substring(plazalane,0,4)==$inputPlaza")
      .filter($"date" === lit(performanceDate))
      .select(col("tag"), col("time").alias("txn_time")).withColumn("inputDay", lit("1"))
      .toDF("tag", "txn_time", "txn_input_day")
    println("perfDaytxn")
   // println(performanceDayTxn.schema)
    
      val nextDayTxn = input.filter(s"substring(plazalane,0,4)==$inputPlaza")
      .filter($"date" === lit(nextDay))
      .select(col("tag"), col("time").alias("txn_time")).withColumn("nextDay", lit("1"))
      .toDF("tag", "next_Time", "txn_next_day")
    // performanceDayTxn.show(3,false)
    val derivedTags = prevDayPlazaTags.join(performanceDayTxn, Seq("tag"), "left_outer")
    .join(nextDayTxn, Seq("tag"), "left_outer")

    // out.show(3,false)

    //val tag=getTagDerived.toDF("tag","plazalane","dst","time","date","inputDay").na.fill("0")
    val allTags = getWholeTags.toDF("tag", "class", "classcode", "com")
    val out2 = derivedTags.join(allTags, Seq("tag"), "left_outer").na.fill("0")
    writeToCSV(out2, "out2.csv")
    val t1 = System.currentTimeMillis()
    println((t1 - t0).toFloat / 1000)
  }

  def find(paths: DataFrame, e: DataFrame, end: String, iteration: Int = 0, acc: Array[String]): Array[String] = {
    // Must alias to avoid common lineage cartesian join confusion.
    // Join the current terminal point for the path to the set of
    // mirrored edges.
    val appendToSeq = udf((x: Seq[String], y: String) => x ++ Seq(y))

    val sp = paths.alias("paths")
      .join(e.alias("e"), col("paths.dst") ===
        col("e.src"))
      .select(
        col("paths.src"),
        col("e.dst"),
        appendToSeq(
          col("paths.path"),
          col("e.dst")).alias("path"),
        lit(col("e.time") + col("paths.time")).alias("time"))
    //println("recr")
    //      sp.show(3)
    //    println(":recr")
    sp.cache()

    // Termination condition is the first traversal that arrives
    // at our destination vertex.
    val filtered = sp.where(s"time > '8'").cache

    val acc_out = if (filtered.count > 0) {
      filtered.select("dst","time").show(20,false)
      val req = filtered.select(filtered("dst")).rdd.map(x => x(0).toString()).collect
      req ++ acc
    } else {
      acc
    }
    val sp1 = sp.except(filtered)

    if (sp1.count() > 0) {

      find(sp.except(filtered), e, end, iteration + 1, acc_out)
    } else {
      acc_out
    }
  }

  def getPlazaData = {

    val spark = getSparkSession()
    // spark.sparkContext.textFile("file:///D:/task/data/TagPlazaCodeEntryExitLaneTime.txt")
    spark.sparkContext.textFile("file:///D:/task/RouteTagging/graph/prev_plaza_lane.csv")

      .map(_.split(","))

      .map(x => (x(0) + x(1), x(2) + x(3), x(7)))

  }
  def getInputData = {

    val spark = getSparkSession()
    spark.sparkContext.textFile("file:///D:/task/data/TagPlazaCodeEntryExitLaneTime.txt")

      .map(_.split(","))
      .map(x => (x(0), x(1) + x(3), x(4), x(4).substring(0, 10)))

    // .map(x => (x(0), x(1), x(2), x(3), LocalDateTime.parse(x(4), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))))

  }
  def getTagDerived = {
    val spark = getSparkSession()
    spark.sparkContext.textFile("file:///D:/task/RouteTagging/graph/graph-1-24-48.csv")

      .map(_.split(","))
      .map(x => (x(0), x(1), x(2), x(3), x(4), x(5)))
  }
  def getWholeTags = {
    val spark = getSparkSession()
    spark.sparkContext.textFile("file:///D:/task/data/TagClubCodeCom.csv")

      .map(_.split(","))
      .map(x => (x(0), x(1), x(2), x(3)))
  }

  def getSparkSession(): SparkSession = {
    SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp")
      .getOrCreate()
  }
  def writeToCSV(df: DataFrame, file: String): Unit = {
    val folder = "file:///D:/task/RouteTagging/graph/12-8001/"
    df.repartition(1).write.format("csv").mode("overwrite").option("header", "true").save(folder + file)
  }

}