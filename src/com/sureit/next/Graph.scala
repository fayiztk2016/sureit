package com.sureit.next
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.graphframes._
object Graph extends App{
  
   val spark = getSparkSession()
   
  val v = spark.sqlContext.createDataFrame(List(
  ("a", "Alice", 34),
  ("b", "Bob", 36),
  ("c", "Charlie", 30),
  ("d", "David", 29),
  ("e", "Esther", 32),
  ("f", "Fanny", 36),
  ("g", "Gabby", 60)
)).toDF("id", "name", "age")
// Edge DataFrame
val e = spark.sqlContext.createDataFrame(List(
  ("a", "b", "friend","10"),
  ("b", "c", "follow","60"),
  ("c", "d", "follow","60"),
   ("d", "e", "follow","60"),
  ("f", "c", "follow","60"),
  ("e", "f", "follow","60"),
  ("e", "d", "friend","60"),
  ("d", "a", "friend","60"),
  ("a", "e", "friend","60")
)).toDF("src", "dst", "relationship","dist")
println("*********************staring")

val g = GraphFrame(v, e)
println("complete creation")

val paths = e.where(s"src = 'a'")
    .select(
        e("src"),
        e("dst"),
        array(e("src"), e("dst")).alias("path"),
        e("dist").alias("dist"))
 val appendToSeq = udf((x: Seq[String], y: String) => x ++ Seq(y))
       
    
val x1=find(paths,e,"200",0)
x1.show()

def find(paths: DataFrame, e: DataFrame, end:
	String, iteration: Int = 0): DataFrame = {
    // Must alias to avoid common lineage cartesian join confusion.
    // Join the current terminal point for the path to the set of
    // mirrored edges.
    val sp = paths.alias("paths")
    .join(e.alias("e"), col("paths.dst") ===
    col("e.src"))
    .select(
        col("paths.src"),
        col("e.dst"),
        appendToSeq(col("paths.path"),
        col("e.dst")).alias("path"),
        lit(col("e.dist")+col("paths.dist")).alias("dist")
    )

    sp.cache()

    // Termination condition is the first traversal that arrives
    // at our destination vertex.
    val filtered = sp.where(s"dist > '$end'")
    if (filtered.count() > 0){
        filtered
    } else {
        find(sp, e, end, iteration + 1)
    }
}














 def getSparkSession(): SparkSession = {
    SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp")
      .getOrCreate()
  }

}