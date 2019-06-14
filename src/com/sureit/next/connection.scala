package com.sureit.next
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import scala.collection.immutable.TreeSet
import scala.util.Try
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
object connection extends App {
 /*val spark = SparkSession.builder()
 .appName("Sample job")
 .master("yarn")
 .config("spark.hadoop.fs.defaultFS","hdfs://192.168.70.7:9000")
 .config("spark.hadoop.yarn.resourcemanager.address","192.168.70.7:8032")
 .config("user","hadoop")
 .config("password","welcome123")
 .enableHiveSupport()
 .getOrCreate() */
 
 def write(uri: String, filePath: String, data: Array[Byte]) = {
//      System.setProperty("HADOOP_USER_NAME", "Mariusz")
      val path = new Path(filePath)
      val conf = new Configuration()
      conf.set("fs.defaultFS", uri)
      val fs = FileSystem.get(conf)
      val os = fs.create(path)
      os.write(data)
      fs.close()
    }

    write("hdfs://192.168.70.7:9000", "hdfs://192.168.70.7:9000/user/cloudera/test.txt", "Hello World".getBytes)


 
 println("spark hadoop connected........")
}
