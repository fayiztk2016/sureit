package com.sureit.next

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object countWord {
   def main(args: Array[String]) {
     
     Logger.getLogger("org").setLevel(Level.ERROR)
     val sc=new SparkContext("local[*]","countWord")
     val lines = sc.textFile("file:///D:/Learning/Spark/udemy/source/SparkScala/book.txt");
     val words=lines.flatMap(x=>x.toUpperCase().split("\\W+"))
     val count=words.countByValue()
    // val sorted=count.toSeq.sortBy(_._2).reverse
    
      //val sortedResults = results.toSeq.sortBy(_._1)
     count.foreach(println)
     
   }
}

object DeathToStrategy  {

  def add(a: Int, b: Int) = a + b
  def subtract(a: Int, b: Int) = a - b
  def multiply(a: Int, b: Int) = a * b
  
  def execute(callback:(Int, Int) => Int, x: Int, y: Int) = callback(x, y)

  println("Add:      " + execute(add, 3, 4))
  println("Subtract: " + execute(subtract, 3, 4))
  println("Multiply: " + execute(multiply, 3, 4))

}