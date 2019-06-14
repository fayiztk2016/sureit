package com.sureit.next

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._  

  
  object Rating{
    
      def main(args:Array[String]){
      Logger.getLogger("org").setLevel(Level.ERROR)

        val sc=new SparkContext("local[*]","Rating")
    var y=123
    println(y)
        
      }
  }
