package Basics

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object First {
  def main(args: Array[String]){
    val hell = "/Users/NagasharathRayapati/Desktop/Data/SalesJan2009.csv"  // A file on my Desktop

    val conf = new SparkConf().setAppName("Parallelize").setMaster("local") //Spark config is required to create a SparkContext
    val sc = new SparkContext(conf) // create SparkContext

    val mem = sc.textFile(hell).persist()  // Creating an RDD by passing through SparkContext and storing in memory by using persist
    val columnnames = mem.first() // Gives the first dataset

    
    println("columnnames of sales file in csv is: " + columnnames)
  }
}