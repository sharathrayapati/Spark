package Basics

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel


object FlatMap {
  def main(args: Array[String]){
    val texts = "/Users/NagasharathRayapati/Desktop/Data/SalesJan2009.csv"

    val conf = new SparkConf().setAppName("ColumnNumber").setMaster("local") //Spark config is required to create a SparkContext
    val sc = new SparkContext(conf) // create SparkContext

    val csv = sc.textFile(texts).persist(StorageLevel.MEMORY_ONLY)
    
    val map1 = csv.map(line => line.split(","))

    val flat = csv.flatMap(line => line.split(",")) // Returns an RDD of length n by applying the function
   
   //flat.collect().foreach(println) // prints no of columns in each row
    println(map1.count())
    println(flat.count())
  }
}