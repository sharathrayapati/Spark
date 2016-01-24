package Basics

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object FlatMap {
  def main(args: Array[String]){
    val texts = "/Users/NagasharathRayapati/Desktop/Data/SalesJan20089.csv"

    val conf = new SparkConf().setAppName("ColumnNumber").setMaster("local") //Spark config is required to create a SparkContext
    val sc = new SparkContext(conf) // create SparkContext

    val csv = sc.textFile(texts).persist()

    val flat = csv.flatMap(line => line.split(",").take(2)) // Returns an RDD of length n by applying the function
   
    flat.collect().foreach(println) // prints no of columns in each row
  }
}