package Basics

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object ColumnNumber {
  def main(args: Array[String]) {

    val texts = "/Users/NagasharathRayapati/Desktop/Data/SalesJan2009.csv"
    
    

    val conf = new SparkConf().setAppName("ColumnNumber").setMaster("local") //Spark config is required to create a SparkContext
    val sc = new SparkContext(conf) // create SparkContext

    val csv = sc.textFile(texts).persist()
    
    val whole = sc.textFile("/Users/NagasharathRayapati/Desktop/Data/*.csv")

    val first = csv.map(line => line.split(",").length) // Returns an RDD of length n by applying the function

    //first.collect().foreach(println) // prints no of columns in each row
    
   // whole.collect().foreach(println)
    
    val numbers = sc.parallelize(List(1, 2, 3, 4))
val alphabets = sc.parallelize(List("a", "b", "c", "d"))
val zippedPairs = numbers.zip(alphabets).collect().foreach(println)

val zippe = alphabets.zipWithIndex().collect().foreach(println)

  }
}