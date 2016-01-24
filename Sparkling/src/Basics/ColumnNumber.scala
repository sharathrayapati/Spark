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

    val first = csv.map(line => line.split(",").length) // Returns an RDD of length n by applying the function

    first.collect().foreach(println) // prints no of columns in each row

  }
}