/**
 * Illustrates a simple count in scala to compute the lines of an RDD.RDD is created from a file 
 */
package Basics

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Countlines {

  def main(args: Array[String]) {

    val hell = "/Users/NagasharathRayapati/Desktop/Data/SalesJan2009.csv"  // A file on my Desktop

    val conf = new SparkConf().setAppName("Parallelize").setMaster("local") //Spark config is required to create a SparkContext
    val sc = new SparkContext(conf) // create SparkContext

    val mem = sc.textFile(hell).persist()  // Creating an RDD by passing through SparkContext and storing in memory by using persist
    val no = mem.count() // Counting the no of rows in a file

    
    println("count of sales file in csv is " + no)

  }
}
