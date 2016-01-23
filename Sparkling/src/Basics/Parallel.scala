/**
 * Illustrates a simple count in scala to compute the length of an RDD. RDD is created with parallelize
 */
package Basics

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Parallel {
  def main (args: Array[String]){
    val conf = new SparkConf().setAppName("Parallelize").setMaster("local") //Spark config is required to create a SparkContext
    val sc = new SparkContext(conf) // create SparkContext
    
    val str = "Helo World!" // An Array of String
    val str1 = sc.parallelize(str) // Creating an RDD by Parallelizing
    val len = str1.count()  // Counting the no of characters by count 
    
    println("Length of String is "+len)
   
  }
}