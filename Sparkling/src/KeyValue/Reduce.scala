package KeyValue

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions

object Reduce {

  def main(args: Array[String]) {

    val hell = "/Users/NagasharathRayapati/Desktop/Data/SalesJan2009.csv" // A file on my Desktop

    val conf = new SparkConf().setAppName("Parallelize").setMaster("local") //Spark config is required to create a SparkContext
    val sc = new SparkContext(conf) // create SparkContext

    val mem = sc.textFile(hell).persist() // Creating an RDD by passing through SparkContext and storing in memory by using persist
    
    val header = mem.first()
    
    val index = mem.filter ( x => x!=header)
    
    val pairs = index.map(x => (x.split(",")(1).trim(), x.split(",")(2).toInt))
    val so1 = pairs.groupByKey()
    
    val so2 = pairs.reduceByKey(_ + _)
    
    val mapval = so2.mapValues(x => x/10)
    
    val sortbykey = so2.sortByKey()
    
    val joined = so2.join(mapval)
    
    val cogro = so2.cogroup(mapval)
    
   
    so1.collect().foreach(println)
    
    so2.collect().foreach(println)
    
    mapval.collect().foreach(println)
    
    sortbykey.collect().foreach(println)
    
    joined.collect().foreach(println)
    
    cogro.collect().foreach(println)
    
  }
}
