package KeyValue

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner

object counter {
  def main(args: Array[String]) {

    val csv = "/Users/NagasharathRayapati/Desktop/Data/SalesJan2009.csv" // A file on my Desktop

    val conf = new SparkConf().setAppName("Parallelize").setMaster("local") //Spark config is required to create a SparkContext
    val sc = new SparkContext(conf) // create SparkContext
    val acc = sc.accumulator(0)
    
    val file = sc.textFile(csv) // Creating an RDD by passing through SparkContext and storing in memory by using persist
    val files = new HashPartitioner(8)
    val header = file.first()

    val index = file.filter(x => x != header)

    val pairs = index.map(x => (x.split(",")(1).trim(), x.split(",")(2).toInt))
    val con = index.map(x=> (if (x.split(",")(1).trim() =="Product1") acc +=1))
    
    con.collect
    println(acc)
    

    
  }
}