package KeyValue

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.Accumulator

object Reduce {

  def main(args: Array[String]) {

    val csv = "/Users/NagasharathRayapati/Desktop/Data/SalesJan2009.csv" // A file on my Desktop

    val conf = new SparkConf().setAppName("Parallelize").setMaster("local") //Spark config is required to create a SparkContext
    val sc = new SparkContext(conf) // create SparkContext
    val acc = sc.accumulator(0)

    val file = sc.textFile(csv).persist() // Creating an RDD by passing through SparkContext and storing in memory by using persist

    val header = file.first()

    val index = file.filter(x => x != header)

    val pairs = index.map(x => (x.split(",")(1).trim(), x.split(",")(2).toInt))
    val group = pairs.groupByKey()
    
    //val comb = pairs.combineByKey(createCombiner, mergeValue, mergeCombiners)

    val red = pairs.reduceByKey(_ + _)

    val mapval = red.mapValues(x => x / 10)

    val sortbykey = red.sortByKey()

    val joined = red.join(mapval)

    val cogro = red.cogroup(mapval)

    group.collect().foreach(println)

    red.collect().foreach(println)

    mapval.collect().foreach(println)

    sortbykey.collect().foreach(println)
    
//    sortbykey.saveAsTextFile("/Users/NagasharathRayapati/Desktop/Data/output.cs")

    joined.collect().foreach(println)

    cogro.collect().foreach(println)

  }
}
