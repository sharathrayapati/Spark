package SparkSql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.Row
import org.apache.spark.sql._
import javolution.io.Struct


object Sql {
  
  def main(args: Array[String]){
    val conf = new SparkConf().setMaster("local").setAppName("sql")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val input = sqlContext.jsonFile("/Users/NagasharathRayapati/Desktop/Data/zips.json")
    val input1 = sqlContext.jsonFile("/Users/NagasharathRayapati/Desktop/Data/zaps.json")
    val input2 = sqlContext.jsonFile("/Users/NagasharathRayapati/Desktop/Data/world_bank1.json")
    input.registerTempTable("zipss")
    input1.registerTempTable("zaps")
    input2.registerTempTable("world")
    
    
 //   val row1 = input1.explain
//    val row1c = input1.dtypes
    
val pairRdd1 = sc.parallelize(List(("a", 1), ("b",2), ("c",3)))
val pairRdd2 = sc.parallelize(List(("b", "second"), ("c","third"), ("d","fourth")))
val leftOuterJoinRdd = pairRdd1.leftOuterJoin(pairRdd2).foreach(println)
    
    
//    val input3 = sqlContext.sql("select z1.city,z1.pop from zipss z1 join zaps z2 on z1.city=z2.city")
     
//    input3.collect().foreach(println)

//    input.join(input1, input1("city") === input("city"),"right").collect().foreach(println)
    
//    input2.printSchema()
//    input1.printSchema()
//    input1.select("city").show(10)
//   // input1.withColumn("country", input2("borrower").cast("String"))
//    //input1.printSchema()
//  
//    input.select("loc").show(10)
//    
  }
  
}