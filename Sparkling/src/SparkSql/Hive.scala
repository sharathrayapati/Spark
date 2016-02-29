package SparkSql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.hadoop.hive.metastore.HiveMetaStore


object Hive {
  def  main(args: Array[String]){
    
  
  val conf = new SparkConf().setMaster("local").setAppName("Hive")
  val sc = new SparkContext(conf)
  val hivecon = new HiveContext(sc)
  
  import hivecon._
  
  val input = hivecon.jsonFile("/Users/NagasharathRayapati/Desktop/Data/zips.json")
  
  input.registerTempTable("zips")
  
  val topzip = hivecon.sql("select * from zips limit 10")
  
  
  
 
  
  input.printSchema()
  topzip.collect().foreach(println)
  
  val grop = hivecon.sql("Select pop,city from zips group by city,pop  order by pop") 
  grop.collect().foreach(println)
  
//  val input3 = hivecon.sql("CREATE TABLE zap AS Select * from zipss limit 10")
//  input3.collect().foreach(println)
//  
 
  }
}