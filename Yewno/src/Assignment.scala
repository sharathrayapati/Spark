import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.auth.Authorization
import twitter4j.auth.AccessToken

object Assignment {
  def main(args: Array[String]) {
        
    System.setProperty("twitter4j.oauth.consumerKey", "a0bOHuj2KANOiQdsXfPnJLR6A")
    System.setProperty("twitter4j.oauth.consumerSecret", "9DtorKGYL7RwjLA81JIXDQGF9AUGz2znb8TF6c0qCY6rx78lRF")
    System.setProperty("twitter4j.oauth.accessToken", "149845965-GC4PYJ3MMuMwcxnWlg1725Sai1Bh2pRrXd62eoS0")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "gjU0C3110l1quk1h7X7pzQ30o2LCESuxn0Vddx1duIU2J")
           
    val conf = new SparkConf().setAppName("Yewno").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(1))  
    val tweets = TwitterUtils.createStream(ssc, None).window(Minutes(5), Seconds(1))
    val tags= tweets.flatMap(reply => reply.getText().split(" ").filter(_.startsWith("#")))
    val removeHash = tags.map(_.substring(1)).map(x=> (x,1))
    val countHash = removeHash.reduceByKey(_+_).map{case(hash,no)=>(no,hash)}
    val top25 = countHash.transform(_.sortByKey(false)).print(25)
    ssc.checkpoint("/Users/NagasharathRayapati/Desktop/Data")
    ssc.start()
    ssc.awaitTermination()    
  }
  
}