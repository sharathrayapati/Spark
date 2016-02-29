
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.twitter.TwitterInputDStream
import org.apache.spark.streaming.twitter.TwitterReceiver
import twitter4j.auth.Authorization
import twitter4j.auth.AccessToken
import twitter4j.TwitterResponse

object Location {
  
  def main(args: Array[String]){
 
    //twitter properties
    System.setProperty("twitter4j.oauth.consumerKey", "a0bOHuj2KANOiQdsXfPnJLR6A")
    System.setProperty("twitter4j.oauth.consumerSecret", "9DtorKGYL7RwjLA81JIXDQGF9AUGz2znb8TF6c0qCY6rx78lRF")
    System.setProperty("twitter4j.oauth.accessToken", "149845965-GC4PYJ3MMuMwcxnWlg1725Sai1Bh2pRrXd62eoS0")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "gjU0C3110l1quk1h7X7pzQ30o2LCESuxn0Vddx1duIU2J")
               
    val conf = new SparkConf().setAppName("YewnoLocation").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(10)) 
    
    //generate a twitter stream
    val tweets = TwitterUtils.createStream(ssc, None).window(Minutes(5), Seconds(10))
    
    //taking tweets from all locations(with location enabled & without) 
    val location = tweets.map(reply=>if (reply.getUser().getLocation()!=null) 
      (reply.getUser().getLocation().toUpperCase()) else ("null"))
           
    //key value pairs by location
    val locationCount = location.map(x=> (x,1))
    
    //counting the number of times tweets from particular location 
    val countHash = locationCount.reduceByKey(_+_).map{case(loc,no)=>(no,loc)} 
    
    //sorting by descending order
    val top25 = countHash.transform(_.sortByKey(false)).saveAsTextFiles("/Users/NagasharathRayapati/Desktop/Data/Location/")
          
    ssc.start()
    ssc.awaitTermination()        
  }
}