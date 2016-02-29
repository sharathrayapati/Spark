
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
import edu.stanford.nlp.util._


object Retweet {
  def main(args: Array[String]) {
        
    System.setProperty("twitter4j.oauth.consumerKey", "a0bOHuj2KANOiQdsXfPnJLR6A")
    System.setProperty("twitter4j.oauth.consumerSecret", "9DtorKGYL7RwjLA81JIXDQGF9AUGz2znb8TF6c0qCY6rx78lRF")
    System.setProperty("twitter4j.oauth.accessToken", "149845965-GC4PYJ3MMuMwcxnWlg1725Sai1Bh2pRrXd62eoS0")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "gjU0C3110l1quk1h7X7pzQ30o2LCESuxn0Vddx1duIU2J")
    
           
    val conf = new SparkConf().setAppName("YewnoRetweet").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(10)) 
    
    //generate a twitter stream by filtering the twitter steam to oscars
    val tweets = TwitterUtils.createStream(ssc, None,Array("#oscars","Madmax","leonardo","leo","dicaprio")).window(Minutes(5), Minutes(1))
    
    //taking retweets in to consideration 
    val message = tweets.map(reply=>if (reply.getText().startsWith("RT") && reply.getRetweetedStatus()!=null) 
      (reply.getRetweetedStatus().getRetweetCount(),reply.getText()) else(reply.getRetweetCount(),reply.getText()))
    
    //sorting the retweets by count   
    val top = message.transform(_.sortByKey(false)).saveAsTextFiles("/Users/NagasharathRayapati/Desktop/Data/Sentiment/")

    //checkpointing for backup
    ssc.checkpoint("/Users/NagasharathRayapati/Desktop/Data")    
    ssc.start()
    ssc.awaitTermination()
}
  
}  