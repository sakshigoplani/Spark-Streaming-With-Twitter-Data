//**************************************//
//                                      //
// File:    AverageTweetLength.scala    //
// Author:  Sakshi Haresh Goplani       //
// Email:   sakshigoplani9@gmail.com    //
//                                      //
//**************************************//



package com.sakshigoplani.sparkstreaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import Utilities._
import java.util.concurrent._
import java.util.concurrent.atomic._

/** Using thread-safe counters to keep track of the average length of
 *  Tweets in a stream.
 */
object AverageTweetLength {
  
  def main(args: Array[String]) {

    // Configuring Twitter credentials through Credentials Text File
    setupTwitter()
    
    // Setting up a Spark streaming context named "AverageTweetLength" that runs locally using
    // all CPU cores and one-second batches of data
    val ssc = new StreamingContext("local[*]", "AverageTweetLength", Seconds(1))
    
    setupLogging()

    // Creating a DStream from Twitter using streaming context
    val tweets = TwitterUtils.createStream(ssc, None)
    
    // Extracting the text of each status update into DStreams using map()
    val statuses = tweets.map(status => status.getText())
    
    // Mapping this to tweet character lengths
    val lengths = statuses.map(status => status.length())
    
    // As we could have multiple processes adding into these running totals
    // at the same time, we'll just Java's AtomicLong class to make sure
    // these counters are thread-safe.
    var totalTweets = new AtomicLong(0)
    var totalChars = new AtomicLong(0)

    
    lengths.foreachRDD((rdd, time) => {
      
      var count = rdd.count()
      if (count > 0) {
        totalTweets.getAndAdd(count)
        
        totalChars.getAndAdd(rdd.reduce((x,y) => x + y))
        
        println("Total tweets: " + totalTweets.get() + 
            " Total characters: " + totalChars.get() + 
            " Average: " + totalChars.get() / totalTweets.get())
      }
    })
    
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }  
}
