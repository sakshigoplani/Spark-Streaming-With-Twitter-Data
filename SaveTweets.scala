//**************************************//
//                                      //
// File:    SaveTweets.scala            //
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

/** Listening to a stream of tweets and saves them to disk. */
object SaveTweets {
  
  def main(args: Array[String]) {

    // Configuring Twitter credentials through Credentials Text File
    setupTwitter()
    
    // Setting up a Spark streaming context named "SaveTweets" that runs locally using
    // all CPU cores and one-second batches of data
    val ssc = new StreamingContext("local[*]", "SaveTweets", Seconds(1))
    
    setupLogging()

    // Creating a DStream from Twitter using streaming context
    val tweets = TwitterUtils.createStream(ssc, None)
    
	// Extracting the text of each status update into RDD's using map()
    val statuses = tweets.map(status => status.getText())
    
    // Keeping count of how many Tweets
    var totalTweets:Long = 0
        
    statuses.foreachRDD((rdd, time) => {

      if (rdd.count() > 0) {
        // Combining each partition's results into a single RDD:
        val repartitionedRDD = rdd.repartition(1).cache()
        // And printing out a directory with the results
        repartitionedRDD.saveAsTextFile("Tweets_" + time.milliseconds.toString)
        // Stopping once we've collected 1000 tweets
        totalTweets += repartitionedRDD.count()
        println("Tweet count: " + totalTweets)
        if (totalTweets > 1000) {
          System.exit(0)
        }
      }
    })
    
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }  
}
