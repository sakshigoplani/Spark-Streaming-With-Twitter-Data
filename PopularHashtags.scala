//**************************************//
//                                      //
// File:    PopularHashtags.scala       //
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

/** Listening to a stream of Tweets and keeps track of the most popular
 *  hashtags over a 5 minute window.
 */
object PopularHashtags {
  
  def main(args: Array[String]) {

    // Configuring Twitter credentials through Credentials Text File
    setupTwitter()
    
    // Setting up a Spark streaming context named "PopularHashtags" that runs locally using
    // all CPU cores and one-second batches of data
    val ssc = new StreamingContext("local[*]", "PopularHashtags", Seconds(1))
    
    setupLogging()

    // Creating a DStream from Twitter using streaming context
    val tweets = TwitterUtils.createStream(ssc, None)
    
    // Extracting the text of each status update into DStreams using map()
    val statuses = tweets.map(status => status.getText())
    
    // Creating a new DStream for each word
    val tweetwords = statuses.flatMap(tweetText => tweetText.split(" "))
    
    // Eliminating anything that's not a hashtag
    val hashtags = tweetwords.filter(word => word.startsWith("#"))
    
    // Mapping each hashtag to a key/value pair of (hashtag, 1) so we can count them up by adding up the values
    val hashtagKeyValues = hashtags.map(hashtag => (hashtag, 1))
    
    // Counting them up over a 5 minute window sliding every one second
    val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow( (x,y) => x + y, (x,y) => x - y, Seconds(300), Seconds(1))
    
    // Sorting the results by the count values
    val sortedResults = hashtagCounts.transform(rdd => rdd.sortBy(x => x._2, false))
    
    // Printing the top 10
    sortedResults.print
    
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }  
}
