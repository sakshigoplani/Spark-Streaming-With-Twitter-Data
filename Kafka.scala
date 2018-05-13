//**************************************//
//                                      //
// File:    Kafka.scala                 //
// Author:  Sakshi Haresh Goplani       //
// Email:   sakshigoplani9@gmail.com    //
//                                      //
//**************************************//


package com.sakshigoplani.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel

import java.util.regex.Pattern
import java.util.regex.Matcher

import Utilities._

import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder

object Kafka {
  
  def main(args: Array[String]) {

    // Creating the context with a 1 second batch size
    val ssc = new StreamingContext("local[*]", "Kafka", Seconds(1))
    
    setupLogging()
    
    // Constructing a regular expression (regex) to extract fields from raw Apache log lines
    val pattern = apacheLogPattern()

    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")
    // Listing of topics we want to listen for from Kafka
    val topics = List("testLogs").toSet
    // Creating Kafka stream, which will contain (topic,message) pairs. Tracking a 
    // map(_._2) at the end in order to only get the messages, which contain individual
    // lines of data.
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics).map(_._2)
     
    // Extracting the request field from each log line
    val requests = lines.map(x => {val matcher:Matcher = pattern.matcher(x); if (matcher.matches()) matcher.group(5)})
    
    // Extracting the URL from the request
    val urls = requests.map(x => {val arr = x.toString().split(" "); if (arr.size == 3) arr(1) else "[error]"})
    
    // Reducing by URL over a 5-minute window sliding every second
    val urlCounts = urls.map(x => (x, 1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(1))
    
    // Sorting and printing the results
    val sortedResults = urlCounts.transform(rdd => rdd.sortBy(x => x._2, false))
    sortedResults.print()

    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}

