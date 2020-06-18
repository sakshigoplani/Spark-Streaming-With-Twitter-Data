# Spark-Streaming-With-Twitter-Data
## Processing Live Tweets Real Time Using Spark Streaming
A Spark Streaming application to process tweets real-time in a distributed environment integrated with SparkSQL and Apache Kafka.

## Project Files
### Kafka.scala
Listens for log data from Kafka's testLogs topic on port 9092.

### PopularHashtags.scala
Listens to a stream of Tweets and keeps track of the most popular hashtags over a 5 minute window.

### AverageTweetLength.scala
Uses thread-safe counters to keep track of the average length of Tweets in a stream.

### SaveTweets.scala
Listens to a stream of tweets and saves them to disk.

### Utilities.scala
Makes sure only ERROR messages get logged to avoid log spam.


