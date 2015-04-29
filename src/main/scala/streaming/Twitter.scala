package streaming

import org.apache.spark.SparkContext
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Twitter extends App {

  val sparkContext = new SparkContext("local[4]", "Twitter")

  /*
   * Configure Twitter security credentials.
   */
  TwitterConfiguration.configureSecurity()

  /*
   * Create a StreamingContext that processes a batch every two seconds.
   */
  val streamingContext = new StreamingContext(sparkContext, Seconds(2))

  /*
   * If a tweet contains one of the following words, it will end up in our stream.
   */
  val keyWords = Array("spark", "scala", "music")
  val stream = TwitterUtils.createStream(streamingContext, None, keyWords)

  /*
   * Count by hashtag and sort
   *
   * Windows are 60 seconds
   */
  val hashTags = stream.flatMap(status => status.getText.split(" ")).filter(_.startsWith("#"))

  val topHashTags = hashTags.map((_, 1))
                            .reduceByKeyAndWindow(_ + _, Seconds(60))
                            .map { case (topic, count) => (count, topic)}
                            .transform(_.sortByKey(ascending = false))

  topHashTags.foreachRDD(rdd => {
    val topList = rdd.take(5).map { case (count, tag) => s"$tag: $count" }
    println(
      topList.mkString("\n")
    )
    println("-------------------------------------------------------")
  })

  streamingContext.start()
}