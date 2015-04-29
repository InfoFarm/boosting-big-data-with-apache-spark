package sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.annotation.tailrec
import scala.io.StdIn

object Napstify extends App {

  val sparkContext = new SparkContext("local[8]", "Napsify")
  val sqlContext = new SQLContext(sparkContext)

  val listenEvents: DataFrame = sqlContext.parquetFile("data/listen_events")
  listenEvents.cache()
  listenEvents.registerTempTable("listen_events")

  startUserInterface()

  def findRecommendations(artist: String) = {
    sqlContext.sql(s"""
      SELECT a.artist,
             b.artist,
             count(a.artist) AS kount
      FROM listen_events a
      JOIN listen_events b ON a.user = b.user
      WHERE a.artist < b.artist
      GROUP BY a.artist, b.artist
      HAVING lower(a.artist) LIKE lower('%$artist%') OR lower(b.artist) LIKE lower('%$artist%')
      ORDER BY kount DESC
    """).take(10).foreach(println)
  }

  @tailrec def startUserInterface(): Unit = {
    print("Artist > ")
    val artist = StdIn.readLine()
    if (artist == "quit") {
      sparkContext.stop()
    } else {
      findRecommendations(artist)
      startUserInterface()
    }
  }
}