package sql

import java.io.Serializable

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext


object UDF extends App {

  val sparkContext = new SparkContext("local[8]", "StockExchange")
  val sqlContext = new SQLContext(sparkContext)

  import sqlContext.implicits._

  /* Define a class Quote */
  case class Quote(stock: String, date: String, price: Double) extends Serializable

  /* Load the data set and convert the CSV to objects of type 'Quote' */
  val closingPrices = sparkContext.textFile("data/closes.csv")
    .map(_.split(",").toList)
    .map { case stock :: date :: price :: Nil =>
    Quote(stock, date, price.toDouble)
  }.toDF()

  closingPrices.registerTempTable("quotes")

  sqlContext.udf.register("stringLength", (s: String) => s.length)

  sqlContext.sql("select distinct stringLength(stock) from quotes").collect().foreach(println)

}
