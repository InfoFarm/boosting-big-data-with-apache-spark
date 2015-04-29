package core

import java.io.Serializable

import org.apache.spark.SparkContext

object StockExchange extends App {

  val sparkContext = new SparkContext("local[8]", "StockExchange")

  /* Define a class Quote */
  case class Quote(stock: String, date: String, price: Double) extends Serializable


  /* Load the data set and convert the CSV to objects of type 'Quote' */
  val closingPrices = sparkContext.textFile("data/closes.csv")
                                  .map(_.split(",").toList)
                                  .map { case stock :: date :: price :: Nil =>
                                          Quote(stock, date, price.toDouble)
                                  }

  /* Group the closing prices by date */
  val pricesByDate = closingPrices.keyBy(quote => quote.date)

  /* Calculate the minimum stocks per date */
  def minimumStocksByDate = pricesByDate.combineByKey[(String, Double)](
    (x: Quote) => (x.stock, x.price),
    (d: (String, Double), l:Quote) => if (d._2 < l.price) d else (l.stock, l.price),
    (d1: (String, Double), d2: (String, Double)) => if (d1._2 < d2._2) d1 else d2
  )

  minimumStocksByDate.cache()

  println(
    minimumStocksByDate.take(2).toList.mkString("\n")
  )

  println(
    minimumStocksByDate.take(2).toList.mkString("\n")
  )

  sparkContext.stop()
}