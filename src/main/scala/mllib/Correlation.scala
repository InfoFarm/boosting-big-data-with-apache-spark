package mllib

import org.apache.spark.SparkContext
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD

object Correlation extends App {

  val sparkContext = new SparkContext("local[4]", "Correlation")

  val x: RDD[Double] = sparkContext.parallelize(List(1.0, 2, 3, 4, 5))
  val y: RDD[Double] = sparkContext.parallelize(List(2.0, 3, 4, 5, 10))

  val correlation = Statistics.corr(x, y, "pearson")

  println(s"Pearson correlation: $correlation")

  sparkContext.stop()

}