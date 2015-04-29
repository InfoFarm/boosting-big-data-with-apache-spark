package core

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


object Syslog extends App {

  /*
   * Create a SparkContext that runs on the local machine for an application called Syslog.
   */
  val sparkContext = new SparkContext("local[8]", "Syslog")


  val data: RDD[Int] = sparkContext.textFile("/var/log/syslog")
    .map(line => line.length()).distinct()

  /*
   * Group by the last digit
   */
  val first = data.groupBy(size => size % 10)

  /*
   * We can also get rid of lengths that are even, and then tuple them with some other computations.
   */
  val second = data.map(size => size + 1).filter(_ % 2 == 0).map(size => (size % 10, size * size))

  /*
   * We can combine both distributed data sets into a single one by joining them.
   */
  val joined = first.join(second)

  println(
    /* And now we ask the cluster for the whole thing: Action */
    joined.take(10).toList.mkString("\n")
  )

  sparkContext.stop()
}
