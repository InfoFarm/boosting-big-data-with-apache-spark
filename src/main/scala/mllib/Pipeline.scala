package mllib

import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.{Row, SQLContext}


object Pipeline extends App {

  /* Labeled and unlabeled instance types. */
  case class LabeledDocument(id: Long, text: String, label: Double)
  case class Document(id: Long, text: String)

  val sparkContext = new SparkContext("local[4]", "Pipeline")
  val sqlContext = new SQLContext(sparkContext)

  import sqlContext.implicits._

  val trainingData = sparkContext.parallelize(Seq(
    LabeledDocument(0L, "a b c d e spark", 1.0),
    LabeledDocument(1L, "b d", 0.0),
    LabeledDocument(2L, "spark f g h", 1.0),
    LabeledDocument(3L, "hadoop mapreduce", 0.0)
  ))

  val testData = sparkContext.parallelize(Seq(
    Document(4L, "spark i j k"),
    Document(5L, "l m n"),
    Document(6L, "mapreduce spark"),
    Document(7L, "apache hadoop")
  ))

  val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
  val hashing = new HashingTF().setNumFeatures(1000).setInputCol(tokenizer.getOutputCol).setOutputCol("features")
  val logisticRegression = new LogisticRegression().setMaxIter(10).setRegParam(0.01)

  val pipeline = new Pipeline().setStages(Array(tokenizer, hashing, logisticRegression))

  val model = pipeline.fit(trainingData.toDF())

  model.transform(testData.toDF())
    .select("id", "text", "probability", "prediction")
    .collect()
    .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
    println(s"($id, $text) \t --> \t Probability=$prob, Prediction=$prediction")
  }

  sparkContext.stop()
}
