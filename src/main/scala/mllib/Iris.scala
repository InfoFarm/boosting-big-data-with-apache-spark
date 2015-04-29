package mllib

import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{SVMWithSGD, LogisticRegressionModel, NaiveBayes}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.Algo

object Iris extends App {

  val sparkContext = new SparkContext("local[4]", "Iris")

  case class IrisMeasure(sepalLength: Double, petalLength: Double, sepalWidth: Double, petalWidth: Double, flower: String)

  val measures = sparkContext.textFile("data/iris.csv").map(_.split(",").toList)
    .map { case sepalLength :: petalLength :: sepalWidth :: petalWidth :: flower :: Nil =>
      IrisMeasure(sepalLength.toDouble, petalLength.toDouble, sepalWidth.toDouble, petalWidth.toDouble, flower)
  }

  val flowerIndex = measures.map(_.flower).distinct().collect().zipWithIndex.toMap

  val features = measures.map { measure =>
    LabeledPoint(flowerIndex(measure.flower),
      Vectors.dense(measure.sepalLength, measure.petalLength, measure.sepalWidth, measure.petalWidth))
  }

  val categoricalFeaturesInfo = Map[Int, Int]()
  val decisionTree = DecisionTree.trainClassifier(features, 3, categoricalFeaturesInfo, "gini", 5, 32)

  val naiveBayes = NaiveBayes.train(features)

  val result = naiveBayes.predict(Vectors.dense(6.7, 2.5, 5.8, 1.8))

  println(flowerIndex.find { case (label, index) =>
      index == result
  }.get._1)

  sparkContext.stop()
}