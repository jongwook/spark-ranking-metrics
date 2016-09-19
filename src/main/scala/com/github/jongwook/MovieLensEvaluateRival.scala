package com.github.jongwook

import net.recommenders.rival.core.DataModel
import net.recommenders.rival.evaluation.metric.ranking.NDCG.TYPE
import net.recommenders.rival.evaluation.metric.ranking._
import org.apache.spark.mllib.recommendation.{Rating, MatrixFactorizationModel}
import org.apache.spark.sql.SparkSession

class F1[U, I](precision: Precision[U, I], recall: Recall[U, I], predictions: DataModel[U, I], test: DataModel[U, I], cutoff: Double, ats: Array[Int])
  extends AbstractRankingMetric[U, I](predictions, test, cutoff, ats) {

  override def getValueAt(at: Int): Double = {
    val p = precision.getValueAt(at)
    val r = recall.getValueAt(at)
    2 * p * r / (p + r)
  }

  override def getValueAt(user: U, at: Int): Double = {
    val p = precision.getValueAt(user, at)
    val r = recall.getValueAt(user, at)
    2 * p * r / (p + r)
  }

  override def compute(): Unit = {
    precision.compute()
    recall.compute()
  }
}

object MovieLensEvaluateRival extends App {{

  val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

  val ratings = spark.sparkContext.textFile("u.data").map(_.split("\t")).map {
    case Array(userId, movieId, rating, timestamp) => Rating(userId.toInt, movieId.toInt, rating.toDouble)
  }

  val Array(trainRatings, testRatings) = ratings.cache().randomSplit(Array(0.99, 0.01), 0)
  val sampled = testRatings.map {
    case Rating(user, item, rating) => ((user, item), rating)
  }.reduceByKey(_ + _).collect()

  val testUsers = sampled.map(_._1._1).toSet

  val model = MatrixFactorizationModel.load(spark.sparkContext, "ml-100k")

  val testUsersBroadcast = spark.sparkContext.broadcast(testUsers)
  val testUserFeatures = model.userFeatures.filter {
    case (user, feature) => testUsersBroadcast.value.contains(user)
  }.repartition(100).cache()

  val testModel = new MatrixFactorizationModel(model.rank, testUserFeatures, model.productFeatures.repartition(100).cache())

  val predictions = new DataModel[Int, Int]()
  val groundtruth = new DataModel[Int, Int]()

  sampled.foreach {
    case ((user, item), score) => groundtruth.addPreference(user, item, score)
  }

  val ats = Array(5, 10, 30, 50, 100)

  for ((user, recommendations) <- testModel.recommendProductsForUsers(ats.max).collect()) {
    recommendations.foreach {
      case Rating(_, item, score) => predictions.addPreference(user, item, score)
    }
  }

  val ndcg = new NDCG(predictions, groundtruth, 0, ats, TYPE.EXP)
  val map = new MAP(predictions, groundtruth, 0, ats)
  val precision = new Precision(predictions, groundtruth, 0, ats)
  val recall = new Recall(predictions, groundtruth, 0, ats)
  val f1 = new F1(precision, recall, predictions, groundtruth, 0, ats)

  val metrics = Seq(ndcg, map, precision, recall, f1)

  metrics.foreach(_.compute())

  println("\n\n              @5          @10         @20         @50         @100")
  for (metric <- metrics) {
    printf("%12s", metric.getClass.getSimpleName)
    for (k <- ats) {
      printf("%12.8f", metric.getValueAt(k))
    }
    println()
  }

}}
