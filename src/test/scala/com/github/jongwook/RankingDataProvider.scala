package com.github.jongwook

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.sql.SparkSession
import org.scalatest._

object RankingDataProvider {

  /** Divide given ratings into 90%/10% splits, and trains an ALS model using Spark
    *
    * @return predicted and the ground truth ratings for the test set
    **/
  def apply(ratings: Seq[Rating], k: Int = 100): (Seq[Rating], Seq[Rating]) = {

    val spark = SparkSession.builder().master(new SparkConf().get("spark.master", "local[8]")).getOrCreate()
    val sc = spark.sparkContext

    val Array(trainRatings, testRatings) = sc.parallelize(ratings).cache().randomSplit(Array(0.9, 0.1), 0)
    val model = ALS.trainImplicit(trainRatings, rank = 10, iterations = 2, lambda = 2, blocks = 100, alpha = 10)

    val testUsers = testRatings.map(_.user).collect().toSet
    val testUsersBroadcast = spark.sparkContext.broadcast(testUsers)
    val testUserFeatures = model.userFeatures.filter {
      case (user, feature) => testUsersBroadcast.value.contains(user)
    }.repartition(100).cache()

    val testModel = new MatrixFactorizationModel(model.rank, testUserFeatures, model.productFeatures.repartition(100).cache())

    val result = testModel.recommendProductsForUsers(k)

    val prediction = result.values.flatMap(ratings => ratings).collect()
    val groundTruth = testRatings.collect()

    (prediction, groundTruth)
  }
}

class RankingDataProvider extends FlatSpec with Matchers {
  "Ranking Data Provider" should "calculate the rankings" in {
    val ratings = MovieLensLoader.load()
    val (prediction, groundTruth) = RankingDataProvider(ratings)
    prediction.map(_.user).distinct.sorted should equal (groundTruth.map(_.user).distinct.sorted)
  }
}
