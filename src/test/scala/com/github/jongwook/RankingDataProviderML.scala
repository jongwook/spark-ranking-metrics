package com.github.jongwook

import org.apache.spark.SparkConf
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{Matchers, FlatSpec}

object RankingDataProviderML {

  /** Divide given ratings into 99%/1% splits, and trains an ALS model using Spark
    *
    * @return predicted and the ground truth ratings for the test set
    **/
  def apply(ratings: Seq[Rating], k: Int = 100): (Seq[Rating], Seq[Rating]) = {

    val spark = SparkSession.builder().master(new SparkConf().get("spark.master", "local[8]")).getOrCreate()
    import spark.implicits._

    val Array(trainRatings, testRatings) = spark.createDataset(ratings).cache().randomSplit(Array(0.9, 0.1), 0)

    val als = new ALS().setRank(10).setMaxIter(20).setRegParam(2.0).setImplicitPrefs(true).setAlpha(10)

    als.setUserCol("user")
    als.setItemCol("product")
    als.setRatingCol("rating")

    val model = als.fit(trainRatings)

    model.setPredictionCol("prediction")

    val result = model.transform(testRatings)

    val prediction = result.map {
      case Row(user: Int, product: Int, rating: Double, prediction: Float) => Rating(user, product, prediction)
    }.collect()
    val groundTruth = testRatings.collect()

    (prediction, groundTruth)
  }
}

class RankingDataProviderML extends FlatSpec with Matchers {
  "Ranking Data Provider" should "calculate the rankings" in {
    val ratings = MovieLensLoader.load()
    val (prediction, groundTruth) = RankingDataProvider(ratings)
    prediction.map(_.user).distinct.sorted should equal (groundTruth.map(_.user).distinct.sorted)
  }
}
