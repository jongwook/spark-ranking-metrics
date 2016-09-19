package com.github.jongwook

import org.apache.spark.mllib.recommendation.{Rating, MatrixFactorizationModel}
import org.apache.spark.sql.SparkSession

object MovieLensEvaluateCustom extends App {{

  val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
  import spark.implicits._

  val ratings = spark.sparkContext.textFile("u.data").map(_.split("\t")).map {
    case Array(userId, movieId, rating, timestamp) => Rating(userId.toInt, movieId.toInt, rating.toDouble)
  }

  val Array(_, testRatings) = ratings.cache().randomSplit(Array(0.99, 0.01), 0)
  val sampled = testRatings.map {
    case Rating(user, item, rating) => ((user, item), rating)
  }.reduceByKey(_ + _)

  val testUsers = sampled.collect().map(_._1._1).toSet

  val model = MatrixFactorizationModel.load(spark.sparkContext, "ml-100k")

  val testUsersBroadcast = spark.sparkContext.broadcast(testUsers)
  val testUserFeatures = model.userFeatures.filter {
    case (user, feature) => testUsersBroadcast.value.contains(user)
  }.repartition(100).cache()

  val testModel = new MatrixFactorizationModel(model.rank, testUserFeatures, model.productFeatures.repartition(100).cache())

  val ats = Array(5, 10, 30, 50, 100)

  val result = testModel.recommendProductsForUsers(ats.max)

  val prediction = spark.createDataset(result.values.flatMap(ratings => ratings))
  val groundTruth = spark.createDataset(testRatings)

  val metrics = DataFrameRankingMetrics(prediction, groundTruth)
  metrics.setItemCol("product")
  metrics.setPredictionCol("rating")

  println("\n\n              @5          @10         @20         @50         @100")

  printf("%12s", "NDCG")
  for (k <- ats) {
    printf("%12.8f", metrics.ndcgAt(k))
  }
  println()

  printf("%12s", "Precision")
  for (k <- ats) {
    printf("%12.8f", metrics.precisionAt(k))
  }
  println()

  printf("%12s", "Recall")
  for (k <- ats) {
    printf("%12.8f", metrics.recallAt(k))
  }
  println()

  printf("%12s", "F1")
  for (k <- ats) {
    printf("%12.8f", metrics.f1At(k))
  }
  println()

  printf("%12s", "MAP")
  for (k <- ats) {
    printf("%12.8f", metrics.mapAt(k))
  }
  println()

}}
