package com.github.jongwook

import org.apache.spark.SparkConf
import org.apache.spark.mllib.evaluation.RankingMetrics
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.sql.SparkSession

object MovieLensEvaluateSpark extends App {{

  val conf = new SparkConf()
  val spark = SparkSession.builder().master(conf.get("spark.master", "local[8]")).enableHiveSupport().getOrCreate()

  val ratings = spark.sparkContext.textFile("u.data").map(_.split("\t")).map {
    case Array(userId, movieId, rating, timestamp) => Rating(userId.toInt, movieId.toInt, rating.toDouble)
  }

  val Array(trainRatings, testRatings) = ratings.cache().randomSplit(Array(0.99, 0.01), 0)
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

  val groundtruth = sampled.map {
    case ((user, item), score) => (user, (item, score))
  }.groupByKey.mapValues {
    _.toSeq.sortBy { case (item, score) => -score }.map { case (item, score) => item }.toArray
  }

  val ats = Array(5, 10, 30, 50, 100)

  val prediction = testModel.recommendProductsForUsers(ats.max)

  val predictionAndValues = prediction.join(groundtruth).values.map {
    case (predicted, truth) => (predicted.sortBy(-_.rating).map(_.product), truth)
  }

  val metrics = new RankingMetrics(predictionAndValues)


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

  printf("%12s", "MAP")
  printf("%12.8f\n", metrics.meanAveragePrecision)

}}
