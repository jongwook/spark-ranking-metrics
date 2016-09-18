package com.github.jongwook

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession


object MovieLensALS extends App {{
  val conf = new SparkConf()
  val spark = SparkSession.builder().master(conf.get("spark.master", "local[8]")).enableHiveSupport().getOrCreate()

  val ratings = spark.sparkContext.textFile("u.data").map(_.split("\t")).map {
    case Array(userId, movieId, rating, timestamp) => Rating(userId.toInt, movieId.toInt, rating.toDouble)
  }

  val Array(trainRatings, testRatings) = ratings.cache().randomSplit(Array(0.99, 0.01), 0)

  val model = ALS.trainImplicit(trainRatings, 10, 20, 2, -1, 10, 0)

  model.save(spark.sparkContext, "ml-100k")

}}










