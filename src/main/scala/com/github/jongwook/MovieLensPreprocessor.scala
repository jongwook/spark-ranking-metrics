package com.github.jongwook

import org.apache.spark.sql.SparkSession

import scala.util.Try

object MovieLensPreprocessor extends App {

  case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Int)

  val spark = SparkSession.builder().master("local").enableHiveSupport().getOrCreate()
  import spark.implicits._

  val sc = spark.sparkContext

  val ratings = sc.textFile(System.getProperty("user.home") + "/Downloads/ml-20m/ratings.csv", 1).map(_.split(",")).flatMap {
    case Array(userId, movieId, rating, timestamp) =>
      Try(Rating(userId.toInt, movieId.toInt, rating.toFloat, timestamp.toInt)).toOption
  }

  val ds = spark.createDataset(ratings)

  ds.write.option("compression", "zlib").orc(System.getProperty("user.home") + "/Downloads/ml-20m/ratings")

}
