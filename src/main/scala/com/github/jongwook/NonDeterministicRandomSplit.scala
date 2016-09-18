package com.github.jongwook

import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.sql.{Row, SparkSession}

object NonDeterministicRandomSplit extends App {{

  val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

  val ratings = spark.read.orc("data.orc").rdd.repartition(2000).map {
    case Row(user: Int, product: Int, rating: Double) => Rating(user, product, rating)
  }

  val Array(_, split) = ratings.cache().randomSplit(Array(0.99, 0.01), 0)
  println(s"sum = ${split.map(_.rating).reduce(_ + _)}")

}}
