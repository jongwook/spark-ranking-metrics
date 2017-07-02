package com.github.jongwook

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object PrintMetrics extends App {
  val (prediction, labels) = RankingDataProvider(MovieLensLoader.load())

  val spark = SparkSession.builder().master(new SparkConf().get("spark.master", "local[8]")).getOrCreate()

  val metrics = new SparkRankingMetrics(spark.createDataFrame(prediction), spark.createDataFrame(labels), itemCol = "product", predictionCol = "rating")

  val ats = Seq(5, 10, 20, 100, Integer.MAX_VALUE)
  val toPrint = Map[String, SparkRankingMetrics => Seq[Int] => Seq[Double]](
    "Precision" -> { m => k => m.precisionAt(k) },
    "Recall" -> { m => k => m.recallAt(k) },
    "F1" -> { m => k => m.f1At(k) },
    "NDCG" -> { m => k => m.ndcgAt(k) },
    "MAP" -> { m => k => m.mapAt(k) },
    "MRR" -> { m => k => m.mrrAt(k) }
  )

  for ((metric, calculator) <- toPrint) {
    printf("%12s", metric)
    val f = calculator(metrics)
    for (x <- f(ats)) {
      printf("%12.8f", x)
    }
    println()
  }

}
