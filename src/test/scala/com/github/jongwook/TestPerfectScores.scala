package com.github.jongwook

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalactic.{Equality, TolerantNumerics}
import org.scalatest.{FlatSpec, Matchers}

/** Tests if the metrics return 1.0 for the perfect prediction */
class TestPerfectScores extends FlatSpec with Matchers {
  import TestFixture._
  implicit val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(eps)

  val scores: Map[Metric, Seq[Double]] = {
    val spark = SparkSession.builder().master(new SparkConf().get("spark.master", "local[8]")).getOrCreate()

    val groundTruthDF = spark.createDataFrame(groundTruth)

    val metrics = SparkRankingMetrics(groundTruthDF, groundTruthDF)
    metrics.setItemCol("product")
    metrics.setPredictionCol("rating")

    Map(
      NDCG -> metrics.ndcgAt(ats),
      MAP -> metrics.mapAt(ats),
      Precision -> metrics.precisionAt(ats),
      Recall -> metrics.recallAt(ats)
    )
  }

  for (metric <- Seq(NDCG, MAP, Precision, Recall)) {
    s"Our $metric implementation" should "return 1.0 for the perfect prediction" in {
      for (score <- scores(metric)) {
        score should equal (1.0)
      }
    }
  }
}
