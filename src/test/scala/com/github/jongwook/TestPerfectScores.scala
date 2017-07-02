package com.github.jongwook

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalactic.{Equality, TolerantNumerics}
import org.scalatest.{FlatSpec, Matchers}

/** Tests if the metrics return 1.0 for the perfect prediction */
class TestPerfectScores extends FlatSpec with Matchers {
  import TestFixture._
  implicit val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(eps)

  val perfectScores: Seq[(String, Map[Metric, Seq[Double]])] = {
    val spark = SparkSession.builder().master(new SparkConf().get("spark.master", "local[8]")).getOrCreate()

    import spark.implicits._

    val predictionDF = spark.createDataset(prediction)
    val groundTruthDF = spark.createDataset(groundTruth)

    for ((name, df) <- Seq("prediction" -> predictionDF, "ground truth" -> groundTruthDF)) yield {
      val metrics = new SparkRankingMetrics(df, df, itemCol = "product", predictionCol = "rating")

      name -> Map[Metric, Seq[Double]](
        NDCG -> metrics.ndcgAt(ats),
        MAP -> metrics.mapAt(ats),
        Precision -> metrics.precisionAt(Seq(Integer.MAX_VALUE)),
        Recall -> metrics.recallAt(Seq(Integer.MAX_VALUE))
      )
    }
  }

  for ((name, scores) <- perfectScores) {
    for (metric <- Seq(NDCG, MAP, Precision, Recall)) {
      s"In $name dataset, our $metric implementation" should s"return 1.0 for the perfect prediction" in {
        for (score <- scores(metric)) {
          score should equal(1.0)
        }
      }
    }
  }
}
