package com.github.jongwook

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.sql.SparkSession
import org.scalactic.TolerantNumerics
import org.scalatest._

object TestEquality {
  val ats = Array(5, 10, 30, 50, 100)
  val eps = 1e-9

  sealed trait Metric
  case object NDCG extends Metric
  case object MAP extends Metric
  case object Precision extends Metric
  case object Recall extends Metric

  lazy val (prediction, groundTruth) = RankingDataProvider(MovieLensLoader.load())

  lazy val ourResults: Map[Metric, Seq[Double]] = {
    val spark = SparkSession.builder().master(new SparkConf().get("spark.master", "local[8]")).getOrCreate()

    val predictionDF = spark.createDataFrame(prediction)
    val groundTruthDF = spark.createDataFrame(groundTruth)

    val metrics = SparkRankingMetrics(predictionDF, groundTruthDF)
    metrics.setItemCol("product")
    metrics.setPredictionCol("rating")

    Map(
      NDCG -> ats.map(metrics.ndcgAt),
      MAP -> metrics.mapAt(ats),
      Precision -> metrics.precisionAt(ats),
      Recall -> metrics.recallAt(ats)
    )
  }

  lazy val rivalResults: Map[Metric, Seq[Double]] = {
    import net.recommenders.rival.core.DataModel
    import net.recommenders.rival.evaluation.metric.{ranking => rival}

    val predictionModel = new DataModel[Int, Int]()
    val groundTruthModel = new DataModel[Int, Int]()

    prediction.foreach {
      case Rating(user, item, score) => predictionModel.addPreference(user, item, score)
    }
    groundTruth.foreach {
      case Rating(user, item, score) => groundTruthModel.addPreference(user, item, score)
    }

    val ndcg = new rival.NDCG(predictionModel, groundTruthModel, eps, ats, rival.NDCG.TYPE.EXP)
    val map = new rival.MAP(predictionModel, groundTruthModel, eps, ats)
    val precision = new rival.Precision(predictionModel, groundTruthModel, eps, ats)
    val recall = new rival.Recall(predictionModel, groundTruthModel, eps, ats)

    Seq(ndcg, map, precision, recall).foreach(_.compute())

    Map(
      NDCG -> ats.map(ndcg.getValueAt),
      MAP -> ats.map(map.getValueAt),
      Precision -> ats.map(precision.getValueAt),
      Recall -> ats.map(recall.getValueAt)
    )
  }

}

class TestEquality extends FlatSpec with Matchers {
  import TestEquality._
  implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(eps)

  ourResults
  rivalResults

  for (metric <- Seq(NDCG, MAP, Precision, Recall)) {
    s"Our $metric implementation" should "produce the same numbers as Rival" in {
      for ((ours, rivals) <- ourResults(metric) zip rivalResults(metric)) {
        ours should equal (rivals)
      }
    }
  }
}
