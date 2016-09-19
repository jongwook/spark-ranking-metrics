package com.github.jongwook

import net.recommenders.rival.core.DataModel
import net.recommenders.rival.evaluation.metric.ranking.NDCG
import net.recommenders.rival.evaluation.metric.ranking.NDCG.TYPE
import org.apache.spark.SparkConf
import org.apache.spark.mllib.evaluation.RankingMetrics
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Random, Try}

/** A rough translation of Kaggle's code for NDCG, found at https://www.kaggle.com/wiki/NormalizedDiscountedCumulativeGain
  * Note that these methods only calculate the ranking for one query (usually corresponding to a user),
  * while reported evaluation metrics are usually average performance over multiple queries.
  */
object KaggleNDCG {

  def evaluateSubmissionSubset(solution: Map[Int, Double], submission: Seq[Int], k: Int): Double = {
    val dcg = calculateDcg(k, submission, solution)
    val optimal = calculateOptimalDcg(k, solution)
    var ndcg = dcg / optimal
    if (optimal <= 0) {
      ndcg = if (dcg == optimal) 1.0 else 0.0
    }
    ndcg
  }

  def calculateDcg(k: Int, items: Seq[Int], itemToRelevances: Map[Int, Double]): Double = {
    val relevances = items.map { item =>
      itemToRelevances.getOrElse(item, 0.0)
    }
    calculateDcg(k, relevances)
  }

  private def calculateOptimalDcg(k: Int, itemToRelevances: Map[Int, Double]): Double = {
    calculateDcg(k, itemToRelevances.values.toSeq.sortBy(x => -x))
  }

  val log2: Double = Math.log(2)

  private def calculateDcg(k: Int, numbers: Seq[Double]): Double = {
    numbers.take(k).zipWithIndex.map {
      case (number, i) => (Math.pow(2.0, number) - 1.0) / Math.log(i + 2) * log2
    }.sum
  }

  /** A test to show that Kaggle's, Rival's, and Spark's NDCG implementations are equivalent */
  def main(args: Array[String]): Unit = {
    // simple synthetic data for ranking query
    val numItems = 10
    val rng = new Random(0)
    val groundTruth = (1 to numItems).map(x => (x, x.toDouble)).toMap
    val prediction = (numItems/2 to numItems).map(x => (x, rng.nextDouble())).toMap

    val groundTruthRanking = groundTruth.toSeq.sortBy(-_._2).map(_._1).toArray
    val predictionRanking = prediction.toSeq.sortBy(-_._2).map(_._1).toArray

    val ats = (1 to numItems).toArray

    // Kaggle
    printf("%10s", "Kaggle")
    for (k <- ats) {
      printf("%15.12f", evaluateSubmissionSubset(groundTruth, predictionRanking, k))
    }
    println()

    // Rival
    val groundTruthModel = new DataModel[Int, Int]()
    val predictionModel = new DataModel[Int, Int]()

    groundTruth.foreach { case (item, rating) => groundTruthModel.addPreference(0, item, rating); }
    prediction.foreach { case (item, rating) => predictionModel.addPreference(0, item, rating); }
    val rivalNDCG = new NDCG[Int, Int](predictionModel, groundTruthModel, 0, ats, TYPE.EXP)
    rivalNDCG.compute()

    printf("%10s", "Rival")
    for (k <- ats) {
      printf("%15.12f", rivalNDCG.getValueAt(k))
    }
    println()

    // Spark
    Try(Class.forName("org.apache.spark.sql.SparkSession")) match {
      case Success(_) =>
        val spark = SparkSession.builder().master(new SparkConf().get("spark.master", "local[8]")).getOrCreate()
        import spark.implicits._

        val predictionAndLabel = spark.sparkContext.parallelize(Seq((predictionRanking, groundTruthRanking)))
        val metrics = new RankingMetrics(predictionAndLabel)

        val sparkNDCGs = ats.map(metrics.ndcgAt)
        printf("%10s", "Spark")
        sparkNDCGs.foreach(ndcg => printf("%15.12f", ndcg))
        println()

        val predictionSet = spark.createDataset(prediction.toSeq.map {
          case (item, rating) => Rating(0, item, rating)
        })
        val groundTruthSet = spark.createDataset(groundTruth.toSeq.map {
          case (item, rating) => Rating(0, item, rating)
        })

        val dataFrameMetrics = DataFrameRankingMetrics(predictionSet, groundTruthSet, 0)
        dataFrameMetrics.setItemCol("product")
        dataFrameMetrics.setPredictionCol("rating")

        val customNDCGs = ats.map(dataFrameMetrics.ndcgAt)
        printf("%10s", "Custom")
        customNDCGs.foreach(ndcg => printf("%15.12f", ndcg))
        println()


      case Failure(_) =>
        println("Spark classes not found")
    }
  }

}
