package com.github.jongwook

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.slf4j.LoggerFactory

/** Contains methods to calculate various ranking metrics.
  * columns "user", "item", "rating", and "prediction" should be present in the input DataFrames,
  * which can be overridden using the set{column name}Col methods.
  *
  * @param predicted
  * a DataFrame that contains the data to be evaluated,
  * where the rankings are implied from the numbers in the "prediction" column
  * should also contain the columns "user" and "item".
  * @param groundTruth
  * a DataFrame that contains the ground truth data
  * should contain the columns "user", "item", and "prediction".
  * @param userCol       column name for user ids. Ids must be in a 32-bit integer type.
  * @param itemCol       column name for item ids. Ids must be in a 32-bit integer type.
  * @param predictionCol column name for predictions, which contains the predicted rating values or scores
  * @param ratingCol     column name for ratings, which contains the ground truth rating values
  * @param relevanceThreshold
  * entries in the ground truth dataset with the rating lower than this value will be ignored
  */
class SparkRankingMetrics(predicted: Dataset[_], groundTruth: Dataset[_],
                          userCol: String = "user", itemCol: String = "item",
                          predictionCol: String = "prediction", ratingCol: String = "rating",
                          relevanceThreshold: Double = 0) {

  val log = LoggerFactory.getLogger(getClass)
  val sqlContext = groundTruth.sqlContext

  val predictionAndLabels: RDD[(Array[Int], Array[(Int, Double)])] = {
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    val p = predicted
    val g = groundTruth

    val user = col(userCol).cast(IntegerType).as("user")
    val item = col(itemCol).cast(IntegerType).as("item")
    val prediction = col(predictionCol).cast(DoubleType).as("prediction")
    val rating = col(ratingCol).cast(DoubleType).as("rating")

    val left = p.select(user, item, prediction).map {
      case Row(user: Int, item: Int, prediction: Double) => (user, (item, prediction))
      case Row(user: Int, item: Int, prediction: Float) => (user, (item, prediction.toDouble))
    }
    val right = g.select(user, item, rating).where(rating >= relevanceThreshold).map {
      case Row(user: Int, item: Int, rating: Double) => (user, (item, rating))
      case Row(user: Int, item: Int, rating: Float) => (user, (item, rating.toDouble))
    }
    (left.rdd cogroup right.rdd).values.map {
      case (predictedItems, groundTruthItems) =>
        val prediction = predictedItems.toArray.sortBy(-_._2).map(_._1)
        val labels = groundTruthItems.toArray.sortBy(-_._2)
        (prediction, labels)
    }.cache()
  }

  /** Computes Precision@k */
  def precisionAt(ats: Seq[Int]): Seq[Double] = {
    require(ats.forall(_ > 0), "ranking position k should be positive")
    val setK = ats.toSet
    val maxK = ats.max
    val lookup = ats.zipWithIndex.toMap

    val (sums, size) = predictionAndLabels.map { case (pred, label) =>
      val labelMap = label.toMap
      val result = new Array[Double](setK.size)

      if (pred.nonEmpty && labelMap.nonEmpty) {
        val n = math.min(pred.length, maxK)
        var i = 0
        var cnt = 0

        while (i < n) {
          if (labelMap.contains(pred(i))) {
            cnt += 1
          }
          i += 1

          if (setK.contains(i)) {
            result(lookup(i)) = cnt.toDouble / i
          }
        }
        for (k <- ats.filter(_ > n)) {
          if (k == Integer.MAX_VALUE) {
            result(lookup(k)) = cnt.toDouble / pred.length
          } else {
            result(lookup(k)) = cnt.toDouble / k
          }
        }
      }

      (result, 1)
    }.reduce(SparkRankingMetrics.addArrayAndSize)

    sums.map(_ / size)
  }

  def precisionAt(k: Int): Double = precisionAt(Seq(k)).head

  /** Computes Recall@k */
  def recallAt(ats: Seq[Int]): Seq[Double] = {
    require(ats.forall(_ > 0), "ranking position k should be positive")
    val setK = ats.toSet
    val maxK = ats.max
    val lookup = ats.zipWithIndex.toMap

    val (sums, size) = predictionAndLabels.map { case (pred, label) =>
      val labelMap = label.toMap
      val result = new Array[Double](setK.size)

      if (labelMap.nonEmpty) {
        val size = labelMap.size

        val n = math.min(pred.length, maxK)
        var i = 0
        var cnt = 0
        while (i < n) {
          if (labelMap.contains(pred(i))) {
            cnt += 1
          }
          i += 1

          if (setK.contains(i)) {
            result(lookup(i)) = cnt.toDouble / size
          }
        }
        for (k <- ats.filter(_ > n)) {
          result(lookup(k)) = cnt.toDouble / size
        }
      }

      (result, 1)
    }.reduce(SparkRankingMetrics.addArrayAndSize)

    sums.map(_ / size)
  }

  def recallAt(k: Int): Double = recallAt(Seq(k)).head

  /** Computes the F1-score at k */
  def f1At(ats: Seq[Int]): Seq[Double] = {
    val precisions = precisionAt(ats)
    val recalls = recallAt(ats)
    (precisions zip recalls).map {
      case (precision, recall) => 2 * precision * recall / (precision + recall)
    }
  }

  def f1At(k: Int): Double = f1At(Seq(k)).head

  /** Computes the mean average precision at k */
  def mapAt(ats: Seq[Int]): Seq[Double] = {
    require(ats.forall(_ > 0), "ranking position k should be positive")
    val setK = ats.toSet
    val maxK = ats.max
    val lookup = ats.zipWithIndex.toMap

    val (sums, size) = predictionAndLabels.map { case (pred, label) =>
      val labelMap = label.toMap
      val labelSize = labelMap.size
      val result = new Array[Double](setK.size)

      if (pred.nonEmpty && labelMap.nonEmpty) {
        var i = 0
        var cnt = 0
        var precSum = 0.0
        val n = math.min(math.max(pred.length, labelSize), maxK)
        while (i < n) {
          if (i < pred.length && labelMap.contains(pred(i))) {
            cnt += 1
            precSum += cnt.toDouble / (i + 1)
          }
          i += 1
          if (setK.contains(i)) {
            result(lookup(i)) = precSum / math.min(pred.length, i)
          }
        }
        for (k <- ats.filter(_ > n) if cnt > 0) {
          result(lookup(k)) = precSum / math.min(pred.length, k)
        }
      }

      (result, 1)
    }.reduce(SparkRankingMetrics.addArrayAndSize)

    sums.map(_ / size)
  }

  def mapAt(k: Int): Double = mapAt(Seq(k)).head

  def map: Double = mapAt(Integer.MAX_VALUE)

  /** Computes the Normalized Discounted Cumulative Gain at k */
  def ndcgAt(ats: Seq[Int]): Seq[Double] = {
    require(ats.forall(_ > 0), "ranking position k should be positive")
    val setK = ats.toSet
    val maxK = ats.max
    val lookup = ats.zipWithIndex.toMap

    val (sums, size) = predictionAndLabels.map { case (pred, truth) =>
      val label = truth.filter(_._2 > 0)
      val labelMap = label.toMap
      val result = new Array[Double](setK.size)

      if (labelMap.nonEmpty) {
        val n = math.min(math.max(pred.length, labelMap.size), maxK)
        var idealDcg = 0.0
        var dcg = 0.0
        var i = 0

        while (i < n) {
          var gain = 0.0
          var ideal = 0.0

          if (i < pred.length) {
            gain = labelMap.get(pred(i)).map(rel => (math.pow(2, rel) - 1) / math.log(i + 2)).getOrElse(0.0)
            dcg += gain
          }

          if (i < label.length) {
            ideal = (math.pow(2, label(i)._2) - 1) / math.log(i + 2)
            idealDcg += ideal
          }

          i += 1

          if (setK.contains(i)) {
            result(lookup(i)) = dcg / idealDcg
          }
        }
        for (k <- ats.filter(_ > n)) {
          result(lookup(k)) = dcg / idealDcg
        }
      }

      (result, 1)
    }.reduce(SparkRankingMetrics.addArrayAndSize)

    sums.map(_ / size)
  }

  def ndcgAt(k: Int): Double = ndcgAt(Seq(k)).head

  /** Computes the Mean Reciprocal Rank at k */
  def mrrAt(ats: Seq[Int]): Seq[Double] = {
    require(ats.forall(_ > 0), "ranking position k should be positive")
    val setK = ats.toSet
    val maxK = ats.max
    val lookup = ats.zipWithIndex.toMap

    val (sums, size) = predictionAndLabels.map { case (pred, label) =>
      val labelRank = label.map { case (item, rating) => item }.zipWithIndex.toMap
      val result = new Array[Double](setK.size)

      if (pred.nonEmpty && labelRank.nonEmpty) {
        val n = math.min(pred.length, maxK)
        var i = 0
        var cumul = 0.0
        while (i < n) {
          labelRank.get(pred(i)).foreach {
            rank => cumul += 1.0 / (rank + 1.0)
          }
          i += 1

          if (setK.contains(i)) {
            result(lookup(i)) = cumul / i
          }
        }
        for (k <- ats.filter(_ > n)) {
          result(lookup(k)) = cumul / pred.length
        }
      }

      (result, 1)
    }.reduce(SparkRankingMetrics.addArrayAndSize)

    sums.map(_ / size)
  }

  def mrrAt(k: Int): Double = mrrAt(Seq(k)).head

  def mrr = mrrAt(Integer.MAX_VALUE)

}

object SparkRankingMetrics {
  private [SparkRankingMetrics] def addArrayAndSize(record1: (Array[Double], Int), record2: (Array[Double], Int)): (Array[Double], Int) = {
    val (array1, size1) = record1
    val (array2, size2) = record2

    for (i <- array2.indices) {
      array1(i) += array2(i)
    }
    (array1, size1 + size2)
  }
}
