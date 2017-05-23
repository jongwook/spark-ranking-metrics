package com.github.jongwook

import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql._
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
  * @param relevanceThreshold
  * entries in the ground truth dataset with the rating lower than this value will be ignored
  */
class SparkRankingMetrics(predicted: DataFrame, groundTruth: DataFrame, relevanceThreshold: Double = 0) extends Params {

  override val uid: String = Identifiable.randomUID(getClass.getSimpleName)

  val userCol = new Param[String](this, "userCol", "column name for user ids. Ids must be within the integer value range.")
  val itemCol = new Param[String](this, "itemCol", "column name for item ids. Ids must be within the integer value range.")
  val ratingCol = new Param[String](this, "ratingCol", "column name for ratings")
  val predictionCol = new Param[String](this, "predictionCol", "prediction column name")

  setDefault(userCol, "user")
  setDefault(itemCol, "item")
  setDefault(ratingCol, "rating")
  setDefault(predictionCol, "prediction")

  def setUserCol(value: String): this.type = set(userCol, value)
  def setItemCol(value: String): this.type = set(itemCol, value)
  def setRatingCol(value: String): this.type = set(ratingCol, value)
  def setPredictionCol(value: String): this.type = set(predictionCol, value)


  lazy val log = LoggerFactory.getLogger(getClass)
  lazy val sqlContext = groundTruth.sqlContext

  def predictionAndLabels: RDD[(Array[Int], Array[(Int, Double)])] = {
    import sqlContext.implicits._
    import org.apache.spark.sql.functions._

    val p = predicted
    val g = groundTruth

    val user = col($(userCol)).cast(IntegerType).as("user")
    val item = col($(itemCol)).cast(IntegerType).as("item")
    val prediction = col($(predictionCol)).cast(DoubleType).as("prediction")
    val rating = col($(ratingCol)).cast(DoubleType).as("rating")

    val left = p.select(user, item, prediction).map {
      case Row(user: Int, item: Int, prediction: Double) => (user, (item, prediction))
    }
    val right = g.select(user, item, rating).where(rating >= relevanceThreshold).map {
      case Row(user: Int, item: Int, rating: Double) => (user, (item, rating))
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

      if (labelMap.nonEmpty) {
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
        if (setK.contains(Integer.MAX_VALUE)) {
          result(lookup(Integer.MAX_VALUE)) = cnt.toDouble / pred.length
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
        if (setK.contains(Integer.MAX_VALUE)) {
          result(lookup(Integer.MAX_VALUE)) = cnt.toDouble / size
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

      if (labelMap.nonEmpty) {
        var i = 0
        var cnt = 0
        var precSum = 0.0
        val n = math.min(math.min(pred.length, labelSize), maxK)
        while (i < n) {
          if (labelMap.contains(pred(i))) {
            cnt += 1
            precSum += cnt.toDouble / (i + 1)
          }
          i += 1
          if (setK.contains(i)) {
            result(lookup(i)) = precSum / labelSize
          }
        }
        if (setK.contains(Integer.MAX_VALUE)) {
          result(lookup(Integer.MAX_VALUE)) = precSum / labelSize
        }
      }

      (result, 1)
    }.reduce(SparkRankingMetrics.addArrayAndSize)

    sums.map(_ / size)
  }

  def mapAt(k: Int): Double = mapAt(Seq(k)).head

  def map = mapAt(Integer.MAX_VALUE)

  /** Computes the Normalized Discounted Cumulative Gain at k */
  def ndcgAt(ats: Seq[Int]): Seq[Double] = {
    require(ats.forall(_ > 0), "ranking position k should be positive")
    val setK = ats.toSet
    val maxK = ats.max
    val lookup = ats.zipWithIndex.toMap

    val (sums, size) = predictionAndLabels.map { case (pred, label) =>
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
        if (setK.contains(Integer.MAX_VALUE)) {
          result(lookup(Integer.MAX_VALUE)) = dcg / idealDcg
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

      if (labelRank.nonEmpty) {
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
        if (setK.contains(Integer.MAX_VALUE)) {
          result(lookup(Integer.MAX_VALUE)) = cumul / pred.length
        }
      }

      (result, 1)
    }.reduce(SparkRankingMetrics.addArrayAndSize)

    sums.map(_ / size)
  }

  def mrrAt(k: Int): Double = mrrAt(Seq(k)).head

  def mrr = mrrAt(Integer.MAX_VALUE)

  override def copy(extra: ParamMap): Params = {
    val copied = new SparkRankingMetrics(predicted, groundTruth)
    copyValues(copied, extra)
    copied
  }
}

object SparkRankingMetrics {
  def apply[P: Encoder, G: Encoder](predicted: Dataset[P], groundTruth: Dataset[G], relevanceThreshold: Double = 0) = {
    new SparkRankingMetrics(predicted.toDF, groundTruth.toDF, relevanceThreshold)
  }

  def apply(predicted: DataFrame, groundTruth: DataFrame) = {
    new SparkRankingMetrics(predicted, groundTruth)
  }

  def apply(predicted: DataFrame, groundTruth: DataFrame, relevanceThreshold: Double) = {
    new SparkRankingMetrics(predicted, groundTruth, relevanceThreshold)
  }

  private [SparkRankingMetrics] def addArrayAndSize(record1: (Array[Double], Int), record2: (Array[Double], Int)): (Array[Double], Int) = {
    val (array1, size1) = record1
    val (array2, size2) = record2

    for (i <- array2.indices) {
      array1(i) += array2(i)
    }
    (array1, size1 + size2)
  }
}
