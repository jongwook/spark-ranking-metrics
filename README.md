spark-ranking-metrics
=====================

Ranking is an important component for building recommender systems, and there are various methods to evaluate the offline performance of ranking algorithms [1].

Users are usually provided with a certain number of recommended items, so it is usual to measure the performance of only the top-*k* recommended items, and this is why the metrics are often suffixed by "@*k*".

Currently, Spark's implementations for ranking metrics are [quite limited](http://spark.apache.org/docs/2.0.0/api/scala/index.html#org.apache.spark.mllib.evaluation.RankingMetrics), and its NDCG implementation assumes that the relevance is always binary, 
producing [different numbers](https://gist.github.com/jongwook/5d4e78290eaef22cb69abbf68b52e597). Meanwhile, [RiVal](https://github.com/recommenders/rival) aims to be a toolkit for reproducible recommender system evaluation and provides a robust implementation for a few ranking metrics, but it is written as a single-threaded application and therefore is not applicable to the scale which Spark users typically encounter.

To complement this, [`SparkRankingMetrics`](src/main/scala/com/github/jongwook/SparkRankingMetrics.scala) contains scalable implementations for NDCG, MAP, Precision, and Recall, using Spark's DataFrame/Dataset idioms.

> [1] Gunawardana, A., & Shani, G. (2015). Evaluating recommendation systems. In *Recommender systems handbook* (pp. 265-308). Springer US.

Usage
-----

```scala
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import com.github.jongwook.SparkRankingMetrics

// A recommendation model obtained by Spark's ALS
val model = MatrixFactorizationModel.load(sc, "path/to/model")
val result: RDD[(Int, Array[Rating])] = model.recommendProductsForUsers(20)

// A flattened RDD that contains all Ratings in the result
val prediction: RDD[Rating] = result.values.flatMap(ratings => ratings)
val groundTruth: RDD[Rating] = /* the ground-truth dataset */

// create DataFrames using the RDDs above
val predictionDF = spark.createDataFrame(prediction)
val groundTruthDF = spark.createDataFrame(groundTruth)

// instantiate using either DataFrames or Datasets
val metrics = SparkRankingMetrics(predictionDF, groundTruthDF)

// override the non-default column names
metrics.setItemCol("product")
metrics.setPredictionCol("rating")

// print the metrics
println(metrics.ndcgAt(10))
println(metrics.mapAt(15))
println(metrics.precisionAt(5))
println(metrics.recallAt(20))
```

Validation
----------

This repository contains [a test case](src/test/scala/com/github/jongwook/TestEquality.scala) that checks the numbers produced by `SparkRankingMetrics` are identical to what RiVal's corresponding implementations produce, using the [MovieLens 100K dataset](http://grouplens.org/datasets/movielens/100k/)
