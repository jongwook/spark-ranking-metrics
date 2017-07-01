package com.github.jongwook

object TestFixture {
  val ats = Array(5, 10, 30, 50, 100)
  val eps = 1e-9

  sealed trait Metric
  case object NDCG extends Metric
  case object MAP extends Metric
  case object Precision extends Metric
  case object Recall extends Metric

  lazy val (prediction, groundTruth) = RankingDataProvider(MovieLensLoader.load())
}
