package com.github.jongwook

import org.slf4j.LoggerFactory

object KaggleNDCG {

  lazy val log = LoggerFactory.getLogger(getClass.getName.stripSuffix("$"))

  def EvaluateSubmissionSubset(solution: Map[Int, Double], submission: Seq[Int], k: Int): Double = {
    val dcg = calculateDcg(k, submission, solution)
    val optimal = calculateOptimalDcg(k, solution)
    var ndcg = dcg / optimal
    if (optimal <= 0) {
      ndcg = if (dcg == optimal) 1.0 else 0.0
    }
    ndcg
  }

  def calculateDcg(k: Int, documents: Seq[Int], documentToRelevances: Map[Int, Double]): Double = {
    val relevances = documents.map { currentDocumentId =>
      documentToRelevances.get(currentDocumentId) match {
        case Some(relevance) => relevance
        case None =>
          log.warn(s"Item '$currentDocumentId' was specified but is unrecognized. Assuming not relevant (i.e. score of 0)")
          0
      }
    }
    calculateDcg(k, relevances)
  }

  private def calculateOptimalDcg(k: Int, documentToRelevances: Map[Int, Double]): Double = {
    calculateDcg(k, documentToRelevances.values.toSeq.sortBy(x => -x))
  }

  val log2: Double = Math.log(2)

  private def calculateDcg(k: Int, numbers: Seq[Double]): Double = {
    numbers.take(k).zipWithIndex.map {
      case (number, i) => (Math.pow(2.0, number) - 1.0) / Math.log(i + 2) * log2
    }.sum
  }

}

object KaggleNDCGTest extends App {
  for (k <- 1 to 5) {
    val ndcg = KaggleNDCG.EvaluateSubmissionSubset(Map(1 -> 1.0, 2 -> 2.0, 3 -> 3.0, 4 -> 4.0, 5 -> 5.0), Seq(4, 5, 3, 2, 1), k)
    println(ndcg)
  }
}
