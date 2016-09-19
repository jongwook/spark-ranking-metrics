package com.github.jongwook

import org.apache.spark.mllib.recommendation.Rating
import org.scalatest._

import scala.io.Source

object MovieLensLoader {
  /** load ml-100k dataset */
  def load(): Seq[Rating] = {
    val input = getClass.getResource("u.data").openStream()
    try {
      Source.fromInputStream(input).getLines().toArray.map {
        _.split("\t") match {
          case Array(user, item, rating, timestamp) => Rating(user.toInt, item.toInt, rating.toDouble)
        }
      }
    } finally {
      input.close()
    }
  }
}

class MovieLensLoader extends FlatSpec with Matchers {
  "MovieLens Loader" should "load the ml-100k data" in {
    val data = MovieLensLoader.load()
    data.size should be (100000)
    data.map(_.rating).max should be (5.0)
    data.map(_.rating).min should be (1.0)
  }
}
