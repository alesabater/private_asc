package de.dlh.smile.axdelivery.data

import org.scalatest.{FlatSpec, Matchers}

class ScoreTest extends FlatSpec with Matchers {

  "FixedStringScore" should "perform an operation" in {
    val fixedStringScore = FixedStringScore("someString", 0.34)
    fixedStringScore.result should equal(0.34)
  }

  "DynamicIntScore" should "perform an operation" in {
    val fixedStringScore = DynamicIntScore(10, 0.34)
    fixedStringScore.result * 10 / 10 should equal(3.4)
  }

  "LeisureScore" should "aggregate several score results" in {
    val scores = List(FixedStringScore("someString", 0.34),
      FixedStringScore("someString", 0.34),
      FixedStringScore("someString", 0.34),
      DynamicIntScore(10, 0.34)
    )
    val leisureScores = LeisureScore(scores)
    BigDecimal(leisureScores.result).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble should equal(0.881)
  }

}
