package de.dlh.smile.axdelivery.models

import de.dlh.smile.axdelivery.Stub
import org.scalatest.{FlatSpec, Matchers}

class LeisureFilteringTest extends FlatSpec with Matchers {

  "getRelevantInformation" should "get all the revelevant info parsed properly" in {
    val df = Stub.dfWebtrendsFormatted
    val dfResult = LeisureFiltering.getRelevantInformation(df)

    val resultArray = dfResult.collect()
    val result0 = resultArray(0)
    val result1 = resultArray(1)
    val result2 = resultArray(2)
    val result3 = resultArray(3)
    val result4 = resultArray(4)

    result0.getString(0) should equal("RT")
    result0.getInt(1) should equal(34)
    result0.getInt(2) should equal(6)
    result0.getInt(3) should equal(2)
    result0.getInt(4) should equal(20)
    result0.getInt(5) should equal(6)
    result0.getString(6) should equal("3-4w")
    result0.getString(7) should equal("Firefox")
    result0.getString(8) should equal("Windows")
    result0.getString(9) should equal("Direct")
    result0.getString(10) should equal("English")
    result0.getString(11) should equal("IK")
    result0.getString(12) should equal("0")

    result1.getString(0) should equal("OW")
    result1.getInt(1) should equal(21)
    result1.getInt(2) should equal(4)
    result1.getInt(3) should equal(2)
    result1.getInt(4) should equal(11)
    result1.getInt(5) should equal(6)
    result1.getString(6) should equal("3-4w")
    result1.getString(7) should equal("Chrome")
    result1.getString(8) should equal("Other")
    result1.getString(9) should equal("Direct")
    result1.getString(10) should equal("English")
    result1.getString(11) should equal("IK")
    result1.getString(12) should equal("0")

    result2.getString(0) should equal("RT")
    result2.getInt(1) should equal(15)
    result2.getInt(2) should equal(5)
    result2.getInt(3) should equal(1)
    result2.getInt(4) should equal(18)
    result2.getInt(5) should equal(6)
    result2.getString(6) should equal(">4w")
    result2.getString(7) should equal("Safari")
    result2.getString(8) should equal("Other")
    result2.getString(9) should equal("Direct")
    result2.getString(10) should equal("Other")
    result2.getString(11) should equal("IK")
    result2.getString(12) should equal("0")

    result3.getString(0) should equal("RT")
    result3.getInt(1) should equal(11)
    result3.getInt(2) should equal(5)
    result3.getInt(3) should equal(1)
    result3.getInt(4) should equal(16)
    result3.getInt(5) should equal(6)
    result3.getString(6) should equal("1-2w")
    result3.getString(7) should equal("Chrome")
    result3.getString(8) should equal("Other")
    result3.getString(9) should equal("Direct")
    result3.getString(10) should equal("Deutsch")
    result3.getString(11) should equal("IK")
    result3.getString(12) should equal("2")

    result4.getString(0) should equal("RT")
    result4.getInt(1) should equal(40)
    result4.getInt(2) should equal(2)
    result4.getInt(3) should equal(6)
    result4.getInt(4) should equal(13)
    result4.getInt(5) should equal(6)
    result4.getString(6) should equal("1-2w")
    result4.getString(7) should equal("Firefox")
    result4.getString(8) should equal("Windows")
    result4.getString(9) should equal("Google")
    result4.getString(10) should equal("Deutsch")
    result4.getString(11) should equal("K")
    result4.getString(12) should equal("0")

    df.show()
    df.printSchema()
    dfResult.show()
    dfResult.printSchema()
  }

  "getLeisureScores" should "get all the scores for each column" in {
    val df = Stub.dfWebtrendsRelevantInfo
    df.show()
    val dfResult = LeisureFiltering.getLeisureScores(df, df.schema)

    val results = dfResult.collect()
    dfResult.show()

    //results(0).getDouble(0)*1000/1000 should equal(-0.964367 + 34*0.008061 + -0.24329 + 0.0 + 1.345292 + 1.201469 + 5.560091458 + -0.281493421 - 0.979952029 + 0.394189713 + 0.215854591 - 0.614505492)
  }
}
