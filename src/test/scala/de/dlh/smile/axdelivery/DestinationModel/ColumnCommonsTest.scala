package de.dlh.smile.axdelivery.DestinationModel

import de.dlh.smile.axdelivery.{Stub, TestSets}
import org.scalatest.{FlatSpec, Matchers}
import de.dlh.smile.axdelivery.DestinationModel.ColumnCommons._
import de.dlh.smile.engine.commons.Contexts
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}

class ColumnCommonsTest extends FlatSpec with Matchers {

/*
  "dateFrom" should "get the specified segment of the date from a String date" in {
    val df = Stub.dfStringDate
    val dfResult = df
      .withColumn("year", dateFrom(col("date"), lit(YEAR.toString), lit("yyyyMMdd")))
      .withColumn("month", dateFrom(col("date"), lit(MONTH.toString), lit("yyyyMMdd")))
      .withColumn("day", dateFrom(col("date"), lit(DAY.toString), lit("yyyyMMdd")))
    dfResult.filter(col("year") === 2016).count should equal(2)
    dfResult.filter(col("month") === 9).count should equal(2)
    dfResult.filter(col("day") === 26).count should equal(1)
    dfResult.filter(col("day") === 27).count should equal(1)
  }
*/

  "udfCreateDateFrom" should "get the specified segment of the date from a String date" in {
    val df = Stub.dfStringDate
    val dfResult = df.withColumn("date", udfCreateDateFrom(col("date"), lit("yyyyMMdd"), lit("yyyy-MM-dd")))
    dfResult.count should equal(4)
    dfResult.filter(col("date").isNotNull).count should equal(2)
    dfResult.take(1)(0).getString(0) should equal("2016-09-27")
  }

  "getFirstIATA" should "get the first IATA code from the IATA string" in {
	  val df = Stub.dfStringIATA
    val dfResult = df.withColumn("BFO", getFirstIATA(col("BFO")))
    dfResult.collect() should equal(Array(Row("MAD"), Row("BCN"), Row(null), Row("MAD")))
  }

  "udfGetTimeInd" should "turn the hour of the day into the time of the day" in {
    val df = Stub.dfOneColInt
    val dfResult = df.withColumn("two", udfGetTimeInd(col("one")))

    dfResult.show
    dfResult.filter(col("two")==="afternoon").count should equal(2)
    dfResult.filter(col("two")==="evening").count should equal(1)
    dfResult.filter(col("two")==="").count should equal(1)
    dfResult.filter(col("two")==="morning").count should equal(2)
  }

  // TODO: wrong string formats makes this test fail
  "udfGetDurationStay" should "turn the hour of the day into the time of the day" in {
    val df = Stub.dfOneColString
    val dfResult = df.withColumn("two", udfGetDurationStay(col("one")))
    dfResult.show
  }

  "udfGetBrowserName" should "turn the hour of the day into the time of the day" in {
    val df = Stub.dfBrowserString
    val dfResult = df.withColumn("two", udfGetBrowserName(col("one")))

    dfResult.filter(col("two")==="Firefox").count should equal(4)
    dfResult.filter(col("two")==="Safari").count should equal(3)
    dfResult.filter(col("two")==="Chrome").count should equal(3)
    dfResult.filter(col("two")==="IE").count should equal(1)
  }

  "udfGetReferrerCat" should "turn the hour of the day into the time of the day" in {
    val df = Stub.dfReferrerCat
    val dfResult = df.withColumn("two", udfGetReferrerCat(col("one")))

    dfResult.filter(col("two")==="Direct").count should equal(1)
    dfResult.filter(col("two")==="Google").count should equal(1)
  }

  "print training data" should "print dataframe" in {
    val df = Contexts.sqlCtx.read.parquet(getClass.getResource("/data/training").getPath)
    df.limit(10).select("refdom").show
  }


}
