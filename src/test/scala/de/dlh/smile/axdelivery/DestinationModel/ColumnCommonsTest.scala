package de.dlh.smile.axdelivery.DestinationModel

import de.dlh.smile.axdelivery.{Stub, TestSets}
import org.scalatest.{FlatSpec, Matchers}
import de.dlh.smile.axdelivery.DestinationModel.ColumnCommons._
import de.dlh.smile.engine.commons.Contexts
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}
import org.joda.time.LocalDate

class ColumnCommonsTest extends FlatSpec with Matchers {

  "udfGetStringDateFormatted" should "get the specified segment of the date from a String date" in {
    val df = Stub.dfStringDate
    val dfResult = df.withColumn("date", udfGetStringDateFormatted(col("date"), lit("yyyyMMdd")))
    dfResult.count should equal(4)
    dfResult.filter(col("date").isNotNull).count should equal(2)
    dfResult.take(1)(0).getString(0) should equal("2016-09-27")
  }

  "udfGetDayOfTheWeek" should "get the specified segment of the date from a String date" in {
    val df = Stub.dfFullStringDate
    val dfResult = df.withColumn("date", udfGetDayOfTheWeek(col("date"), lit("yyyy-MM-dd HH:mm:ss.S")))
    dfResult.show()
    dfResult.count should equal(4)
    dfResult.filter(col("date").isNotNull).count should equal(2)
    dfResult.take(1)(0).getInt(0) should equal(2)
    dfResult.take(2)(1).getInt(0) should equal(1)
  }

  "udfGetHourOfDay" should "get the specified segment of the date from a String date" in {
    val df = Stub.dfFullStringDate
    val dfResult = df.withColumn("date", udfGetHourOfDay(col("date"), lit("yyyy-MM-dd HH:mm:ss.S")))
    dfResult.count should equal(4)
    dfResult.filter(col("date").isNotNull).count should equal(2)
    dfResult.take(1)(0).getInt(0) should equal(7)
    dfResult.take(2)(1).getInt(0) should equal(11)
  }

  "getFirstIATA" should "get the first IATA code from the IATA string" in {
	  val df = Stub.dfStringIATA
    val dfResult = df.withColumn("BFO", udfGetFirstIATA(col("BFO")))
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

  "udfGetBrowserName" should "Get the browser client name" in {
    val df = Stub.dfBrowserString
    val dfResult = df.withColumn("two", udfGetBrowserName(col("one")))

    dfResult.filter(col("two")==="Firefox").count should equal(4)
    dfResult.filter(col("two")==="Safari").count should equal(3)
    dfResult.filter(col("two")==="Chrome").count should equal(3)
    dfResult.filter(col("two")==="Trident").count should equal(1)
  }

  "udfGetOSName" should "Get the OS name" in {
    val df = Stub.dfBrowserString
    val dfResult = df.withColumn("two", udfGetOSName(col("one")))

    dfResult.show()

    dfResult.filter(col("two")==="Windows").count should equal(5)
    dfResult.filter(col("two")==="Macintosh").count should equal(3)
    dfResult.filter(col("two")==="Other").count should equal(3)
  }

  "udfGetReferrerCat" should "turn the hour of the day into the time of the day" in {
    val df = Stub.dfReferrerCat
    val dfResult = df.withColumn("two", udfGetReferrerCat(col("one")))

    dfResult.filter(col("two")==="Direct").count should equal(1)
    dfResult.filter(col("two")==="Google").count should equal(1)
  }

  "udfGetLangauge" should "get the language" in {
    val df = Stub.dfLanguage
    val dfResult = df.withColumn("two", udfGetLanguage(col("one")))
    dfResult.show()
    dfResult.filter(col("two")==="English").count should equal(4)
    dfResult.filter(col("two")==="Deutsch").count should equal(4)
    dfResult.filter(col("two")==="Other").count should equal(3)
  }

  "print training data" should "print dataframe" in {
    val df = Contexts.sqlCtx.read.parquet(getClass.getResource("/data/training").getPath)
    df.limit(10).select("refdom").show
  }


}
