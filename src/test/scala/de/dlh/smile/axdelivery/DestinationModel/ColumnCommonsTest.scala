package de.dlh.smile.axdelivery.DestinationModel

import de.dlh.smile.axdelivery.{Stub, TestSets}
import org.scalatest.{FlatSpec, Matchers}
import de.dlh.smile.axdelivery.DestinationModel.ColumnCommons._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row

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

  "dateFrom" should "get the specified segment of the date from a String date" in {
    val df = Stub.dfStringDate
    val dfResult = df.withColumn("date", dateFrom(col("date"), lit("yyyyMMdd")))

    dfResult.show
  }

  "getFirstIATA" should "get the first IATA code from the IATA string" in {
	  val df = Stub.dfStringIATA
    val dfResult = df.withColumn("BFO", getFirstIATA(col("BFO")))
    dfResult.collect() should equal(Array(Row("MAD"), Row("BCN"), Row(null), Row("MAD")))
  }


}