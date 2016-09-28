package de.dlh.smile.axdelivery

import de.dlh.smile.engine.commons.Contexts
import org.apache.spark.sql.functions._
import org.joda.time.DateTime
import org.scalatest.{FlatSpec, Matchers}
import de.dlh.smile.axdelivery.DestinationModel.DataFrameCommons._
import de.dlh.smile.axdelivery.DestinationModel.Transformations
import org.joda.time.format.DateTimeFormat

class TransformationsTest extends FlatSpec with Matchers {

  "execute" should "run the complete program" in {
    val df = Contexts.sqlCtx.read.parquet(getClass.getResource("/data").getPath)
    println(df.select(col("cs_uri_query")).take(1)(0).getMap(0))
    df.printSchema()
  }

/*  "filterFromMap" should "filter dataframe based on a Map column key and value" in {
    val df = Contexts.sqlCtx.read.parquet(getClass.getResource("/data").getPath)
    df.filterValueMapEquals("cs_uri_query", "Screen", "FOFP")
  }*/

  "formatAndRenameDataFrame" should "format the dataframe as expected" in {
    val df = Contexts.sqlCtx.read.parquet(getClass.getResource("/data").getPath)
    df.show()
    val tableName = "weblogs"

    val dfResult = Transformations.formatAndRegisterDataFrame(df, tableName).filter(col("BFTuDep")!== null)
    dfResult.show
  }
/*
  "filterByDate" should "filter dataframe based on dates" in {
    val df = Stub.dfFilterDate
    val dfResult = df.filterPartitionFieldsOneYearFrom(2016, 9)
    dfResult.count() should equal(2)
  }
*/
  it should "test" in {
    val date = DateTime.now.getMonthOfYear
    println(date)
  }

  "date" should "test" in {
    val date = "20160810"
    val formatStr = "yyyyMMdd"
    val format = DateTimeFormat.forPattern(formatStr)
    println(format.parseLocalDate(date).getDayOfMonth.toString())
  }
/*
  "a1" should  "a1" in {
    val df = Contexts.sqlCtx.read.parquet(getClass.getResource("/data").getPath)
    df.getStringDateYearMonthDay("cs_uri_query", "BFDepDate").filter(col("BFDepDate")!== null).show
    println(df.schema.toString())
  }*/

}
