package de.dlh.smile.axdelivery.DestinationModel

import de.dlh.smile.axdelivery.Stub
import de.dlh.smile.axdelivery.commons.DataFrameOperations._
import de.dlh.smile.engine.commons.Contexts
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.scalatest.{FlatSpec, Matchers}

class DataFrameOperationsTest extends FlatSpec with Matchers {

  "getBFTUDEPField" should "create a column with the BFTUDEP column" in {
    val df = Stub.dfMapDate
    val dfResult = df.getBFTUDEPField("date_prev", "date_dep", "k1")
    dfResult.filter(col("BFTuDep").isNotNull).count should equal(4)
    dfResult.select("BFTuDep").take(4) should equal(Array(Row(12), Row(11), Row(-4), Row(620)))
  }

  "flatMapType" should "flat desired keys with their values out of a Map Column" in {
    val df = Stub.dfMapDate
    val dfResult = df.flatMapType("date_dep", List("k1", "k.2"))
    dfResult.show
    dfResult.columns should equal(Array("date_prev", "k1", "k2"))
    dfResult.select(col("k1")).take(7).filter(row => row.getString(0) != null).length should equal(5)
    dfResult.select(col("k1")).take(7).filter(row => row.getString(0) == null).length should equal(2)
  }

  "filterByDate" should "filter dataframe based on dates" in {
    val df = Stub.dfFilterDate
    val dfResult = df.filterPartitionFieldsOneYearFrom(2016, 9)
    dfResult.count() should equal(2)
  }

  "filterFromMap" should "filter dataframe based on a Map column key and value" in {
    val df = Stub.dfFilterCsUriQuery
    val dfFiltered = df.filterValueMapEquals("cs_uri_query", "Screen", "FOFP")
    val dfRenamed = df.withColumnRenamed("cs_uri_query", "7")
    val dfNoColumn = dfRenamed.filterValueMapEquals("cs_uri_query", "Screen", "FOFP")

    dfFiltered.schema should equal(df.schema)
    dfFiltered.count() should equal(4)
    dfNoColumn.collect() should equal(dfRenamed.collect())
  }

  "airportToCityCode" should "overwrite the airport codes with city codes" in {
    val df = Stub.dfWebtendsAfterFormat
    val dfAirportMap = Contexts.sqlCtx.read.json(getClass.getResource("/data/airporttocity.json").getPath)
    val dfResult = df.airportToCityCode(dfAirportMap, "BFO")
    dfResult.filter(col("BFO")==="NYC").count() should equal(6)
    dfResult.filter(col("BFO")==="FRA").count() should equal(15)
    dfResult.filter(col("BFO")==="MAD").count() should equal(1)
  }

  "filterRT" should "filter all the rows with BFTripType different than RT" in {
    val df = Stub.dfWebtendsAfterFormat
    val dfResult = df.filterRT()
    dfResult.count() should equal(22)
  }

  "filterOrigin" should "filter BFO with a defined set of originCities" in {
    val df = Stub.dfStringOrigin
    val dfResult = df.filterOrigin()
    dfResult.collect() should equal(Array(Row("MAD"), Row("BCN")))
  }

  "groupByDistinctColumn" should "group DF based on given columns and agg by the distinctCount of an specific column" in {
    val df = Stub.dfAfterLeisureFilter
    val dfGrouped = df.groupByDistinctColumn("session_guid","BFO","BFD","year","month")
    dfGrouped.filter(col("BFO")==="NYC" && col("BFD")==="FRA" && col("year")===2016 && col("month")===8)
      .select("freq").take(1)(0)(0) should equal(2)
    dfGrouped.filter(col("BFO")==="NYC" && col("BFD")==="FRA" && col("year")===2016 && col("month")===11)
      .select("freq").take(1)(0)(0) should equal(1)
    dfGrouped.filter(col("BFO")==="MAD" && col("BFD")==="MUC" && col("year")===2016 && col("month")===9)
      .select("freq").take(1)(0)(0) should equal(2)
    dfGrouped.filter(col("BFO")==="DUB" && col("BFD")==="FRA" && col("year")===2016 && col("month")===11)
      .select("freq").take(1)(0)(0) should equal(1)
  }

}
