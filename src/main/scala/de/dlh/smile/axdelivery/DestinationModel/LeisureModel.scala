package de.dlh.smile.axdelivery.DestinationModel

import de.dlh.smile.axdelivery.{IO, LoadedProperties}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import de.dlh.smile.axdelivery.DestinationModel.DataFrameCommons._
import de.dlh.smile.axdelivery.DestinationModel.ColumnCommons._
import org.apache.spark.sql.types.StringType

object LeisureModel {

  def cleanData(df: DataFrame, io: IO): DataFrame = {
    val dfAirportCodes = io.readAirportCodes
    val dfFormatted = formatAndRegisterDataFrame(df, dfAirportCodes)
    dfFormatted.show(10)
    val dfValues = prepareNominalValues(dfFormatted)
    dfValues
//    dfFormatted
  }

  def formatAndRegisterDataFrame(df: DataFrame, dfAirportMap: DataFrame): DataFrame = {
    df.filterPartitionFieldsOneYearFrom()
      .filterValueMapEquals("cs_uri_query", "Screen", "FOFP")
      .getBFTUDEPField("date_dt", "cs_uri_query", "BFDepDate", "yyyyMMdd")
      .flatMapType("cs_uri_query", LoadedProperties.fromMapColumns)
      .select(LoadedProperties.webtrendsColumns.map(col(_)): _*)
      .withColumn("BFO", udfGetFirstIATA(col("BFO")))
      .withColumn("BFD", udfGetFirstIATA(col("BFD")))
      .airportToCityCode(dfAirportMap, "BFO")
      .airportToCityCode(dfAirportMap, "BFD")
      .filterOrigin()
      .filterRT()
  }

  def prepareForModel(df: DataFrame): DataFrame = {
    ???
  }

  def getCoeficients():DataFrame = ???

  def prepareNominalValues(df: DataFrame): DataFrame = {
    df.withColumn("bf_trip_type", col("BFTripType"))
      .withColumn("bf_tu_dep", col("BFTuDep"))
      .withColumn("bf_ret_date", udfGetDayOfTheWeek(col("BFRetDate"), lit("yyyyMMdd")))
      .withColumn("date_dt_day", udfGetDayOfTheWeek(col("date_dt").cast(StringType), lit("yyyy-MM-dd HH:mm:ss")))
      .withColumn("date_dt_hour", udfGetHourOfDay(col("date_dt").cast(StringType), lit("yyyy-MM-dd HH:mm:ss")))
      .withColumn("bf_dep_date", udfGetDayOfTheWeek(col("BFDepDate"), lit("yyyyMMdd")))
      .withColumn("bf_dur_stay", col("BFDurStay"))
      .withColumn("cs_user_browser", udfGetBrowserName(col("cs_user")))
      .withColumn("cs_user_os", udfGetOSName(col("cs_user")))
      .withColumn("ed_refdom", udfGetReferrerCat(col("ed_refdom")))
      .withColumn("language", udfGetLanguage(col("Language")))
      .withColumn("bft", udfGetLanguage(col("BFT")))
      .select(LoadedProperties.leisureModelColumns.map(col(_)): _*)
  }
}
