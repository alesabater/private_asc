package de.dlh.smile.axdelivery.DestinationModel

import de.dlh.smile.axdelivery.{IO, LoadedProperties}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import de.dlh.smile.axdelivery.DestinationModel.DataFrameCommons._
import de.dlh.smile.axdelivery.DestinationModel.ColumnCommons._

object LeisureModel {

  def cleanData(df: DataFrame, io: IO): DataFrame = {
    val dfAirportCodes = io.readAirportCodes
    val dfFormatted = formatAndRegisterDataFrame(df, dfAirportCodes)
    dfFormatted
  }

  def formatAndRegisterDataFrame(df: DataFrame, dfAirportMap: DataFrame): DataFrame = {
    df.filterPartitionFieldsOneYearFrom()
      .filterValueMapEquals("cs_uri_query", "Screen", "FOFP")
      .getBFTUDEPField("date_dt", "cs_uri_query", "BFDepDate")
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
}
