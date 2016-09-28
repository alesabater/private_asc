package de.dlh.smile.axdelivery.DestinationModel

import de.dlh.smile.engine.commons.Contexts
import org.apache.spark.sql.DataFrame
import de.dlh.smile.axdelivery.DestinationModel.DataFrameCommons._
import de.dlh.smile.axdelivery.DestinationModel.ColumnCommons._
import de.dlh.smile.axdelivery.LoadedProperties
import org.apache.spark.sql.functions._
import de.dlh.smile.engine.commons._

object Transformations {

  def formatAndRegisterDataFrame(df: DataFrame, dfAirportMap: DataFrame): DataFrame = {
    df.filterPartitionFieldsOneYearFrom()
      .filterValueMapEquals("cs_uri_query", "Screen", "FOFP")
      .getBFTUDEPField("date", "cs_uri_query", "BFDepDate")
      .flatMapType("cs_uri_query", LoadedProperties.fromMapColumns)
      .select(LoadedProperties.webtrendsColumns.map(col(_)): _*)
      .withColumn("BFO", getFirstIATA(col("BFO")))
      .withColumn("BFD", getFirstIATA(col("BFD")))
      .airportToCityCode(dfAirportMap, "BFO")
      .airportToCityCode(dfAirportMap, "BFD")
  }
  
  
  def filterOrigin(df: DataFrame): DataFrame = {
    df.filter(col("BFO").isin(LoadedProperties.originCities))
  }
  
}
