package de.dlh.smile.axdelivery.DestinationModel

import de.dlh.smile.engine.commons.Contexts
import org.apache.spark.sql.DataFrame
import de.dlh.smile.axdelivery.DestinationModel.DataFrameCommons._
import de.dlh.smile.axdelivery.LoadedProperties
import org.apache.spark.sql.functions._
import de.dlh.smile.engine.commons._

object Transformations {

  def formatAndRegisterDataFrame(df: DataFrame): DataFrame = {
    df.filterPartitionFieldsOneYearFrom()
      .filterValueMapEquals("cs_uri_query", "Screen", "FOFP")
      .getBFTUDEPField("date", "cs_uri_query", "BFDepDate")
      .flatMapType("cs_uri_query", LoadedProperties.fromMapColumns)
      .select(LoadedProperties.webtrendsColumns.map(col(_)): _*)
  }


}
