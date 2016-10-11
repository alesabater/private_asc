package de.dlh.smile.axdelivery.DestinationModel

import de.dlh.smile.axdelivery.IO
import org.apache.spark.sql.DataFrame

object LeisureModel {

  def cleanData(df: DataFrame, io: IO): DataFrame = {
    val dfAirportCodes = io.readAirportCodes
    val dfFormatted = Transformations.formatAndRegisterDataFrame(df, dfAirportCodes)
    dfFormatted
  }

}
