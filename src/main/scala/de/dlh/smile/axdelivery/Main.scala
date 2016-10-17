package de.dlh.smile.axdelivery

import de.dlh.smile.axdelivery.commons.Transformations
import de.dlh.smile.axdelivery.models.LeisureFiltering
import de.dlh.smile.engine.commons.{LoadedProperties => EngineLoadedProperties}
import de.dlh.smile.engine.commons.Contexts
import org.apache.spark.sql.DataFrame

object Main {

  def main(args: Array[String]) {
    execute(new IO())
  }

  def execute(io: IO): DataFrame = {

    val dfWebtrends = io.readWebtrendsData
    val dfAirportCity = io.readAirportCityMapping
    val dfWebtrendsFormatted = Transformations.formatWebtrendsData(dfWebtrends, dfAirportCity)
    LeisureFiltering.filter(dfWebtrendsFormatted)

    /*
     *     val df = Contexts.sqlCtx.read.parquet(getClass.getResource("/data/webtrends").getPath)
    val dfAirportMap = Contexts.sqlCtx.read.json(getClass.getResource("/data/airport_codes/airporttocity.json").getPath)
    val dfResult1 = Transformations.filterRT(Transformations.formatAndRegisterDataFrame(df, dfAirportMap))
    val dfResult2 = Transformations.filterOrigin(dfResult1)
    //val dfResult3 = Transformations.scoreTravelReason(dfResult2)
    //val dfResult4 = Transformations.filterLeisure(dfResult3)
    val dfResult5 = MovingAverage.getMovingAverage(dfResult2)
    val dfResult6 = DestinationRecommender.getRecommendedDestinations(dfResult5)
    //val dfResult7 = dfResult6.groupBy("BFO").pivot("mdlrank", Seq(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)).agg(UDAFGroupConcat(col("BFD")))
    dfResult6.show()
    dfResult6.printSchema()
     */
  }
}

class IO {

  val conf = EngineLoadedProperties.conf
  val webtrendsInputPath = conf.getString("weblogs.path")
  val airportCityInputPath = conf.getString("airport_codes.path")
  def readAirportCityMapping: DataFrame = Contexts.sqlCtx.read.json(airportCityInputPath)
  def readWebtrendsData: DataFrame = Contexts.sqlCtx.read.parquet(webtrendsInputPath)
}