package de.dlh.smile.axdelivery

import de.dlh.smile.axdelivery.commons.Transformations
import de.dlh.smile.axdelivery.models.{DestinationRecommender, LeisureFiltering}
import de.dlh.smile.engine.commons.{LoadedProperties => EngineLoadedProperties}
import de.dlh.smile.engine.commons.Contexts
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

object Main {

  def main(args: Array[String]) {
    execute(new IO())
  }

  def execute(io: IO): DataFrame = {
    val dfWebtrends = io.readWebtrendsData
    val dfAirportCity = io.readAirportCityMapping
    val dfWebtrendsFormatted = Transformations.formatWebtrendsData(dfWebtrends, dfAirportCity)
    val dfFilteredByLeisure = LeisureFiltering.filter(dfWebtrendsFormatted)
    dfFilteredByLeisure
//    val dfResult =
//      if (dfFilteredByLeisure.count() > 0) DestinationRecommender.recommend(dfFilteredByLeisure)
//      else Contexts.sqlCtx.emptyDataFrame
//    io.writeSearchModelResult(dfResult)
  }
}

class IO {

  val hiveCtx = new HiveContext(Contexts.sc)

  val conf = EngineLoadedProperties.conf
  val webtrendsInputPath = conf.getString("weblogs.inputPath")

  def readAirportCityMapping: DataFrame = Contexts.sqlCtx.read.json(getClass.getResource("data/airporttocity.json").getPath)

  def readWebtrendsData: DataFrame = Contexts.sqlCtx.read.parquet(webtrendsInputPath)

  def writeSearchModelResult(df: DataFrame): Unit = df.write.parquet(conf.getString("weblogs.outputPath"))
}