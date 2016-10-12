package de.dlh.smile.axdelivery

import de.dlh.smile.axdelivery.DestinationModel.LeisureModel
import de.dlh.smile.engine.commons
import de.dlh.smile.engine.commons.{Contexts, LoadedProperties}
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}

object Main {

  def main(io: IO) {
    execute(io.read, io)
  }

  def execute(df: DataFrame, io: IO): DataFrame = {
    LeisureModel.cleanData(df,io)
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

  val conf = commons.LoadedProperties.conf
  val inputPath = conf.getString("weblogs.path")
  val airportCodesPath = conf.getString("airport_codes.path")
  def readAirportCodes: DataFrame = Contexts.sqlCtx.read.json(airportCodesPath)
  def read: DataFrame = Contexts.sqlCtx.read.parquet(inputPath)
}