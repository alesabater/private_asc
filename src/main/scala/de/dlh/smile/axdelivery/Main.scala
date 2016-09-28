package de.dlh.smile.axdelivery

import de.dlh.smile.engine.commons
import de.dlh.smile.engine.commons.{Contexts, LoadedProperties}
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}

object Main {

  def main(io: IO) {
    execute(io.read(), io.readAirportCodes)
  }

  def execute(df: DataFrame, dfAirport: DataFrame) = ???
}

class IO {

  val conf = commons.LoadedProperties.conf
  val inputPath = conf.getString("weblogs.path")
  val airportCodesPath = conf.getString("airport_codes.path")
  def readAirportCodes: DataFrame = Contexts.sqlCtx.read.json(airportCodesPath)
  def read(): DataFrame = Contexts.sqlCtx.read.parquet(inputPath)
}