package de.dlh.smile.axdelivery

import de.dlh.smile.engine.commons.{Contexts, LoadedProperties}
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}

object Main {

  def main(io: IO) {
    execute(io.read())
  }

  def execute(df: DataFrame) = ???
}

class IO {

  val conf = LoadedProperties.conf
  val inputPath = conf.getString("weblogs.path")
  def read(): DataFrame = Contexts.sqlCtx.read.parquet(inputPath)
}