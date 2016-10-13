package de.dlh.smile.axdelivery.DestinationModel

import de.dlh.smile.engine.commons.Contexts
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext


object MovingAverage {
  def getMovingAverage(df: DataFrame): DataFrame = {
    // Compute number of searches per OnD, year and month
    val dfTmp = df.select("BFO", "BFD", "year", "month", "session_guid")
      .groupBy("BFO", "BFD", "year", "month")
      .agg(countDistinct("session_guid").alias("freq"))
    // Compute the moving average 1/4, 1/2, 1/4
    val windowSpec = Window.partitionBy(col("BFO"), col("BFD")).orderBy(col("month")).rangeBetween(-1, 1)
    val moving_average = sum(col("freq")).over(windowSpec)
    val dfResult = dfTmp.select(
        col("BFO"),
        col("BFD"),
        col("year"),
        col("month"),
        col("freq"),
        moving_average.alias("smoothedfreq"))
        .select(col("BFO"),
            col("BFD"),
            col("year"),
            col("month"),
            ((col("smoothedfreq") + col("freq") /3) * 3/4).alias("freq")
           )
   dfResult
  }
}