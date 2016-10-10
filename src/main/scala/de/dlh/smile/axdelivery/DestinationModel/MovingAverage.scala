package de.dlh.smile.axdelivery.DestinationModel

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

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
  //// Compute number of searches per OnD, year and month
//
//
//
//// Compute the moving average 1/4, 1/2, 1/4
//var windowSpec = Window.partitionBy('BFO, 'BFD).orderBy('month).rangeBetween(-1, 1)
//var moving_average = sum('freq).over(windowSpec)
//val data =
//aggregated.select(
//    'BFO,
//    'BFD,
//    'year,
//    'month,
//    'freq,
//    moving_average.alias("smoothedfreq")).select(
//    'BFO,
//    'BFD,
//    'year,
//    'month,
//    (('smoothedfreq + 'freq /3) * 3/4).alias("freq")
//)
}