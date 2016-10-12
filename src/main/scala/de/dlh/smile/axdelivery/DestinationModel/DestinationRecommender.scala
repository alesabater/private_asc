package de.dlh.smile.axdelivery.DestinationModel

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import de.dlh.smile.axdelivery.DestinationModel.DataFrameCommons._

object DestinationRecommender {
    def getMovingAverage(df: DataFrame): DataFrame = {
    // Compute number of searches per OnD, year and month
    val dfTmp = df
      .select("BFO", "BFD", "year", "month", "session_guid")
      .groupBy("BFO", "BFD", "year", "month")
      .agg(countDistinct("session_guid").alias("freq"))
    
    // The months of Jan and Dec do not have enough data points to compute the moving average
      // for that reason we add month 0 (same as Dec) and month 13 (same as Jan) for the computations
    val dfTmpEnlarged = dfTmp.unionAll(
        dfTmp.filter(col("month") === 12).select(col("BFO"), col("BFD"), col("year"), lit(0).alias("month"), col("freq")))
        .unionAll(dfTmp.filter(col("month") === 1).select(col("BFO"), col("BFD"), col("year"), lit(13).alias("month"), col("freq")))
        
      // Compute the moving average 1/4, 1/2, 1/4
    val windowSpec = Window.partitionBy(col("BFO"), col("BFD")).orderBy(col("month")).rangeBetween(-1, 1)
    val moving_average = sum(col("freq")).over(windowSpec)
    val dfResult = dfTmpEnlarged.select(
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
           .filter((col("month") >= 1) and col("month") <= 12) // remove the fake months that we added
   dfResult
  }
    
  def getRecommendedDestinations(df: DataFrame) : DataFrame = {
    // this should be done before the smoothing
    // Before doing the ranking, we scale the searches based on last year same month
//    val yearToday = df.select(col("year"), col("month")).distinct().sort(col("year"), col("month")).select(col("year")).first().getInt(0)
//		val monthToday = df.select(col("year"), col("month")).distinct().sort(col("year"), col("month")).select(col("month")).first().getInt(0)
//    val currentMonth = df.filter((col("year") === yearToday) and (col("month") === monthToday))
//    val lastYearMonth = df.filter((col("year") === yearToday - 1) and (col("month") === monthToday))
//    val yearFactor = currentMonth.join(lastYearMonth,
//        (currentMonth("BFO") === lastYearMonth("BFO")) and (currentMonth("BFD") === lastYearMonth("BFD")),
//        "inner")
//        .select(col("currentMonth.BFO"),
//            col("currentMonth.BFD"),
//            col("currentMonth.freq")/col("lastYearMonth.freq").alias("factor"),
//            col("lastYearMonth.year"))
//    
//    val dfScaled = df
//      .filter((col("year") !== yearToday-1) or (col("month") !== monthToday))
//      .join(yearFactor,
//        (df("year") === yearFactor("year")) and (df("BFO") === yearFactor("BFO")) and (df("BFD") === yearFactor("BFD")),
//        "left").select(col("BFO"), 
//            col("BFD"), 
//            col("year"), 
//            col("month"), 
//            col("freq"), 
//            (col("freq") * col("factor")).alias("freq_scaled"))
        
    // Compute the rank and keep up to rank 50
    val byOrigin = Window
      .partitionBy(col("BFO"), col("year"), col("month"))
      .orderBy(-col("freq"))
    val dfRankedByO = df
      .withColumn("rank", rank over byOrigin)
      .filter("rank <= 50")

    // Compute month rank by origin and destination pair (need the full year for this)
    val byOnD = Window
      .partitionBy(col("BFO"), col("BFD"))
      .orderBy(-col("freq"))
    val dfRankedByOnD = dfRankedByO
      .withColumn("monthrank", rank over byOnD)

    // It is convenient to keep here the only month that we are interested in order to reduce computation time
    val yearPred = df.select(col("year"), col("month")).distinct().sort(col("year"), col("month")).select(col("year")).first().getInt(0)
		val monthPred = df.select(col("year"), col("month")).distinct().sort(col("year"), col("month")).select(col("month")).first().getInt(0)
    val dfRankedPredictionMonth = dfRankedByOnD.filterPartitionFieldsOneMonth(yearPred, monthPred)
    
    // Compute the model rank as a combination of the previous two & filter keeping only the top 16
    val dfResultScoring = dfRankedPredictionMonth.select(col("BFO"), col("BFD"), col("year"), col("month"), (col("rank") + col("monthrank") * col("monthrank")).alias("mdlrank"))
    val byOriginMdlRank = Window
      .partitionBy(col("BFO"), col("year"), col("month"))
      .orderBy(col("mdlrank"))
    val dfResult = dfResultScoring
      .withColumn("mdlrank", rank over byOriginMdlRank)
      .filter("mdlrank <= 16")
      .select(col("BFO"), col("BFD"), col("year"), col("month"), (col("mdlrank") - 1).alias("mdlrank"))
    
    dfResult
  }
}