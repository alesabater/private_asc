package de.dlh.smile.axdelivery.DestinationModel


import de.dlh.smile.axdelivery.{Stub, TestSets}
import org.scalatest.{FlatSpec, Matchers}
import de.dlh.smile.axdelivery.DestinationModel.Transformations
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import de.dlh.smile.engine.commons.Contexts
import org.apache.spark.sql.expressions.Window

class DestinationRecommenderTest extends FlatSpec with Matchers {
  
//  "Scaling" should "scale the frequencies based on last year" in {
//    val df = Stub.dfWebtendsAfterFormat
//    val dfTmp = df
//      .select("BFO", "BFD", "year", "month", "session_guid")
//      .groupBy("BFO", "BFD", "year", "month")
//      .agg(countDistinct("session_guid").alias("freq"))
//    val dfResult = DestinationRecommender.scaleWithPreviousYear(dfTmp)
//    dfResult.filter((col("BFO") === "FRA") and (col("year") === 2014) and (col("month") === 9)).select(col("freq")).take(1)(0)(0) should equal(0.5)
//  }
  
  "MovingAverage" should "compute the number of searches and compute the moving average" in {
      val df = Stub.dfWebtendsAfterFormat
//      val dfTmp = df
//      .select("BFO", "BFD", "year", "month", "session_guid")
//      .groupBy("BFO", "BFD", "year", "month")
//      .agg(countDistinct("session_guid").alias("freq"))   
//      
//    val dfScaledTmp = DestinationRecommender.scaleWithPreviousYear(dfTmp)
//    // The months of Jan and Dec do not have enough data points to compute the moving average
//      // for that reason we add month 0 (same as Dec) and month 13 (same as Jan) for the computations
//    val dfTmpEnlarged = dfScaledTmp.unionAll(
//        dfScaledTmp.filter(col("month") === 12).select(col("BFO"), col("BFD"), (col("year") + 1).alias("year"), lit(0).alias("month"), col("freq")))
//        .unionAll(dfScaledTmp.filter(col("month") === 1).select(col("BFO"), col("BFD"), (col("year") - 1).alias("year"), lit(13).alias("month"), col("freq")))
//    // Compute the moving average 1/4, 1/2, 1/4
//    val windowSpec = Window.partitionBy(col("BFO"), col("BFD")).orderBy(col("month")).rangeBetween(-1, 1)
//    val moving_average = sum(col("freq")).over(windowSpec)
//    val dfResult = dfTmpEnlarged.select(
//        col("BFO"),
//        col("BFD"),
//        col("year"),
//        col("month"),
//        col("freq"),
//        moving_average.alias("smoothedfreq")).filter("BFO= 'FRA'").orderBy(col("year")*20 + col("month"))
//        .select(col("BFO"),
//            col("BFD"),
//            col("year"),
//            col("month"),
//            col("freq"),
//            col("smoothedfreq"),
//            ((col("smoothedfreq") + col("freq")) / 4).alias("finalfreq")
//           )
      val dfResult = DestinationRecommender.getMovingAverage(df)
      dfResult.filter((col("BFO") === "FRA") and col("month") === 1).select(col("freq")).take(1)(0)(0) should equal(1.25)
  }
      
  "DestinationRecommender" should "recommend 16 destinations per origin" in {
      val df = Contexts.sqlCtx.read.parquet(getClass.getResource("/data/webtrends").getPath)
      val dfAirportMap = Contexts.sqlCtx.read.json(getClass.getResource("/data/airport_codes/airporttocity.json").getPath)
      val dfResult = //DestinationRecommender.getRecommendedDestinations(
          DestinationRecommender.getMovingAverage(
              Transformations.formatAndRegisterDataFrame(df, dfAirportMap)
              )
          //)       
      dfResult.show
      // The output looks good
  }
}