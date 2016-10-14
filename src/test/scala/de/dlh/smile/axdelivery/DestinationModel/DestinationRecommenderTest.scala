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
//  
//  "MovingAverage" should "compute the number of searches and compute the moving average" in {
//      val df = Stub.dfWebtendsAfterFormat
//      val dfResult = DestinationRecommender.getMovingAverage(df)
//      dfResult.filter((col("BFO") === "FRA") and col("month") === 1).select(col("freq")).take(1)(0)(0) should equal(1.25)
//  }
      
  "DestinationRecommender" should "recommend 16 destinations per origin" in {
//      val df = Contexts.sqlCtx.read.parquet(getClass.getResource("/data/webtrends").getPath)
//      val dfAirportMap = Contexts.sqlCtx.read.json(getClass.getResource("/data/airport_codes/airporttocity.json").getPath)
//      val dfResult = //DestinationRecommender.getRecommendedDestinations(
//          DestinationRecommender.getMovingAverage(
//              Transformations.formatAndRegisterDataFrame(df, dfAirportMap)
//              )
//          //)       
////      dfResult.show
//      
//      dfResult.write.parquet("D:/Users/U553574/Documents/destination_modelV2/src/test/resources/data/outputma.parquet")
//     // val df = Contexts.sqlCtx.read.parquet("D:/Users/U553574/Documents/destination_modelV2/src/test/resources/data/outputma.parquet")
      ////dfResult.filter(col("BFO") === "JKT" and col("BFD") === "MIL").show()
      // The output looks good
//    val df = Contexts.sqlCtx.read.parquet("D:/Users/U553574/Documents/destination_modelV2/src/test/resources/data/outputma.parquet")
//    val dfResult = FormatOutput.formatOutput(DestinationRecommender.getRecommendedDestinations(df))
//    dfResult.coalesce(1).write.parquet("D:/Users/U553574/Documents/destination_modelV2/src/test/resources/data/outputrecommendations.parquet")
    val df = Contexts.sqlCtx.read.parquet("D:/Users/U553574/Documents/destination_modelV2/src/test/resources/data/outputrecommendations.parquet")
    df.collect().foreach {println} //should equal(1000)
  }
}