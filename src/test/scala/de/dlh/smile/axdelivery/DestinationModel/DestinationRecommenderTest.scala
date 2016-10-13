package de.dlh.smile.axdelivery.DestinationModel


import de.dlh.smile.axdelivery.{Stub, TestSets}
import org.scalatest.{FlatSpec, Matchers}
import de.dlh.smile.axdelivery.DestinationModel.Transformations
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import de.dlh.smile.engine.commons.Contexts

class DestinationRecommenderTest extends FlatSpec with Matchers {
  
  "Scaling" should "scale the frequencies based on last year" in {
    val df = Stub.dfWebtendsAfterFormat
    val dfTmp = df
      .select("BFO", "BFD", "year", "month", "session_guid")
      .groupBy("BFO", "BFD", "year", "month")
      .agg(countDistinct("session_guid").alias("freq"))
    //val year = df.select(col("year"), col("month")).distinct().sort(col("year"), col("month"))
    val dfResult = DestinationRecommender.scaleWithPreviousYear(dfTmp)
    //dfTmp.show()
    dfResult.show()
    dfResult.printSchema()
    // Yes yes!! Now it is time to put it inside the moving average, just after the aggregation
  }
//  
//  "MovingAverage" should "compute the number of searches and compute the moving average" in {
//      val df = Stub.dfWebtendsAfterFormat
//      val dfResult = DestinationRecommender.getMovingAverage(df)        
//      dfResult.show
//  }
//      
//  "DestinationRecommender" should "recommend 16 destinations per origin" in {
//      val df = Contexts.sqlCtx.read.parquet(getClass.getResource("/data/webtrends").getPath)
//      val dfAirportMap = Contexts.sqlCtx.read.json(getClass.getResource("/data/airport_codes/airporttocity.json").getPath)
//      val dfResult = DestinationRecommender.getRecommendedDestinations(
//          DestinationRecommender.getMovingAverage(
//              Transformations.formatAndRegisterDataFrame(df, dfAirportMap)
//              )
//          )       
//      dfResult.show
//  }
}