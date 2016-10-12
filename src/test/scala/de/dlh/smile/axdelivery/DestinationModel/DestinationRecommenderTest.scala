package de.dlh.smile.axdelivery.DestinationModel


import de.dlh.smile.axdelivery.{Stub, TestSets}
import org.scalatest.{FlatSpec, Matchers}
import de.dlh.smile.axdelivery.DestinationModel.Transformations
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import de.dlh.smile.engine.commons.Contexts

class DestinationRecommenderTest extends FlatSpec with Matchers {
  "MovingAverage" should "compute the number of searches and compute the moving average" in {
      val df = Stub.dfWebtendsAfterFormat
      val dfResult = DestinationRecommender.getMovingAverage(df)        
      dfResult.show
  }
      
  "DestinationRecommender" should "recommend 16 destinations per origin" in {
      val df = Contexts.sqlCtx.read.parquet(getClass.getResource("/data/webtrends").getPath)
      val dfAirportMap = Contexts.sqlCtx.read.json(getClass.getResource("/data/airport_codes/airporttocity.json").getPath)
      val dfResult = DestinationRecommender.getRecommendedDestinations(
          DestinationRecommender.getMovingAverage(
              Transformations.formatAndRegisterDataFrame(df, dfAirportMap)
              )
          )       
      dfResult.show
  }
}