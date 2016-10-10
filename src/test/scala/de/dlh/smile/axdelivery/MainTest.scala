package de.dlh.smile.axdelivery

import de.dlh.smile.axdelivery.DestinationModel.{Transformations, MovingAverage, DestinationRecommender}
import de.dlh.smile.engine.commons.Contexts
import org.scalatest.{FlatSpec, Matchers}

class MainTest extends FlatSpec with Matchers {
  
  "execute" should "run the complete program" in {
    val df = Contexts.sqlCtx.read.parquet(getClass.getResource("/data/webtrends").getPath)
    val dfAirportMap = Contexts.sqlCtx.read.json(getClass.getResource("/data/airport_codes/airporttocity.json").getPath)
    val dfResult1 = Transformations.formatAndRegisterDataFrame(df, dfAirportMap)
    val dfResult2 = Transformations.filterOrigin(dfResult1)
    val dfResult3 = Transformations.scoreTravelReason(dfResult2)
    val dfResult4 = Transformations.filterLeisure(dfResult3)
    val dfResult5 = MovingAverage.getMovingAverage(dfResult4)
    val dfResult6 = DestinationRecommender.getRecommendedDestinations(dfResult5)
    dfResult5.show()
    dfResult5.printSchema()
    dfResult6.show()
  }
}
