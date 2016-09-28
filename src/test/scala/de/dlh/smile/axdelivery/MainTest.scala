package de.dlh.smile.axdelivery

import de.dlh.smile.axdelivery.DestinationModel.Transformations
import de.dlh.smile.engine.commons.Contexts
import org.scalatest.{FlatSpec, Matchers}

class MainTest extends FlatSpec with Matchers {
  
  "execute" should "run the complete program" in {
    val df = Contexts.sqlCtx.read.parquet(getClass.getResource("/data/webtrends").getPath)
    val dfAirportMap = Contexts.sqlCtx.read.json(getClass.getResource("/data/airport_codes/airporttocity.json").getPath)
    val dfResult = Transformations.formatAndRegisterDataFrame(df, dfAirportMap)
    dfResult.show()
    dfResult.printSchema()
  }
}
