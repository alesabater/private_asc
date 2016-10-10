package de.dlh.smile.axdelivery.DestinationModel

import de.dlh.smile.axdelivery.Stub
import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, Matchers}
import de.dlh.smile.engine.commons.Contexts

class TransformationsTest extends FlatSpec with Matchers {

	"filterOrigin" should "filter BFO with a defined set of originCities" in {
		val df = Stub.dfStringOrigin
				val dfResult = Transformations.filterOrigin(df)
				dfResult.collect() should equal(Array(Row("MAD"),Row("BCN")))
	}

	"scoreTravelReason" should "return the dataFrame with an extra column called scoreTRM" in {
	  val df = Contexts.sqlCtx.read.parquet(getClass.getResource("/data/webtrends").getPath)
    val dfAirportMap = Contexts.sqlCtx.read.json(getClass.getResource("/data/airport_codes/airporttocity.json").getPath)
    val dfTmp = Transformations.formatAndRegisterDataFrame(df, dfAirportMap)
    val dfResult = Transformations.scoreTravelReason(dfTmp)
    dfResult.show()
    dfResult.printSchema()
    // At this moment we have to believe that it is correct
	}
}


