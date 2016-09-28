package de.dlh.smile.axdelivery.DestinationModel

import de.dlh.smile.axdelivery.DestinationModel.DataFrameCommons._
import de.dlh.smile.axdelivery.Stub
import de.dlh.smile.engine.commons.Contexts
import org.apache.spark.sql.functions._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.scalatest.{FlatSpec, Matchers}
import org.mockito.Mockito._
import de.dlh.smile.axdelivery.LoadedProperties

class TransformationsTest extends FlatSpec with Matchers {

	"filterOrigin" should "filter BFO with a defined set of originCities" in {
		val df = Stub.dfStringOrigin
				val dfResult = Transformations.filterOrigin(df)
				dfResult.show
	}

}
