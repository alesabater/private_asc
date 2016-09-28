package de.dlh.smile.axdelivery.DestinationModel

import de.dlh.smile.axdelivery.Stub
import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, Matchers}

class TransformationsTest extends FlatSpec with Matchers {

	"filterOrigin" should "filter BFO with a defined set of originCities" in {
		val df = Stub.dfStringOrigin
				val dfResult = Transformations.filterOrigin(df)
				dfResult.collect() should equal(Array(Row("MAD"),Row("BCN")))
	}

}
