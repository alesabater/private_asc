package de.dlh.smile.axdelivery.DestinationModel

import de.dlh.smile.axdelivery.Stub
import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, Matchers}
//import de.dlh.smile.engine.commons.Contexts
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext

class TransformationsTest extends FlatSpec with Matchers {
  
  "formatAndRegister" should "format and register data frame" in {
    val df = Stub.sqlCtx.read.parquet(getClass.getResource("/data/webtrends").getPath)
    val dfAirportMap = Stub.sqlCtx.read.json(getClass.getResource("/data/airport_codes/airporttocity.json").getPath)
    val dfResult = Transformations.formatAndRegisterDataFrame(df, dfAirportMap)
    dfResult.show()
  }
  
  "filterRT" should "filter all the rows with BFTripType different than RT" in {
    val df = Stub.dfWebtendsAfterFormat
    val dfResult = Transformations.filterRT(df)
    dfResult.count() should equal(3)
  }

	"filterOrigin" should "filter BFO with a defined set of originCities" in {
		val df = Stub.dfStringOrigin
				val dfResult = Transformations.filterOrigin(df)
				dfResult.collect() should equal(Array(Row("MAD"),Row("BCN")))
	}

//	"scoreTravelReason" should "return the dataFrame with an extra column called scoreTRM" in {
//	  val df = Stub.dfWebtendsAfterFormat
//    val dfResult = Transformations.scoreTravelReason(df)
//    dfResult.show()
//    dfResult.printSchema()
//    // At this moment we have to believe that it is correct
//	}
}


