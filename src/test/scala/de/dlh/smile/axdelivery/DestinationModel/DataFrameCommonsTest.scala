package de.dlh.smile.axdelivery.DestinationModel

import de.dlh.smile.axdelivery.Stub
import org.scalatest.{FlatSpec, Matchers}
import de.dlh.smile.axdelivery.DestinationModel.DataFrameCommons._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import de.dlh.smile.engine.commons.Contexts

class DataFrameCommonsTest extends FlatSpec with Matchers {

	"getBFTUDEPField" should "create a column with the BFTUDEP column" in {
		val df = Stub.dfMapDate
				val dfResult = df.getBFTUDEPField("date_prev","date_dep","k1")
				//dfResult.show
				//dfResult.printSchema()
				dfResult.filter(col("BFTUDEP").isNotNull).count should equal(4)
				dfResult.select("BFTUDEP").take(4) should equal(Array(Row(12),Row(11),Row(-4),Row(620)))
	}

  "flatMapType" should "flat desired keys with their values out of a Map Column" in {
    val df = Stub.dfMapDate
    val dfResult = df.flatMapType("date_dep", List("k1", "k.2"))
    dfResult.show
    dfResult.columns should equal (Array("date_prev", "k1", "k2"))
    dfResult.select(col("k1")).take(7).filter( row => row.getString(0) != null).length should equal(5)
    dfResult.select(col("k1")).take(7).filter( row => row.getString(0) == null).length should equal(2)
  }

	"filterByDate" should "filter dataframe based on dates" in {
		val df = Stub.dfFilterDate
				val dfResult = df.filterPartitionFieldsOneYearFrom(2016, 9)
				dfResult.count() should equal(2)
	}

	"filterFromMap" should "filter dataframe based on a Map column key and value" in {
		val df = Contexts.sqlCtx.read.parquet(getClass.getResource("/data").getPath)
		    df.count()
				df.filterValueMapEquals("cs_uri_query", "Screen", "FOFP")
				df.count()
				df.show
	}
	
	"airportToCityCode" should "overwrite the airport codes with city codes" in {
	  val df = Stub.dfWebtendsAfterFormat
	  val dfAirportMap = Contexts.sqlCtx.read.json(getClass.getResource("/data/airport_codes/airporttocity.json").getPath)
	  val dfResult = df.airportToCityCode(dfAirportMap, "BFO")
	  dfResult.show()
	}

	"filterRT" should "filter all the rows with BFTripType different than RT" in {
		val df = Stub.dfWebtendsAfterFormat
		val dfResult = df.filterRT()
		dfResult.count() should equal(3)
	}

	"filterOrigin" should "filter BFO with a defined set of originCities" in {
		val df = Stub.dfStringOrigin
		val dfResult = df.filterOrigin()
		dfResult.collect() should equal(Array(Row("MAD"),Row("BCN")))
	}
}
