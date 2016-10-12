package de.dlh.smile.axdelivery.DestinationModel

import org.apache.spark.sql.functions._
import de.dlh.smile.axdelivery.DestinationModel.CustomMean._
import de.dlh.smile.axdelivery.DestinationModel.UDAFGroupConcat._
import org.scalatest.{FlatSpec, Matchers}
import de.dlh.smile.engine.commons.Contexts
import de.dlh.smile.axdelivery.{Stub, TestSets}
import org.scalatest.{FlatSpec, Matchers}
import org.apache.spark.sql.Row


class udafTest extends FlatSpec with Matchers {
	"GroupConcat" should "do the group_concat aggregation" in {
		val df = Stub.dfCustomerSales
		val dfResult = df.groupBy("Customer").pivot("Day", Seq("Mon", "Tue", "Wed", "Fri")).agg(UDAFGroupConcat(col("Month")).as("Months"))
		dfResult.show()
	}

	"customMean2" should "compute custom mean without pivot function" in {
	  // get distinct days from data (this assumes there are not too many of them):
	  val df = Stub.dfCustomerSales
    val days: Array[String] = df.select("Day")
      .distinct()
      .collect()
      .map(_.getAs[String]("Day"))
  
    // add column for each day with the Sale value if days match:
    val withDayColumns = days.foldLeft(df) { 
        case (data, day) => data.selectExpr("*", s"IF(Day = '$day', Sales, 0) AS $day")
    }

    // wrap it up 
    val result = withDayColumns
       .drop("Day")
       .drop("Sales")
       .groupBy("Customer")
       .agg(CustomMean(days.map(col(_)): _*)) // I believe the function would need to be changed in order to take an array
           //.select(LoadedProperties.webtrendsColumns.map(col(_)): _*)
    result.show()
	}
}