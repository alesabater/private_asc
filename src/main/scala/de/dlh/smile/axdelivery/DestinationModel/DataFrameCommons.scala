package de.dlh.smile.axdelivery.DestinationModel

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.joda.time.DateTime
import de.dlh.smile.axdelivery.DestinationModel.ColumnCommons._
import org.apache.spark.sql.types.StringType

import scala.util.{Failure, Success, Try}

object DataFrameCommons {
	// DataFrame Conversions
	implicit def dataFrame2dataFrameUpdatable(df: DataFrame): DataFrameUpdatable = DataFrameUpdatable(df)

			implicit def dataFrameUpdatable2dataFrame(up: DataFrameUpdatable): DataFrame = up.df
}

case class DataFrameUpdatable(df: DataFrame) {

	import DataFrameCommons._

	def filterValueMapEquals(column: String, key: String, value: String): DataFrameUpdatable = {
			df.filter(col(column).getItem(key) === value)
	}

	def filterPartitionFieldsOneYearFrom(year: Int = DateTime.now.getYear, month: Int = DateTime.now.getMonthOfYear): DataFrameUpdatable = {
			df.filter(
					(col("year") === year and col("month") <= month) or
					(col("year") === (year - 1) and col("month") > month)
					)
	}

	def getBFTUDEPField(columnDate: String, columnMap: String, key: String, inFormat: String = "yyyyMMdd", outFormat: String = "yyyy-MM-dd"): DataFrameUpdatable = {
			df.withColumn("dateTmp", udfCreateDateFrom(col(columnMap).getItem(key), lit(inFormat), lit(outFormat)))
			.withColumn("BFTuDep", datediff(to_date(col("dateTmp")), to_date(col(columnDate))))
	}

	def flatMapType(columnMap: String, keys: List[String]): DataFrame = {
			val udf1 = udf[Option[String], Map[String, String], String]((map: Map[String, String], key: String) => {
				val mapNoNulls = map.filter(_._2 != null)
						if (mapNoNulls.contains(key)) Try(mapNoNulls.get(key)) match {
						case Success (s) => s;
						case Failure (f) => None
						} else None
			})
			if (df.columns.contains(columnMap)) {
				keys.foldLeft(df) {
					(data, key) =>
					data.withColumn(key.replace(".", ""), udf1(df(columnMap), lit(key)))
				}.drop(columnMap)
			}
			else df
	}

	def airportToCityCode(dfAirportMap: DataFrame, colName: String): DataFrame = {
			df.join(dfAirportMap, df(colName) === dfAirportMap("Airport"), "left").drop("Airport").drop(colName).withColumnRenamed("City", colName)
	}
	
}