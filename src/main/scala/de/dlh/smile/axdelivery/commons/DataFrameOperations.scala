package de.dlh.smile.axdelivery.commons

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.joda.time.DateTime
import de.dlh.smile.axdelivery.commons.DataFrameColumnsOperations._
import org.apache.spark.Logging

import scala.util.{Failure, Success, Try}

object DataFrameOperations {
  // DataFrame Conversions
  implicit def dataFrame2dataFrameUpdatable(df: DataFrame): DataFrameUpdatable = DataFrameUpdatable(df)

  implicit def dataFrameUpdatable2dataFrame(up: DataFrameUpdatable): DataFrame = up.df
}

case class DataFrameUpdatable(df: DataFrame) extends Logging{

  import DataFrameOperations._

  def filterValueMapEquals(column: String, key: String, value: String): DataFrameUpdatable = {
    Try(df.filter(col(column).getItem(key) === value)) match {
      case Success(s) => s
      case Failure(f) => {log.warn("Map \"cs_uri_query\" is not a columns, can not filter out of a non existing column")
      df}
    }
  }

  def filterPartitionFieldsOneYearFrom(year: Int = DateTime.now.getYear, month: Int = DateTime.now.getMonthOfYear): DataFrameUpdatable = {
    df.filter(
      (col("year") === year and col("month") <= month) or
        (col("year") === (year - 1) and col("month") >= month) // modified to read one year and one month
    )
  }

  def getBFTUDEPField(columnDate: String, columnMap: String, key: String, inFormat: String = "yyyyMMdd"): DataFrameUpdatable = {
    df.withColumn("dateTmp", udfGetStringDateFormatted(col(columnMap).getItem(key), lit(inFormat)))
      .withColumn("BFTuDep", datediff(to_date(col("dateTmp")), to_date(col(columnDate))))
  }

  def flatMapType(columnMap: String, keys: List[String]): DataFrame = {
    val udf1 = udf[Option[String], Map[String, String], String]((map: Map[String, String], key: String) => {
      val mapNoNulls = map.filter(_._2 != null)
      if (mapNoNulls.contains(key)) Try(mapNoNulls.get(key)) match {
        case Success(s) => s;
        case Failure(f) => None
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
    df.join(dfAirportMap, df(colName) === dfAirportMap("Airport"), "left")
      .withColumn("tmp", coalesce(col("City"),col(colName)))
      .drop(colName)
      .drop("Airport")
      .drop("City")
      .withColumnRenamed("tmp", colName)
  }

  def filterOrigin(): DataFrame = {
    df.filter(col("BFO").isin(LoadedProperties.originCities: _*))
  }

  def filterRT(): DataFrame = {
    df.filter((col("BFTripType") === "RT") and
      (col("BFO") !== "null") and
      (col("BFD") !== "null"))
  }


  def filterPartitionFieldsOneMonth(year: Int = DateTime.now.getYear, month: Int = DateTime.now.getMonthOfYear): DataFrameUpdatable = {
    df.filter(col("year") === year and col("month") === month)
  }

  def groupByDistinctColumn(distinctCol: String, groupByCols: String*): DataFrameUpdatable = {
    df.groupBy(groupByCols.map(col(_)): _*)
      .agg(countDistinct(distinctCol).alias("freq"))
  }
}