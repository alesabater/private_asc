package de.dlh.smile.axdelivery.DestinationModel

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.joda.time.DateTime
import de.dlh.smile.axdelivery.DestinationModel.ColumnCommons._

object DataFrameCommons {
  // DataFrame Conversions
  implicit def dataFrame2upgradable(df: DataFrame): Updatable = Updatable(df)
  implicit def upgradable2dataFrame(up: Updatable): DataFrame = up.df
}

case class Updatable(df: DataFrame) {

  import DataFrameCommons._

  def filterValueMapEquals(column: String, key: String, value: String): Updatable = {
    df.filter(col(column).getItem(key) === value)
  }

  def filterPartitionFieldsOneYearFrom(year: Int = DateTime.now.getYear, month: Int = DateTime.now.getMonthOfYear): Updatable = {
    df.filter(
      (col("year") === year and col("month") <= month) or
        (col("year") === (year - 1) and col("month") > month)
    )
  }

  def getBFTUDEPField(columnDate: String, columnMap: String, key: String, format: String = "yyyyMMdd"): Updatable = {
    df.withColumn("dateTmp", dateFrom(col(columnMap).getItem(key) ,lit(format)))
      .withColumn("BFTUDEP", datediff(to_date(col("dateTmp")),to_date(col(columnDate))))
  }

}