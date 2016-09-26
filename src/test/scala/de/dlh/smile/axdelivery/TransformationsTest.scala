package de.dlh.smile.axdelivery

import de.dlh.smile.axdelivery.Model.Transformations
import de.dlh.smile.engine.commons.Contexts
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.joda.time.DateTime
import org.scalatest.{FlatSpec, Matchers}

class TransformationsTest extends FlatSpec with Matchers {

  "formatAndRenameDataFrame" should "format the dataframe as expected" in {
    val df = Contexts.sqlCtx.read.parquet(getClass.getResource("/data").getPath)
    val tableName = "weblogs"

    val dfResult = Transformations.formatAndRegisterDataFrame(df, tableName)
  }

  "filterByDate" should "filter dataframe based on dates" in {
    val df = Stub.dfFilterDate
    val dfResult = Transformations.filterDates(df, 2016, 9)
    dfResult.count() should equal(2)
  }

  it should "test" in {
    val date = DateTime.now.getMonthOfYear
    println(date)
  }

}
