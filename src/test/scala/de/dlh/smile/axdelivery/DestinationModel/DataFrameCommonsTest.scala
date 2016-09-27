package de.dlh.smile.axdelivery.DestinationModel

import de.dlh.smile.axdelivery.Stub
import org.scalatest.{FlatSpec, Matchers}
import de.dlh.smile.axdelivery.DestinationModel.DataFrameCommons._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

class DataFrameCommonsTest extends FlatSpec with Matchers {

  "getBFTUDEPField" should "create a column with the BFTUDE column" in {
    val df = Stub.dfMapDate
    val dfResult = df.getBFTUDEPField("date_prev","date_dep","k1")
    dfResult.show
    dfResult.printSchema()
    dfResult.filter(col("BFTUDEP").isNotNull).count should equal(4)
    dfResult.select("BFTUDEP").take(4) should equal(Array(Row(12),Row(11),Row(-4),Row(620)))
  }

}
