package de.dlh.smile.axdelivery.DestinationModel

import de.dlh.smile.axdelivery.Stub
import org.apache.spark.sql.functions._
import org.scalatest.{FlatSpec, Matchers}
import org.apache.spark.sql.Row
import de.dlh.smile.engine.commons.Contexts
//import de.dlh.smile.axdelivery.DestinationModel.UDAFGroupConcat
import scala.collection.mutable.ArrayBuffer

class FormatOutputTest extends FlatSpec with Matchers {
  "formatOutput" should "format the output and place it in the expected output format" in {
    val df = Stub.dfResultRecommendation
    val dfResult = FormatOutput.formatOutput(df)

    dfResult.show
  }
}