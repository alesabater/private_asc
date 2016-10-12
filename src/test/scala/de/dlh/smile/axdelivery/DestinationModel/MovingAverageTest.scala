package de.dlh.smile.axdelivery.DestinationModel

import de.dlh.smile.axdelivery.{Stub, TestSets}
import org.scalatest.{FlatSpec, Matchers}
import de.dlh.smile.axdelivery.DestinationModel.MovingAverage
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row

class MovingAverageTest extends FlatSpec with Matchers {
    "MovingAverage" should "compute the number of searches and compute the moving average" in {
      val df = Stub.dfWebtendsAfterFormat
      val dfResult = MovingAverage.getMovingAverage(df)        
      dfResult.show
  }
}