package de.dlh.smile.axdelivery

import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Matchers}

class HarnessTest extends FlatSpec with Matchers {
  "execute" should "be called by Main class" in {
    val df: DataFrame = Harness.execute()
    df.toJSON.collect().mkString should equal("{\"age\":19,\"name\":\"Jane\"}{\"age\":32,\"name\":\"Joe\"}")
  }
}
