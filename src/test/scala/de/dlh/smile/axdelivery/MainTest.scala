package de.dlh.smile.axdelivery

import de.dlh.smile.engine.commons.Contexts
import org.scalatest.{FlatSpec, Matchers}

class MainTest extends FlatSpec with Matchers {

  "read airport codes" should "read airport codes from a JSON" in {
    val json = Contexts.sqlCtx.read.json(getClass.getResource("/data/airport_codes/airporttocity.json").getPath)
    json.show()
    json.printSchema()
  }
}
