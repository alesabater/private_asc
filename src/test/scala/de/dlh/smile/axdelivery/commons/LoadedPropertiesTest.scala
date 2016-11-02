package de.dlh.smile.axdelivery.commons

import org.scalatest.{FlatSpec, Matchers}

class LoadedPropertiesTest extends FlatSpec with Matchers {

  "createMap" should "load maps from configuration file" in {
    val props = LoadedProperties.bfTripTypeScores
    props.toString() should equal("Map(RT -> -0.96437, OW -> 1.185844)")
    //println(props)
  }
}
