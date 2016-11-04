package de.dlh.smile.axdelivery.commons

import org.scalatest.{FlatSpec, Matchers}

class LoadedPropertiesTest extends FlatSpec with Matchers {

  "createMap" should "load maps from configuration file" in {
    val props = LoadedProperties.bfTripTypeScores
    props shouldBe a [Map[String, Double]]
    props.isEmpty should equal(false)
    println(props)
  }
}
