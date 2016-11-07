package de.dlh.smile.axdelivery

import de.dlh.smile.engine.commons.Contexts
import de.dlh.smile.engine.commons.{LoadedProperties => EngineLoadedProperties}
import org.apache.spark.sql.DataFrame
import org.mockito.Mockito._
import org.scalatest.{FlatSpec, Matchers}

class IOTest extends FlatSpec with Matchers {

  "get Properties" should "read properties" in {
    val inputPath = EngineLoadedProperties.conf.getString("weblogs.inputPath")
    val outputPath = EngineLoadedProperties.conf.getString("weblogs.outputPath")
    inputPath shouldBe a[String]
    inputPath shouldNot equal(null)
    outputPath shouldBe a[String]
    outputPath shouldNot equal(null)
  }

  "read json file" should "read airport codes from a JSON" in {
    val json = Contexts.sqlCtx.read.json(getClass.getResource("/data/airporttocity.json").getPath)
    json shouldBe a[DataFrame]
  }

}
