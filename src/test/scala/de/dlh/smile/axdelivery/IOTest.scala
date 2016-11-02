package de.dlh.smile.axdelivery

import de.dlh.smile.engine.commons.Contexts
import de.dlh.smile.engine.commons.{LoadedProperties => EngineLoadedProperties}
import org.mockito.Mockito._
import org.scalatest.{FlatSpec, Matchers}

class IOTest extends FlatSpec with Matchers {

  "read" should "read a parquet file" in {
    val ioMock = mock(classOf[IO])
    //when(ioMock.inputPath).thenReturn(getClass.getResource("/data").getPath)
    val df = Contexts.sqlCtx.read.parquet(getClass.getResource("/data").getPath)
    //val df = ioMock.read()

    df.show()
    df.printSchema()

    df.columns.contains("year") should equal(true)
    df.columns.contains("month") should equal(true)
    df.columns.contains("day") should equal(true)
  }

  "get Properties" should "read properties" in {
    val inputPath = EngineLoadedProperties.conf.getString("weblogs.inputPath")
    val outputPath = EngineLoadedProperties.conf.getString("weblogs.outputPath")
    inputPath shouldBe a[String]
    inputPath shouldNot equal(null)
    outputPath shouldBe a[String]
    outputPath shouldNot equal(null)
  }

  "read airport codes" should "read airport codes from a JSON" in {
    val json = Contexts.sqlCtx.read.json(getClass.getResource("/data/airport_codes/airporttocity.json").getPath)
    json.show()
    json.printSchema()
  }

}
