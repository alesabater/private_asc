package de.dlh.smile.axdelivery

import de.dlh.smile.engine.commons.{Contexts, LoadedProperties}
import org.mockito.Mockito._
import org.scalatest.{FlatSpec, Matchers}

class IOTest extends FlatSpec with Matchers {

  "execute" should  "run the whole program" in {
    ???
  }

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
    val path = LoadedProperties.conf.getString("weblogs.path")
    path shouldBe a [String]
    path shouldNot equal(null)
  }

}
