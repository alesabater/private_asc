package de.dlh.smile.axdelivery


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.mockito.ArgumentCaptor
import org.mockito.Mockito._
import org.scalatest.{FlatSpec, Matchers}

class MainTest extends FlatSpec with Matchers {
  "execute" should "run the complete program" in {
    val dfArg = ArgumentCaptor.forClass(classOf[DataFrame])
    val ioMock:IO = mock(classOf[IO])

    when(ioMock.readWebtrendsData) thenReturn Stub.dfInput.sample(false,0.2)
    when(ioMock.readAirportCityMapping) thenReturn Stub.dfAirportCodes

    val df = Main.execute(ioMock)
    df.printSchema()
    df.show()


//    df.show()
//    println(df.count())

    verify(ioMock, atLeastOnce()).writeSearchModelResult(dfArg.capture)


  }

  "a" should "b" in {
    val df = Stub.dfInput.sample(false,0.2).select(col("cs_uri_query").getItem("BFO").as("1")).filter(col("1").isNotNull)
    df.show
  }
}
