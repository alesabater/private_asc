package de.dlh.smile.axdelivery


import org.apache.spark.sql.DataFrame
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

    verify(ioMock, atLeastOnce()).writeSearchModelResult(dfArg.capture)


  }
}
