package de.dlh.smile.axdelivery


import org.mockito.Mockito._
import org.scalatest.{FlatSpec, Matchers}

class MainTest extends FlatSpec with Matchers {
  
  "execute" should "run the complete program" in {
    val ioMock:IO = mock(classOf[IO])
    when(ioMock.readWebtrendsData) thenReturn Stub.dfInput
    when(ioMock.readAirportCityMapping) thenReturn Stub.dfAirportCodes
    val df = Main.execute(ioMock)
    df.show(100)
  }
}
