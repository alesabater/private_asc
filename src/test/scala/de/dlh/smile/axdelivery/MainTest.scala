package de.dlh.smile.axdelivery


import org.mockito.Mockito._
import org.scalatest.{FlatSpec, Matchers}

class MainTest extends FlatSpec with Matchers {
  
  "execute" should "run the complete program" in {
    val ioMock:IO = mock(classOf[IO])
    when(ioMock.read) thenReturn Stub.dfInput
    when(ioMock.readAirportCodes) thenReturn Stub.dfAirportCodes
    val df = Main.execute(ioMock.read, ioMock)

    df.printSchema()
    df.show(100)
  }
}
