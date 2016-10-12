package de.dlh.smile.axdelivery

import de.dlh.smile.axdelivery.DestinationModel.{Transformations, MovingAverage, DestinationRecommender, UDAFGroupConcat}
import de.dlh.smile.engine.commons.Contexts
import org.scalatest.{FlatSpec, Matchers}
import org.mockito.Mockito._

class MainTest extends FlatSpec with Matchers {
  
  "execute" should "run the complete program" in {
    val ioMock:IO = mock(classOf[IO])
    when(ioMock.read) thenReturn Stub.dfInput
    when(ioMock.readAirportCodes) thenReturn Stub.dfAirportCodes
    val df = Main.execute(ioMock.read, ioMock)
    val df1 = Stub.dfBeforeModel
    df.printSchema()
    df1.printSchema()
    df.show()
    df1.show()
  }
}
