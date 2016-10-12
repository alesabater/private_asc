package de.dlh.smile.axdelivery


import org.mockito.Mockito._
import org.scalatest.{FlatSpec, Matchers}
import org.apache.spark.sql.functions.col

class MainTest extends FlatSpec with Matchers {
  
  "execute" should "run the complete program" in {
    val ioMock:IO = mock(classOf[IO])
    when(ioMock.read) thenReturn Stub.dfInput
    when(ioMock.readAirportCodes) thenReturn Stub.dfAirportCodes
    val dfIn = ioMock.read
    val df = Main.execute(ioMock.read, ioMock)
    df.printSchema()
    df.show()
    dfIn.select(col("cs_uri_query").getItem("Screen"),col("year"),col("month")).show(1000)
  }
}
