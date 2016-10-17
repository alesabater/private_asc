package de.dlh.smile.axdelivery


import org.mockito.Mockito._
import org.scalatest.{FlatSpec, Matchers}

class MainTest extends FlatSpec with Matchers {
  
  "execute" should "run the complete program" in {
    val ioMock:IO = mock(classOf[IO])
    when(ioMock.readWebtrendsData) thenReturn Stub.dfInput.sample(true, 0.05)
    when(ioMock.readAirportCityMapping) thenReturn Stub.dfAirportCodes
    val df = Main.execute(ioMock)
    val dfLeisure = df.filter(df("leisure_score")<=0.5)
    val dfBusiness = df.filter(df("leisure_score")>0.5)
    println("the total searchs are ==> " + df.count())
    df.show(100)
    println("the leisure searchs are ==> " + dfLeisure.count())
    dfLeisure.show(100)
    println("the business searchs are ==> " + dfBusiness.count())
    dfBusiness.show(100)
  }
}
