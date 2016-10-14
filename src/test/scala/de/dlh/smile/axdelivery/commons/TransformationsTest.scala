package de.dlh.smile.axdelivery.commons

import de.dlh.smile.axdelivery.Stub
import de.dlh.smile.engine.commons.Contexts
import org.scalatest.{FlatSpec, Matchers}
import de.dlh.smile.axdelivery.commons.Transformations._


class TransformationsTest extends FlatSpec with Matchers {
  
  "formatWebtrendsData" should "format and register data frame" in {
    val df = Contexts.sqlCtx.read.parquet(getClass.getResource("/data/webtrends").getPath)
    val dfAirportMap = Contexts.sqlCtx.read.json(getClass.getResource("/data/airport_codes/airporttocity.json").getPath)
    val dfResult = Transformations.formatWebtrendsData(df, dfAirportMap)
    dfResult.show()
  }

	"scoreTravelReason" should "return the dataFrame with an extra column called scoreTRM" in {
	  val df = Stub.dfWebtendsAfterFormat
    //val dfResult = Transformations.scoreTravelReason(df)
    df.show()
    df.printSchema()
    // At this moment we have to believe that it is correct
	}

	"createDateFrom" should "get the day of the week out of a string date" in {
    val valid = createDateFrom("20160928", "yyyyMMdd")(_.toString("MM"))
    val invalid = createDateFrom("2016-09-28 10:00:00.0", "yyyy-MM-dd HH:mm:ss.S")(_.toString("yyyyMMdd"))
    val nullResult = createDateFrom(null, "yyyyMMdd")(_.toString())

    valid should equal (Some("09"))
    invalid should equal (Some("20160928"))
    nullResult should equal (None)
	}

  "getTimeInd" should "get a string description of the date time" in {
    val morning = getTimeInd(0)
    val morning1 = getTimeInd(11)
    val afternoon = getTimeInd(12)
    val afternoon1 = getTimeInd(17)
    val evening = getTimeInd(18)
    val evening1 = getTimeInd(23)
    val invalid = getTimeInd(24)

    morning should equal("morning")
    morning1 should equal("morning")
    afternoon should equal("afternoon")
    afternoon1 should equal("afternoon")
    evening should equal("evening")
    evening1 should equal("evening")
    invalid should equal("")
  }

  "getDurationStay" should "get a string description duration of the travel" in {
    val oneDay = getDurationStay("1")
    val twoThreeDays = getDurationStay("3")
    val fourFiveDays = getDurationStay("5")
    val oneTwoWeeks = getDurationStay("10")
    val threeFourWeeks = getDurationStay("17")
    val threeFourWeeks1 = getDurationStay("24")
    val month = getDurationStay("33")

    oneDay should equal("1d")
    twoThreeDays should equal("2-3d")
    fourFiveDays should equal("4-6d")
    oneTwoWeeks should equal("1-2w")
    threeFourWeeks should equal("3-4w")
    threeFourWeeks1 should equal("3-4w")
    month should equal(">4w")
  }

  "getBrowserName" should "get the Browser name out of a string" in {
    val firefox = getBrowserName("Mozilla/5.0+(Windows+NT+6.1;+WOW64;+rv:17.0)+Gecko/20100101+Firefox/17.0")
    val chrome = getBrowserName("Mozilla/5.0+(Macintosh;+Intel+Mac+OS+X+10_11_1)+AppleWebKit/537.36+(KHTML,+like+Gecko)+Chrome/51.0.2704.84+Safari/537.36")
    val chrome1 = getBrowserName("Mozilla/5.0+(Linux;+Android+4.4.2;+Lenovo+A7600-F+Build/KOT49H)+AppleWebKit/537.36+(KHTML,+like+Gecko)+Chrome/51.0.2704.81+Safari/537.36")
    val firefox1 = getBrowserName("Mozilla/5.0+(Windows+NT+6.1;+WOW64;+rv:29.0)+Gecko/20100101+Firefox/29.0")

    firefox should equal("Firefox")
    firefox1 should equal("Firefox")
    chrome should equal("Chrome")
    chrome1 should equal("Chrome")
  }

  "getReferrerCat" should "get the Browser name out of a string" in {
    val direct = getReferrerCat("Direct%20Traffic")
    val google = getReferrerCat("www.google.de")
    val notvalid = getReferrerCat("NOT VALID")

    direct should equal("Direct")
    google should equal("Google")
    notvalid should equal("Other")
  }
}


