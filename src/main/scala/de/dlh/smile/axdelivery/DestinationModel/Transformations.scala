package de.dlh.smile.axdelivery.DestinationModel



import de.dlh.smile.engine.commons.Contexts
import org.apache.spark.sql.DataFrame
import de.dlh.smile.axdelivery.DestinationModel.DataFrameCommons._
import de.dlh.smile.axdelivery.DestinationModel.ColumnCommons._
import de.dlh.smile.axdelivery.LoadedProperties
import org.apache.spark.sql.functions._
import de.dlh.smile.engine.commons._
import org.apache.spark.Logging
import org.joda.time.format.DateTimeFormat
import scala.util.{Failure, Success, Try}



object Transformations extends Logging{

  def formatAndRegisterDataFrame(df: DataFrame, dfAirportMap: DataFrame): DataFrame = {
    df.filterPartitionFieldsOneYearFrom()
      .filterValueMapEquals("cs_uri_query", "Screen", "FOFP")
      .getBFTUDEPField("date", "cs_uri_query", "BFDepDate")
      .flatMapType("cs_uri_query", LoadedProperties.fromMapColumns)
      .select(LoadedProperties.webtrendsColumns.map(col(_)): _*)
      .withColumn("BFO", getFirstIATA(col("BFO")))
      .withColumn("BFD", getFirstIATA(col("BFD")))
      .airportToCityCode(dfAirportMap, "BFO")
      .airportToCityCode(dfAirportMap, "BFD")
      .filterOrigin()
      .filterRT()
  }

  def filterLeisure(df: DataFrame): DataFrame = {
    df.filter(col("scoreTRM") <= 0.5)
    // Leisure bookings have a scoreTRM <= 0.5
  }

  def createDateFrom(date: String, inFormat: String, outFormat: String) = {
    val inFormatter = Try(DateTimeFormat.forPattern(inFormat)) match {
      case Success(s) => s
      case Failure(f) => log.warn("The provided input format: \"" + inFormat + "\" is not a valid DateTime format. Using default pattern yyyyMMdd");
        DateTimeFormat.forPattern("yyyyMMdd")
    }
    val outFormatter = Try(DateTimeFormat.forPattern(outFormat)) match {
      case Success(s) => s
      case Failure(f) => log.warn("The provided output format: \"" + outFormat + "\" is not a valid DateTime format. Using default toString");
        DateTimeFormat.forPattern("yyyyMMdd")
    }
    val dateResult = date match {
      case "" | "null" | null => null
      case _ => {
        val formattedDateTime = Try(inFormatter.parseLocalDate(date))
        formattedDateTime match {
          case Success(s) => s
          case Failure(f) => null
        }
      }
    }
    dateResult match {
      case null => None
      case _ => Some(dateResult.toString(outFormatter))
    }
  }

  // TODO: When the row has a null value this will return a 0
  def getTimeInd(hour: Int): String = {
    if (hour < 12) "morning"
    else if (hour >= 12 & hour < 18) "afternoon"
    else if (hour >= 18 & hour < 24) "evening"
    else ""
  }

  def getDurationStay(durstayString: String) = {
    // TODO: if this value is not possible to turn into a String it will fail
    val durStay = durstayString.toInt
    if (durStay == 1) "1d"
    else if (durStay >=2 & durStay <= 3) "2-3d"
    else if (durStay >=4 & durStay <= 6) "4-6d"
    else if (durStay >=7 & durStay <= 13) "1-2w"
    else if (durStay >=14 & durStay <= 27) "3-4w"
    else if (durStay >=28) ">4w"
    else ""
  }

  def getBrowserName(cs_userString: String) = {
    if (cs_userString.indexOf("SamsungBrowser") != -1) "SamsungBrowser"
    else if (cs_userString.indexOf("Firefox") != -1) "Firefox"
    else if (cs_userString.indexOf("Opera") != -1) "Opera"
    else if (cs_userString.indexOf("Trident") != -1) "IE"
    else if (cs_userString.indexOf("MSIE") != -1) "IE-compatible"
    else if (cs_userString.indexOf("Chrome") != -1) "Chrome"
    else if (cs_userString.indexOf("Safari") != -1) "Safari"
    else "Other"
  }

  def getReferrerCat(referrerString: String) = {
    if (referrerString.indexOf("Direct") != -1 | referrerString.indexOf("lufthansa") != -1 | referrerString.indexOf("miles-and-more") != -1) "Direct"
    else if (referrerString.indexOf("google") != -1) "Google"
    else "Other"
  }
}
