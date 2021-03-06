package de.dlh.smile.axdelivery.commons

import DataFrameColumnsOperations._
import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.joda.time.LocalDateTime
import org.joda.time.format.DateTimeFormat
import de.dlh.smile.axdelivery.commons.DataFrameColumnsOperations._
import de.dlh.smile.axdelivery.commons.DataFrameOperations._

import scala.util.{Failure, Success, Try}



object Transformations extends Logging{

  def formatWebtrendsData(dfWebtrends: DataFrame, dfAirport: DataFrame): DataFrame = {
    dfWebtrends.filterPartitionFieldsOneYearFrom()
      .filterValueMapEquals("cs_uri_query", "Screen", "FOFP")
      .getBFTUDEPField("date_dt", "cs_uri_query", "BFDepDate", "yyyyMMdd")
      .flatMapType("cs_uri_query", LoadedProperties.fromMapColumns)
      .select(LoadedProperties.webtrendsColumns.map(col(_)): _*)
      .withColumn("BFO", udfGetFirstIATA(col("BFO")))
      .withColumn("BFD", udfGetFirstIATA(col("BFD")))
      .airportToCityCode(dfAirport, "BFO")
      .airportToCityCode(dfAirport, "BFD")
      .filterOrigin()
      .filterRT()
  }

  def getFirstIATA(iataString: String) = iataString match {
    case null => None
    case _ => Some(iataString.substring(0, 3))
  }

  def filterLeisure(df: DataFrame): DataFrame = {
    df.filter(col("scoreTRM") <= 0.5)
    // Leisure bookings have a scoreTRM <= 0.5
  }

  def createDateFrom[A](date: String, inFormat: String)(fn: LocalDateTime => A): Option[A] = {
    val inFormatter = Try(DateTimeFormat.forPattern(inFormat)) match {
      case Success(s) => s
      case Failure(f) => log.warn("The provided input format: \"" + inFormat + "\" is not a valid DateTime format. Using default pattern yyyyMMdd");
        DateTimeFormat.forPattern("yyyyMMdd")
    }
    val dateResult = date match {
      case "" | "null" | null => null
      case _ => {
        val formattedDateTime = Try(inFormatter.parseLocalDateTime(date))
        formattedDateTime match {
          case Success(s) => s
          case Failure(f) => null
        }
      }
    }
    dateResult match {
      case null => None
      case _ => Some(fn(dateResult))
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
    val durStay = Try(durstayString.toInt) match {
      case Success(s) => s
      case Failure(f) => -1
    }
    if (durStay == 1) "1d"
    else if (durStay >=2 & durStay <= 3) "2-3d"
    else if (durStay >=4 & durStay <= 6) "4-6d"
    else if (durStay >=7 & durStay <= 13) "1-2w"
    else if (durStay >=14 & durStay <= 27) "3-4w"
    else if (durStay >=28) ">4w"
    else "Other"
  }

  def getBrowserName(cs_userString: String) = {
    if (cs_userString.indexOf("SamsungBrowser") != -1) "SamsungBrowser"
    else if (cs_userString.indexOf("Firefox") != -1) "Firefox"
    else if (cs_userString.indexOf("Opera") != -1) "Opera"
    else if (cs_userString.indexOf("Trident") != -1) "Trident"
    else if (cs_userString.indexOf("MSIE") != -1) "IE-compatible"
    else if (cs_userString.indexOf("Chrome") != -1) "Chrome"
    else if (cs_userString.indexOf("Safari") != -1) "Safari"
    else "Other"
  }

  def getOSName(cs_userString: String) = {
    if (cs_userString.indexOf("Windows") != -1) "Windows"
    else if (cs_userString.indexOf("Macintosh") == -1 ) "Macintosh"
    else if (cs_userString.indexOf("Macintosh") == -1 && cs_userString.indexOf("Linux") == -1 & cs_userString.indexOf("Windows") == -1) "Unknown"
    else "Other"
  }

  def getReferrerCat(referrerString: String) = {
    if (referrerString.indexOf("Direct") != -1 | referrerString.indexOf("lufthansa") != -1 | referrerString.indexOf("miles-and-more") != -1) "Direct"
    else if (referrerString.indexOf("google") != -1) "Google"
    else "Other"
  }

  def getLanguage(referrerString: String) = {
    if (referrerString.indexOf("en") != -1 ) "English"
    else if (referrerString.indexOf("de") != -1) "Deutsch"
    else "Other"
  }

  def getBftType(referrerString: String) = {
    if (referrerString.indexOf("IK") != -1 ) "IK"
    else if (referrerString.indexOf("K") != -1) "K"
    else "Other"
  }


}
