package de.dlh.smile.axdelivery.DestinationModel

import org.apache.spark.Logging
import org.apache.spark.sql.functions._
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}
import org.joda.time.{DateTime, DateTimeZone}

import scala.util.{Failure, Success, Try}

object ColumnCommons extends Enumeration with Logging {


  def createDateFrom(date: String, format: String) = {
    val formatter = Try(DateTimeFormat.forPattern(format)) match {
      case Success(s) => s
      case Failure(f) => log.warn("The provided format: \"" + format + "\" is not a valid DateTime format. Using default pattern yyyyMMdd");
        DateTimeFormat.forPattern("yyyyMMdd")
    }
    val dateResult = date match {
      case "" | "null" | null => null
      case _ => {
        val formattedDateTime = Try(formatter.parseLocalDate(date))
        formattedDateTime match {
          case Success(s) => s
          case Failure(f) => null
        }
      }
    }
    dateResult match {
      case null => None
      case _ => Some(dateResult.toString())
    }
  }

  val dateFrom = udf((dateString: String, format: String) => createDateFrom(dateString, format))
  
  val getFirstIATA = udf((iataString: String) => iataString match {
    case null => None
    case _ => Some(iataString.substring(0, 3))
  }
  )
}