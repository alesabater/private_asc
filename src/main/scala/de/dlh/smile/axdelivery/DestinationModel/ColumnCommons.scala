package de.dlh.smile.axdelivery.DestinationModel

import org.apache.spark.Logging
import org.apache.spark.sql.functions._
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}
import org.joda.time.{DateTime, DateTimeZone}

import scala.util.{Failure, Success, Try}

object ColumnCommons extends Enumeration with Logging {

  val udfCreateDateFrom = udf((dateString: String, inFormat: String, outFormat: String) => Transformations.createDateFrom(dateString, inFormat, outFormat))
  val udfGetTimeInd = udf((ind: Int) => Transformations.getTimeInd(ind))
  val udfGetDurationStay = udf((duration: String) => Transformations.getDurationStay(duration))
  val udfGetBrowserName = udf((userAgent: String) => Transformations.getBrowserName(userAgent))
  val udfGetReferrerCat = udf((referrer: String) => Transformations.getReferrerCat(referrer))
  
  val getFirstIATA = udf((iataString: String) => iataString match {
    case null => None
    case _ => Some(iataString.substring(0, 3))
  }
  )
}