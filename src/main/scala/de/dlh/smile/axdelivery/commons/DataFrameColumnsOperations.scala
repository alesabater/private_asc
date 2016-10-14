package de.dlh.smile.axdelivery.commons

import org.apache.spark.Logging
import org.apache.spark.sql.functions._
import org.joda.time.LocalDateTime

object DataFrameColumnsOperations extends Enumeration with Logging {

  def getDayOfTheWeek: ((String, String) => Option[Int]) = (date: String, format: String) => Transformations.createDateFrom(date,format)((date: LocalDateTime) => date.getDayOfWeek)
  def udfGetDayOfTheWeek = udf(getDayOfTheWeek)

  def getStringDateFormatted: ((String, String) => Option[String]) = (date: String, format: String) => Transformations.createDateFrom(date,format)((date: LocalDateTime) => date.toString("yyyy-MM-dd"))
  def udfGetStringDateFormatted = udf(getStringDateFormatted)

  def getHourOfDay: ((String, String) => Option[Int]) = (date: String, format: String) => Transformations.createDateFrom(date,format)((date: LocalDateTime) => date.getHourOfDay)
  def udfGetHourOfDay = udf(getHourOfDay)

  val udfGetTimeInd = udf((ind: Int) => Transformations.getTimeInd(ind))
  val udfGetDurationStay = udf((duration: String) => Transformations.getDurationStay(duration))
  val udfGetBrowserName = udf((userAgent: String) => Transformations.getBrowserName(userAgent))
  val udfGetOSName = udf((userAgent: String) => Transformations.getOSName(userAgent))
  val udfGetReferrerCat = udf((referrer: String) => Transformations.getReferrerCat(referrer))
  val udfGetFirstIATA = udf((iataString: String) => Transformations.getFirstIATA(iataString))
  val udfGetLanguage = udf((language: String) => Transformations.getLanguage(language))
  val udfGetType = udf((typ: String) => Transformations.getType(typ))
}