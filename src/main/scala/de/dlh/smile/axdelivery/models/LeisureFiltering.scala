package de.dlh.smile.axdelivery.models

import de.dlh.smile.axdelivery.commons.DataFrameColumnsOperations._
import de.dlh.smile.axdelivery.commons.LoadedProperties
import de.dlh.smile.axdelivery.data.{DynamicIntScore, FixedStringScore, LeisureScore, Score}
import de.dlh.smile.engine.commons.Contexts
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

import scala.util.{Failure, Success, Try}

object LeisureFiltering {

  val mapScores = Map(
    "bf_trip_type" -> LoadedProperties.bfTripTypeScores,
    "bf_tu_dep" -> LoadedProperties.bfTuDepScores,
    "bf_ret_date" -> LoadedProperties.bfRetRateScores,
    "date_dt_day" -> LoadedProperties.dateDtWeekScores,
    "date_dt_hour" -> LoadedProperties.dateDtHourScores,
    "bf_dep_date" -> LoadedProperties.bfDepDateScores,
    "bf_dur_stay" -> LoadedProperties.bfDurStayScores,
    "cs_user_browser" -> LoadedProperties.userBrowserScores,
    "cs_user_os" -> LoadedProperties.userOsScores,
    "ed_refdom" -> LoadedProperties.edRefDomScores,
    "language" -> LoadedProperties.languageScores,
    "bft" -> LoadedProperties.bftScores
  )

  def filter(df: DataFrame): DataFrame = {
    val dfValueable = getRelevantInformation(df)
    val dfLeisureScores = getLeisureScores(dfValueable, dfValueable.schema)
    val dfLeisureFiltered = dfLeisureScores.filter(col("leisure_score")<=0.5).drop(col("leisure_scores"))
    dfLeisureFiltered
  }

  def getRelevantInformation(df: DataFrame): DataFrame = {
    df.withColumn("bf_trip_type", col("BFTripType"))
      .withColumn("bf_tu_dep", col("BFTuDep"))
      .withColumn("bf_ret_date", udfGetDayOfTheWeek(col("BFRetDate"), lit("yyyyMMdd")))
      .withColumn("date_dt_day", udfGetDayOfTheWeek(col("date_dt").cast(StringType), lit("yyyy-MM-dd HH:mm:ss")))
      .withColumn("date_dt_hour", udfGetHourOfDay(col("date_dt").cast(StringType), lit("yyyy-MM-dd HH:mm:ss")))
      .withColumn("bf_dep_date", udfGetDayOfTheWeek(col("BFDepDate"), lit("yyyyMMdd")))
      .withColumn("bf_dur_stay", udfGetDurationStay(col("BFDurStay")))
      .withColumn("cs_user_browser", udfGetBrowserName(col("cs_user")))
      .withColumn("cs_user_os", udfGetOSName(col("cs_user")))
      .withColumn("ed_refdom", udfGetReferrerCat(col("ed_refdom")))
      .withColumn("language", udfGetLanguage(col("Language")))
      .withColumn("bft", udfGetBftType(col("BFT")))
      .select(LoadedProperties.leisureModelColumns.map(col(_)): _*)
  }

  def getLeisureScores(df: DataFrame, schema: StructType): DataFrame = {

    val monthIdx = schema.fieldIndex("month")
    val yearIdx = schema.fieldIndex("year")
    val bfoIdx = schema.fieldIndex("BFO")
    val bfdIdx = schema.fieldIndex("BFD")
    val sessionIdx = schema.fieldIndex("session_guid")
    val rddRowWithScores = df.map(row => {
      val scores = mapScores.map( keyValue => {
        val key: String = keyValue._1
        val scoresMap: Map[String, Double] = keyValue._2
        val rowValue = row.get(row.schema.fieldIndex(key))
        createLeisureScoreFor(key, rowValue, scoresMap)}).toList
      Row(
        row.getString(bfoIdx),
        row.getString(bfdIdx),
        row.getInt(yearIdx),
        row.getInt(monthIdx),
        row.getString(sessionIdx),
        LeisureScore(scores).result)
    })
    Contexts.sqlCtx.createDataFrame(rddRowWithScores, leisureResultSchema)
  }

  def createLeisureScoreFor(key: String, value: Any, mapScores: Map[String, Double]): Score[_ >: Int with String] = {
    val delta = Try(mapScores(value.toString)) match {
      case Success(s) => s
      case Failure(f) => 0.0
    }
    val score = key match {
      case "bf_tu_dep" => DynamicIntScore(value.asInstanceOf[Int], mapScores("coeficient"))
      case _ => FixedStringScore(value.toString, delta)
    }
    score
  }

  val leisureResultSchema = StructType(Seq(
    StructField("bfo", StringType, true),
    StructField("bfd", StringType, true),
    StructField("year", IntegerType, true),
    StructField("month", IntegerType, true),
    StructField("session_guid", StringType, true),
    StructField("leisure_score", DoubleType, true)
  ))
}
