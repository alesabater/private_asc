package de.dlh.smile.axdelivery.commons

import java.util.Map.Entry

import com.typesafe.config.{Config, ConfigFactory, ConfigObject, ConfigValue}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object LoadedProperties {

  def createMap(path: String, config: Config): Map [String, Double] = {
    val list : Iterable[ConfigObject] = config.getObjectList(path).asScala
    (for {
      item : ConfigObject <- list
      entry : Entry[String, ConfigValue] <- item.entrySet().asScala
      key = entry.getKey
      value = entry.getValue.unwrapped().asInstanceOf[Double]
    } yield (key, value)).toMap
  }

  val recommendation_conf = ConfigFactory.load("recommendationModel")


  def fromMapColumns = recommendation_conf.getStringList("dataframe.select.map_columns").toList
  def webtrendsColumns = recommendation_conf.getStringList("dataframe.select.webtrends").toList
  def originCities = recommendation_conf.getStringList("originCities").toList


  // Filtering Properties
  //
  //////////////////////////////////////////////////////////////
  //                                                          //
  //     Only leisure filtering model related properties      //
  //                                                          //
  //////////////////////////////////////////////////////////////

  def leisureModelColumns = recommendation_conf.getStringList("leisure_model.columns_order").toList
  def bfTripTypeScores = createMap("leisure_model.coeficients.bf_trip_type", recommendation_conf)
  def bfTuDepScores = createMap("leisure_model.coeficients.bf_tu_dep", recommendation_conf)
  def bfRetRateScores = createMap("leisure_model.coeficients.bf_ret_rate", recommendation_conf)
  def dateDtWeekScores = createMap("leisure_model.coeficients.date_dt_week", recommendation_conf)
  def bfDepDateScores = createMap("leisure_model.coeficients.bf_dep_date", recommendation_conf)
  def dateDtHourScores = createMap("leisure_model.coeficients.date_dt_hour", recommendation_conf)
  def bfDurStayScores = createMap("leisure_model.coeficients.bf_dur_stay", recommendation_conf)
  def userBrowserScores = createMap("leisure_model.coeficients.user_browser", recommendation_conf)
  def userOsScores = createMap("leisure_model.coeficients.user_os", recommendation_conf)
  def edRefDomScores = createMap("leisure_model.coeficients.ed_ref_dom", recommendation_conf)
  def languageScores = createMap("leisure_model.coeficients.language", recommendation_conf)
  def bftScores = createMap("leisure_model.coeficients.bft", recommendation_conf)
  //def leisureModelColumns = recommendation_conf.getStringList("leisure_model.columns_order").toList
  
}
