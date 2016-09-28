package de.dlh.smile.axdelivery

import java.util.Map.Entry

import com.typesafe.config.{Config, ConfigFactory, ConfigObject, ConfigValue}

import scala.collection.JavaConverters._
import java.util.Map.Entry

import collection.JavaConversions._

object LoadedProperties {

  def createMap(path: String, config: Config): Map [String, String] = {
    val list : Iterable[ConfigObject] = config.getObjectList(path).asScala
    (for {
      item : ConfigObject <- list
      entry : Entry[String, ConfigValue] <- item.entrySet().asScala
      key = entry.getKey
      value = entry.getValue.unwrapped().toString
    } yield (key, value)).toMap
  }

  lazy val recommendation_conf = ConfigFactory.load("recommendationModel")

  def fromMapColumns = recommendation_conf.getStringList("dataframe.select.map_columns").toList
  def webtrendsColumns = recommendation_conf.getStringList("dataframe.select.webtrends").toList

}
