package de.dlh.smile.axdelivery

import de.dlh.smile.engine.commons.Contexts
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object Stub {
  val dfFilterDate = Contexts.sqlCtx.createDataFrame(Contexts.sc.parallelize(TestSets.dateFilterSeq), Schemas.dateFilterSchema)
  val dfStringDate = Contexts.sqlCtx.createDataFrame(Contexts.sc.parallelize(TestSets.dateStringSeq), Schemas.dateStringSchema)
  val dfStringIATA = Contexts.sqlCtx.createDataFrame(Contexts.sc.parallelize(TestSets.IATAStringSeq), Schemas.IATAStringSchema)
  val dfStringOrigin = Contexts.sqlCtx.createDataFrame(Contexts.sc.parallelize(TestSets.originStringSeq), Schemas.IATAStringSchema)
  val dfMapDate = Contexts.sqlCtx.createDataFrame(Contexts.sc.parallelize(TestSets.dateMapSeq), Schemas.dateMapSchema)
  val dfOneColInt = Contexts.sqlCtx.createDataFrame(Contexts.sc.parallelize(TestSets.oneColInt), Schemas.oneColIntSchema)
  val dfOneColString = Contexts.sqlCtx.createDataFrame(Contexts.sc.parallelize(TestSets.oneColString), Schemas.oneColStringSchema)
  val dfBrowserString = Contexts.sqlCtx.createDataFrame(Contexts.sc.parallelize(TestSets.browserString), Schemas.oneColStringSchema)
  val dfReferrerCat = Contexts.sqlCtx.createDataFrame(Contexts.sc.parallelize(TestSets.referrerCat), Schemas.oneColStringSchema)
  val dfInput = Contexts.sqlCtx.read.parquet(getClass.getResource("/data/webtrends").getPath)
  val dfAirportCodes = Contexts.sqlCtx.read.json(getClass.getResource("/data/airport_codes").getPath)
  val dfBeforeModel = Contexts.sqlCtx.read.parquet(getClass.getResource("/data/training").getPath)
}

object TestSets {

  val dateFilterSeq = Seq(
    Row(2016, 9, "a"),
    Row(2015, 3, "b"),
    Row(2016, 6, "c"))

  val dateStringSeq = Seq(
    Row("20160927"),
    Row("20160926"),
    Row(null),
    Row("Invalid date")
  )
  
  val IATAStringSeq = Seq(
    Row("MAD;FRA;jkh"),
    Row("BCN"),
    Row(null),
    Row("MAD%3D;FRA")
  )
  
  val originStringSeq = Seq(
    Row("MAD"),
    Row("BCN"),
    Row(null),
    Row("FAKE"),
    Row("CCS")
  )

  val dateMapSeq = Seq(
    Row("2016-09-15", Map("k1" -> "20160927", "k2" -> "val")),
    Row("2016-09-15", Map("k1" -> "20160926", "k2" -> "val")),
    Row("2016-09-30", Map("k1" -> "20160926", "k2" -> "val")),
    Row("2015-01-15", Map("k1" -> "20160926", "k2" -> "val")),
    Row(null, Map("k1" -> "20160926", "k2" -> "val")),
    Row("2016-09-15", Map("k2" -> "val")),
    Row("2016-09-15", Map("k1" -> null, "k2" -> "val"))
  )

  val oneColInt = Seq(
    Row(1),
    Row(11),
    Row(12),
    Row(15),
    Row(19),
    Row(25),
    Row(null)
  )

  val oneColString = Seq(
    Row("1"),
    Row("11"),
    Row("12"),
    Row("15"),
    Row("19"),
    Row("25"),
    Row("33"),
    Row("Not an Int"),
    Row(null)
  )

  val browserString = Seq(
    Row("Mozilla/5.0+(Windows+NT+6.1;+WOW64;+rv:47.0)+Gecko/20100101+Firefox/47.0"),
    Row("Mozilla/5.0+(Windows+NT+6.2;+WOW64;+rv:42.0)+Gecko/20100101+Firefox/42.0"),
    Row("Mozilla/5.0+(Windows+NT+6.1;+Win64;+x64;+rv:49.0)+Gecko/20100101+Firefox/49.0"),
    Row("Mozilla/5.0+(iPhone;+CPU+iPhone+OS+9_3_1+like+Mac+OS+X)+AppleWebKit/601.1.46+(KHTML,+like+Gecko)+Version/9.0+Mobile/13E238+Safari/601.1"),
    Row("Mozilla/5.0+(Macintosh;+Intel+Mac+OS+X+10_11_6)+AppleWebKit/537.36+(KHTML,+like+Gecko)+Chrome/39.0.2171.95+Safari/537.36"),
    Row("Mozilla/5.0+(Macintosh;+Intel+Mac+OS+X+10_7_5)+AppleWebKit/534.57.7+(KHTML,+like+Gecko)+Version/5.1.7+Safari/534.57.7"),
    Row("Mozilla/5.0+(Windows+NT+6.3;+WOW64;+Trident/7.0;+Touch;+tb-webde/2.6.6;+ASU2JS;+rv:11.0)+like+Gecko"),
    Row("Mozilla/5.0+(iPhone;+CPU+iPhone+OS+8_4_1+like+Mac+OS+X)+AppleWebKit/600.1.4+(KHTML,+like+Gecko)+Version/8.0+Mobile/12H321+Safari/600.1.4"),
    Row("Mozilla/5.0+(Linux;+Android+5.0.2;+P023+Build/LRX22G;+wv)+AppleWebKit/537.36+(KHTML,+like+Gecko)+Version/4.0+Chrome/47.0.2526.100+Safari/537.36"),
    Row("Mozilla/5.0+(Windows+NT+10.0;+WOW64;+rv:47.0)+Gecko/20100101+Firefox/47.0+TO-Browser/TOB7.47.0.111_03"),
    Row("Mozilla/5.0+(Macintosh;+Intel+Mac+OS+X+10_11_1)+AppleWebKit/537.36+(KHTML,+like+Gecko)+Chrome/51.0.2704.84+Safari/537.36")
  )

  val referrerCat = Seq(
    Row("Direct%20Traffic"),
    Row("www.google.de")
  )
}

object Schemas {

  val dateFilterSchema = StructType(Seq(
  StructField("year", IntegerType, true),
  StructField("month", IntegerType, true),
  StructField("data", StringType, true)
  ))

  val dateStringSchema = StructType(Seq(
    StructField("date", StringType, true)
  ))
  
  val IATAStringSchema = StructType(Seq(
    StructField("BFO", StringType, true)
  ))

  val dateMapSchema = StructType(Seq(
    StructField("date_prev", StringType, true),
    StructField("date_dep", MapType(StringType,StringType,true), true)
  ))

  val oneColIntSchema = StructType(Seq(
    StructField("one", IntegerType, true)
  ))

  val oneColStringSchema = StructType(Seq(
    StructField("one", StringType, true)
  ))
}