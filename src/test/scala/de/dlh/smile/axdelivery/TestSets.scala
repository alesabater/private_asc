package de.dlh.smile.axdelivery

import java.sql.Timestamp

import de.dlh.smile.engine.commons.Contexts
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._


object Stub {
  val dfFilterDate = Contexts.sqlCtx.createDataFrame(Contexts.sc.parallelize(TestSets.dateFilterSeq), Schemas.dateFilterSchema)
  val dfStringDate = Contexts.sqlCtx.createDataFrame(Contexts.sc.parallelize(TestSets.dateStringSeq), Schemas.dateStringSchema)
  val dfFullStringDate = Contexts.sqlCtx.createDataFrame(Contexts.sc.parallelize(TestSets.dateFullStringSeq), Schemas.dateStringSchema)
  val dfStringIATA = Contexts.sqlCtx.createDataFrame(Contexts.sc.parallelize(TestSets.IATAStringSeq), Schemas.IATAStringSchema)
  val dfStringOrigin = Contexts.sqlCtx.createDataFrame(Contexts.sc.parallelize(TestSets.originStringSeq), Schemas.IATAStringSchema)
  val dfMapDate = Contexts.sqlCtx.createDataFrame(Contexts.sc.parallelize(TestSets.dateMapSeq), Schemas.dateMapSchema)
  val dfOneColInt = Contexts.sqlCtx.createDataFrame(Contexts.sc.parallelize(TestSets.oneColInt), Schemas.oneColIntSchema)
  val dfOneColString = Contexts.sqlCtx.createDataFrame(Contexts.sc.parallelize(TestSets.oneColString), Schemas.oneColStringSchema)
  val dfBrowserString = Contexts.sqlCtx.createDataFrame(Contexts.sc.parallelize(TestSets.browserString), Schemas.oneColStringSchema)
  val dfReferrerCat = Contexts.sqlCtx.createDataFrame(Contexts.sc.parallelize(TestSets.referrerCat), Schemas.oneColStringSchema)
  val dfLanguage = Contexts.sqlCtx.createDataFrame(Contexts.sc.parallelize(TestSets.languageString), Schemas.oneColStringSchema)
  val dfBftType = Contexts.sqlCtx.createDataFrame(Contexts.sc.parallelize(TestSets.bftType), Schemas.oneColStringSchema)
  val dfInput = Contexts.sqlCtx.read.parquet(getClass.getResource("/data/webtrends").getPath)
  val dfAirportCodes = Contexts.sqlCtx.read.json(getClass.getResource("/data/airporttocity.json").getPath)
  val dfBeforeModel = Contexts.sqlCtx.read.parquet(getClass.getResource("/data/training").getPath)
  val dfCustomerSales = Contexts.sqlCtx.createDataFrame(Contexts.sc.parallelize(TestSets.customerSalesSeq), Schemas.customerSalesSchema)
  val dfWebtendsAfterFormat = Contexts.sqlCtx.createDataFrame(Contexts.sc.parallelize(TestSets.webtrendsAfterFormatSeq), Schemas.webtrendsAfterFormatSchema)
  val dfResultRecommendation = Contexts.sqlCtx.createDataFrame(Contexts.sc.parallelize(TestSets.resultRecommendationSeq), Schemas.resultRecommendationSchema)
  val dfWebtrendsFormatted = Contexts.sqlCtx.createDataFrame(Contexts.sc.parallelize(TestSets.webtrendsDataFormatted), Schemas.webtrendsFormattedSchema)
  val dfWebtrendsRelevantInfo = Contexts.sqlCtx.createDataFrame(Contexts.sc.parallelize(TestSets.webtrendsDataRelevantInfo), Schemas.webtrendsRelevantinfoSchema)
  val dfFilterCsUriQuery = Contexts.sqlCtx.createDataFrame(Contexts.sc.parallelize(TestSets.csUriQueryMap), Schemas.csUriQuerySchema)
  val dfAfterLeisureFilter = Contexts.sqlCtx.createDataFrame(Contexts.sc.parallelize(TestSets.afterLeisureFilter), Schemas.afterLeisureFilteringSchema)
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

  val dateFullStringSeq = Seq(
    Row("2015-11-15 05:17:43.0"),
    Row("2015-11-15 05:17:43.0"),
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
    Row("2016-09-15 22:12:22.5", Map("k1" -> "20160927", "k2" -> "val")),
    Row("2016-09-15 11:12:22.5", Map("k1" -> "20160926", "k2" -> "val")),
    Row("2016-09-30 22:12:22.5", Map("k1" -> "20160926", "k2" -> "val")),
    Row("2015-01-15 23:72:22.5", Map("k1" -> "20160926", "k2" -> "val")),
    Row(null, Map("k1" -> "20160926", "k2" -> "val")),
    Row("2016-09-15 12:12:22.5", Map("k2" -> "val")),
    Row("2016-09-15 12:42:22.5", Map("k1" -> null, "k2" -> "val"))
  )

  val customerSalesSeq = Seq(
    Row(1, "Mon", 12.0, "Jan"),
    Row(1, "Tue", 10.0, "Jan"),
    Row(1, "Thu", 15.0, "Jan"),
    Row(1, "Fri", 2.0, "Jan"),
    Row(2, "Sun", 10.0, "Jan"),
    Row(2, "Wed", 5.0, "Jan"),
    Row(2, "Thu", 4.0, "Jan"),
    Row(2, "Fri", 3.0, "Jan"),
    Row(1, "Fri", 2.0, "Feb"),
    Row(2, "Sun", 10.0, "Feb"),
    Row(2, "Wed", 5.0, "Feb"),
    Row(2, "Thu", 4.0, "Feb")
  )

  val webtrendsAfterFormatSeq = Seq(
    Row("RT", 49, "20160324", "2015-1-15 18:57:...", "20160103", "81", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2015, 1, 15, "be734313-0a88-40e...", "MAD", "SAO"),
    Row("RT", 65, "20160327", "2015-1-15 22:04:...", "20160119", "68", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2015, 1, 15, "187c293b-3c58-45e...", "FRA", "SAO"),
    Row("RT", 65, "20160325", "2015-1-15 22:08:...", "20160119", "66", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2015, 1, 15, "187c293b-3c58-45e...", "FRA", "SAO"),
    Row("RT", 49, "20160324", "2015-2-15 18:57:...", "20160103", "81", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2015, 2, 15, "be734313-0a88-40e...", "JFK", "SAO"),
    Row("RT", 65, "20160327", "2015-2-15 22:04:...", "20160119", "68", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2015, 2, 15, "187c293b-3c58-45e...", "JFK", "SAO"),
    Row("RT", 65, "20160325", "2015-2-15 22:08:...", "20160119", "66", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2015, 2, 15, "187c293b-3c58-45e...", "FRA", "SAO"),
    Row("RT", 49, "20160324", "2015-3-15 18:57:...", "20160103", "81", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2015, 3, 15, "be734313-0a88-40e...", "JFK", "SAO"),
    Row("RT", 65, "20160327", "2015-3-15 22:04:...", "20160119", "68", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2015, 3, 15, "187c293b-3c58-45e...", "FRA", "SAO"),
    Row("RT", 65, "20160325", "2015-3-15 22:08:...", "20160119", "66", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2015, 3, 15, "187c293b-3c58-45e...", "FRA", "SAO"),
    Row("RT", 65, "20160327", "2014-12-15 22:04:...", "20160119", "68", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2014, 12, 15, "187c293b-3c58-45e...", "FRA", "SAO"),
    Row("RT", 65, "20160327", "2014-12-15 22:04:...", "20160119", "68", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2014, 12, 15, "187c293c58-45e...", "FRA", "SAO"),
    Row("RT", 65, "20160327", "2014-11-15 22:04:...", "20160119", "68", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2014, 11, 15, "187c293b-3c58-45e...", "JFK", "SAO"),
    Row("RT", 65, "20160327", "2014-10-15 22:04:...", "20160119", "68", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2014, 10, 15, "187c293b-3c58-45e...", "FRA", "SAO"),
    Row("RT", 65, "20160327", "2014-10-15 22:04:...", "20160119", "68", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2014, 10, 15, "187c293b-3sdasc58-45e...", "FRA", "SAO"),
    Row("RT", 65, "20160327", "2014-9-15 22:04:...", "20160119", "68", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2014, 9, 15, "187c293b-3c58-45e...", "FRA", "SAO"),
    Row("RT", 65, "20160327", "2014-8-15 22:04:...", "20160119", "68", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2014, 8, 15, "187c293b-3c58-45e...", "FRA", "SAO"),
    Row("RT", 65, "20160327", "2014-7-15 22:04:...", "20160119", "68", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2014, 7, 15, "187c293b-3c58-45e...", "FRA", "SAO"),
    Row("RT", 65, "20160327", "2014-6-15 22:04:...", "20160119", "68", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2014, 6, 15, "187c293b-3c58-45e...", "JFK", "SAO"),
    Row("RT", 65, "20160327", "2014-5-15 22:04:...", "20160119", "68", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2014, 5, 15, "187c293b-3c58-45e...", "JFK", "SAO"),
    Row("RT", 65, "20160327", "2014-4-15 22:04:...", "20160119", "68", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2014, 4, 15, "187c293b-3c58-45e...", "FRA", "SAO"),
    Row("RT", 65, "20160327", "2014-3-15 22:04:...", "20160119", "68", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2014, 3, 15, "187c293b-3c58-45e...", "FRA", "SAO"),
    Row("RT", 65, "20160327", "2014-3-15 22:04:...", "20160119", "68", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2014, 3, 15, "187c293b-58-45e...", "FRA", "SAO")
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

  val languageString = Seq(
    Row("en"),
    Row("it"),
    Row("de"),
    Row("es"),
    Row("en"),
    Row("de"),
    Row("de"),
    Row("de"),
    Row("en"),
    Row("en"),
    Row("es")
  )

  val referrerCat = Seq(
    Row("Direct%20Traffic"),
    Row("www.google.de")
  )

  val bftType = Seq(
    Row("IK%20Traffic"),
    Row("K"),
    Row("IK"),
    Row("Nothing")
  )

  val resultRecommendationSeq = Seq(
    Row("MAD", "FRA", 0),
    Row("MAD", "PAR", 1),
    Row("MAD", "PHF", 2),
    Row("MAD", "SVG", 3),
    Row("MAD", "BCN", 4),
    Row("MAD", "HGJ", 5),
    Row("MAD", "HSL", 6),
    Row("MAD", "TYU", 7),
    Row("MAD", "THY", 8),
    Row("MAD", "PEK", 9),
    Row("MAD", "THO", 10),
    Row("MAD", "GEN", 11),
    Row("MAD", "GIN", 12),
    Row("MAD", "HAL", 13),
    Row("MAD", "HAW", 14),
    Row("MAD", "HOW", 15),
    Row("FRA", "MAD", 0),
    Row("FRA", "PAR", 1),
    Row("FRA", "PHF", 2),
    Row("FRA", "SVG", 3),
    Row("FRA", "BCN", 4),
    Row("FRA", "HGJ", 5),
    Row("FRA", "HSL", 6),
    Row("BER", "FRA", 0),
    Row("BER", "PAR", 1),
    Row("BER", "PHF", 2),
    Row("BER", "SVG", 3),
    Row("BER", "BCN", 4),
    Row("BER", "HGJ", 5),
    Row("BER", "HSL", 6),
    Row("BER", "TYU", 7),
    Row("BER", "THY", 8),
    Row("BER", "PEK", 9),
    Row("BER", "THO", 10),
    Row("BER", "GEN", 11),
    Row("BER", "GIN", 12)
  )

  val webtrendsDataFormatted = Seq(
    Row("RT", 34, "20160102", new Timestamp(115, 11, 15, 20, 15, 0, 0), "20151219", "14", "Mozilla/5.0+(Windows+NT+6.1;+WOW64;+rv:47.0)+Gecko/20100101+Firefox/47.0", "Direct%20Traffic", "en", "IK;IK", "0", "FOFP", 2015, 11, 15, "1f3935e6-ae76-410", "SHA", "MIL"),
    Row("OW", 21, "20160707", new Timestamp(116, 1, 2, 10, 65, 0, 0), "20151219", "24", "Mozilla/5.0+(Macintosh;+Intel+Mac+OS+X+10_11_6)+AppleWebKit/537.36+(KHTML,+like+Gecko)+Chrome/39.0.2171.95+Safari/537.36", "Direct%20Traffic", "en", "IK;IK", "0", "FOFP", 2015, 11, 15, "1f3935e6-ae76-410", "BCN", "MIL"),
    Row("RT", 15, "20160506", new Timestamp(116, 2, 7, 18, 45, 0, 0), "20151219", "33", "Mozilla/5.0+(Macintosh;+Intel+Mac+OS+X+10_7_5)+AppleWebKit/534.57.7+(KHTML,+like+Gecko)+Version/5.1.7+Safari/534.57.7", "Direct%20Traffic", "es", "IK;IK", "0", "FOFP", 2015, 11, 15, "1f3935e6-ae76-410", "MAD", "MIL"),
    Row("RT", 11, "20160212", new Timestamp(116, 1, 1, 16, 25, 0, 0), "20151219", "11", "Mozilla/5.0+(Macintosh;+Intel+Mac+OS+X+10_11_1)+AppleWebKit/537.36+(KHTML,+like+Gecko)+Chrome/51.0.2704.84+Safari/537.36", "Direct%20Traffic", "de", "IK;IK", "2", "FOFP", 2015, 11, 15, "1f3935e6-ae76-410", "DUB", "MIL"),
    Row("RT", 40, "20160322", new Timestamp(115, 9, 10, 13, 25, 0, 0), "20151219", "12", "Mozilla/5.0+(Windows+NT+10.0;+WOW64;+rv:47.0)+Gecko/20100101+Firefox/47.0+TO-Browser/TOB7.47.0.111_03", "www.google.it", "de", "K;K", "0", "FOFP", 2015, 11, 15, "1f3935e6-ae76-410", "FRA", "MIL"),
    Row("RT", 23, "20160513", new Timestamp(116, 3, 18, 10, 35, 0, 0), "20151219", "20", "Mozilla/5.0+(Windows+NT+10.0;+WOW64;+rv:47.0)+Gecko/20100101+Firefox/47.0+TO-Browser/TOB7.47.0.111_03", "www.google.it", "en", "IK;K", "1", "FOFP", 2015, 11, 15, "1f3935e6-ae76-410", "ZCH", "MIL")
  )


  val webtrendsDataRelevantInfo = Seq(
    Row("RT", 34, 6, 2, 20, 6, "3-4w", "Firefox", "Windows", "Direct", "English", "IK", "0", 2015, 11, "SHA", "MIL", "1f3935e6-ae76-410"),
    Row("OW", 21, 4, 2, 11, 6, "3-4w", "Chrome", "Other", "Direct", "English", "IK", "0", 2015, 11, "BCN", "MIL", "1f3935e6-ae76-410"),
    Row("RT", 15, 5, 1, 18, 6, ">4w", "Safari", "Other", "Direct", "Other", "IK", "0", 2015, 11, "MAD", "MIL", "1f3935e6-ae76-410"),
    Row("RT", 11, 5, 1, 16, 6, "1-2w", "Chrome", "Other", "Direct", "Deutsch", "IK", "2", 2015, 11, "DUB", "MIL", "1f3935e6-ae76-410"),
    Row("RT", 40, 2, 6, 13, 6, "1-2w", "Firefox", "Windows", "Google", "Deutsch", "K", "0", 2015, 11, "FRA", "MIL", "1f3935e6-ae76-410"),
    Row("RT", 23, 5, 1, 10, 6, "3-4w", "Firefox", "Windows", "Google", "English", "IK", "1", 2015, 11, "ZCH", "MIL", "1f3935e6-ae76-410")
  )

  val csUriQueryMap = Seq(
    Row("RT", 34, 6, 2, 20, 6, Map("Screen" -> "FOFP")),
    Row("OW", 21, 4, 2, 11, 6, Map("Screen" -> "FOFP")),
    Row("RT", 15, 5, 1, 18, 6, Map("Screen" -> "NA")),
    Row("RT", 11, 5, 1, 16, 6, Map("Screen" -> "FOFP")),
    Row("RT", 40, 2, 6, 13, 6, Map("Screen" -> "NA")),
    Row("RT", 23, 5, 1, 10, 6, Map("Screen" -> "FOFP"))
  )

  val afterLeisureFilter = Seq(
    Row("BCN", "FRA", 2016, 11, "1234", 0.45),
    Row("MAD", "MUC", 2016, 9, "1236", 0.45),
    Row("BCN", "FRA", 2016, 11, "1234", 0.45),
    Row("MAD", "MUC", 2016, 9, "1235", 0.45),
    Row("BCN", "FRA", 2016, 11, "a", 0.45),
    Row("DUB", "FRA", 2016, 11, "1234", 0.45),
    Row("DUB", "FRA", 2015, 12, "a", 0.45),
    Row("DUB", "FRA", 2015, 12, "b", 0.45),
    Row("DUB", "FRA", 2015, 12, "b", 0.45),
    Row("NYC", "FRA", 2016, 11, "1234", 0.45),
    Row("NYC", "FRA", 2016, 8, "1234", 0.45),
    Row("NYC", "FRA", 2016, 8, "1235", 0.45)
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
    StructField("date_dep", MapType(StringType, StringType, true), true)
  ))

  val csUriQuerySchema = StructType(Seq(
    StructField("1", StringType, true),
    StructField("2", IntegerType, true),
    StructField("3", IntegerType, true),
    StructField("4", IntegerType, true),
    StructField("5", IntegerType, true),
    StructField("6", IntegerType, true),
    StructField("cs_uri_query", MapType(StringType, StringType, true), true)
  ))

  val customerSalesSchema = StructType(Seq(
    StructField("Customer", IntegerType, true),
    StructField("Day", StringType, true),
    StructField("Sales", DoubleType, true),
    StructField("Month", StringType, true)
  ))

  val webtrendsAfterFormatSchema = StructType(Seq(
    StructField("BFTripType", StringType, true),
    StructField("BFTuDep", IntegerType, true),
    StructField("BFRetDate", StringType, true),
    StructField("date_dt", StringType, true),
    StructField("BFDepDate", StringType, true),
    StructField("BFDurStay", StringType, true),
    StructField("cs_user", StringType, true),
    StructField("ed_refdom", StringType, true),
    StructField("Language", StringType, true),
    StructField("BFT", StringType, true),
    StructField("ed_age", StringType, true),
    StructField("Screen", StringType, true),
    StructField("year", IntegerType, true),
    StructField("month", IntegerType, true),
    StructField("day", IntegerType, true),
    StructField("session_guid", StringType, true),
    StructField("BFO", StringType, true),
    StructField("BFD", StringType, true)
  ))
  val oneColIntSchema = StructType(Seq(
    StructField("one", IntegerType, true)
  ))

  val oneColStringSchema = StructType(Seq(
    StructField("one", StringType, true)
  ))

  val resultRecommendationSchema = StructType(Seq(
    StructField("BFO", StringType, true),
    StructField("BFD", StringType, true),
    StructField("mdlrank", IntegerType, true)
  ))

  val webtrendsFormattedSchema = StructType(Seq(
    StructField("BFTripType", StringType, true),
    StructField("BFTuDep", IntegerType, true),
    StructField("BFRetDate", StringType, true),
    StructField("date_dt", TimestampType, true),
    StructField("BFDepDate", StringType, true),
    StructField("BFDurStay", StringType, true),
    StructField("cs_user", StringType, true),
    StructField("ed_refdom", StringType, true),
    StructField("Language", StringType, true),
    StructField("BFT", StringType, true),
    StructField("ed_age", StringType, true),
    StructField("Screen", StringType, true),
    StructField("year", IntegerType, true),
    StructField("month", IntegerType, true),
    StructField("day", IntegerType, true),
    StructField("session_guid", StringType, true),
    StructField("BFO", StringType, true),
    StructField("BFD", StringType, true)
  ))

  val webtrendsRelevantinfoSchema = StructType(Seq(
    StructField("bf_trip_type", StringType, true),
    StructField("bf_tu_dep", IntegerType, true),
    StructField("bf_ret_date", IntegerType, true),
    StructField("date_dt_day", IntegerType, true),
    StructField("date_dt_hour", IntegerType, true),
    StructField("bf_dep_date", IntegerType, true),
    StructField("bf_dur_stay", StringType, true),
    StructField("cs_user_browser", StringType, true),
    StructField("cs_user_os", StringType, true),
    StructField("ed_refdom", StringType, true),
    StructField("language", StringType, true),
    StructField("bft", StringType, true),
    StructField("ed_age", StringType, true),
    StructField("year", IntegerType, true),
    StructField("month", IntegerType, true),
    StructField("BFO", StringType, true),
    StructField("BFD", StringType, true),
    StructField("session_guid", StringType, true)
  ))

  val afterLeisureFilteringSchema = StructType(Seq(
    StructField("BFO", StringType, true),
    StructField("BFD", StringType, true),
    StructField("year", IntegerType, true),
    StructField("month", IntegerType, true),
    StructField("session_guid", StringType, true),
    StructField("leisure_score", DoubleType, true)
  ))
}