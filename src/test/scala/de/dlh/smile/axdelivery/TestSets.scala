package de.dlh.smile.axdelivery

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
  val dfInput = Contexts.sqlCtx.read.parquet(getClass.getResource("/data/webtrends").getPath)
  val dfAirportCodes = Contexts.sqlCtx.read.json(getClass.getResource("/data/airport_codes").getPath)
  val dfBeforeModel = Contexts.sqlCtx.read.parquet(getClass.getResource("/data/training").getPath)
  val dfCustomerSales = Contexts.sqlCtx.createDataFrame(Contexts.sc.parallelize(TestSets.customerSalesSeq), Schemas.customerSalesSchema)
	val dfWebtendsAfterFormat = Contexts.sqlCtx.createDataFrame(Contexts.sc.parallelize(TestSets.webtrendsAfterFormatSeq), Schemas.webtrendsAfterFormatSchema)
	val dfResultRecommendation = Contexts.sqlCtx.createDataFrame(Contexts.sc.parallelize(TestSets.resultRecommendationSeq), Schemas.resultRecommendationSchema)
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
			Row(1 , "Mon", 12.0, "Jan"),
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
//      Row("RT", 49, "20160324", "2015-11-15 18:57:...", "20160103", "81", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2015, 11, 15, "be734313-0a88-40e...", "MAD", "SAO"),
//      Row("RT", 65, "20160327", "2015-11-15 22:04:...", "20160119", "68", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2015, 11, 15, "187c293b-3c58-45e...", "FRA", "SAO"),
//      Row("RT", 65, "20160325", "2015-11-15 22:08:...", "20160119", "66", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2015, 11, 15, "187c293b-3c58-45e...", "FRA", "SAO"),
//      Row("RT", 94, "20160303", "2015-11-15 19:18:...", "20160217", "15", "Mozilla/5.0+(Maci...", "Direct%20Traffic", "en", "IK;IK", "104", "FOFP", 2015, 11, 15, "e4ec7169-2a3d-4c1...", "null", "null"),
//      Row("RT", 94, "20160303", "2015-11-15 19:24:...", "20160217", "15", "Mozilla/5.0+(Maci...", "Direct%20Traffic", "en", "IK;IK", "104", "FOFP", 2015, 11, 15, "e4ec7169-2a3d-4c1...", "null", "null"),
//      Row("OW", 94, "20160303", "2015-11-15 19:26:...", "20160217", "15", "Mozilla/5.0+(Maci...", "Direct%20Traffic", "en", "IK;IK", "104", "FOFP", 2015, 11, 15, "e4ec7169-2a3d-4c1...", "null", "null"),
//      Row("RT", 94, "20160303", "2015-11-15 20:02:...", "20160217", "15", "Mozilla/5.0+(Maci...", "Direct%20Traffic", "en", "IK;IK", "104", "FOFP", 2015, 11, 15, "e4ec7169-2a3d-4c1...", "null", "null"),
//      Row("RT", 76, "20160214", "2015-11-15 22:59:...", "20160130", "15", "Mozilla/5.0+(Wind...", "Direct%20Traffic", "de", "IK;IK", "0", "FOFP", 2015, 11, 15, "1fe7b6b8-e359-4bd...", "null", "null"),
//      Row("RT", 77, "20160212", "2015-11-15 14:14:...", "20160131", "12", "Mozilla/5.0+(Wind...", "www.google.de", "de", "IK;IK", "0", "FOFP", 2015, 11, 15, "057e1660-b9cf-41d...", "null", "null"),
//      Row("RT", 130, "20160410", "2015-11-15 09:22:...", "20160324", "16", "Mozilla/5.0+(Maci...", "www.google.de", "de", "IK;IK", "0", "FOFP", 2015, 11, 15, "e6514a3a-56c9-4c3...", "null", "null"),
//      Row("RT", 130, "20160411", "2015-11-15 09:23:...", "20160324", "17", "Mozilla/5.0+(Maci...", "www.google.de", "de", "IK;IK", "0", "FOFP", 2015, 11, 15, "e6514a3a-56c9-4c3...", "null", "null"),
//      Row("RT", 130, "20160410", "2015-11-15 09:28:...", "20160324", "16", "Mozilla/5.0+(Maci...", "www.google.de", "de", "IK;IK", "0", "FOFP", 2015, 11, 15, "e6514a3a-56c9-4c3...", "null", "null"),
//      Row("RT", 130, "20160410", "2015-11-15 09:29:...", "20160324", "16", "Mozilla/5.0+(Maci...", "www.google.de", "de", "IK;IK", "0", "FOFP", 2015, 11, 15, "e6514a3a-56c9-4c3...", "null", "null"),
//      Row("RT", 129, "20160328", "2015-11-15 22:32:...", "20160323", "4", "Mozilla/5.0+(Maci...", "Direct%20Traffic", "de", "K;K", "0", "FOFP", 2015, 11, 15, "b9845589-0b99-4f7...", "null", "null"),
//      Row("RT", 157, "20160504", "2015-11-15 15:38:...", "20160420", "14", "Mozilla/5.0+(Wind...", "www.google.co.il", "en", "IK;IK", "327", "FOFP", 2015, 11, 15, "eb06551f-222e-4e1...", "null", "null"),
//      Row("RT", 24, "20151210", "2015-11-15 15:58:...", "20151209", "1", "Mozilla/5.0+(Linu...", "Direct%20Traffic", "de", "D;D", "0", "FOFP", 2015, 11, 15, "dcc9f165-e6ae-4aa...", "null", "null"),
//      Row("RT", 7, "20160116", "2015-11-15 20:05:...", "20160101", "15", "Mozilla/5.0+(X11;...", "Direct%20Traffic", "de", "K;K", "0", "FOFP", 2015, 11, 15, "9da674f4-97f6-441...", "null", "null"),
//      Row("RT", 1, "20160101", "2015-11-15 23:11:...", "20151116", "46", "Mozilla/5.0+(Wind...", "www.google.pl", "pl", "IK;IK", "0", "FOFP", 2015, 11, 15, "2c782716-e190-412...", "null", "null"),
//      Row("RT", 121, "20160319", "2015-11-15 11:14:...", "20160315", "4", "Mozilla/5.0+(Wind...", "www.google.fr", "fr", "IK;IK", "4", "FOFP", 2015, 11, 15, "2b67c82b-11ac-4fb...", "null", "null"),
//      Row("RT", 166, "20160515", "2015-11-15 17:25:...", "20160429", "16", "Mozilla/5.0+(Wind...", "www.google.de", "de", "K;K", "0", "FOFP", 2015, 11, 15, "ff4fa86c-6855-425...", "null", "null"),
//      Row("RT", 49, "20160324", "2015-12-15 18:57:...", "20160103", "81", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2015, 12, 15, "be734313-0a88-40e...", "MAD", "SAO"),
//      Row("RT", 65, "20160327", "2015-12-15 22:04:...", "20160119", "68", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2015, 12, 15, "187c293b-3c58-45e...", "FRA", "SAO"),
//      Row("RT", 65, "20160325", "2015-12-15 22:08:...", "20160119", "66", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2015, 12, 15, "187c293b-3c58-45e...", "FRA", "SAO"),
//      Row("RT", 49, "20160324", "2015-10-15 18:57:...", "20160103", "81", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2015, 10, 15, "be734313-0a88-40e...", "MAD", "SAO"),
//      Row("RT", 65, "20160327", "2015-10-15 22:04:...", "20160119", "68", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2015, 10, 15, "187c293b-3c58-45e...", "FRA", "SAO"),
//      Row("RT", 65, "20160325", "2015-10-15 22:08:...", "20160119", "66", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2015, 10, 15, "187c293b-3c58-45e...", "FRA", "SAO"),
      Row("RT", 49, "20160324", "2015-1-15 18:57:...", "20160103", "81", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2015, 1, 15, "be734313-0a88-40e...", "MAD", "SAO"),
      Row("RT", 65, "20160327", "2015-1-15 22:04:...", "20160119", "68", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2015, 1, 15, "187c293b-3c58-45e...", "FRA", "SAO"),
      Row("RT", 65, "20160325", "2015-1-15 22:08:...", "20160119", "66", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2015, 1, 15, "187c293b-3c58-45e...", "FRA", "SAO"),
      Row("RT", 49, "20160324", "2015-2-15 18:57:...", "20160103", "81", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2015, 2, 15, "be734313-0a88-40e...", "MAD", "SAO"),
      Row("RT", 65, "20160327", "2015-2-15 22:04:...", "20160119", "68", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2015, 2, 15, "187c293b-3c58-45e...", "FRA", "SAO"),
      Row("RT", 65, "20160325", "2015-2-15 22:08:...", "20160119", "66", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2015, 2, 15, "187c293b-3c58-45e...", "FRA", "SAO"),
      Row("RT", 49, "20160324", "2015-3-15 18:57:...", "20160103", "81", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2015, 3, 15, "be734313-0a88-40e...", "MAD", "SAO"),
      Row("RT", 65, "20160327", "2015-3-15 22:04:...", "20160119", "68", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2015, 3, 15, "187c293b-3c58-45e...", "FRA", "SAO"),
      Row("RT", 65, "20160325", "2015-3-15 22:08:...", "20160119", "66", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2015, 3, 15, "187c293b-3c58-45e...", "FRA", "SAO"),
      Row("RT", 65, "20160327", "2014-12-15 22:04:...", "20160119", "68", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2014, 12, 15, "187c293b-3c58-45e...", "FRA", "SAO"),
      Row("RT", 65, "20160327", "2014-12-15 22:04:...", "20160119", "68", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2014, 12, 15, "187c293c58-45e...", "FRA", "SAO"),
      Row("RT", 65, "20160327", "2014-11-15 22:04:...", "20160119", "68", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2014, 11, 15, "187c293b-3c58-45e...", "FRA", "SAO"),
      Row("RT", 65, "20160327", "2014-10-15 22:04:...", "20160119", "68", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2014, 10, 15, "187c293b-3c58-45e...", "FRA", "SAO"),
      Row("RT", 65, "20160327", "2014-10-15 22:04:...", "20160119", "68", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2014, 10, 15, "187c293b-3sdasc58-45e...", "FRA", "SAO"),
      Row("RT", 65, "20160327", "2014-9-15 22:04:...", "20160119", "68", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2014, 9, 15, "187c293b-3c58-45e...", "FRA", "SAO"),
      Row("RT", 65, "20160327", "2014-8-15 22:04:...", "20160119", "68", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2014, 8, 15, "187c293b-3c58-45e...", "FRA", "SAO"),
      Row("RT", 65, "20160327", "2014-7-15 22:04:...", "20160119", "68", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2014, 7, 15, "187c293b-3c58-45e...", "FRA", "SAO"),
      Row("RT", 65, "20160327", "2014-6-15 22:04:...", "20160119", "68", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2014, 6, 15, "187c293b-3c58-45e...", "FRA", "SAO"),
      Row("RT", 65, "20160327", "2014-5-15 22:04:...", "20160119", "68", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2014, 5, 15, "187c293b-3c58-45e...", "FRA", "SAO"),
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
}