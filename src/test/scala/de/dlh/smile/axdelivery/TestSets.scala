package de.dlh.smile.axdelivery

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object Stub {
	lazy val conf = new SparkConf()
	.setAppName("destination_model")
	.setMaster("local")
	.set("spark.yarn.app.id", "destination_model" + "_" + System.currentTimeMillis())

	lazy val sc: SparkContext = new SparkContext(conf)
	lazy val sqlCtx: SQLContext = new SQLContext(sc)

	val dfFilterDate = sqlCtx.createDataFrame(sc.parallelize(TestSets.dateFilterSeq), Schemas.dateFilterSchema)
	val dfStringDate = sqlCtx.createDataFrame(sc.parallelize(TestSets.dateStringSeq), Schemas.dateStringSchema)
	val dfStringIATA = sqlCtx.createDataFrame(sc.parallelize(TestSets.IATAStringSeq), Schemas.IATAStringSchema)
	val dfStringOrigin = sqlCtx.createDataFrame(sc.parallelize(TestSets.originStringSeq), Schemas.IATAStringSchema)
	val dfMapDate = sqlCtx.createDataFrame(sc.parallelize(TestSets.dateMapSeq), Schemas.dateMapSchema)
	val dfCustomerSales = sqlCtx.createDataFrame(sc.parallelize(TestSets.customerSalesSeq), Schemas.customerSalesSchema)
	val dfWebtendsAfterFormat = sqlCtx.createDataFrame(sc.parallelize(TestSets.webtrendsAfterFormatSeq), Schemas.webtrendsAfterFormatSchema)
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
      Row("RT", 49, "20160324", "2015-11-15 18:57:...", "20160103", "81", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2015, 11, 15, "be734313-0a88-40e...", "MAD", "SAO"),
      Row("RT", 65, "20160327", "2015-11-15 22:04:...", "20160119", "68", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2015, 11, 15, "187c293b-3c58-45e...", "FRA", "SAO"),
      Row("RT", 65, "20160325", "2015-11-15 22:08:...", "20160119", "66", "Mozilla/5.0+(Wind...", "www.google.it", "it", "IK;IK", "0", "FOFP", 2015, 11, 15, "187c293b-3c58-45e...", "FRA", "SAO"),
      Row("RT", 94, "20160303", "2015-11-15 19:18:...", "20160217", "15", "Mozilla/5.0+(Maci...", "Direct%20Traffic", "en", "IK;IK", "104", "FOFP", 2015, 11, 15, "e4ec7169-2a3d-4c1...", "null", "null"),
      Row("RT", 94, "20160303", "2015-11-15 19:24:...", "20160217", "15", "Mozilla/5.0+(Maci...", "Direct%20Traffic", "en", "IK;IK", "104", "FOFP", 2015, 11, 15, "e4ec7169-2a3d-4c1...", "null", "null"),
      Row("OW", 94, "20160303", "2015-11-15 19:26:...", "20160217", "15", "Mozilla/5.0+(Maci...", "Direct%20Traffic", "en", "IK;IK", "104", "FOFP", 2015, 11, 15, "e4ec7169-2a3d-4c1...", "null", "null"),
      Row("RT", 94, "20160303", "2015-11-15 20:02:...", "20160217", "15", "Mozilla/5.0+(Maci...", "Direct%20Traffic", "en", "IK;IK", "104", "FOFP", 2015, 11, 15, "e4ec7169-2a3d-4c1...", "null", "null"),
      Row("RT", 76, "20160214", "2015-11-15 22:59:...", "20160130", "15", "Mozilla/5.0+(Wind...", "Direct%20Traffic", "de", "IK;IK", "0", "FOFP", 2015, 11, 15, "1fe7b6b8-e359-4bd...", "null", "null"),
      Row("RT", 77, "20160212", "2015-11-15 14:14:...", "20160131", "12", "Mozilla/5.0+(Wind...", "www.google.de", "de", "IK;IK", "0", "FOFP", 2015, 11, 15, "057e1660-b9cf-41d...", "null", "null"),
      Row("RT", 130, "20160410", "2015-11-15 09:22:...", "20160324", "16", "Mozilla/5.0+(Maci...", "www.google.de", "de", "IK;IK", "0", "FOFP", 2015, 11, 15, "e6514a3a-56c9-4c3...", "null", "null"),
      Row("RT", 130, "20160411", "2015-11-15 09:23:...", "20160324", "17", "Mozilla/5.0+(Maci...", "www.google.de", "de", "IK;IK", "0", "FOFP", 2015, 11, 15, "e6514a3a-56c9-4c3...", "null", "null"),
      Row("RT", 130, "20160410", "2015-11-15 09:28:...", "20160324", "16", "Mozilla/5.0+(Maci...", "www.google.de", "de", "IK;IK", "0", "FOFP", 2015, 11, 15, "e6514a3a-56c9-4c3...", "null", "null"),
      Row("RT", 130, "20160410", "2015-11-15 09:29:...", "20160324", "16", "Mozilla/5.0+(Maci...", "www.google.de", "de", "IK;IK", "0", "FOFP", 2015, 11, 15, "e6514a3a-56c9-4c3...", "null", "null"),
      Row("RT", 129, "20160328", "2015-11-15 22:32:...", "20160323", "4", "Mozilla/5.0+(Maci...", "Direct%20Traffic", "de", "K;K", "0", "FOFP", 2015, 11, 15, "b9845589-0b99-4f7...", "null", "null"),
      Row("RT", 157, "20160504", "2015-11-15 15:38:...", "20160420", "14", "Mozilla/5.0+(Wind...", "www.google.co.il", "en", "IK;IK", "327", "FOFP", 2015, 11, 15, "eb06551f-222e-4e1...", "null", "null"),
      Row("RT", 24, "20151210", "2015-11-15 15:58:...", "20151209", "1", "Mozilla/5.0+(Linu...", "Direct%20Traffic", "de", "D;D", "0", "FOFP", 2015, 11, 15, "dcc9f165-e6ae-4aa...", "null", "null"),
      Row("RT", 7, "20160116", "2015-11-15 20:05:...", "20160101", "15", "Mozilla/5.0+(X11;...", "Direct%20Traffic", "de", "K;K", "0", "FOFP", 2015, 11, 15, "9da674f4-97f6-441...", "null", "null"),
      Row("RT", 1, "20160101", "2015-11-15 23:11:...", "20151116", "46", "Mozilla/5.0+(Wind...", "www.google.pl", "pl", "IK;IK", "0", "FOFP", 2015, 11, 15, "2c782716-e190-412...", "null", "null"),
      Row("RT", 121, "20160319", "2015-11-15 11:14:...", "20160315", "4", "Mozilla/5.0+(Wind...", "www.google.fr", "fr", "IK;IK", "4", "FOFP", 2015, 11, 15, "2b67c82b-11ac-4fb...", "null", "null"),
      Row("RT", 166, "20160515", "2015-11-15 17:25:...", "20160429", "16", "Mozilla/5.0+(Wind...", "www.google.de", "de", "K;K", "0", "FOFP", 2015, 11, 15, "ff4fa86c-6855-425...", "null", "null")
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
}