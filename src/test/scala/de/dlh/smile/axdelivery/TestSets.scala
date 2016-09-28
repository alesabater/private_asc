package de.dlh.smile.axdelivery

import de.dlh.smile.engine.commons.Contexts
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object Stub {
  val dfFilterDate = Contexts.sqlCtx.createDataFrame(Contexts.sc.parallelize(TestSets.dateFilterSeq), Schemas.dateFilterSchema)
  val dfStringDate = Contexts.sqlCtx.createDataFrame(Contexts.sc.parallelize(TestSets.dateStringSeq), Schemas.dateStringSchema)
  val dfMapDate = Contexts.sqlCtx.createDataFrame(Contexts.sc.parallelize(TestSets.dateMapSeq), Schemas.dateMapSchema)
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

  val dateMapSeq = Seq(
    Row("2016-09-15", Map("k1" -> "20160927", "k2" -> "val")),
    Row("2016-09-15", Map("k1" -> "20160926", "k2" -> "val")),
    Row("2016-09-30", Map("k1" -> "20160926", "k2" -> "val")),
    Row("2015-01-15", Map("k1" -> "20160926", "k2" -> "val")),
    Row(null, Map("k1" -> "20160926", "k2" -> "val")),
    Row("2016-09-15", Map("k2" -> "val")),
    Row("2016-09-15", Map("k1" -> null, "k2" -> "val"))
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

  val dateMapSchema = StructType(Seq(
    StructField("date_prev", StringType, true),
    StructField("date_dep", MapType(StringType,StringType,true), true)
  ))
}