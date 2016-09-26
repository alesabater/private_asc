package de.dlh.smile.axdelivery

import de.dlh.smile.engine.commons.Contexts
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Stub {

  val dfFilterDate = Contexts.sqlCtx.createDataFrame(Contexts.sc.parallelize(TestSets.dateFilterSeq), Schemas.dateFilterSchema)

}

object TestSets {

  val dateFilterSeq = Seq(
    Row(2016, 9, "a"),
    Row(2015, 3, "b"),
    Row(2016, 6, "c"))

}

object Schemas {

  val dateFilterSchema = StructType(Seq(
    StructField("year", IntegerType, true),
    StructField("month", IntegerType, true),
    StructField("data", StringType, true)
  ))
}