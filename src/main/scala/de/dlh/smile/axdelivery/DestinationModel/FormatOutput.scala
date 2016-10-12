package de.dlh.smile.axdelivery.DestinationModel

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import de.dlh.smile.axdelivery.DestinationModel.DataFrameCommons._

object FormatOutput {
  def formatOutput(df: DataFrame, pivotCol: String): DataFrame = {
    val distinctValues: Array[Integer] = df.select(pivotCol)
    .distinct()
    .collect()
    .map(_.getAs[Integer](pivotCol))
    
    // add column for each day with the Sale value if days match:
    val withDayColumns = distinctValues.foldLeft(dfTmp) { 
      case (data, dvalue) => data.selectExpr("*", s"IF(mdlrank = $dvalue, BFD, '') AS $dvalue")
    }
		
		val dfResult = withDayColumns
     .drop("mdlrank")
     .drop("BFD")
     .groupBy("BFO")
     .agg(UDAFGroupConcat(col("0")).alias("search_stream_dest_0"),
         UDAFGroupConcat(col("1")).alias("search_stream_dest_1"),
         UDAFGroupConcat(col("2")).alias("search_stream_dest_2"),
         UDAFGroupConcat(col("3")).alias("search_stream_dest_3"),
         UDAFGroupConcat(col("4")).alias("search_stream_dest_4"),
         UDAFGroupConcat(col("5")).alias("search_stream_dest_5"),
         UDAFGroupConcat(col("6")).alias("search_stream_dest_6"),
         UDAFGroupConcat(col("7")).alias("search_stream_dest_7"),
         UDAFGroupConcat(col("8")).alias("search_stream_dest_8"),
         UDAFGroupConcat(col("9")).alias("search_stream_dest_9"),
         UDAFGroupConcat(col("10")).alias("search_stream_dest_10"),
         UDAFGroupConcat(col("11")).alias("search_stream_dest_11"),
         UDAFGroupConcat(col("12")).alias("search_stream_dest_12"),
         UDAFGroupConcat(col("13")).alias("search_stream_dest_13"),
         UDAFGroupConcat(col("14")).alias("search_stream_dest_14"),
         UDAFGroupConcat(col("15")).alias("search_stream_dest_15")
         )
  }
}