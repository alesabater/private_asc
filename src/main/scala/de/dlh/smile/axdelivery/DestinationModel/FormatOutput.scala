package de.dlh.smile.axdelivery.DestinationModel

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import de.dlh.smile.axdelivery.DestinationModel.DataFrameCommons._

object FormatOutput {
  def formatOutput(df: DataFrame, pivotCol: String = "mdlrank"): DataFrame = {
//		// get distinct days from data (this assumes there are not too many of them):
//		val distinctValues: Array[Integer] = df.select(pivotCol)
//    .distinct()
//    .collect()
//    .map(_.getAs[Integer](pivotCol))
//    
//    // add column for each day with the Sale value if days match:
//    val withDayColumns = distinctValues.foldLeft(df) { 
//      case (data, dvalue) => data.selectExpr("*", s"IF(mdlrank = $dvalue, BFD, '') AS _$dvalue")
//    }
//		
//		val dfResult = withDayColumns
//     .drop("mdlrank")
//     .drop("BFD")
//     .groupBy("BFO")
//     .agg(UDAFGroupConcat(col("_0")).alias("search_stream_dest_0"),
//         UDAFGroupConcat(col("_1")).alias("search_stream_dest_1"),
//         UDAFGroupConcat(col("_2")).alias("search_stream_dest_2")
//         UDAFGroupConcat(col("_3")).alias("search_stream_dest_3"),
//         UDAFGroupConcat(col("_4")).alias("search_stream_dest_4"),
//         UDAFGroupConcat(col("_5")).alias("search_stream_dest_5"),
//         UDAFGroupConcat(col("_6")).alias("search_stream_dest_6"),
//         UDAFGroupConcat(col("_7")).alias("search_stream_dest_7"),
//         UDAFGroupConcat(col("_8")).alias("search_stream_dest_8"),
//         UDAFGroupConcat(col("_9")).alias("search_stream_dest_9"),
//         UDAFGroupConcat(col("_10")).alias("search_stream_dest_10"),
//         UDAFGroupConcat(col("_11")).alias("search_stream_dest_11"),
//         UDAFGroupConcat(col("_12")).alias("search_stream_dest_12"),
//         UDAFGroupConcat(col("_13")).alias("search_stream_dest_13"),
//         UDAFGroupConcat(col("_14")).alias("search_stream_dest_14"),
//         UDAFGroupConcat(col("_15")).alias("search_stream_dest_15")
//         )
//
//		dfResult
    df
  }
}