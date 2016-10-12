package de.dlh.smile.axdelivery.DestinationModel

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import de.dlh.smile.axdelivery.DestinationModel.DataFrameCommons._

object DestinationRecommender {
  def getRecommendedDestinations(df: DataFrame) : DataFrame = {
    // Compute the rank and keep up to rank 50
    val byOrigin = Window
      .partitionBy(col("BFO"), col("year"), col("month"))
      .orderBy(-col("freq"))
    val dfRankedByO = df
      .withColumn("rank", rank over byOrigin)
      .filter("rank <= 50")

    // Compute month rank by origin and destination pair (need the full year for this)
    val byOnD = Window
      .partitionBy(col("BFO"), col("BFD"))
      .orderBy(-col("freq"))
    val dfRankedByOnD = dfRankedByO
      .withColumn("monthrank", rank over byOnD)

    // It is convenient to keep here the only month that we are interested in order to reduce computation time
    val year = df.select(col("year"), col("month")).distinct().sort(col("year"), col("month")).select(col("year")).first().getInt(0)
		val month = df.select(col("year"), col("month")).distinct().sort(col("year"), col("month")).select(col("month")).first().getInt(0)
    val dfRankedPredictionMonth = dfRankedByOnD.filterPartitionFieldsOneMonth(year, month)
    
    // Compute the model rank as a combination of the previous two & filter keeping only the top 16
    val dfResultScoring = dfRankedPredictionMonth.select(col("BFO"), col("BFD"), col("year"), col("month"), (col("rank") + col("monthrank") * col("monthrank")).alias("mdlrank"))
    val byOriginMdlRank = Window
      .partitionBy(col("BFO"), col("year"), col("month"))
      .orderBy(col("mdlrank"))
    val dfResult = dfResultScoring
      .withColumn("mdlrank", rank over byOriginMdlRank)
      .filter("mdlrank <= 16")
      .select(col("BFO"), col("BFD"), col("year"), col("month"), (col("mdlrank") - 1).alias("mdlrank"))
    
    dfResult
  }
}