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

    // it would be convenient to keep here the only month that we are interested in order to reduce computation time
    val dfRankedPredictionMonth = dfRankedByOnD.filterPartitionFieldsOneMonth(2015, 11)
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