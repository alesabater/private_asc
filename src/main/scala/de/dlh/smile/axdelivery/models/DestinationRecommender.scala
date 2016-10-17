package de.dlh.smile.axdelivery.models

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import de.dlh.smile.axdelivery.commons.DataFrameOperations._


object DestinationRecommender {

  def recommend(df: DataFrame): DataFrame = ???

  def scaleWithPreviousYear(df: DataFrame): DataFrame = {
    val year = df.select(col("year"), col("month")).distinct().sort(col("year"), col("month")).select(col("year")).first().getInt(0)
    val month = df.select(col("year"), col("month")).distinct().sort(col("year"), col("month")).select(col("month")).first().getInt(0)
    val currentMonth = df.filter((col("year") === year + 1) and (col("month") === month))
      .select(col("BFO").alias("current_BFO"),
        col("BFD").alias("current_BFD"),
        col("year").alias("current_year"),
        col("month").alias("current_month"),
        col("freq").alias("current_freq"))
    val lastYearMonth = df.filter((col("year") === year) and (col("month") === month))
      .select(col("BFO").alias("last_BFO"),
        col("BFD").alias("last_BFD"),
        col("year").alias("last_year"),
        col("month").alias("last_month"),
        col("freq").alias("last_freq"))
    val yearFactor = currentMonth.join(lastYearMonth,
      (currentMonth("current_BFO") === lastYearMonth("last_BFO")) and (currentMonth("current_BFD") === lastYearMonth("last_BFD")),
      "inner")
      .select(col("current_BFO").alias("factor_BFO"),
        col("current_BFD").alias("factor_BFD"),
        (col("current_freq") / col("last_freq")).alias("factor"),
        col("last_year").alias("factor_year")).distinct()

    val dfResult = yearFactor.rdd.isEmpty() match {
      case true => df.select(col("BFO"),
        col("BFD"),
        col("year"),
        col("month"),
        col("freq"))
      case false => df.filter((col("year") !== year) or (col("month") !== month))
        .join(yearFactor,
          (df("year") === yearFactor("factor_year")) and (df("BFO") === yearFactor("factor_BFO")) and (df("BFD") === yearFactor("factor_BFD")),
          "left")
        .select(col("BFO"),
          col("BFD"),
          col("year"),
          col("month"),
          //col("freq"),
          //col("factor"),
          (col("freq") * coalesce(col("factor"), lit(1))).alias("freq"))
    }

    dfResult
  }

  def getMovingAverage(df: DataFrame): DataFrame = {
    // Compute number of searches per OnD, year and month
    val dfTmp = df
      .select("BFO", "BFD", "year", "month", "session_guid")
      .groupBy("BFO", "BFD", "year", "month")
      .agg(countDistinct("session_guid").alias("freq"))

    // Scale based in last year data
    val dfScaledTmp = scaleWithPreviousYear(dfTmp)

    // The months of Jan and Dec do not have enough data points to compute the moving average
    // for that reason we add month 0 (same as Dec) and month 13 (same as Jan) for the computations
    val dfTmpEnlarged = dfScaledTmp.unionAll(
      dfScaledTmp.filter(col("month") === 12).select(col("BFO"), col("BFD"), (col("year") + 1).alias("year"), lit(0).alias("month"), col("freq")))
      .unionAll(dfScaledTmp.filter(col("month") === 1).select(col("BFO"), col("BFD"), (col("year") - 1).alias("year"), lit(13).alias("month"), col("freq")))

    // Compute the moving average 1/4, 1/2, 1/4
    val windowSpec = Window.partitionBy(col("BFO"), col("BFD")).orderBy(col("month")).rangeBetween(-1, 1)
    val moving_average = sum(col("freq")).over(windowSpec)
    val dfResult = dfTmpEnlarged.select(
      col("BFO"),
      col("BFD"),
      col("year"),
      col("month"),
      col("freq"),
      moving_average.alias("smoothedfreq"))
      .select(col("BFO"),
        col("BFD"),
        col("year"),
        col("month"),
        ((col("smoothedfreq") + col("freq")) / 4).alias("freq")
      )
      .filter((col("month") >= 1) and col("month") <= 12) // remove the fake months that we added
    dfResult
  }

  def getRecommendedDestinations(df: DataFrame): DataFrame = {
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
    val yearPred = df.select(col("year"), col("month")).distinct().sort(col("year"), col("month")).select(col("year")).first().getInt(0)
    val monthPred = df.select(col("year"), col("month")).distinct().sort(col("year"), col("month")).select(col("month")).first().getInt(0)
    val dfRankedPredictionMonth = dfRankedByOnD.filterPartitionFieldsOneMonth(yearPred, monthPred)

    // Compute the model rank as a combination of the previous two & filter keeping only the top 16
    val dfResultScoring = dfRankedPredictionMonth.select(col("BFO"), col("BFD"), col("year"), col("month"), (col("rank") + col("monthrank") * col("monthrank")).alias("mdlrank"))
    val byOriginMdlRank = Window
      .partitionBy(col("BFO"), col("year"), col("month"))
      .orderBy(col("mdlrank"))
    val dfResult = dfResultScoring
      .withColumn("mdlrank", rowNumber over byOriginMdlRank)
      .filter("mdlrank <= 16")
      .select(col("BFO"), col("BFD"), col("year"), col("month"), (col("mdlrank") - 1).alias("mdlrank"))

    dfResult
  }

  def formatOutput(df: DataFrame,
                   pivotCol: String = "mdlrank",
                   concatCol: String = "BFD",
                   rowCol: String = "BFO"): DataFrame = {
    // get distinct days from data (this assumes there are not too many of them):
    //		val distinctValues: Array[Integer] = df.select(pivotCol)
    //    .distinct()
    //    .collect()
    //    .map(_.getAs[Integer](pivotCol))
    //
    val distinctValues = Array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)

    val dfRenamend = df.select(
      col(pivotCol),
      col(concatCol),
      col(rowCol).alias("origin"))

    // add column for each day with the Sale value if days match:
    val withDayColumns = distinctValues.foldLeft(dfRenamend) {
      case (data, dvalue) => data.selectExpr("*", s"IF($pivotCol = $dvalue, $concatCol, '') AS _$dvalue")
    }

    //		import scala.collection.mutable.ArrayBuffer
    //		val arrayUDAFs = ArrayBuffer[Any]()
    //    val distinctValues = Array(0,1,2,3)
    //    for (value <- distinctValues) {
    //      arrayUDAFs += value
    //    }
    //    arrayUDAFs.foreach{println}

    val arrayfunctions =
      Array(
        RankingPivotOperation(col("_0")).alias("search_stream_dest_0"),
        RankingPivotOperation(col("_1")).alias("search_stream_dest_1"),
        RankingPivotOperation(col("_2")).alias("search_stream_dest_2"),
        RankingPivotOperation(col("_3")).alias("search_stream_dest_3"),
        RankingPivotOperation(col("_4")).alias("search_stream_dest_4"),
        RankingPivotOperation(col("_5")).alias("search_stream_dest_5"),
        RankingPivotOperation(col("_6")).alias("search_stream_dest_6"),
        RankingPivotOperation(col("_7")).alias("search_stream_dest_7"),
        RankingPivotOperation(col("_8")).alias("search_stream_dest_8"),
        RankingPivotOperation(col("_9")).alias("search_stream_dest_9"),
        RankingPivotOperation(col("_10")).alias("search_stream_dest_10"),
        RankingPivotOperation(col("_11")).alias("search_stream_dest_11"),
        RankingPivotOperation(col("_12")).alias("search_stream_dest_12"),
        RankingPivotOperation(col("_13")).alias("search_stream_dest_13"),
        RankingPivotOperation(col("_14")).alias("search_stream_dest_14"),
        RankingPivotOperation(col("_15")).alias("search_stream_dest_15")
      )

    val dfResult = withDayColumns
      .drop(pivotCol)
      .drop(concatCol)
      .groupBy(col("origin"))
      .agg(arrayfunctions.head, arrayfunctions.tail: _*)

    dfResult
  }


  object RankingPivotOperation extends UserDefinedAggregateFunction {
    // input data type schema
    def inputSchema: StructType = StructType(Array(StructField("item", StringType)))

    // intermediate schema
    def bufferSchema = StructType(Array(
      StructField("sum", StringType),
      StructField("cnt", LongType)
    ))

    // Returned data type
    def dataType: DataType = StringType

    // self-explaining
    def deterministic = true

    // this function is called whenever key changes
    def initialize(buffer: MutableAggregationBuffer) = {
      buffer(0) = "" // set sum to zero
      buffer(1) = 0L // set number of items to 0
    }

    // iterate over each entry of a group
    def update(buffer: MutableAggregationBuffer, input: Row) = {
      val stringInput = input.getString(0) match {
        case "" | "null" | null => ""
        case _ => input.getString(0)
      }
      buffer(0) = buffer.getString(0) + ";" + stringInput
      buffer(1) = buffer.getLong(1) + 1
    }

    // merge two partial aggregates
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
      buffer1(0) = buffer1.getString(0) + ";" + buffer2.getString(0)
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    // called after all the entries are exhausted
    def evaluate(buffer: Row) = {
      buffer.getString(0).replaceAll("^;+", "").replaceAll(";+$", "").replaceAll(";+", ";")
    }

  }

}
