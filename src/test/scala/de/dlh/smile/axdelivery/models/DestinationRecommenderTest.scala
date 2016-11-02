package de.dlh.smile.axdelivery.models

import de.dlh.smile.axdelivery.Stub
import de.dlh.smile.axdelivery.commons.Transformations
import de.dlh.smile.axdelivery.models.DestinationRecommender.RankingPivotOperation
import de.dlh.smile.engine.commons.Contexts
import org.apache.spark.sql.functions._
import org.scalatest.{FlatSpec, Matchers}

class DestinationRecommenderTest extends FlatSpec with Matchers {

  "Scaling" should "scale the frequencies based on last year" in {
    val df = Stub.dfWebtendsAfterFormat
    val dfTmp = df
      .select("bfo", "bfd", "year", "month", "session_guid")
      .groupBy("bfo", "bfd", "year", "month")
      .agg(countDistinct("session_guid").alias("freq"))
    val dfResult = DestinationRecommender.scaleWithPreviousYear(dfTmp)
    dfResult.filter((col("bfo") === "FRA") and (col("year") === 2014) and (col("month") === 9)).select(col("freq")).take(1)(0)(0) should equal(0.5)
  }


  "MovingAverage" should "compute the number of searches and compute the moving average" in {
    val df = Stub.dfWebtendsAfterFormat
    val dfResult = DestinationRecommender.getMovingAverage(df)
    dfResult.filter((col("bfo") === "FRA") and col("month") === 1).select(col("freq")).take(1)(0)(0) should equal(1.25)
  }

  "DestinationRecommender" should "recommend 16 destinations per origin" in {
    //      val df = Contexts.sqlCtx.read.parquet(getClass.getResource("/data/webtrends").getPath)
    //      val dfAirportMap = Contexts.sqlCtx.read.json(getClass.getResource("/data/airport_codes/airporttocity.json").getPath)
    //      val dfResult = //DestinationRecommender.getRecommendedDestinations(
    //          DestinationRecommender.getMovingAverage(
    //              Transformations.formatAndRegisterDataFrame(df, dfAirportMap)
    //              )
    //          //)
    ////      dfResult.show
    //
    //      dfResult.write.parquet("D:/Users/U553574/Documents/destination_modelV2/src/test/resources/data/outputma.parquet")
    //     // val df = Contexts.sqlCtx.read.parquet("D:/Users/U553574/Documents/destination_modelV2/src/test/resources/data/outputma.parquet")
    ////dfResult.filter(col("BFO") === "JKT" and col("BFD") === "MIL").show()
    // The output looks good
    //    val df = Contexts.sqlCtx.read.parquet("D:/Users/U553574/Documents/destination_modelV2/src/test/resources/data/outputma.parquet")
    //    val dfResult = FormatOutput.formatOutput(DestinationRecommender.getRecommendedDestinations(df))
    //    dfResult.coalesce(1).write.parquet("D:/Users/U553574/Documents/destination_modelV2/src/test/resources/data/outputrecommendations.parquet")
    val df = Contexts.sqlCtx.read.parquet("D:/Users/U553574/Documents/destination_modelV2/src/test/resources/data/outputrecommendations.parquet")
    df.collect().foreach {
      println
    } //should equal(1000)
    // The output looks good
  }







  //	"GroupConcat" should "do the group_concat aggregation" in {
  //		val df = Stub.dfCustomerSales
  //
  //		// get distinct days from data (this assumes there are not too many of them):
  //		val distinctValues: Array[String] = df.select("Day")
  //    .distinct()
  //    .collect()
  //    .map(_.getAs[String]("Day"))
  //
  //    // add column for each day with the Sale value if days match:
  //    val withDayColumns = distinctValues.foldLeft(df) {
  //      case (data, day) => data.selectExpr("*", s"IF(Day = '$day', Month, '') AS $day")
  //    }
  //
  //		val dfResult = withDayColumns
  //     .drop("Day")
  //     .drop("Month")
  //     .groupBy("Customer")
  //     .agg(UDAFGroupConcat(col("Mon")).alias("Mon"),
  //         UDAFGroupConcat(col("Fri")).alias("Fri"))
  //
  //		//val dfResult = df.groupBy("Customer").pivot("Day", Seq("Mon", "Tue", "Wed", "Fri")).agg(UDAFGroupConcat(col("Month")).as("Months"))
  //		dfResult.show()
  //	}
  //
  "GroupConcat2" should "format the sample data in the expected output format" in {
    val df = Stub.dfCustomerSales

    // get distinct days from data (this assumes there are not too many of them):
    val distinctValues: Array[Integer] = df.select("Customer")
      .distinct()
      .collect()
      .map(_.getAs[Integer]("Customer"))

    // add column for each day with the Sale value if days match:
    val withDayColumns = distinctValues.foldLeft(df) {
      case (data, dvalue) => data.selectExpr("*", s"IF(Customer = $dvalue, Month, '') AS _$dvalue")
    }

    val dfResult = withDayColumns
      .drop("Customer")
      .drop("Month")
      .groupBy("Day")
      .agg(RankingPivotOperation(col("_1")).alias("Cust_1"),
        RankingPivotOperation(col("_2")).alias("Cust_2")
      )
    dfResult.show()
  }

  "GroupConcat3" should "format the data in the expected output format" in {
    val df = Stub.dfWebtendsAfterFormat

    val dfTmp = DestinationRecommender.getRecommendedDestinations(
      DestinationRecommender.getMovingAverage(df)
    )
    dfTmp.show

    // get distinct days from data (this assumes there are not too many of them):
    val distinctValues: Array[Integer] = dfTmp.select("mdlrank")
      .distinct()
      .collect()
      .map(_.getAs[Integer]("mdlrank"))

    // add column for each day with the Sale value if days match:
    val withDayColumns = distinctValues.foldLeft(dfTmp) {
      case (data, dvalue) => data.selectExpr("*", s"IF(mdlrank = $dvalue, BFD, '') AS _$dvalue")
    }

    val dfResult = withDayColumns
      .drop("mdlrank")
      .drop("BFD")
      .groupBy("BFO")
      .agg(RankingPivotOperation(col("_0")).alias("search_stream_dest_0"),
        RankingPivotOperation(col("_1")).alias("search_stream_dest_1"),
        RankingPivotOperation(col("_2")).alias("search_stream_dest_2")
        //         UDAFGroupConcat(col("3")).alias("search_stream_dest_3"),
        //         UDAFGroupConcat(col("4")).alias("search_stream_dest_4"),
        //         UDAFGroupConcat(col("5")).alias("search_stream_dest_5"),
        //         UDAFGroupConcat(col("6")).alias("search_stream_dest_6"),
        //         UDAFGroupConcat(col("7")).alias("search_stream_dest_7"),
        //         UDAFGroupConcat(col("8")).alias("search_stream_dest_8"),
        //         UDAFGroupConcat(col("9")).alias("search_stream_dest_9"),
        //         UDAFGroupConcat(col("10")).alias("search_stream_dest_10"),
        //         UDAFGroupConcat(col("11")).alias("search_stream_dest_11"),
        //         UDAFGroupConcat(col("12")).alias("search_stream_dest_12"),
        //         UDAFGroupConcat(col("13")).alias("search_stream_dest_13"),
        //         UDAFGroupConcat(col("14")).alias("search_stream_dest_14"),
        //UDAFGroupConcat(col("_15")).alias("search_stream_dest_15")
      )

    //val dfResult = df.groupBy("Customer").pivot("Day", Seq("Mon", "Tue", "Wed", "Fri")).agg(UDAFGroupConcat(col("Month")).as("Months"))
    dfResult.show()
  }



  "GroupConcat2" should "format the data in the expected output format" in {
    val df = Contexts.sqlCtx.read.parquet(getClass.getResource("/data/webtrends").getPath)
    val dfAirportMap = Contexts.sqlCtx.read.json(getClass.getResource("/data/airport_codes/airporttocity.json").getPath)
    val dfTmp = DestinationRecommender.getRecommendedDestinations(
      DestinationRecommender.getMovingAverage(
        Transformations.formatWebtrendsData(df, dfAirportMap)
      )
    )
    dfTmp.show

    // get distinct days from data (this assumes there are not too many of them):
    val distinctValues: Array[Integer] = dfTmp.select("mdlrank")
      .distinct()
      .collect()
      .map(_.getAs[Integer]("mdlrank"))

    // add column for each day with the Sale value if days match:
    val withDayColumns = distinctValues.foldLeft(dfTmp) {
      case (data, dvalue) => data.selectExpr("*", s"IF(mdlrank = $dvalue, BFD, '') AS _$dvalue")
    }

    val dfResult = withDayColumns
      .drop("mdlrank")
      .drop("BFD")
      .groupBy("BFO")
      .agg(RankingPivotOperation(col("0")).alias("search_stream_dest_0"),
        RankingPivotOperation(col("1")).alias("search_stream_dest_1"),
        RankingPivotOperation(col("2")).alias("search_stream_dest_2"),
        RankingPivotOperation(col("3")).alias("search_stream_dest_3"),
        RankingPivotOperation(col("4")).alias("search_stream_dest_4"),
        RankingPivotOperation(col("5")).alias("search_stream_dest_5"),
        RankingPivotOperation(col("6")).alias("search_stream_dest_6"),
        RankingPivotOperation(col("7")).alias("search_stream_dest_7"),
        RankingPivotOperation(col("8")).alias("search_stream_dest_8"),
        RankingPivotOperation(col("9")).alias("search_stream_dest_9"),
        RankingPivotOperation(col("10")).alias("search_stream_dest_10"),
        RankingPivotOperation(col("11")).alias("search_stream_dest_11"),
        RankingPivotOperation(col("12")).alias("search_stream_dest_12"),
        RankingPivotOperation(col("13")).alias("search_stream_dest_13"),
        RankingPivotOperation(col("14")).alias("search_stream_dest_14"),
        RankingPivotOperation(col("15")).alias("search_stream_dest_15")
      )

    //val dfResult = df.groupBy("Customer").pivot("Day", Seq("Mon", "Tue", "Wed", "Fri")).agg(UDAFGroupConcat(col("Month")).as("Months"))
    dfResult.show()
  }

  "formatOutput" should "format the output and place it in the expected output format" in {
    val df = Stub.dfResultRecommendation
    val dfResult = DestinationRecommender.formatOutput(df)

    dfResult.show
  }
}