/*
////////////////////////////////
// Initialization of Contexts //
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import org.apache.spark.sql.hive.HiveContext
val hiveContext = new HiveContext(sc)
import sqlContext.implicits._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.mllib.rdd.RDDFunctions._


// Read properties


///////////////////////////////
// Data load and preparation //
val alllogs = sqlContext.read.parquet(datalocation)
alllogs.registerTempTable("alllogs")


// select only the interesting variables
val selection = hiveContext.sql("""
    select
        cs_uri_query['BFTripType'] as BFTripType ,
        datediff(to_date(concat_ws('-', substr(cs_uri_query['BFDepDate'], 0, 4), substr(cs_uri_query['BFDepDate'], 5, 2), substr(cs_uri_query['BFDepDate'], 7, 2))), to_date(date_dt)) as BFTuDep,
        cs_uri_query['BFRetDate'] as BFRetDate ,
        date_dt  ,
        cs_uri_query['BFDepDate'] as BFDepDate ,
        cs_uri_query['BFDurStay']  as BFDurStay,
        cs_user  ,
        cs_uri_query['ed_refdom']  as ed_refdom,
        cs_uri_query['Language']  as Language,
        cs_uri_query['BFT'] as BFT,
        cs_uri_query['ed_age'] as ed_age,
        cs_uri_query['BFO'] as BFO,
        cs_uri_query['BFD'] as BFD,
        cs_uri_query['Screen'] as Screen,
        year,
        month,
        day,
        session_guid
    from alllogs
""")



// filter and keep only the screen FOFP and the relevant dates, up to one year in the past. One year is needed to compute the month rank
val filter1 = selection.filter("Screen = 'FOFP' and ((year = " + year + " and month < " + month + ") or (year + 1 = " + year + " and month >= " + month + "))")


// Replace airport codes by city codes
filter1.registerTempTable("weblogs")
airporttocity.registerTempTable("airporttocity")

val filter2 = hiveContext.sql("""
    select
        BFTripType ,
        BFTuDep,
        BFRetDate ,
        date_dt  ,
        BFDepDate ,
        BFDurStay,
        cs_user  ,
        ed_refdom,
        Language,
        BFT,
        ed_age,
        coalesce(b.City, substr(a.BFO, 1, 3)) as BFO,
        coalesce(c.City, substr(a.BFD, 1, 3)) as BFD,
        Screen,
        session_guid,
        year,
        month,
        day
    from weblogs a 
    left join airporttocity b
        on substr(a.BFO, 1, 3) = b.Airport
    left join airporttocity c
        on substr(a.BFD, 1, 3) = c.Airport
""")


// filter keeping only the relevant origin markets
val filter3 = filter2.filter("BFO in " + origincities)


////////////////////////////////////////////// TO DO ////////////////////////////////////////
// filter keeping only leisure searches
//val filter4 = filter1.filter("... = 'red'") // lets finalize that later...
val filter4 = filter3
/////////////////////////////////////////////////////////////////////////////////////////////	


// Compute number of searches per OnD, year and month
val aggregated = filter4.select("BFO", "BFD", "year", "month", "session_guid").groupBy("BFO", "BFD", "year", "month").agg(countDistinct("session_guid").alias("freq"))


// Compute the moving average 1/4, 1/2, 1/4
var windowSpec = Window.partitionBy('BFO, 'BFD).orderBy('month).rangeBetween(-1, 1)
var moving_average = sum('freq).over(windowSpec)
val data =
aggregated.select(
    'BFO,
    'BFD,
    'year,
    'month,
    'freq,
    moving_average.alias("smoothedfreq")).select(
    'BFO,
    'BFD,
    'year,
    'month,
    (('smoothedfreq + 'freq /3) * 3/4).alias("freq")
)


// Compute the rank and keep up to rank 50
val byOrigin = Window.partitionBy('BFO, 'year, 'month).orderBy(-'freq)
val flatsorted2 = data.withColumn("rank", row_number over byOrigin).filter("rank <= 50")


// Compute month rank by origin and destination pair (need the full year for this
val byOnD = Window.partitionBy('BFO, 'BFD).orderBy(-'freq)
val flatsorted3 = flatsorted2.withColumn("monthrank", row_number over byOnD)


// Compute the modelrank as a combination of the previous two
// Filter keeping only the top 16
val flatsorted4 = flatsorted3.select('BFO, 'BFD, 'year, 'month, ('rank + 'monthrank * 'monthrank).alias("modelscore"))
val byOriginMdlRank = Window.partitionBy('BFO, 'year, 'month).orderBy('modelscore)
val recommendations = flatsorted4.withColumn("mdlrank", row_number over byOriginMdlRank).filter("mdlrank <= 16").select('BFO, 'BFD, 'year, 'month, ('mdlrank - 1).alias("mdlrank"))


/////////////////////////////////// TO DO //////////////////////////////////////////
// Format output
//// val idx: Array[Int] = recommendations.select("rank").distinct().collect().map(_.getAs[Int]("rank"))

////val withIdxColumns = idx.foldLeft(recommendations) { 
////    case (data, idx) => data.selectExpr("*", s"IF(rank = '$idx', bfd, '') AS recommendation_$idx")
////}

////val colidx: Array[String] = recommendations.select("rank"+'_').distinct().collect().map(_.getAs[Int]("rank"))

////val result = withIdxColumns.drop("rank").drop("bfd").groupBy("bfo").agg(concat(: _*)

////result.show()

////val rdd = recommendations.rdd
// Split the matrix into one number per line.
////val byColumnAndRow = rdd.zipWithIndex.flatMap {
////  case (row, rowIndex) => row.zipWithIndex.map {
////    case (number, columnIndex) => columnIndex -> (rowIndex, number)
////  }
////}
// Build up the transposed matrix. Group and sort by column index first.
////val byColumn = byColumnAndRow.groupByKey.sortByKey().values
// Then sort by row index.
////val transposed = byColumn.map {
////  indexedRow => indexedRow.toSeq.sortBy(_._1).map(_._2)
////}


////recommendations.registerTempTable("recommendations")
////hiveContext.sql("SELECT bfo, group_concat(bfd) as bfd FROM recommendations where rank = 0 group by bfo LIMIT 10")


// trying partitioning and joining
var output = recommendations.filter("rank = 0").select("bfo", "bfd").toDF(Seq("bfo", "recommendation_0"): _*)
// for (i <- 1 to 15) {
var i = 1
val toappend = recommendations.filter("rank = " + i).select("bfo", "bfd").toDF(Seq("bfo2", "recommendation_" + i): _*)
val tmp = output.join(toappend, output("bfo") === toappend("bfo2"), "left").drop("bfo2")
val output = tmp
//}



*/
