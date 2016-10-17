//////////////////////////////////
//// Initialization of Contexts //
//val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//import de.dlh.smile.engine.commons.Contexts
//import org.apache.spark.sql.DataFrame
//import org.apache.spark.sql.hive.HiveContext
//val hiveContext = new HiveContext(sc)
//import sqlContext.implicits._
//import org.apache.spark.sql.expressions.Window
//import org.apache.spark.mllib.rdd.RDDFunctions._
//
//
//// Read properties
//
/////////////////////////////////
//// Data load and preparation //
//val alllogs = sqlContext.read.parquet(datalocation)
//alllogs.registerTempTable("alllogs")
//
//
//// select only the interesting variables
//val selection = hiveContext.sql("""
//    select
//        cs_uri_query['BFTripType'] as BFTripType ,
//        datediff(to_date(concat_ws('-', substr(cs_uri_query['BFDepDate'], 0, 4), substr(cs_uri_query['BFDepDate'], 5, 2), substr(cs_uri_query['BFDepDate'], 7, 2))), to_date(date_dt)) as BFTuDep,
//        cs_uri_query['BFRetDate'] as BFRetDate ,
//        date_dt  ,
//        cs_uri_query['BFDepDate'] as BFDepDate ,
//        cs_uri_query['BFDurStay']  as BFDurStay,
//        cs_user  ,
//        cs_uri_query['ed_refdom']  as ed_refdom,
//        cs_uri_query['Language']  as Language,
//        cs_uri_query['BFT'] as BFT,
//        cs_uri_query['ed_age'] as ed_age,
//        cs_uri_query['BFO'] as BFO,
//        cs_uri_query['BFD'] as BFD,
//        cs_uri_query['Screen'] as Screen,
//        year,
//        month,
//        day,
//        session_guid
//    from alllogs
//""")
//
//
//
//// filter and keep only the screen FOFP and the relevant dates, up to one year in the past. One year is needed to compute the month rank
//val filter1 = selection.filter("Screen = 'FOFP' and ((year = " + year + " and month < " + month + ") or (year + 1 = " + year + " and month >= " + month + "))")
//
//
//// Replace airport codes by city codes
//filter1.registerTempTable("weblogs")
//airporttocity.registerTempTable("airporttocity")
//
//val filter2 = hiveContext.sql("""
//    select
//        BFTripType ,
//        BFTuDep,
//        BFRetDate ,
//        date_dt  ,
//        BFDepDate ,
//        BFDurStay,
//        cs_user  ,
//        ed_refdom,
//        Language,
//        BFT,
//        ed_age,
//        coalesce(b.City, substr(a.BFO, 1, 3)) as BFO,
//        coalesce(c.City, substr(a.BFD, 1, 3)) as BFD,
//        Screen,
//        session_guid,
//        year,
//        month,
//        day
//    from weblogs a
//    left join airporttocity b
//        on substr(a.BFO, 1, 3) = b.Airport
//    left join airporttocity c
//        on substr(a.BFD, 1, 3) = c.Airport
//""")
//
//
//// filter keeping only the relevant origin markets
//val filter3 = filter2.filter("BFO in " + origincities)
//
//
//
// // WE ARE HERE
//
//
//////////////////////////////////////////////// TO DO ////////////////////////////////////////
//// filter keeping only leisure searches
////val filter4 = filter1.filter("... = 'red'") // lets finalize that later...
//val filter4 = scoreTravelReason(filter3)
//
//def scoreTravelReason(df: DataFrame): DataFrame = {
//  df.registerTempTable("data")
//  val outputDf = Contexts.sqlCtx.sql("""
//      select *,
//        1/(1+exp(-(-2.416664151 +
//        BFDepDateScore    +
//        BFDurStayScore    +
//        BFRetDateScore    +
//        BFTripTypeScore   +
//        BFTScore    +
//        BFTuDepScore      +
//        cs_userScore      +
//        cs_userScore2     +
//        date_dtScore      +
//        date_dtScore2     +
//        ed_ageScore +
//        LanguageScore     +
//        ed_refdomScore))) as scoreTRM
//        from (
//          select *,
//          coalesce(case when  BFTripType  = "OW" then 1.185844453
//                           when  BFTripType = "RT" then -0.964367351 end, 0) as BFTripTypeScore ,
//             (datediff(to_date(concat_ws('-', substr(BFDepDate, 0, 4), substr(BFDepDate, 5, 2), substr(BFDepDate, 7, 2))), to_date(date_dt))) * 0.00806075    as BFTuDepScore ,
//          coalesce(case when from_unixtime(unix_timestamp( BFRetDate ,'yyyyMMdd'),'u') = 4 then -0.699992499
//                           when from_unixtime(unix_timestamp( BFRetDate ,'yyyyMMdd'),'u') = 5 then -0.837568385
//                           when from_unixtime(unix_timestamp( BFRetDate ,'yyyyMMdd'),'u') = 3 then -0.418893077
//                           when from_unixtime(unix_timestamp( BFRetDate ,'yyyyMMdd'),'u') = 1 then 0.718319464
//                          when from_unixtime(unix_timestamp( BFRetDate ,'yyyyMMdd'),'u') = 6 then -0.243287888
//                          when from_unixtime(unix_timestamp( BFRetDate ,'yyyyMMdd'),'u') = 7 then 0.776233326 end, 0) as BFRetDateScore ,
//          coalesce(case when  from_unixtime(unix_timestamp(date_dt),'u') = 4 then -0.076416826
//                           when  from_unixtime(unix_timestamp(date_dt),'u') = 5 then -0.104279508
//                          when  from_unixtime(unix_timestamp(date_dt),'u') = 3 then -0.01552181
//                          when  from_unixtime(unix_timestamp(date_dt),'u')= 1 then 0.006655212
//                          when  from_unixtime(unix_timestamp(date_dt),'u') = 6 then 0.715534707
//                          when  from_unixtime(unix_timestamp(date_dt),'u') = 7 then 0.970783294 end, 0) as date_dtScore ,
//          coalesce(case when hour(  date_dt  ) in (12, 13, 14, 15, 16, 17) then 0.210723606
//                           when hour(  date_dt  ) in (18, 19, 20, 21, 22, 23) then 1.345292152 end, 0) as date_dtScore2 ,
//          coalesce(case when from_unixtime(unix_timestamp( BFDepDate  ,'yyyyMMdd'),'u') = 4 then 0.336332951
//                           when from_unixtime(unix_timestamp( BFDepDate  ,'yyyyMMdd'),'u') = 5 then 0.900746848
//                           when from_unixtime(unix_timestamp( BFDepDate  ,'yyyyMMdd'),'u') = 3 then 0.046036661
//                           when from_unixtime(unix_timestamp( BFDepDate  ,'yyyyMMdd'),'u') = 1 then -0.371070258
//                           when from_unixtime(unix_timestamp( BFDepDate  ,'yyyyMMdd'),'u') = 6 then 1.201468709
//                           when from_unixtime(unix_timestamp( BFDepDate  ,'yyyyMMdd'),'u') = 7 then 0.194986895 end, 0) as BFDepDateScore ,
//          coalesce(case when  BFDurStay  between 7 and 13 then 4.558221934
//                          when  BFDurStay  = 1 then 1.110370893
//                          when  BFDurStay  between 2 and 3 then 2.225105797
//                          when  BFDurStay  between 14 and 27 then 5.560091458
//                          when  BFDurStay  between 4 and 6 then 3.155241667
//                          when  BFDurStay  >= 28 then 5.045302384 end, 0) as BFDurStayScore ,
//          coalesce(case when instr(  cs_user  , 'Firefox') > 0 then -0.281493421
//                           when instr(  cs_user  , 'Trident') > 0 then -0.286226028
//                           when instr(  cs_user  , 'Chrome') > 0 then 0
//                           when instr(  cs_user  , 'Safari') > 0 then -0.055488038 end, 0) as cs_userScore ,
//          coalesce(case when instr(  cs_user  , 'Macintosh') > 0 then -1.00053254
//                           when   cs_user   not rlike '(Windows)|(Macintosh)|(Linux)' then 0.5300526
//                           when instr(  cs_user  , 'Windows') > 0 then -0.979952029 end, 0) as cs_userScore2 ,
//          coalesce(case when instr( ed_refdom , 'google') > 0 then 0.193237947
//                        when (instr( ed_refdom , 'Direct') > 0 or instr( ed_refdom , 'lufthansa') > 0 or instr( ed_refdom , 'miles-and-more') > 0) then 0 else 0.394189713 end, 0) as ed_refdomScore ,
//          coalesce(case when  Language  like 'en%' then 0.215854591
//                        when  Language  like 'de%' then 0 else 0.24643095 end, 0) as LanguageScore ,
//          coalesce(case when instr( BFT , 'IK') > 0 then -0.6145054z92
//                           when instr( BFT , 'K') > 0 then 0.947487761 end, 0) as BFTScore ,
//          coalesce(case when  ed_age  > 0 then -0.391163038 end, 0) as ed_ageScore
//        from data
//      ) a
//                                     """)
//  outputDf.drop("BFTripTypeScore")
//    .drop("BFTuDepScore")
//    .drop("BFRetDateScore")
//    .drop("date_dtScore")
//    .drop("date_dtScore2")
//    .drop("BFDepDateScore")
//    .drop("BFDurStayScore")
//    .drop("cs_userScore")
//    .drop("cs_userScore2")
//    .drop("ed_refdomScore")
//    .drop("LanguageScore")
//    .drop("BFTScore")
//    .drop("ed_ageScore")
//}
//
//def filterLeisure(df: DataFrame): DataFrame = {
//  df.filter(col("scoreTRM") <= 0.5)
//  // Leisure bookings have a scoreTRM <= 0.5
//}
///////////////////////////////////////////////////////////////////////////////////////////////
//
//
//// Compute number of searches per OnD, year and month
//val aggregated = filter4.select("BFO", "BFD", "year", "month", "session_guid").groupBy("BFO", "BFD", "year", "month").agg(countDistinct("session_guid").alias("freq"))
//
//
//// Compute the moving average 1/4, 1/2, 1/4
//var windowSpec = Window.partitionBy('BFO, 'BFD).orderBy('month).rangeBetween(-1, 1)
//var moving_average = sum('freq).over(windowSpec)
//val data =
//aggregated.select(
//    'BFO,
//    'BFD,
//    'year,
//    'month,
//    'freq,
//    moving_average.alias("smoothedfreq")).select(
//    'BFO,
//    'BFD,
//    'year,
//    'month,
//    (('smoothedfreq + 'freq /3) * 3/4).alias("freq")
//)
//
//
//// Compute the rank and keep up to rank 50
//val byOrigin = Window.partitionBy('BFO, 'year, 'month).orderBy(-'freq)
//val flatsorted2 = data.withColumn("rank", row_number over byOrigin).filter("rank <= 50")
//
//
//// Compute month rank by origin and destination pair (need the full year for this
//val byOnD = Window.partitionBy('BFO, 'BFD).orderBy(-'freq)
//val flatsorted3 = flatsorted2.withColumn("monthrank", row_number over byOnD)
//
//
//// Compute the modelrank as a combination of the previous two
//// Filter keeping only the top 16
//val flatsorted4 = flatsorted3.select('BFO, 'BFD, 'year, 'month, ('rank + 'monthrank * 'monthrank).alias("modelscore"))
//val byOriginMdlRank = Window.partitionBy('BFO, 'year, 'month).orderBy('modelscore)
//val recommendations = flatsorted4.withColumn("mdlrank", row_number over byOriginMdlRank).filter("mdlrank <= 16").select('BFO, 'BFD, 'year, 'month, ('mdlrank - 1).alias("mdlrank"))
//
//
///////////////////////////////////// TO DO //////////////////////////////////////////
//// Format output
////// val idx: Array[Int] = recommendations.select("rank").distinct().collect().map(_.getAs[Int]("rank"))
//
//////val withIdxColumns = idx.foldLeft(recommendations) {
//////    case (data, idx) => data.selectExpr("*", s"IF(rank = '$idx', bfd, '') AS recommendation_$idx")
//////}
//
//////val colidx: Array[String] = recommendations.select("rank"+'_').distinct().collect().map(_.getAs[Int]("rank"))
//
//////val result = withIdxColumns.drop("rank").drop("bfd").groupBy("bfo").agg(concat(: _*)
//
//////result.show()
//
//////val rdd = recommendations.rdd
//// Split the matrix into one number per line.
//////val byColumnAndRow = rdd.zipWithIndex.flatMap {
//////  case (row, rowIndex) => row.zipWithIndex.map {
//////    case (number, columnIndex) => columnIndex -> (rowIndex, number)
//////  }
//////}
//// Build up the transposed matrix. Group and sort by column index first.
//////val byColumn = byColumnAndRow.groupByKey.sortByKey().values
//// Then sort by row index.
//////val transposed = byColumn.map {
//////  indexedRow => indexedRow.toSeq.sortBy(_._1).map(_._2)
//////}
//
//
//////recommendations.registerTempTable("recommendations")
//////hiveContext.sql("SELECT bfo, group_concat(bfd) as bfd FROM recommendations where rank = 0 group by bfo LIMIT 10")
//
//
//// trying partitioning and joining
//var output = recommendations.filter("rank = 0").select("bfo", "bfd").toDF(Seq("bfo", "recommendation_0"): _*)
//// for (i <- 1 to 15) {
//var i = 1
//val toappend = recommendations.filter("rank = " + i).select("bfo", "bfd").toDF(Seq("bfo2", "recommendation_" + i): _*)
//val tmp = output.join(toappend, output("bfo") === toappend("bfo2"), "left").drop("bfo2")
//val output = tmp
//
//
//
////}
//
//
//
