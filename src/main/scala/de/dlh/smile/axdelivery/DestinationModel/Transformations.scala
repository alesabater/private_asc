package de.dlh.smile.axdelivery.DestinationModel

import de.dlh.smile.engine.commons.Contexts
import org.apache.spark.sql.DataFrame
import de.dlh.smile.axdelivery.DestinationModel.DataFrameCommons._
import de.dlh.smile.axdelivery.DestinationModel.ColumnCommons._
import de.dlh.smile.axdelivery.LoadedProperties
import org.apache.spark.sql.functions._
import de.dlh.smile.engine.commons._

object Transformations {

  def formatAndRegisterDataFrame(df: DataFrame, dfAirportMap: DataFrame): DataFrame = {
    df.filterPartitionFieldsOneYearFrom()
      .filterValueMapEquals("cs_uri_query", "Screen", "FOFP")
      .getBFTUDEPField("date", "cs_uri_query", "BFDepDate")
      .flatMapType("cs_uri_query", LoadedProperties.fromMapColumns)
      .select(LoadedProperties.webtrendsColumns.map(col(_)): _*)
      .withColumn("BFO", getFirstIATA(col("BFO")))
      .withColumn("BFD", getFirstIATA(col("BFD")))
      .airportToCityCode(dfAirportMap, "BFO")
      .airportToCityCode(dfAirportMap, "BFD")
  }
  
  def filterRT(df: DataFrame): DataFrame = {
    df.filter("BFTripType = 'RT'").filter("BFO <> 'null'").filter("BFD <> 'null'")
  }
  
  def filterOrigin(df: DataFrame): DataFrame = {
    df.filter(col("BFO").isin(LoadedProperties.originCities: _*))
  }
  
  def scoreTravelReason(df: DataFrame): DataFrame = {
    df.registerTempTable("data")
    val outputDf = Contexts.sqlCtx.sql("""
      select *,
        1/(1+exp(-(-2.416664151 +
        BFDepDateScore	+
        BFDurStayScore	+
        BFRetDateScore	+
        BFTripTypeScore	+
        BFTScore	+
        BFTuDepScore	+
        cs_userScore	+
        cs_userScore2	+
        date_dtScore	+
        date_dtScore2	+
        ed_ageScore	+
        LanguageScore	+
        ed_refdomScore))) as scoreTRM
        from (
          select *,
          coalesce(case when  BFTripType  = "OW" then 1.185844453    
  	                    when  BFTripType = "RT" then -0.964367351 end, 0) as BFTripTypeScore ,
  	      (datediff(to_date(concat_ws('-', substr(BFDepDate, 0, 4), substr(BFDepDate, 5, 2), substr(BFDepDate, 7, 2))), to_date(date_dt))) * 0.00806075    as BFTuDepScore ,
          coalesce(case when from_unixtime(unix_timestamp( BFRetDate ,'yyyyMMdd'),'u') = 4 then -0.699992499
  	                    when from_unixtime(unix_timestamp( BFRetDate ,'yyyyMMdd'),'u') = 5 then -0.837568385    
  	                    when from_unixtime(unix_timestamp( BFRetDate ,'yyyyMMdd'),'u') = 3 then -0.418893077    
  	                    when from_unixtime(unix_timestamp( BFRetDate ,'yyyyMMdd'),'u') = 1 then 0.718319464     
                      	when from_unixtime(unix_timestamp( BFRetDate ,'yyyyMMdd'),'u') = 6 then -0.243287888    
                      	when from_unixtime(unix_timestamp( BFRetDate ,'yyyyMMdd'),'u') = 7 then 0.776233326 end, 0) as BFRetDateScore ,
          coalesce(case when  from_unixtime(unix_timestamp(date_dt),'u') = 4 then -0.076416826    
  	                    when  from_unixtime(unix_timestamp(date_dt),'u') = 5 then -0.104279508    
                      	when  from_unixtime(unix_timestamp(date_dt),'u') = 3 then -0.01552181    
                      	when  from_unixtime(unix_timestamp(date_dt),'u')= 1 then 0.006655212    
                      	when  from_unixtime(unix_timestamp(date_dt),'u') = 6 then 0.715534707    
                      	when  from_unixtime(unix_timestamp(date_dt),'u') = 7 then 0.970783294 end, 0) as date_dtScore ,
          coalesce(case when hour(  date_dt  ) in (12, 13, 14, 15, 16, 17) then 0.210723606    
  	                    when hour(  date_dt  ) in (18, 19, 20, 21, 22, 23) then 1.345292152 end, 0) as date_dtScore2 ,
          coalesce(case when from_unixtime(unix_timestamp( BFDepDate  ,'yyyyMMdd'),'u') = 4 then 0.336332951    
  	                    when from_unixtime(unix_timestamp( BFDepDate  ,'yyyyMMdd'),'u') = 5 then 0.900746848    
  	                    when from_unixtime(unix_timestamp( BFDepDate  ,'yyyyMMdd'),'u') = 3 then 0.046036661    
  	                    when from_unixtime(unix_timestamp( BFDepDate  ,'yyyyMMdd'),'u') = 1 then -0.371070258    
  	                    when from_unixtime(unix_timestamp( BFDepDate  ,'yyyyMMdd'),'u') = 6 then 1.201468709    
  	                    when from_unixtime(unix_timestamp( BFDepDate  ,'yyyyMMdd'),'u') = 7 then 0.194986895 end, 0) as BFDepDateScore ,
          coalesce(case when  BFDurStay  between 7 and 13 then 4.558221934    
                      	when  BFDurStay  = 1 then 1.110370893    
                      	when  BFDurStay  between 2 and 3 then 2.225105797    
                      	when  BFDurStay  between 14 and 27 then 5.560091458    
                      	when  BFDurStay  between 4 and 6 then 3.155241667    
                      	when  BFDurStay  >= 28 then 5.045302384 end, 0) as BFDurStayScore ,
          coalesce(case when instr(  cs_user  , 'Firefox') > 0 then -0.281493421    
  	                    when instr(  cs_user  , 'Trident') > 0 then -0.286226028
  	                    when instr(  cs_user  , 'Chrome') > 0 then 0
  	                    when instr(  cs_user  , 'Safari') > 0 then -0.055488038 end, 0) as cs_userScore ,
          coalesce(case when instr(  cs_user  , 'Macintosh') > 0 then -1.00053254    
  	                    when   cs_user   not rlike '(Windows)|(Macintosh)|(Linux)' then 0.5300526    
  	                    when instr(  cs_user  , 'Windows') > 0 then -0.979952029 end, 0) as cs_userScore2 ,
          coalesce(case when instr( ed_refdom , 'google') > 0 then 0.193237947 
                        when (instr( ed_refdom , 'Direct') > 0 or instr( ed_refdom , 'lufthansa') > 0 or instr( ed_refdom , 'miles-and-more') > 0) then 0 else 0.394189713 end, 0) as ed_refdomScore ,
          coalesce(case when  Language  like 'en%' then 0.215854591
                        when  Language  like 'de%' then 0 else 0.24643095 end, 0) as LanguageScore ,
          coalesce(case when instr( BFT , 'IK') > 0 then -0.614505492    
  	                    when instr( BFT , 'K') > 0 then 0.947487761 end, 0) as BFTScore ,
          coalesce(case when  ed_age  > 0 then -0.391163038 end, 0) as ed_ageScore 
        from data
      ) a
      """)
    outputDf.drop("BFTripTypeScore")
      .drop("BFTuDepScore")
      .drop("BFRetDateScore")
      .drop("date_dtScore")
      .drop("date_dtScore2")
      .drop("BFDepDateScore")
      .drop("BFDurStayScore")
      .drop("cs_userScore")
      .drop("cs_userScore2")
      .drop("ed_refdomScore")
      .drop("LanguageScore")
      .drop("BFTScore")
      .drop("ed_ageScore")
 
  }

  def filterLeisure(df: DataFrame): DataFrame = {
    df.filter(col("scoreTRM") <= 0.5)
    // Leisure bookings have a scoreTRM <= 0.5
  }
}
