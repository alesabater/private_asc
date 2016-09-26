package de.dlh.smile.axdelivery.Model

import de.dlh.smile.engine.commons.Contexts
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.joda.time.DateTime

object Transformations {

  def formatAndRegisterDataFrame(df: DataFrame, tableName: String): DataFrame = {
    df.registerTempTable(tableName)
    Contexts.sqlCtx.sql(
      s"""
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
    from $tableName
    where cs_uri_query['Screen'] = 'FOFP'
      """)
  }

  def filterDates(df: DataFrame, year: Int, month: Int): DataFrame = {
    /*val month = DateTime.now().getMonthOfYear
    val year = DateTime.now().getYear*/
    val dfFiltered = df.filter(
      (col("year") === year and col("month") <= month) or
        (col("year") === (year - 1) and col("month") > month)
    )
    dfFiltered
  }
}