package de.dlh.smile.axdelivery

import de.dlh.smile.axdelivery.Main
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object Harness {

  /**
    * Standard harness execution point,
    * custom application logic has to start here.
    * Code can branch out into other objects, functions, threads...
    * as long is it's initial execution starts here.
    *
    * @param sc     lazy initialized spark context
    * @param sqlCtx lazy initialized sqlCtx
    */
  def execute(sc: => SparkContext = Main.sc, sqlCtx: => SQLContext = Main.sqlCtx) = {
    val df = sqlCtx.read.json(sc.parallelize("""[{"name":"Jane", "age":19},{"name":"Joe", "age":32}]""" :: Nil))
    df
  }
}

