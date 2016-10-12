package de.dlh.smile.axdelivery.DestinationModel

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._


object UDAFGroupConcat extends UserDefinedAggregateFunction  {
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