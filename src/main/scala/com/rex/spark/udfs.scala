package com.rex.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.encoders.RowEncoder

object udfs {
  // UDF
  val plusXY = FsqUDF.plusXY

  // UDAF
  val myAverage = functions.udaf(MyAvg)
  val combineMaps = functions.udaf(CombineMaps,
    RowEncoder(StructType(Array(StructField("map", MapType(StringType, StringType))))))

  // Register the UDFs to the given spark session
  def registerUdfs(spark: SparkSession): Unit = {
    spark.udf.register("plusXY", plusXY)
    spark.udf.register("myAverage", myAverage)
    spark.udf.register("combineMaps", combineMaps)
  }
}
