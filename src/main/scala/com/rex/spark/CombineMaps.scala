package com.rex.spark

import org.apache.spark.sql.Row
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import scala.collection.mutable.{Map => tMap}
import scala.collection.{Map => iMap}

object CombineMaps extends Aggregator[Row, tMap[String, Double], iMap[String, Double]] {
  // A zero value for this aggregation. Should satisfy the property that any b + zero = b
  def zero: tMap[String, Double] = tMap[String, Double]()

  // Combine two values to produce a new value. For performance, the function may modify `buffer`
  // and return it instead of constructing a new object
  def reduce(buffer: tMap[String, Double], data: Row): tMap[String, Double] = {
    for ((k, v) <- data.getAs[iMap[String, String]](0)) {
      var t = 0D
      if (v != null) {
        t = v.toDouble
      }

      buffer(k) = buffer.getOrElse(k, 0D) + v.toDouble
    }

    buffer
  }

  // Merge two intermediate values
  def merge(mp1: tMap[String, Double], mp2: tMap[String, Double]): tMap[String, Double] = {
    for ((k, v) <- mp2) {
      mp1(k) = mp1.getOrElse(k, 0D) + v
    }

    mp1
  }

  // Transform the output of the reduction
  def finish(reduction: tMap[String, Double]): iMap[String, Double] = reduction.toMap

  // Specifies the Encoder for the intermediate value type
  def bufferEncoder: Encoder[tMap[String, Double]] = ExpressionEncoder()

  // Specifies the Encoder for the final output value type
  def outputEncoder: Encoder[iMap[String, Double]] = ExpressionEncoder()
}