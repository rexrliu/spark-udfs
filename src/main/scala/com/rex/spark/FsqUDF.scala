package com.rex.spark

import org.apache.spark.sql.functions.udf

object FsqUDF {
  val plusXY = udf((x: Int, y: Int) => {
    ((x + y) * 2)
  })

}
