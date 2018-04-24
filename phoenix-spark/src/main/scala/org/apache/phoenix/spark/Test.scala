package org.apache.phoenix.spark

import org.apache.spark.sql.execution.datasources.json.JacksonParser
import org.apache.spark.sql.types.{DataType, StructField, StructType}

/**
  * Created by WLU on 4/24/2018.
  */
object Test  extends App {
  val configSchema = List(
    ("eventid", "long"),
    ("eventtime", "timestamp"),
    ("eventtype", "string"),
    ("name", "string"),
    ("birth", "timestamp"),
    ("age", "integer")
  )

  val schema = StructType(configSchema.map{
    cf => StructField(cf._1, DataType.fromJson("\"" + cf._2 + "\""))
  })

  JacksonParser.pa
}
