package org.openivm.spark.common

import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, from_json, struct, to_json}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/** Shared wire format for derived window caches, independent of the session time zone. */
private[spark] object WindowStateJson {
  val options: Map[String, String] = Map(
    "ignoreNullFields"   -> "false",
    "timeZone"           -> "UTC",
    "timestampFormat"    -> "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX",
    "timestampNTZFormat" -> "yyyy-MM-dd'T'HH:mm:ss.SSSSSS",
    "mode"               -> "FAILFAST"
  )

  def encode(columns: Seq[String]): Column =
    to_json(struct(columns.map(name => col("`" + name.replace("`", "``") + "`")): _*), options)

  def decode(spark: SparkSession, payloads: Seq[String], schema: StructType): DataFrame = {
    // These bounded cache values are already driver-local. Preserve their
    // cardinality for the optimizer instead of creating a distributed JSON scan.
    val rows = new java.util.ArrayList[Row](payloads.size)
    payloads.foreach(payload => rows.add(Row(payload)))
    spark
      .createDataFrame(rows, StructType(Seq(StructField("payload", StringType))))
      .select(from_json(col("payload"), schema, options).as("state"))
      .select("state.*")
  }
}
