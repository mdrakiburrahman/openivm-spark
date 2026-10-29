package org.openivm.spark.common

import org.apache.spark.sql.{Column, DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions.{col, struct, to_json}
import org.apache.spark.sql.types.StructType

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

  def decode(spark: SparkSession, payloads: Seq[String], schema: StructType): DataFrame =
    spark.read.schema(schema).options(options).json(spark.createDataset(payloads)(Encoders.STRING))
}
