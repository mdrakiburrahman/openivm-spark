package org.openivm.spark.common

import org.apache.spark.sql.SparkSession

private[common] trait CdfWatermarkBackend {
  def ensureTables(spark: SparkSession): Unit
  def get(spark: SparkSession, viewName: String, source: String): Option[Long]
  def put(spark: SparkSession, viewName: String, source: String, version: Long): Unit
  def putAll(spark: SparkSession, viewName: String, versionsBySource: Map[String, Long]): Unit
  def removeForBaseTable(spark: SparkSession, baseTable: String): Unit
  def removeForView(spark: SparkSession, viewName: String): Unit
}

private[common] object CdfWatermarkBackend {
  def forSession(spark: SparkSession): CdfWatermarkBackend =
    if (FeatureGate.deltaCatalogEnabled(spark)) DeltaCdfWatermarkBackend
    else RocksDbCdfWatermarkBackend
}
