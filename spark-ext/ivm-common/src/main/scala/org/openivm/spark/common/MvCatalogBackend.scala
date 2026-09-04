package org.openivm.spark.common

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier

private[common] trait MvCatalogBackend {
  def ensureTables(spark: SparkSession): Unit
  def upsert(spark: SparkSession, meta: MvMetadata): Unit
  def lookup(spark: SparkSession, name: TableIdentifier): Option[MvMetadata]
  def list(spark: SparkSession): Seq[MvMetadata]
  def viewsForSource(spark: SparkSession, table: String): Seq[MvMetadata]
  def advance(spark: SparkSession, name: TableIdentifier, newVersion: Long): Unit
  def updateProperties(spark: SparkSession, name: TableIdentifier, properties: Map[String, String]): Unit
  def remove(spark: SparkSession, name: TableIdentifier): Unit
}

private[common] object MvCatalogBackend {
  def forSession(spark: SparkSession): MvCatalogBackend =
    if (FeatureGate.deltaCatalogEnabled(spark)) DeltaMvCatalogBackend
    else RocksDbMvCatalogBackend
}
