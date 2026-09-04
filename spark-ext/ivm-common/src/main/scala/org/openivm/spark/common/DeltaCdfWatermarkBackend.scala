package org.openivm.spark.common

import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.openivm.spark.telemetry.metrics.OpenIvmMetrics

private[common] object DeltaCdfWatermarkBackend extends CdfWatermarkBackend with DeltaRetrySupport {
  private val ViewName = "view_name"
  private val Source   = "source"
  private val Version  = "version"

  private val schema = StructType(
    Seq(
      StructField(ViewName, StringType, nullable = false),
      StructField(Source, StringType, nullable = false),
      StructField(Version, LongType, nullable = false)
    )
  )

  private def path(spark: SparkSession): String = FeatureGate.catalogPath(spark).stripSuffix("/") + "/cdf_watermarks"

  override def withDeltaRetry[T](operation: => T): T =
    RetryPolicy.DeltaCatalogConflicts.executeWithRetryCallback((_, _) =>
      OpenIvmMetrics.recordCatalogRetry("cdf_watermark")
    )(operation)

  override def ensureTables(spark: SparkSession): Unit = OpenIvmMetrics.time("catalog.cdf_watermark.ensure_tables") {
    withDeltaRetry {
      DeltaTable.createIfNotExists(spark).location(path(spark)).addColumns(schema).partitionedBy(ViewName).execute()
    }
  }

  override def get(spark: SparkSession, viewName: String, source: String): Option[Long] = {
    getAll(spark, viewName, Seq(source)).get(source)
  }

  override def getAll(spark: SparkSession, viewName: String, sources: Seq[String]): Map[String, Long] = {
    val distinctSources = sources.distinct
    if (distinctSources.isEmpty) return Map.empty
    ensureTables(spark)
    spark.read
      .format("delta")
      .load(path(spark))
      .filter(col(ViewName) === lit(viewName) && col(Source).isin(distinctSources: _*))
      .select(Source, Version)
      .collect()
      .map(row => row.getString(0) -> row.getLong(1))
      .toMap
  }

  override def put(spark: SparkSession, viewName: String, source: String, version: Long): Unit =
    putAll(spark, viewName, Map(source -> version))

  override def putAll(spark: SparkSession, viewName: String, versionsBySource: Map[String, Long]): Unit = {
    if (versionsBySource.isEmpty) return
    OpenIvmMetrics.time("catalog.cdf_watermark.put_all") {
      withDeltaRetry {
        ensureTables(spark)
        val rows     = versionsBySource.toSeq.map { case (source, version) => Row(viewName, source, version) }
        val incoming = spark.createDataFrame(spark.sparkContext.parallelize(rows, 1), schema)
        DeltaTable
          .forPath(spark, path(spark))
          .as("target")
          .merge(
            incoming.as("incoming"),
            col(s"target.$ViewName") === lit(viewName) &&
              col(s"target.$ViewName") === col(s"incoming.$ViewName") &&
              col(s"target.$Source") === col(s"incoming.$Source")
          )
          .whenMatched(s"incoming.$Version > target.$Version")
          .updateAll()
          .whenNotMatched()
          .insertAll()
          .execute()
      }
    }
  }

  override def removeForBaseTable(spark: SparkSession, baseTable: String): Unit = OpenIvmMetrics.time(
    "catalog.cdf_watermark.remove_for_base_table"
  ) {
    withDeltaRetry {
      ensureTables(spark)
      DeltaTable.forPath(spark, path(spark)).delete(col(Source) === lit(baseTable))
    }
  }

  override def removeForView(spark: SparkSession, viewName: String): Unit = OpenIvmMetrics.time(
    "catalog.cdf_watermark.remove_for_view"
  ) {
    withDeltaRetry {
      ensureTables(spark)
      DeltaTable.forPath(spark, path(spark)).delete(col(ViewName) === lit(viewName))
    }
  }
}
