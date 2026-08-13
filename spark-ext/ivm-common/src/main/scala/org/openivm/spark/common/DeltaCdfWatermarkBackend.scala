package org.openivm.spark.common

import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

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

  override def ensureTables(spark: SparkSession): Unit = withDeltaRetry {
    DeltaTable.createIfNotExists(spark).location(path(spark)).addColumns(schema).execute()
  }

  override def get(spark: SparkSession, viewName: String, source: String): Option[Long] = {
    ensureTables(spark)
    spark.read
      .format("delta")
      .load(path(spark))
      .filter(col(ViewName) === lit(viewName) && col(Source) === lit(source))
      .select(Version)
      .take(1)
      .headOption
      .map(_.getLong(0))
  }

  override def put(spark: SparkSession, viewName: String, source: String, version: Long): Unit =
    putAll(spark, viewName, Map(source -> version))

  override def putAll(spark: SparkSession, viewName: String, versionsBySource: Map[String, Long]): Unit = {
    if (versionsBySource.isEmpty) return
    withDeltaRetry {
      ensureTables(spark)
      val rows     = versionsBySource.toSeq.map { case (source, version) => Row(viewName, source, version) }
      val incoming = spark.createDataFrame(spark.sparkContext.parallelize(rows, 1), schema)
      DeltaTable
        .forPath(spark, path(spark))
        .as("target")
        .merge(
          incoming.as("incoming"),
          s"target.$ViewName = incoming.$ViewName AND target.$Source = incoming.$Source"
        )
        .whenMatched(s"incoming.$Version > target.$Version")
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .execute()
    }
  }

  override def removeForBaseTable(spark: SparkSession, baseTable: String): Unit = withDeltaRetry {
    ensureTables(spark)
    DeltaTable.forPath(spark, path(spark)).delete(col(Source) === lit(baseTable))
  }

  override def removeForView(spark: SparkSession, viewName: String): Unit = withDeltaRetry {
    ensureTables(spark)
    DeltaTable.forPath(spark, path(spark)).delete(col(ViewName) === lit(viewName))
  }
}
