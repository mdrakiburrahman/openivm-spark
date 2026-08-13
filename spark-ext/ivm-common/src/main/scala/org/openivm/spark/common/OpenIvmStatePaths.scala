package org.openivm.spark.common

import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.rocksdb.RocksDBCodec

import java.io.File
import java.nio.file.{Files, Path, Paths}
import scala.collection.JavaConverters._

/** Deterministic local paths for sharded OpenIVM state. */
private[common] object OpenIvmStatePaths {
  val PerMvColumnFamilies: Seq[String]            = Seq("meta", "properties", "consumed", "cdf_watermarks")
  val BaseTableColumnFamilies: Seq[String]        = Seq("staging")
  val SourceDependencyColumnFamilies: Seq[String] = Seq("dependent_mvs")

  private def canonicalLocalPath(path: String): String =
    new File(RocksDBCodec.requireLocalPath(path)).getCanonicalPath

  def warehouseRoot(spark: SparkSession): Path =
    Paths.get(canonicalLocalPath(FeatureGate.stateWarehouse(spark)))

  def openIvmRoot(spark: SparkSession): Path = warehouseRoot(spark).resolve("_openivm")

  def indexDbPath(spark: SparkSession): String =
    openIvmRoot(spark).resolve("index").resolve("rocksdb").toString

  def mvsRoot(spark: SparkSession): Path = openIvmRoot(spark).resolve("mvs")

  def tablesRoot(spark: SparkSession): Path = openIvmRoot(spark).resolve("tables")

  def sourcesRoot(spark: SparkSession): Path = openIvmRoot(spark).resolve("sources")

  def perMvDbPath(spark: SparkSession, serializedName: String): String =
    mvsRoot(spark).resolve(RocksDBCodec.safePathSegment(serializedName)).resolve("rocksdb").toString

  def baseTableDbPath(spark: SparkSession, baseTable: String): String =
    tablesRoot(spark).resolve(RocksDBCodec.safePathSegment(baseTable)).resolve("rocksdb").toString

  def sourceDependencyDbPath(spark: SparkSession, sourceTable: String): String =
    sourcesRoot(spark).resolve(RocksDBCodec.safePathSegment(sourceTable)).resolve("rocksdb").toString

  def existingMvDbPaths(spark: SparkSession): Seq[String] =
    existingShardDbPaths(mvsRoot(spark))

  def existingBaseTableDbPaths(spark: SparkSession): Seq[String] =
    existingShardDbPaths(tablesRoot(spark))

  def isExistingDb(path: String): Boolean = Files.exists(Paths.get(path).resolve("CURRENT"))

  private def existingShardDbPaths(root: Path): Seq[String] = {
    if (!Files.isDirectory(root)) return Seq.empty
    val children = Files.list(root)
    try {
      children
        .iterator()
        .asScala
        .map(_.resolve("rocksdb"))
        .filter(path => Files.exists(path.resolve("CURRENT")))
        .map(_.toString)
        .toVector
        .sorted
    } finally {
      children.close()
    }
  }
}
