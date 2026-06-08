package org.openivm.spark.common

import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.rocksdb.{OpenIvmRocksDB, OpenIvmRocksDBBatchOps, OpenIvmRocksDBRegistry, RocksDBCodec}

import java.nio.file.{Files, Paths}

/**
 * RocksDB-backed catalog of `(viewName, source)` last-consumed Delta versions
 * used by [[CdfChangePropagation]].
 *
 * Layout: shares the index DB at `<warehouse>/_openivm/index/rocksdb` with
 * [[MvCatalog]] and [[StagingCatalog]] via a dedicated column family
 * `cdf_watermarks`.
 *
 * Key encoding:
 *   `RocksDBCodec.compositeKey(Seq(utf8(viewName), utf8(source)))`
 * Value encoding:
 *   `RocksDBCodec.encodeLongBE(lastConsumedVersion)`
 */
object CdfWatermarkCatalog {

  private val Cf: String                          = IndexDbColumnFamilies.CdfWatermarks
  private[common] val ColumnFamilies: Seq[String] = IndexDbColumnFamilies.All

  private def warehouseDir(spark: SparkSession): String =
    RocksDBCodec.requireLocalPath(spark.conf.get("spark.sql.warehouse.dir").stripSuffix("/"))

  private def indexDbPath(spark: SparkSession): String =
    Paths.get(warehouseDir(spark), "_openivm", "index", "rocksdb").toString

  private def openIndexDb(spark: SparkSession): OpenIvmRocksDB =
    OpenIvmRocksDBRegistry.getOrOpen(spark, indexDbPath(spark), ColumnFamilies)

  private def key(viewName: String, source: String): Array[Byte] =
    RocksDBCodec.compositeKey(Seq(RocksDBCodec.utf8(viewName), RocksDBCodec.utf8(source)))

  def ensureTables(spark: SparkSession): Unit = {
    openIndexDb(spark)
    ()
  }

  def get(spark: SparkSession, viewName: String, source: String): Option[Long] =
    openIndexDb(spark).get(Cf, key(viewName, source)).map(RocksDBCodec.decodeLongBE)

  def put(spark: SparkSession, viewName: String, source: String, version: Long): Unit = {
    val db = openIndexDb(spark)
    db.withBatch { batch =>
      OpenIvmRocksDBBatchOps.put(db, batch, Cf, key(viewName, source), RocksDBCodec.encodeLongBE(version))
    }
  }

  def removeForBaseTable(spark: SparkSession, baseTable: String): Unit = {
    if (!Files.exists(Paths.get(indexDbPath(spark)))) return
    val db        = openIndexDb(spark)
    val baseBytes = RocksDBCodec.utf8(baseTable)
    val victims = db
      .prefixScan(Cf, Array.emptyByteArray)
      .flatMap { case (k, _) =>
        val parts = RocksDBCodec.splitComposite(k, 2)
        if (parts.length == 2 && java.util.Arrays.equals(parts(1), baseBytes)) Iterator.single(k.clone())
        else Iterator.empty
      }
      .toList
    if (victims.nonEmpty) {
      db.withBatch { batch =>
        victims.foreach(k => OpenIvmRocksDBBatchOps.delete(db, batch, Cf, k))
      }
    }
  }

  def removeForView(spark: SparkSession, viewName: String): Unit = {
    if (!Files.exists(Paths.get(indexDbPath(spark)))) return
    val db        = openIndexDb(spark)
    val viewBytes = RocksDBCodec.utf8(viewName)
    val victims = db
      .prefixScan(Cf, Array.emptyByteArray)
      .flatMap { case (k, _) =>
        val parts = RocksDBCodec.splitComposite(k, 2)
        if (parts.length == 2 && java.util.Arrays.equals(parts(0), viewBytes)) Iterator.single(k.clone())
        else Iterator.empty
      }
      .toList
    if (victims.nonEmpty) {
      db.withBatch { batch =>
        victims.foreach(k => OpenIvmRocksDBBatchOps.delete(db, batch, Cf, k))
      }
    }
  }
}
