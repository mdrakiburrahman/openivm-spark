package org.openivm.spark.common

import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.rocksdb.{OpenIvmRocksDB, OpenIvmRocksDBBatchOps, OpenIvmRocksDBRegistry, RocksDBCodec}

import java.nio.file.Files

/**
 * Per-MV RocksDB catalog of last-consumed Delta versions.
 *
 * New state lives beside the owning MV metadata at
 * `<warehouse>/_openivm/mvs/<view>/rocksdb`, so independent refreshes never
 * contend on a global watermark database. Reads lazily migrate legacy rows
 * from the former shared index DB.
 */
object CdfWatermarkCatalog {

  private val Cf: String       = IndexDbColumnFamilies.CdfWatermarks
  private val LegacyCf: String = IndexDbColumnFamilies.CdfWatermarks

  private def openMvDb(spark: SparkSession, viewName: String): OpenIvmRocksDB =
    OpenIvmRocksDBRegistry.getOrOpen(
      spark,
      OpenIvmStatePaths.perMvDbPath(spark, viewName),
      OpenIvmStatePaths.PerMvColumnFamilies
    )

  private def sourceKey(source: String): Array[Byte] = RocksDBCodec.utf8(source)

  private def legacyKey(viewName: String, source: String): Array[Byte] =
    RocksDBCodec.compositeKey(Seq(RocksDBCodec.utf8(viewName), RocksDBCodec.utf8(source)))

  private def openLegacyIndexDb(spark: SparkSession): Option[OpenIvmRocksDB] = {
    val path = OpenIvmStatePaths.indexDbPath(spark)
    if (OpenIvmStatePaths.isExistingDb(path)) {
      Some(OpenIvmRocksDBRegistry.getOrOpen(spark, path, IndexDbColumnFamilies.All))
    } else None
  }

  def ensureTables(spark: SparkSession): Unit = {
    Files.createDirectories(OpenIvmStatePaths.mvsRoot(spark))
    ()
  }

  def get(spark: SparkSession, viewName: String, source: String): Option[Long] = {
    val db  = openMvDb(spark, viewName)
    val key = sourceKey(source)
    db.get(Cf, key).map(RocksDBCodec.decodeLongBE).orElse {
      openLegacyIndexDb(spark)
        .flatMap(_.get(LegacyCf, legacyKey(viewName, source)))
        .map { encoded =>
          db.withBatch { batch =>
            OpenIvmRocksDBBatchOps.put(db, batch, Cf, key, encoded)
          }
          RocksDBCodec.decodeLongBE(encoded)
        }
    }
  }

  def put(spark: SparkSession, viewName: String, source: String, version: Long): Unit =
    putAll(spark, viewName, Map(source -> version))

  def putAll(spark: SparkSession, viewName: String, versionsBySource: Map[String, Long]): Unit = {
    if (versionsBySource.isEmpty) return
    val db = openMvDb(spark, viewName)
    db.withBatch { batch =>
      versionsBySource.foreach { case (source, version) =>
        OpenIvmRocksDBBatchOps.put(db, batch, Cf, sourceKey(source), RocksDBCodec.encodeLongBE(version))
      }
    }
  }

  def removeForBaseTable(spark: SparkSession, baseTable: String): Unit = {
    val key = sourceKey(baseTable)
    OpenIvmStatePaths.existingMvDbPaths(spark).foreach { path =>
      val db = OpenIvmRocksDBRegistry.getOrOpen(spark, path, OpenIvmStatePaths.PerMvColumnFamilies)
      if (db.get(Cf, key).nonEmpty) {
        db.withBatch(batch => OpenIvmRocksDBBatchOps.delete(db, batch, Cf, key))
      }
    }

    openLegacyIndexDb(spark).foreach { db =>
      val baseBytes = RocksDBCodec.utf8(baseTable)
      val victims = db
        .prefixScan(LegacyCf, Array.emptyByteArray)
        .flatMap { case (candidate, _) =>
          val parts = RocksDBCodec.splitComposite(candidate, 2)
          if (parts.length == 2 && java.util.Arrays.equals(parts(1), baseBytes)) Iterator.single(candidate.clone())
          else Iterator.empty
        }
        .toList
      if (victims.nonEmpty) {
        db.withBatch { batch =>
          victims.foreach(candidate => OpenIvmRocksDBBatchOps.delete(db, batch, LegacyCf, candidate))
        }
      }
    }
  }

  def removeForView(spark: SparkSession, viewName: String): Unit = {
    val path = OpenIvmStatePaths.perMvDbPath(spark, viewName)
    if (OpenIvmStatePaths.isExistingDb(path)) {
      val db      = openMvDb(spark, viewName)
      val victims = db.prefixScan(Cf, Array.emptyByteArray).map { case (key, _) => key.clone() }.toList
      if (victims.nonEmpty) {
        db.withBatch { batch =>
          victims.foreach(key => OpenIvmRocksDBBatchOps.delete(db, batch, Cf, key))
        }
      }
    }

    openLegacyIndexDb(spark).foreach { db =>
      val prefix  = RocksDBCodec.compositeKey(Seq(RocksDBCodec.utf8(viewName), Array.emptyByteArray))
      val victims = db.prefixScan(LegacyCf, prefix).map { case (key, _) => key.clone() }.toList
      if (victims.nonEmpty) {
        db.withBatch { batch =>
          victims.foreach(key => OpenIvmRocksDBBatchOps.delete(db, batch, LegacyCf, key))
        }
      }
    }
  }
}
