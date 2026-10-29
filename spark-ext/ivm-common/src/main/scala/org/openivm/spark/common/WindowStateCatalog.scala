package org.openivm.spark.common

import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.rocksdb.{OpenIvmRocksDB, OpenIvmRocksDBBatchOps, OpenIvmRocksDBRegistry, RocksDBCodec}
import org.rocksdb.RocksDB

/** Versioned adaptation of the rocksdb-perf-tuning WindowStateCatalog prototype.
  * Delta remains authoritative: the stamp and partition rows are one durable
  * RocksDB batch, and readers only accept an exact table/program/version match.
  */
object WindowStateCatalog {
  // The default family already exists in every MV shard. Its owned namespace
  // avoids creating/syncing an unused family at CREATE, and remains outside
  // metadata-only snapshots. Old window_state-family caches simply miss here.
  private val Cf       = RocksDBCodec.fromUtf8(RocksDB.DEFAULT_COLUMN_FAMILY)
  private val StampKey = RocksDBCodec.utf8("window_state:stamp")
  private val RowStart = RocksDBCodec.utf8("window_state:row:")
  private val RowEnd   = RocksDBCodec.utf8("window_state:row;")

  private def openDb(spark: SparkSession, view: String): OpenIvmRocksDB =
    OpenIvmRocksDBRegistry.getOrOpen(
      spark,
      OpenIvmStatePaths.perMvDbPath(spark, view),
      OpenIvmStatePaths.PerMvColumnFamilies
    )

  private def key(partitionKey: String): Array[Byte]         = RocksDBCodec.utf8(s"window_state:row:$partitionKey")
  private def stamp(identity: String, version: Long): String = s"$version\n$identity"

  def getMany(
      spark: SparkSession,
      view: String,
      identity: String,
      version: Long,
      partitionKeys: Seq[String]
  ): Map[String, String] = {
    val db = openDb(spark, view)
    db.withSession {
      if (!db.get(Cf, StampKey).map(RocksDBCodec.fromUtf8).contains(stamp(identity, version))) Map.empty
      else
        partitionKeys
          .zip(db.multiGet(Cf, partitionKeys.map(key)))
          .flatMap { case (partitionKey, payload) =>
            payload.map(bytes => partitionKey -> RocksDBCodec.fromUtf8(bytes))
          }
          .toMap
    }
  }

  /** Advance unaffected partitions with the table, replace suffix states and
    * remove keys whose partition was recomputed. A stale generation is cleared
    * before any new rows are installed, so old keys cannot acquire a new stamp.
    */
  def putAll(
      spark: SparkSession,
      view: String,
      identity: String,
      previousVersion: Long,
      version: Long,
      affectedKeys: Seq[String],
      entries: Map[String, String]
  ): Unit = {
    val db = openDb(spark, view)
    db.withBatch { batch =>
      if (!db.get(Cf, StampKey).map(RocksDBCodec.fromUtf8).contains(stamp(identity, previousVersion)))
        OpenIvmRocksDBBatchOps.deleteRange(db, batch, Cf, RowStart, RowEnd)
      affectedKeys.foreach { partitionKey =>
        entries.get(partitionKey) match {
          case Some(payload) => OpenIvmRocksDBBatchOps.put(db, batch, Cf, key(partitionKey), RocksDBCodec.utf8(payload))
          case None          => OpenIvmRocksDBBatchOps.delete(db, batch, Cf, key(partitionKey))
        }
      }
      OpenIvmRocksDBBatchOps.put(db, batch, Cf, StampKey, RocksDBCodec.utf8(stamp(identity, version)))
    }
    ()
  }
}
