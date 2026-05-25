package org.openivm.spark.common

import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.rocksdb.{OpenIvmRocksDB, OpenIvmRocksDBBatchOps, OpenIvmRocksDBRegistry, RocksDBCodec}

import java.io.File
import java.nio.file.{Path, Paths}
import java.sql.Timestamp

/** One persisted refresh-profile row, mirroring DuckDB-OpenIVM's
  * `openivm_refresh_profile` table 1:1.
  *
  * Reference schema (see `.temp/openivm/src/openivm_extension.cpp:268-271`):
  *   - `refresh_id        STRING`    — view + `_` + nanos (REFRESH) or view + `_create_mv_` + nanos (CREATE)
  *   - `view_name         STRING`    — fully-qualified `db.table` form
  *   - `profile_timestamp TIMESTAMP` — wall-clock at the moment the step finished
  *   - `step_order        INT`       — monotonic per-refresh, 0-based
  *   - `step_name         STRING`    — DuckDB step-name vocabulary
  *   - `duration_ms       BIGINT`    — wall-clock for this step only
  *   - `detail            STRING`    — optional `key=value;k2=v2` blob
  */
final case class RefreshProfileRow(
    refreshId: String,
    viewName: String,
    profileTimestamp: Timestamp,
    stepOrder: Int,
    stepName: String,
    durationMs: Long,
    detail: String
)

/** RocksDB-backed catalog for refresh-profile rows.
  *
  * Mirrors the DuckDB-OpenIVM `openivm_refresh_profile` table. Storage layout:
  * a single column family `refresh_profile` in a dedicated RocksDB DB at
  * `<warehouse>/_openivm/refresh_profile/rocksdb` — separate from the
  * read-hot index DB so write-hot profile commits don't contend with
  * MV/staging-index reads.
  *
  * Cumulative by default: `scanAll` returns every row ever written since the
  * last `removeAll`. The `SHOW OPENIVM REFRESH PROFILE` statement reads via
  * `scanAll`; bench-side exporters tag exported rows with their batch number
  * at SELECT time, so no on-engine drain is needed.
  */
object RefreshProfileCatalog {

  private[common] val CfName: String              = "refresh_profile"
  private[common] val ColumnFamilies: Seq[String] = Seq(CfName)

  private def warehouseRoot(spark: SparkSession): Path =
    Paths.get(
      new File(
        RocksDBCodec.requireLocalPath(spark.conf.get("spark.sql.warehouse.dir"))
      ).getCanonicalPath
    )

  /** Filesystem path of the RocksDB DB backing this catalog. */
  def dbPath(spark: SparkSession): String =
    warehouseRoot(spark).resolve("_openivm").resolve("refresh_profile").resolve("rocksdb").toString

  private def openDb(spark: SparkSession): OpenIvmRocksDB =
    OpenIvmRocksDBRegistry.getOrOpen(spark, dbPath(spark), ColumnFamilies)

  /** Idempotent — opens the DB and registers the column family if absent. */
  def ensureTables(spark: SparkSession): Unit = {
    openDb(spark)
    ()
  }

  private def tsToMicros(ts: Timestamp): Long =
    ts.getTime * 1000L + ((ts.getNanos.toLong % 1000000L) / 1000L)

  private def microsToTs(micros: Long): Timestamp = {
    val out = new Timestamp(micros / 1000L)
    out.setNanos(((micros % 1000000L) * 1000L).toInt)
    out
  }

  private def encodeKey(refreshId: String, profileTsMicros: Long, stepOrder: Int): Array[Byte] =
    RocksDBCodec.compositeKey(
      Seq(
        RocksDBCodec.encodeLongBE(profileTsMicros),
        RocksDBCodec.utf8(refreshId),
        RocksDBCodec.encodeLongBE(stepOrder.toLong)
      )
    )

  private def decodeKey(key: Array[Byte]): (Long, String, Int) = {
    val parts = RocksDBCodec.splitComposite(key, maxParts = 3)
    require(parts.length == 3, s"Malformed refresh_profile key (expected 3 parts, found ${parts.length})")
    val tsMicros = RocksDBCodec.decodeLongBE(parts(0))
    val refresh  = RocksDBCodec.fromUtf8(parts(1))
    val stepOrd  = RocksDBCodec.decodeLongBE(parts(2)).toInt
    (tsMicros, refresh, stepOrd)
  }

  private def encodeValue(row: RefreshProfileRow): Array[Byte] =
    RocksDBCodec.compositeKey(
      Seq(
        RocksDBCodec.utf8(row.viewName),
        RocksDBCodec.utf8(row.stepName),
        RocksDBCodec.encodeLongBE(row.durationMs),
        RocksDBCodec.utf8(row.detail)
      )
    )

  private def decodeValue(value: Array[Byte]): (String, String, Long, String) = {
    val parts = RocksDBCodec.splitComposite(value, maxParts = 4)
    require(parts.length >= 3, s"Malformed refresh_profile value (expected >=3 parts, found ${parts.length})")
    val viewName = RocksDBCodec.fromUtf8(parts(0))
    val stepName = RocksDBCodec.fromUtf8(parts(1))
    val durMs    = RocksDBCodec.decodeLongBE(parts(2))
    val detail   = if (parts.length >= 4) RocksDBCodec.fromUtf8(parts(3)) else ""
    (viewName, stepName, durMs, detail)
  }

  /** Batch-write all profile rows for a single refresh / create lifecycle. */
  def record(spark: SparkSession, rows: Seq[RefreshProfileRow]): Unit = {
    if (rows.isEmpty) return
    val db = openDb(spark)
    db.withBatch { batch =>
      rows.foreach { row =>
        val key   = encodeKey(row.refreshId, tsToMicros(row.profileTimestamp), row.stepOrder)
        val value = encodeValue(row)
        OpenIvmRocksDBBatchOps.put(db, batch, CfName, key, value)
      }
    }
    ()
  }

  /** Scan every row in the catalog, ordered by
    * `(profile_timestamp, refresh_id, step_order)` to match DuckDB's
    * `SELECT * FROM openivm_refresh_profile ORDER BY ...`.
    */
  def scanAll(spark: SparkSession): Seq[RefreshProfileRow] = {
    val db  = openDb(spark)
    val it  = db.prefixScan(CfName, Array.emptyByteArray)
    val out = scala.collection.mutable.ArrayBuffer.empty[RefreshProfileRow]
    try {
      while (it.hasNext) {
        val (k, v)                                   = it.next()
        val (tsMicros, refreshId, stepOrder)         = decodeKey(k)
        val (viewName, stepName, durationMs, detail) = decodeValue(v)
        out += RefreshProfileRow(
          refreshId = refreshId,
          viewName = viewName,
          profileTimestamp = microsToTs(tsMicros),
          stepOrder = stepOrder,
          stepName = stepName,
          durationMs = durationMs,
          detail = detail
        )
      }
    } finally {
      it match {
        case c: AutoCloseable =>
          try c.close()
          catch { case _: Throwable => () }
        case _ => ()
      }
    }
    // Composite key encoding preserves the natural ordering of the BE-Long
    // timestamp prefix, so the iterator already yields rows in
    // (ts, refresh_id, step_order) order. Make the contract explicit with a
    // defensive sort here — it is cheap (≤ thousands of rows in the bench).
    out.sortBy(r => (r.profileTimestamp.getTime, r.refreshId, r.stepOrder)).toSeq
  }

  /** Test helper — purge every row in the column family. */
  def removeAll(spark: SparkSession): Unit = {
    val db = openDb(spark)
    db.withBatch { batch =>
      OpenIvmRocksDBBatchOps.deleteRange(
        db,
        batch,
        CfName,
        Array.emptyByteArray,
        Array.fill(128)(0xff.toByte)
      )
    }
    ()
  }
}
