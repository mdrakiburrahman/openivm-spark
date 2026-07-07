package org.openivm.spark.common

import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.rocksdb.{OpenIvmRocksDB, OpenIvmRocksDBBatchOps, OpenIvmRocksDBRegistry, RocksDBCodec}

import java.io.File
import java.nio.file.{Path, Paths}
import java.sql.Timestamp

/** One persisted refresh-sql-log row, mirroring DuckDB-OpenIVM's
  * `openivm_refresh_profile` table shape but carrying the full SQL text
  * actually executed for one statement of a CREATE / REFRESH MV lifecycle.
  *
  * Sibling to [[RefreshProfileRow]] — shares `refreshId` so the two
  * catalogs can be joined to align "what ran" against "how long it took".
  *
  *   - `refresh_id        STRING`    — `<view>_<nanos>` (REFRESH) or `<view>_create_mv_<nanos>` (CREATE)
  *   - `view_name         STRING`    — fully-qualified `db.table` form
  *   - `profile_timestamp TIMESTAMP` — wall-clock when the statement finished (`-1` duration for events that did NOT execute, e.g. `original_query`)
  *   - `stmt_order        INT`       — monotonic per-refresh, 0-based; matches the order Spark actually executed statements
  *   - `attempt_idx       INT`       — 0 for the first attempt; 1+ for Delta-OCC retries. Each retry attempt is recorded as its own row so the bench can see all the work Spark did.
  *   - `mode              STRING`    — `create` or `refresh`
  *   - `category          STRING`    — one of `original_query`, `initial_load_ctas`, `aggregate_having_view`, `register_source_delta`, `rewritten_stmt`, `count_monoid_cleanup`, `post_cleanup_stage`, `drop_cleanup`, `full_refresh_stmt`, `fused_view_delta_select`
  *   - `stmt_kind         STRING`    — reuses [[org.openivm.spark.commands.RefreshPerf.classify]] (e.g. `merge` / `insert_into` / `view_delta_ctas`)
  *   - `duration_ms       BIGINT`    — wall-clock for this single execution (-1 if the statement was not executed by us, e.g. `original_query` capture, or 0 for synthetic events)
  *   - `sql_text          STRING`    — full SQL — **no truncation**
  */
final case class RefreshSqlLogRow(
    refreshId: String,
    viewName: String,
    profileTimestamp: Timestamp,
    stmtOrder: Int,
    attemptIdx: Int,
    mode: String,
    category: String,
    stmtKind: String,
    durationMs: Long,
    sqlText: String
)

/** RocksDB-backed catalog for refresh-sql-log rows.
  *
  * Sibling to [[RefreshProfileCatalog]] — same composite key + value pattern,
  * separate column family (`refresh_sql_log`) inside a dedicated RocksDB DB at
  * `<warehouse>/_openivm/refresh_sql_log/rocksdb` so write-hot SQL appends do
  * not contend with read-hot MV / staging-index lookups.
  *
  * Cumulative by default: `scanAll` returns every row ever written since the
  * last `removeAll`. The `SHOW OPENIVM QUERY LOG` statement reads via
  * `scanAll`; bench-side exporters tag rows with their batch number at SELECT
  * time, so no on-engine drain is needed.
  */
object RefreshSqlLogCatalog {

  private[common] val CfName: String              = "refresh_sql_log"
  private[common] val ColumnFamilies: Seq[String] = Seq(CfName)

  private def warehouseRoot(spark: SparkSession): Path =
    Paths.get(
      new File(
        RocksDBCodec.requireLocalPath(FeatureGate.stateWarehouse(spark))
      ).getCanonicalPath
    )

  /** Filesystem path of the RocksDB DB backing this catalog. */
  def dbPath(spark: SparkSession): String =
    warehouseRoot(spark).resolve("_openivm").resolve("refresh_sql_log").resolve("rocksdb").toString

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

  // BE-encoded Int prefix so iteration order matches numeric (stmtOrder, attemptIdx) ordering.
  // We use `encodeLongBE` with a widened Long so we can reuse the existing codec helper.
  private def encodeKey(
      refreshId: String,
      profileTsMicros: Long,
      stmtOrder: Int,
      attemptIdx: Int
  ): Array[Byte] =
    RocksDBCodec.compositeKey(
      Seq(
        RocksDBCodec.encodeLongBE(profileTsMicros),
        RocksDBCodec.utf8(refreshId),
        RocksDBCodec.encodeLongBE(stmtOrder.toLong),
        RocksDBCodec.encodeLongBE(attemptIdx.toLong)
      )
    )

  private def decodeKey(key: Array[Byte]): (Long, String, Int, Int) = {
    val parts = RocksDBCodec.splitComposite(key, maxParts = 4)
    require(parts.length == 4, s"Malformed refresh_sql_log key (expected 4 parts, found ${parts.length})")
    val tsMicros = RocksDBCodec.decodeLongBE(parts(0))
    val refresh  = RocksDBCodec.fromUtf8(parts(1))
    val stmtOrd  = RocksDBCodec.decodeLongBE(parts(2)).toInt
    val attempt  = RocksDBCodec.decodeLongBE(parts(3)).toInt
    (tsMicros, refresh, stmtOrd, attempt)
  }

  private def encodeValue(row: RefreshSqlLogRow): Array[Byte] =
    RocksDBCodec.compositeKey(
      Seq(
        RocksDBCodec.utf8(row.viewName),
        RocksDBCodec.utf8(row.mode),
        RocksDBCodec.utf8(row.category),
        RocksDBCodec.utf8(row.stmtKind),
        RocksDBCodec.encodeLongBE(row.durationMs),
        RocksDBCodec.utf8(row.sqlText)
      )
    )

  private def decodeValue(value: Array[Byte]): (String, String, String, String, Long, String) = {
    val parts = RocksDBCodec.splitComposite(value, maxParts = 6)
    require(parts.length == 6, s"Malformed refresh_sql_log value (expected 6 parts, found ${parts.length})")
    val viewName = RocksDBCodec.fromUtf8(parts(0))
    val mode     = RocksDBCodec.fromUtf8(parts(1))
    val category = RocksDBCodec.fromUtf8(parts(2))
    val stmtKind = RocksDBCodec.fromUtf8(parts(3))
    val durMs    = RocksDBCodec.decodeLongBE(parts(4))
    val sql      = RocksDBCodec.fromUtf8(parts(5))
    (viewName, mode, category, stmtKind, durMs, sql)
  }

  /** Batch-write all sql-log rows for a single refresh / create lifecycle. */
  def record(spark: SparkSession, rows: Seq[RefreshSqlLogRow]): Unit = {
    if (rows.isEmpty) return
    val db = openDb(spark)
    db.withBatch { batch =>
      rows.foreach { row =>
        val key = encodeKey(
          row.refreshId,
          tsToMicros(row.profileTimestamp),
          row.stmtOrder,
          row.attemptIdx
        )
        val value = encodeValue(row)
        OpenIvmRocksDBBatchOps.put(db, batch, CfName, key, value)
      }
    }
    ()
  }

  /** Scan every row in the catalog, ordered by
    * `(profile_timestamp, refresh_id, stmt_order, attempt_idx)` so the bench
    * exporter can write `.sql` files in execution order.
    */
  def scanAll(spark: SparkSession): Seq[RefreshSqlLogRow] = {
    val db  = openDb(spark)
    val it  = db.prefixScan(CfName, Array.emptyByteArray)
    val out = scala.collection.mutable.ArrayBuffer.empty[RefreshSqlLogRow]
    try {
      while (it.hasNext) {
        val (k, v)                                                    = it.next()
        val (tsMicros, refreshId, stmtOrder, attemptIdx)              = decodeKey(k)
        val (viewName, mode, category, stmtKind, durationMs, sqlText) = decodeValue(v)
        out += RefreshSqlLogRow(
          refreshId = refreshId,
          viewName = viewName,
          profileTimestamp = microsToTs(tsMicros),
          stmtOrder = stmtOrder,
          attemptIdx = attemptIdx,
          mode = mode,
          category = category,
          stmtKind = stmtKind,
          durationMs = durationMs,
          sqlText = sqlText
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
    // Composite-key ordering already gives us (ts, refresh, stmt, attempt) order.
    // Sort defensively to make the contract explicit; cheap (≤ thousands of rows).
    out
      .sortBy(r => (r.profileTimestamp.getTime, r.refreshId, r.stmtOrder, r.attemptIdx))
      .toSeq
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
