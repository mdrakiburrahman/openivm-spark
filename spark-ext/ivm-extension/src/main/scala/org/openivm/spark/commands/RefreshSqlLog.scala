package org.openivm.spark.commands

import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.{FeatureGate, RefreshSqlLogAsyncFlusher, RefreshSqlLogCatalog, RefreshSqlLogRow}

import java.sql.Timestamp

/** Per-refresh / per-create lifecycle SQL collector.
  *
  * Sibling to [[RefreshProfile]]: each `CREATE MATERIALIZED VIEW` /
  * `REFRESH MATERIALIZED VIEW` invocation constructs ONE instance via
  * [[RefreshSqlLog.start]], calls [[record]] from each SQL execution site,
  * then [[flush]] in a finally block to hand the buffer to
  * [[RefreshSqlLogAsyncFlusher]] for off-thread RocksDB persistence.
  *
  * Hot-path cost when ACTIVE: one `System.currentTimeMillis()` + one
  * `ArrayBuffer.append` per record (~200 ns). No IO, no `nanoTime`, no
  * synchronization — the buffer is per-instance, per-thread (see
  * `RefreshMutex` invariant).
  *
  * Hot-path cost when INACTIVE: zero. `start(...)` returns [[NoOp]] when
  * the [[FeatureGate.QueryLogEnabledKey]] gate is OFF, and `record(...)` is
  * a static no-op. Callers do not need to check the gate.
  *
  * The `refresh_id` is **shared** with [[RefreshProfile]] so the two
  * catalogs join cleanly (e.g. "what ran" against "how long it took").
  * For that reason the `RefreshSqlLog.start` signature accepts an
  * externally-minted `refreshId` rather than minting its own.
  */
final class RefreshSqlLog private (
    spark: SparkSession,
    val refreshId: String,
    val viewName: String,
    val mode: String, // "create" or "refresh"
    private val active: Boolean
) {

  // Per-instance, per-thread buffer. RefreshMutex serialises refreshes of
  // the same MV in the same JVM, and CREATE is never concurrent with itself
  // on the same MV, so no contention here.
  private val buffer = scala.collection.mutable.ArrayBuffer.empty[RefreshSqlLogRow]

  def isActive: Boolean = active

  /** Append one row. Hot path is ~200 ns when active, zero-cost when
    * inactive. NEVER throws — telemetry must not fail the refresh.
    *
    * @param category   stable enum tag (`original_query`, `initial_load_ctas`,
    *                   `aggregate_having_view`, `register_source_delta`,
    *                   `rewritten_stmt`, `count_monoid_cleanup`,
    *                   `post_cleanup_stage`, `drop_cleanup`,
    *                   `full_refresh_stmt`, `fused_view_delta_select`).
    * @param stmtOrder  monotonic position in the lifecycle (0-based). -1 for
    *                   the `original_query` event (which is not executed by
    *                   us — it's the user's CREATE-MV body text).
    * @param attemptIdx 0 for first attempt; 1+ for Delta-OCC retries.
    * @param stmtKind   classifier (`merge`, `insert_into`, `view_delta_ctas`,
    *                   `ctas`, `temp_view`, `delete`, `select`, `other`, etc.)
    *                   Reuses [[RefreshPerf.classify]] when available.
    * @param sql        full SQL text — **no truncation**.
    * @param durationMs wall-clock for this single execution. -1 for events
    *                   that were not actually executed by Spark (the
    *                   user-supplied `original_query`).
    */
  def record(
      category: String,
      stmtOrder: Int,
      attemptIdx: Int,
      stmtKind: String,
      sql: String,
      durationMs: Long
  ): Unit = {
    if (!active) return
    try {
      buffer += RefreshSqlLogRow(
        refreshId = refreshId,
        viewName = viewName,
        profileTimestamp = new Timestamp(System.currentTimeMillis()),
        stmtOrder = stmtOrder,
        attemptIdx = attemptIdx,
        mode = mode,
        category = category,
        stmtKind = stmtKind,
        durationMs = durationMs,
        sqlText = if (sql == null) "" else sql
      )
    } catch {
      case t: Throwable =>
        RefreshPerfBridge.logProfileFailure(refreshId, viewName, t)
    }
  }

  /** Hand the buffered rows to [[RefreshSqlLogAsyncFlusher]] for off-thread
    * RocksDB persistence. Returns immediately (the queue.offer call is
    * O(1)). Idempotent — a no-op on inactive instances or after a prior
    * flush. NEVER throws.
    */
  def flush(): Unit = {
    if (!active || buffer.isEmpty) return
    val rows = buffer.toVector
    buffer.clear()
    try RefreshSqlLogAsyncFlusher.submit(spark, rows)
    catch {
      case t: Throwable =>
        RefreshPerfBridge.logProfileFailure(refreshId, viewName, t)
    }
  }
}

object RefreshSqlLog {

  val ModeCreate: String  = "create"
  val ModeRefresh: String = "refresh"

  /** Construct a query-log collector for `viewName` (fully-qualified
    * db.table form). When the gate is OFF the returned instance is [[NoOp]]
    * — callers do not need to check the gate.
    *
    * The `refreshId` MUST be the same one used for the corresponding
    * [[RefreshProfile]] instance so the two catalogs join cleanly.
    */
  def start(
      spark: SparkSession,
      refreshId: String,
      viewName: String,
      mode: String
  ): RefreshSqlLog = {
    val active = FeatureGate.queryLogEnabled(spark)
    if (active) RefreshSqlLogCatalog.ensureTables(spark)
    new RefreshSqlLog(spark, refreshId, viewName, mode, active)
  }

  /** Inactive instance for code paths that never opt into the query log. */
  val NoOp: RefreshSqlLog =
    new RefreshSqlLog(null, "", "", "", active = false)
}
