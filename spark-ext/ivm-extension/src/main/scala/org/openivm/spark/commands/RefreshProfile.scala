package org.openivm.spark.commands

import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.{FeatureGate, RefreshProfileCatalog, RefreshProfileRow}
import org.openivm.spark.common.rocksdb.OpenIvmRocksDBTelemetry

import java.sql.Timestamp
import java.util.concurrent.atomic.AtomicInteger

/** Per-refresh / per-create lifecycle profile collector.
  *
  * Each REFRESH or CREATE MATERIALIZED VIEW invocation constructs ONE instance
  * via [[RefreshProfile.start]], times its phases via [[timeStep]] /
  * [[appendStep]], then calls [[flush]] in a finally block to persist all
  * collected rows in a single RocksDB batch.
  *
  * The collector is silent (no allocation, no measurement overhead) when
  * `spark.openivm.profile.refresh=false` (the default). When enabled it
  * mirrors DuckDB-OpenIVM's `openivm_refresh_profile` table 1:1, including
  * the `<view>_<nanos>` / `<view>_create_mv_<nanos>` refresh-id convention
  * (see `.temp/openivm/src/upsert/refresh.cpp:44-46` and
  * `.temp/openivm/src/core/parser_ddl.cpp:38-46`).
  *
  * Dual-write semantics: when active, every emitted step is ALSO recorded in
  * the existing [[RefreshPerf]] Log4j stream so log-based debug workflows
  * remain untouched. The Log4j stream is retired in a follow-up PR.
  */
final class RefreshProfile private (
    spark: SparkSession,
    val refreshId: String,
    val viewName: String,
    private val active: Boolean,
    private val rocksdbTelemetry: Option[OpenIvmRocksDBTelemetry.Session],
    private val spanStartEpochMs: Long,
    private val spanStartNanos: Long
) {

  private val stepOrder     = new AtomicInteger(0)
  private val buffer        = scala.collection.mutable.ArrayBuffer.empty[RefreshProfileRow]
  private var spanCompleted = false

  /** True when this profile is wired to a real RocksDB sink. False instances
    * pay zero allocation per step.
    */
  def isActive: Boolean = active

  /** Time `body` and append a profile row tagged with `stepName` + `detail`.
    * Body is always executed even when the profile is inactive (zero overhead
    * other than two `System.nanoTime` calls).
    */
  def timeStep[A](stepName: String, detail: String = "")(body: => A): A = {
    if (!active) return body
    val t0 = System.nanoTime()
    try body
    finally {
      val elapsedMs = (System.nanoTime() - t0) / 1000000L
      appendStep(stepName, detail, elapsedMs)
    }
  }

  /** Append a row with an explicitly-measured duration. Use for marker-style
    * events (e.g. `total_refresh` at end of lifecycle) or where the duration
    * is computed by an outer scope (e.g. lock-acquisition time measured
    * outside the lock body).
    */
  def appendStep(stepName: String, detail: String, durationMs: Long): Unit = {
    if (!active) return
    buffer += RefreshProfileRow(
      refreshId = refreshId,
      viewName = viewName,
      profileTimestamp = new Timestamp(System.currentTimeMillis()),
      stepOrder = stepOrder.getAndIncrement(),
      stepName = stepName,
      durationMs = durationMs,
      detail = detail
    )
  }

  /** Record one wall-clock span suitable for a trace/Gantt view. */
  def completeSpan(outcome: String, threadName: String): Unit = synchronized {
    if (!active || spanCompleted) return
    val endEpochMs = System.currentTimeMillis()
    appendStep(
      "query_span",
      s"start_epoch_ms=$spanStartEpochMs;end_epoch_ms=$endEpochMs;thread=$threadName;outcome=$outcome",
      (System.nanoTime() - spanStartNanos) / 1000000L
    )
    spanCompleted = true
  }

  /** Persist all buffered rows in a single RocksDB batch and clear the
    * buffer. Idempotent — a no-op on inactive instances or after a prior
    * flush.
    */
  def flush(): Unit = {
    if (!active) return
    try {
      completeSpan("failed_before_end", Thread.currentThread().getName)
      rocksdbTelemetry.toSeq.flatMap(_.finish()).foreach { summary =>
        appendStep(
          stepName = "rocksdb_operation",
          detail = Seq(
            s"db_scope=${summary.dbScope}",
            s"operation=${summary.operation}",
            s"multi_process=${summary.multiProcess}",
            s"operation_count=${summary.operationCount}",
            s"failed_count=${summary.failedCount}",
            s"total_ns=${summary.totalNanos}",
            s"jvm_lock_wait_ns=${summary.jvmLockWaitNanos}",
            s"max_jvm_lock_wait_ns=${summary.maxJvmLockWaitNanos}",
            s"jvm_lock_held_ns=${summary.jvmLockHeldNanos}",
            s"external_lock_wait_ns=${summary.externalLockWaitNanos}",
            s"max_external_lock_wait_ns=${summary.maxExternalLockWaitNanos}",
            s"native_open_ns=${summary.nativeOpenNanos}",
            s"native_close_ns=${summary.nativeCloseNanos}",
            s"body_ns=${summary.bodyNanos}"
          ).mkString(";"),
          durationMs = summary.totalNanos / 1000000L
        )
      }
      if (buffer.nonEmpty) {
        val rows = buffer.toVector
        buffer.clear()
        try RefreshProfileCatalog.record(spark, rows)
        catch {
          case t: Throwable =>
            // Telemetry must never fail the refresh — log the error and move on.
            RefreshPerfBridge.logProfileFailure(refreshId, viewName, t)
        }
      }
    } finally RefreshProfile.clearActive(this)
  }
}

object RefreshProfile {

  private val current = new ThreadLocal[RefreshProfile]()

  private[commands] def clearActive(profile: RefreshProfile): Unit =
    if (current.get() eq profile) current.remove()

  def flushActive(): Unit = Option(current.get()).foreach(_.flush())

  /** Lifecycle mode — drives the `refresh_id` shape so the chart's
    * `_create_mv_` substring discriminator routes CREATE-phase rows to the
    * `create_mv_total` panel.
    */
  sealed trait Mode
  object Mode {
    case object Refresh extends Mode
    case object Create  extends Mode
  }

  /** Construct a profile collector for `viewName` (fully-qualified db.table
    * form). When the gate is OFF the returned instance is a noop (no buffer
    * allocation, no measurement) — callers do not need to check the gate.
    */
  def start(spark: SparkSession, viewName: String, mode: Mode): RefreshProfile = {
    val active = FeatureGate.profileRefreshEnabled(spark)
    if (active) RefreshProfileCatalog.ensureTables(spark)
    val rocksdbTelemetry = if (active) Some(OpenIvmRocksDBTelemetry.start()) else None
    val suffix = mode match {
      case Mode.Refresh => s"_${System.nanoTime()}"
      case Mode.Create  => s"_create_mv_${System.nanoTime()}"
    }
    val profile = new RefreshProfile(
      spark,
      viewName + suffix,
      viewName,
      active,
      rocksdbTelemetry,
      System.currentTimeMillis(),
      System.nanoTime()
    )
    if (active) current.set(profile)
    profile
  }

  /** Inactive instance for code paths that never opt into profiling. */
  val NoOp: RefreshProfile =
    new RefreshProfile(null, "", "", active = false, rocksdbTelemetry = None, 0L, 0L)
}

/** Side-channel for `RefreshProfile` to emit its own failure diagnostics
  * via the existing perf log without circular package dependencies.
  */
private[commands] object RefreshPerfBridge extends org.apache.spark.internal.Logging {
  def logProfileFailure(refreshId: String, view: String, t: Throwable): Unit =
    logWarning(
      s"[openivm-perf] refresh_id='$refreshId' view='$view' phase='profile_flush_failed' " +
        s"error='${t.getClass.getSimpleName}: ${Option(t.getMessage).getOrElse("")}'"
    )
}
