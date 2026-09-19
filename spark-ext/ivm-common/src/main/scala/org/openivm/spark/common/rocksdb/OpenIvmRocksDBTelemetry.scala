package org.openivm.spark.common.rocksdb

import scala.collection.mutable

/** Aggregated timing for one RocksDB operation shape during a CREATE or REFRESH
  * lifecycle. Timings are inclusive: an outer `withBatch` body can include
  * nested `get` or `prefix_scan` calls.
  */
final case class OpenIvmRocksDBTelemetrySummary(
    dbScope: String,
    operation: String,
    multiProcess: Boolean,
    operationCount: Long,
    failedCount: Long,
    totalNanos: Long,
    jvmLockWaitNanos: Long,
    maxJvmLockWaitNanos: Long,
    jvmLockHeldNanos: Long,
    externalLockWaitNanos: Long,
    maxExternalLockWaitNanos: Long,
    nativeOpenNanos: Long,
    nativeCloseNanos: Long,
    bodyNanos: Long
)

/** Thread-local, opt-in collector for RocksDB contention telemetry.
  *
  * Spark executes CREATE/REFRESH command coordination and its RocksDB catalog
  * accesses on the calling driver thread. A [[Session]] therefore attributes
  * low-level operations to exactly one command without mixing concurrently
  * executing MVs through process-global counters. When no session is active,
  * `record` is a single ThreadLocal lookup and returns immediately.
  */
object OpenIvmRocksDBTelemetry {

  private final case class Key(dbScope: String, operation: String, multiProcess: Boolean)

  private final class MutableSummary {
    var operationCount: Long           = 0L
    var failedCount: Long              = 0L
    var totalNanos: Long               = 0L
    var jvmLockWaitNanos: Long         = 0L
    var maxJvmLockWaitNanos: Long      = 0L
    var jvmLockHeldNanos: Long         = 0L
    var externalLockWaitNanos: Long    = 0L
    var maxExternalLockWaitNanos: Long = 0L
    var nativeOpenNanos: Long          = 0L
    var nativeCloseNanos: Long         = 0L
    var bodyNanos: Long                = 0L
  }

  private final class Collector {
    val summaries: mutable.LinkedHashMap[Key, MutableSummary] = mutable.LinkedHashMap.empty
  }

  final class Session private[rocksdb] (finishSession: () => Seq[OpenIvmRocksDBTelemetrySummary]) {
    private var finished = false

    /** Stop collection on the current thread and return stable, sorted totals.
      * Idempotent: subsequent calls return an empty sequence.
      */
    def finish(): Seq[OpenIvmRocksDBTelemetrySummary] = {
      if (finished) return Seq.empty
      finished = true
      finishSession()
    }
  }

  private val active = new ThreadLocal[Collector]()

  def start(): Session = {
    val previous  = active.get()
    val collector = new Collector
    active.set(collector)
    new Session(() => finish(collector, previous))
  }

  private def finish(collector: Collector, previous: Collector): Seq[OpenIvmRocksDBTelemetrySummary] = {
    if (active.get() eq collector) {
      if (previous == null) active.remove() else active.set(previous)
    }
    collector.summaries.toSeq
      .sortBy { case (key, _) => (key.dbScope, key.operation, key.multiProcess) }
      .map { case (key, value) =>
        OpenIvmRocksDBTelemetrySummary(
          dbScope = key.dbScope,
          operation = key.operation,
          multiProcess = key.multiProcess,
          operationCount = value.operationCount,
          failedCount = value.failedCount,
          totalNanos = value.totalNanos,
          jvmLockWaitNanos = value.jvmLockWaitNanos,
          maxJvmLockWaitNanos = value.maxJvmLockWaitNanos,
          jvmLockHeldNanos = value.jvmLockHeldNanos,
          externalLockWaitNanos = value.externalLockWaitNanos,
          maxExternalLockWaitNanos = value.maxExternalLockWaitNanos,
          nativeOpenNanos = value.nativeOpenNanos,
          nativeCloseNanos = value.nativeCloseNanos,
          bodyNanos = value.bodyNanos
        )
      }
  }

  private[rocksdb] def isActive: Boolean = active.get() != null

  private[rocksdb] def record(
      dbPath: String,
      operation: String,
      multiProcess: Boolean,
      failed: Boolean,
      totalNanos: Long,
      jvmLockWaitNanos: Long,
      jvmLockHeldNanos: Long,
      externalLockWaitNanos: Long,
      nativeOpenNanos: Long,
      nativeCloseNanos: Long,
      bodyNanos: Long
  ): Unit = {
    val collector = active.get()
    if (collector == null) return

    val key     = Key(scopeForPath(dbPath), operation, multiProcess)
    val summary = collector.summaries.getOrElseUpdate(key, new MutableSummary)
    summary.operationCount += 1L
    if (failed) summary.failedCount += 1L
    summary.totalNanos += totalNanos
    summary.jvmLockWaitNanos += jvmLockWaitNanos
    summary.maxJvmLockWaitNanos = math.max(summary.maxJvmLockWaitNanos, jvmLockWaitNanos)
    summary.jvmLockHeldNanos += jvmLockHeldNanos
    summary.externalLockWaitNanos += externalLockWaitNanos
    summary.maxExternalLockWaitNanos = math.max(summary.maxExternalLockWaitNanos, externalLockWaitNanos)
    summary.nativeOpenNanos += nativeOpenNanos
    summary.nativeCloseNanos += nativeCloseNanos
    summary.bodyNanos += bodyNanos
  }

  private[rocksdb] def scopeForPath(dbPath: String): String = {
    val normalized = dbPath.replace('\\', '/')
    val marker     = "/_openivm/"
    val markerPos  = normalized.lastIndexOf(marker)
    if (markerPos < 0) {
      "other"
    } else {
      val relative = normalized.substring(markerPos + marker.length)
      if (relative.startsWith("index/")) "index"
      else if (relative.startsWith("mvs/")) "mv"
      else if (relative.startsWith("tables/")) "table"
      else if (relative.startsWith("sources/")) "source"
      else if (relative.startsWith("refresh_profile/")) "refresh_profile"
      else if (relative.startsWith("refresh_sql_log/")) "refresh_sql_log"
      else "other"
    }
  }
}
