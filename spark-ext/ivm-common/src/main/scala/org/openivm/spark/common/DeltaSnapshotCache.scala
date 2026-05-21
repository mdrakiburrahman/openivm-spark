package org.openivm.spark.common

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog

import java.util.concurrent.ConcurrentHashMap

/**
 * Version-aware snapshot cache for a single Delta-backed metadata table.
 *
 * The cache is keyed by the absolute Delta table path so two SparkSessions
 * pointing at the same warehouse share it (within the same JVM). A cache
 * entry is invalidated either:
 *
 *  - implicitly, by [[snapshot]] noticing that the Delta log version on
 *    disk has advanced since the last load (cross-driver / cross-session
 *    safety net), or
 *  - explicitly, by [[invalidate]] (called immediately after any local
 *    write so the next reader in the same process picks up its own write
 *    without an extra log round-trip).
 *
 * The freshness check (`DeltaLog.update().snapshot.version`) is much
 * cheaper than re-collecting the full Delta table because it only scans
 * the `_delta_log/` directory listing (one `FileSystem.listStatus` call
 * plus a small number of small commit-file reads) rather than the
 * Parquet data files.
 *
 * Used by [[MvCatalog]] and [[StagingCatalog]] to remove the
 * O(refreshes × MVs) Delta-scan amplification observed in TPC-DI benches
 * (`viewsForSource` called per base-table DML, `list` called per REFRESH).
 */
private[common] object DeltaSnapshotCache {
  private final case class Entry[T <: AnyRef](version: Long, rows: T)
}

private[common] class DeltaSnapshotCache[T <: AnyRef] {
  import DeltaSnapshotCache.Entry

  private val cache: ConcurrentHashMap[String, Entry[T]] = new ConcurrentHashMap[String, Entry[T]]()

  /** Read the current snapshot under `path`, refreshing from disk only when
    * the Delta log version has advanced since the cached entry was loaded.
    *
    * `loader` is invoked with the current SparkSession to produce a fresh
    * value when the cache misses or is stale.
    */
  def snapshot(spark: SparkSession, path: String, loader: SparkSession => T): T = {
    val currentVersion =
      try DeltaLog.forTable(spark, path).update().version
      catch { case _: Throwable => -1L }
    val existing = cache.get(path)
    if (existing != null && existing.version == currentVersion && currentVersion >= 0L) {
      return existing.rows
    }
    val fresh = loader(spark)
    if (currentVersion >= 0L) cache.put(path, Entry[T](currentVersion, fresh))
    fresh
  }

  /** Force the next [[snapshot]] call for `path` to reload from disk.
    *
    * Call after any local write (MERGE/UPDATE/DELETE) so the next read in
    * the same process sees the write without an extra log round-trip — the
    * write has advanced the Delta log version anyway, but explicit
    * invalidation avoids the small race window between commit and
    * `update()` cache visibility.
    */
  def invalidate(path: String): Unit = {
    cache.remove(path)
    ()
  }

  /** Clear all cached entries — used by test fixtures that share a JVM
    * across multiple warehouses with overlapping paths.
    */
  def clear(): Unit = cache.clear()

  /** Lightweight introspection for diagnostics: which paths are currently
    * cached, and at what version.
    */
  def entries: Map[String, Long] = {
    val m = scala.collection.mutable.Map.empty[String, Long]
    cache.forEach { (k, v) => m += (k -> v.version); () }
    m.toMap
  }
}
