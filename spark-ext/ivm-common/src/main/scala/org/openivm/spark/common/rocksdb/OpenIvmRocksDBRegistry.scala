package org.openivm.spark.common.rocksdb

import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.SparkSession
import org.openivm.spark.telemetry.metrics.OpenIvmMetrics
import org.slf4j.LoggerFactory

import java.io.File
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.locks.ReentrantLock
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

object OpenIvmRocksDBRegistry {
  private val log = LoggerFactory.getLogger(getClass)

  /** Invariant snapshot used by deterministic tests to assert the registry has
    * quiesced: no live native handles, no map-reachable entries, no slots.
    */
  private[rocksdb] final case class HandleSnapshot(
      liveHandleCount: Int,
      reachableHandleCount: Int,
      slotCount: Int
  )

  private final case class Entry(
      db: OpenIvmRocksDB,
      columnFamilies: Set[String],
      appIds: Set[String]
  )

  private final class Slot(canonicalPath: String) {
    private val lock         = new ReentrantLock()
    private var entry: Entry = null
    // Set (under `lock`) at the instant this slot is pruned from `entries`.
    // A thread that fetched this slot from the map BEFORE the prune can still
    // call `getOrOpen`; without this flag it would re-populate a slot that is
    // no longer the map's canonical slot for the path, leaving an ORPHANED live
    // RocksDB whose `<dbPath>/LOCK` a fresh slot then fails to acquire
    // ("lock hold by current process"). A retired slot refuses `getOrOpen` so
    // the registry re-fetches the current slot instead.
    private var retired: Boolean = false

    def getOrOpen(spark: SparkSession, appId: String, requestedColumnFamilies: Set[String]): OpenIvmRocksDB = {
      lock.lock()
      try {
        if (retired) throw SlotRetiredSignal
        val current = entry
        if (current != null) {
          OpenIvmMetrics.increment("rocksdb.registry.get_or_open.hit")
          assertSubset(canonicalPath, current, requestedColumnFamilies)
          if (!current.appIds.contains(appId)) {
            entry = current.copy(appIds = current.appIds + appId)
          }
          current.db
        } else {
          OpenIvmMetrics.increment("rocksdb.registry.get_or_open.miss")
          val conf = OpenIvmRocksDBConf.fromSpark(spark)
          val db = new OpenIvmRocksDB(
            canonicalPath,
            conf,
            requestedColumnFamilies.toSeq.sorted.filterNot(_ == OpenIvmRocksDB.DefaultColumnFamilyName)
          )
          try {
            db.load()
            OpenIvmMaintenanceCoordinator.register(db, conf)
            entry = Entry(db, requestedColumnFamilies, Set(appId))
            publishOpenEntryCount(openEntryCount.incrementAndGet())
            db
          } catch {
            case error: Throwable =>
              cleanupFailedOpen(db, error)
          }
        }
      } finally {
        lock.unlock()
      }
    }

    def closeNow(): Boolean = {
      lock.lock()
      try {
        if (entry != null) {
          closeEntry(canonicalPath, entry)
          entry = null
          publishOpenEntryCount(openEntryCount.decrementAndGet())
        }
        entry == null
      } finally {
        lock.unlock()
      }
    }

    def closeForAppId(appId: String): Boolean = {
      lock.lock()
      try {
        val current = entry
        if (current != null && current.appIds.contains(appId)) {
          val remainingAppIds = current.appIds - appId
          if (remainingAppIds.isEmpty) {
            closeEntry(canonicalPath, current)
            entry = null
            publishOpenEntryCount(openEntryCount.decrementAndGet())
          } else {
            entry = current.copy(appIds = remainingAppIds)
          }
        }
        entry == null
      } finally {
        lock.unlock()
      }
    }

    def overrideAppIds(appIds: Set[String]): Unit = {
      lock.lock()
      try {
        val current = entry
        if (current == null) {
          throw new IllegalArgumentException(s"No open RocksDB registry entry found for $canonicalPath")
        }
        entry = current.copy(appIds = appIds)
      } finally {
        lock.unlock()
      }
    }

    /** Atomically retire-and-report for pruning: if the slot holds no open
      * entry it is marked retired (so no later `getOrOpen` reuses it) and `true`
      * is returned, telling the caller it is safe to remove from `entries`. A
      * slot that still has an entry is left untouched. Called inside the map's
      * `computeIfPresent` so the retire + map removal are atomic with respect to
      * a concurrent `getOrOpen` that has already fetched this slot.
      */
    def retireIfEmpty(): Boolean = {
      lock.lock()
      try {
        if (entry == null) {
          retired = true
          true
        } else {
          false
        }
      } finally {
        lock.unlock()
      }
    }

    def isRetired: Boolean = {
      lock.lock()
      try retired
      finally lock.unlock()
    }

    def hasEntry: Boolean = {
      lock.lock()
      try entry != null
      finally lock.unlock()
    }

    def currentDbForTesting: Option[OpenIvmRocksDB] = {
      lock.lock()
      try Option(entry).map(_.db)
      finally lock.unlock()
    }

    /** Close any live entry, then run `afterClose` (a caller-supplied filesystem
      * delete) — all while holding `lock`, so no concurrent [[getOrOpen]] can
      * install a handle on this path between the close and the delete (the
      * classic close/delete TOCTOU). The slot is marked `retired` before the
      * lock is released, so an opener that fetched this slot earlier observes
      * the retirement (via `SlotRetiredSignal`) and retries against a fresh
      * slot rather than reopening half-deleted state. Returns false WITHOUT
      * running `afterClose` when the slot was already retired, telling the
      * caller to retry with a fresh slot.
      *
      * `retired` is set in a `finally` so it holds even when `afterClose`
      * throws: the handle is already closed and the dead slot must still be
      * evicted, so lifecycle state stays safe and retryable while the delete
      * failure propagates to the caller instead of being silently swallowed.
      */
    def closeAndRun(afterClose: () => Unit): Boolean = {
      lock.lock()
      try {
        if (retired) {
          false
        } else {
          if (entry != null) {
            closeEntry(canonicalPath, entry)
            entry = null
            publishOpenEntryCount(openEntryCount.decrementAndGet())
          }
          try {
            afterClose()
          } finally {
            retired = true
          }
          true
        }
      } finally {
        lock.unlock()
      }
    }
  }

  private val entries               = new ConcurrentHashMap[String, Slot]()
  private val listenerAppIds        = ConcurrentHashMap.newKeySet[String]()
  private val openEntryCount        = new AtomicInteger(0)
  private val shutdownHookInstalled = new AtomicBoolean(false)

  // Test-only park point, invoked after `slotFor` selects a slot but before the
  // slot lock is acquired. Lets a deterministic test force the exact
  // select-then-retire-then-open interleaving. A volatile no-op in production.
  @volatile private var afterSlotSelectionHookForTesting: String => Unit = (_: String) => ()

  // Internal control signal: a `getOrOpen` reached a slot that was retired by a
  // concurrent prune. The caller re-fetches the current slot and retries. Never
  // escapes the registry; carries no stack trace (hot path, not an error).
  private object SlotRetiredSignal extends RuntimeException("openivm-rocksdb slot retired") {
    override def fillInStackTrace(): Throwable = this
  }
  // A retired slot is removed from the map in the same `computeIfPresent` that
  // retires it, so the retry converges within a couple of spins; the bound only
  // guards against an unforeseen livelock.
  private val MaxSlotRetries = 1024

  def getOrOpen(spark: SparkSession, dbPath: String, columnFamilies: Seq[String]): OpenIvmRocksDB = {
    installShutdownHookIfNeeded()
    OpenIvmStateSync.maybeRestore(spark)

    val appId         = spark.sparkContext.applicationId
    val canonicalPath = canonicalLocalPath(dbPath)
    val requestedColumnFamilies =
      (Set(OpenIvmRocksDB.DefaultColumnFamilyName) ++ columnFamilies.filterNot(
        _ == OpenIvmRocksDB.DefaultColumnFamilyName
      ))

    registerSparkListenerIfNeeded(spark, appId)
    var attempts = 0
    while (true) {
      val slot = slotFor(canonicalPath)
      afterSlotSelectionHookForTesting(canonicalPath)
      try {
        return slot.getOrOpen(spark, appId, requestedColumnFamilies)
      } catch {
        case SlotRetiredSignal =>
          // The slot was retired by a concurrent prune or closeAndDelete. Help
          // evict the exact dead slot (identity- and retired-checked, so a fresh
          // concurrent slot is never dropped), then retry against a live slot.
          evictIfRetired(canonicalPath, slot)
          attempts += 1
          if (attempts >= MaxSlotRetries) {
            throw new IllegalStateException(
              s"openivm-rocksdb registry could not obtain a live slot for $canonicalPath " +
                s"after $MaxSlotRetries retries (persistent prune/get race)"
            )
          }
          OpenIvmMetrics.increment("rocksdb.registry.get_or_open.slot_retired_retry")
      }
    }
    // Unreachable: the loop only exits via `return` or `throw`.
    throw new IllegalStateException("unreachable")
  }

  def close(dbPath: String): Unit = {
    val canonicalPath = canonicalLocalPath(dbPath)
    val slot          = entries.get(canonicalPath)
    if (slot != null) {
      slot.closeNow()
      pruneSlotIfEmpty(canonicalPath)
    }
    OpenIvmMaintenanceCoordinator.shutdownIfIdle()
  }

  /** Close the RocksDB at `dbPath` (if open) and run `delete` under the SAME
    * per-path slot lock, so no concurrent [[getOrOpen]] can reopen the path
    * between the close and the delete (the classic close/delete TOCTOU). The
    * native `<dbPath>/LOCK` is released by the close before `delete` runs; the
    * slot is then retired and removed so an opener that raced in retries into a
    * fresh slot (a brand-new DB) instead of observing half-deleted state.
    *
    * `delete` receives the canonical local path and MUST be idempotent: a lost
    * close/delete race can run it again on an already-removed directory. If
    * `delete` throws, the handle is already closed and the dead slot is still
    * retired and evicted, but the exception is propagated — a cleanup failure
    * is never silently swallowed, and the path stays reopenable. Exclusion is
    * per-slot; other paths are never blocked.
    */
  def closeAndDelete(dbPath: String)(delete: String => Unit): Unit = {
    val canonicalPath = canonicalLocalPath(dbPath)
    var completed     = false
    while (!completed) {
      val slot = slotFor(canonicalPath)
      afterSlotSelectionHookForTesting(canonicalPath)
      try {
        completed = slot.closeAndRun(() => delete(canonicalPath))
      } finally {
        evictIfRetired(canonicalPath, slot)
      }
    }
    OpenIvmMaintenanceCoordinator.shutdownIfIdle()
  }

  def closeAllForSparkContext(appId: String): Unit = {
    listenerAppIds.remove(appId)
    entries.entrySet().asScala.toSeq.foreach { mapEntry =>
      mapEntry.getValue.closeForAppId(appId)
      pruneSlotIfEmpty(mapEntry.getKey)
    }
    OpenIvmMaintenanceCoordinator.shutdownIfIdle()
  }

  def closeAll(): Unit = {
    entries.entrySet().asScala.toSeq.foreach { mapEntry =>
      mapEntry.getValue.closeNow()
      pruneSlotIfEmpty(mapEntry.getKey)
    }
    OpenIvmMaintenanceCoordinator.shutdownIfIdle()
  }

  private def slotFor(canonicalPath: String): Slot = {
    val fresh    = new Slot(canonicalPath)
    val existing = entries.putIfAbsent(canonicalPath, fresh)
    if (existing != null) existing else fresh
  }

  private def publishOpenEntryCount(count: Int): Unit =
    OpenIvmMetrics.OpenDbHandles.set(count)

  private def pruneSlotIfEmpty(canonicalPath: String): Unit =
    entries.computeIfPresent(
      canonicalPath,
      new java.util.function.BiFunction[String, Slot, Slot] {
        override def apply(path: String, slot: Slot): Slot =
          // Retire + report atomically: `computeIfPresent` holds the map bin so
          // the retire (under the slot lock) and the removal (returning null)
          // are one step relative to a concurrent `getOrOpen` on this slot.
          if (slot.retireIfEmpty()) null else slot
      }
    )

  // Remove a slot from the map only if it is the exact slot we looked up AND it
  // is retired. Identity-checked so a fresh slot installed by a concurrent
  // open+retry is never evicted. Runs under the map bin lock, then the slot lock
  // (via `isRetired`) — the same map -> slot order as `pruneSlotIfEmpty`, and no
  // thread ever holds a slot lock while touching the map, so there is no lock
  // inversion or deadlock.
  private def evictIfRetired(canonicalPath: String, expected: Slot): Unit =
    entries.computeIfPresent(
      canonicalPath,
      new java.util.function.BiFunction[String, Slot, Slot] {
        override def apply(path: String, slot: Slot): Slot =
          if ((slot eq expected) && slot.isRetired) null else slot
      }
    )

  private def canonicalLocalPath(path: String): String =
    new File(RocksDBCodec.requireLocalPath(path)).getCanonicalPath

  private[rocksdb] def overrideAppIdsForTesting(dbPath: String, appIds: Set[String]): Unit = {
    val canonicalPath = canonicalLocalPath(dbPath)
    val slot = Option(entries.get(canonicalPath)).getOrElse(
      throw new IllegalArgumentException(s"No open RocksDB registry entry found for $canonicalPath")
    )
    slot.overrideAppIds(appIds)
  }

  private[rocksdb] def setAfterSlotSelectionHookForTesting(hook: String => Unit): Unit =
    afterSlotSelectionHookForTesting = hook

  private[rocksdb] def clearAfterSlotSelectionHookForTesting(): Unit =
    afterSlotSelectionHookForTesting = (_: String) => ()

  /** Invariant probe: the live, map-reachable RocksDB handle for `dbPath`, if
    * any. Proves an opened handle is reachable (not orphaned in an evicted slot).
    */
  private[rocksdb] def liveHandleForTesting(dbPath: String): Option[OpenIvmRocksDB] =
    Option(entries.get(canonicalLocalPath(dbPath))).flatMap(_.currentDbForTesting)

  private[rocksdb] def mappedSlotCountForTesting: Int = entries.size

  private[rocksdb] def openEntryCountForTesting: Int = openEntryCount.get()

  private[rocksdb] def handleSnapshotForTesting: HandleSnapshot =
    HandleSnapshot(
      liveHandleCount = openEntryCount.get(),
      reachableHandleCount = entries.values().asScala.count(_.hasEntry),
      slotCount = entries.size()
    )

  private def installShutdownHookIfNeeded(): Unit =
    if (shutdownHookInstalled.compareAndSet(false, true)) {
      Runtime.getRuntime.addShutdownHook(new Thread(() => closeAll()))
    }

  private def registerSparkListenerIfNeeded(spark: SparkSession, appId: String): Unit =
    if (listenerAppIds.add(appId)) {
      spark.sparkContext.addSparkListener(new SparkListener {
        override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit =
          OpenIvmRocksDBRegistry.closeAllForSparkContext(appId)
      })
    }

  private def assertSubset(canonicalPath: String, entry: Entry, requestedColumnFamilies: Set[String]): Unit = {
    val missing = requestedColumnFamilies.diff(entry.columnFamilies)
    if (missing.nonEmpty) {
      val missingName = missing.toSeq.sorted.head
      throw new IllegalArgumentException(
        s"OpenIVM RocksDB at $canonicalPath is already open without required column family '$missingName'. " +
          s"Registered column families: ${entry.columnFamilies.toSeq.sorted.mkString(", ")}"
      )
    }
  }

  private def cleanupFailedOpen(db: OpenIvmRocksDB, error: Throwable): Nothing = {
    try {
      OpenIvmMaintenanceCoordinator.unregister(db)
    } catch {
      case NonFatal(closeError) => error.addSuppressed(closeError)
    }

    try {
      db.close()
    } catch {
      case NonFatal(closeError) => error.addSuppressed(closeError)
    }

    throw error
  }

  private def closeEntry(canonicalPath: String, entry: Entry): Unit = {
    var firstError: Throwable = null

    try {
      OpenIvmMaintenanceCoordinator.unregister(entry.db)
    } catch {
      case NonFatal(error) => firstError = error
    }

    try {
      entry.db.close()
    } catch {
      case NonFatal(error) =>
        if (firstError == null) {
          firstError = error
        } else {
          firstError.addSuppressed(error)
        }
    }

    if (firstError != null) {
      log.warn(s"openivm-rocksdb registry failed to close $canonicalPath", firstError)
    }
  }
}
