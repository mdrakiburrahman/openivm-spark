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
  }

  private val entries               = new ConcurrentHashMap[String, Slot]()
  private val listenerAppIds        = ConcurrentHashMap.newKeySet[String]()
  private val openEntryCount        = new AtomicInteger(0)
  private val shutdownHookInstalled = new AtomicBoolean(false)

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
      try {
        return slotFor(canonicalPath).getOrOpen(spark, appId, requestedColumnFamilies)
      } catch {
        case SlotRetiredSignal =>
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

  private def canonicalLocalPath(path: String): String =
    new File(RocksDBCodec.requireLocalPath(path)).getCanonicalPath

  private[rocksdb] def overrideAppIdsForTesting(dbPath: String, appIds: Set[String]): Unit = {
    val canonicalPath = canonicalLocalPath(dbPath)
    val slot = Option(entries.get(canonicalPath)).getOrElse(
      throw new IllegalArgumentException(s"No open RocksDB registry entry found for $canonicalPath")
    )
    slot.overrideAppIds(appIds)
  }

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
