package org.openivm.spark.common.rocksdb

import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.io.File
import scala.collection.mutable
import scala.util.control.NonFatal

object OpenIvmRocksDBRegistry {
  private val log = LoggerFactory.getLogger(getClass)

  private final case class Entry(
      db: OpenIvmRocksDB,
      conf: OpenIvmRocksDBConf,
      columnFamilies: Set[String],
      appIds: Set[String]
  )

  private val entries        = mutable.HashMap.empty[String, Entry]
  private val listenerAppIds = mutable.HashSet.empty[String]

  @volatile private var shutdownHookInstalled = false

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

    this.synchronized {
      entries.get(canonicalPath) match {
        case Some(entry) =>
          assertSubset(canonicalPath, entry, requestedColumnFamilies)
          val updatedEntry =
            if (entry.appIds.contains(appId)) entry else entry.copy(appIds = entry.appIds + appId)
          entries.update(canonicalPath, updatedEntry)
          entry.db

        case None =>
          val conf = OpenIvmRocksDBConf.fromSpark(spark)
          val db = new OpenIvmRocksDB(
            canonicalPath,
            conf,
            requestedColumnFamilies.toSeq.sorted.filterNot(_ == OpenIvmRocksDB.DefaultColumnFamilyName)
          )
          try {
            db.load()
            OpenIvmMaintenanceCoordinator.register(db, conf)
            entries.put(canonicalPath, Entry(db, conf, requestedColumnFamilies, Set(appId)))
            db
          } catch {
            case error: Throwable =>
              cleanupFailedOpen(db, error)
          }
      }
    }
  }

  def close(dbPath: String): Unit = {
    val canonicalPath = canonicalLocalPath(dbPath)
    val entry = this.synchronized {
      entries.remove(canonicalPath)
    }
    entry.foreach(closeEntry(canonicalPath, _))
  }

  def closeAllForSparkContext(appId: String): Unit = {
    val toClose = this.synchronized {
      listenerAppIds -= appId
      entries.toSeq.flatMap { case (path, entry) =>
        if (!entry.appIds.contains(appId)) {
          None
        } else {
          val remainingAppIds = entry.appIds - appId
          if (remainingAppIds.isEmpty) {
            entries.remove(path)
            Some(path -> entry)
          } else {
            entries.update(path, entry.copy(appIds = remainingAppIds))
            None
          }
        }
      }
    }

    toClose.foreach { case (path, entry) =>
      closeEntry(path, entry)
    }
  }

  def closeAll(): Unit = {
    val toClose = this.synchronized {
      val snapshot = entries.toSeq
      entries.clear()
      listenerAppIds.clear()
      snapshot
    }

    try {
      toClose.foreach { case (path, entry) =>
        closeEntry(path, entry)
      }
    } finally {
      OpenIvmMaintenanceCoordinator.shutdown()
    }
  }

  private def canonicalLocalPath(path: String): String =
    new File(RocksDBCodec.requireLocalPath(path)).getCanonicalPath

  private[rocksdb] def overrideAppIdsForTesting(dbPath: String, appIds: Set[String]): Unit = this.synchronized {
    val canonicalPath = canonicalLocalPath(dbPath)
    val entry = entries.getOrElse(
      canonicalPath,
      throw new IllegalArgumentException(s"No open RocksDB registry entry found for $canonicalPath")
    )
    entries.update(canonicalPath, entry.copy(appIds = appIds))
  }

  private def installShutdownHookIfNeeded(): Unit = this.synchronized {
    if (!shutdownHookInstalled) {
      Runtime.getRuntime.addShutdownHook(new Thread(() => closeAll()))
      shutdownHookInstalled = true
    }
  }

  private def registerSparkListenerIfNeeded(spark: SparkSession, appId: String): Unit = {
    val shouldRegister = this.synchronized {
      if (listenerAppIds.contains(appId)) {
        false
      } else {
        listenerAppIds += appId
        true
      }
    }

    if (shouldRegister) {
      spark.sparkContext.addSparkListener(new SparkListener {
        override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit =
          OpenIvmRocksDBRegistry.closeAllForSparkContext(appId)
      })
    }
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
