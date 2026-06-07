package org.openivm.spark.common.rocksdb

import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.control.NonFatal

object OpenIvmMaintenanceCoordinator {
  private val log = LoggerFactory.getLogger(getClass)

  private val registrations = mutable.HashMap.empty[OpenIvmRocksDB, OpenIvmRocksDBConf]

  @volatile private var running    = false
  private var daemonThread: Thread = _

  def register(db: OpenIvmRocksDB, conf: OpenIvmRocksDBConf): Unit = {
    val threadToInterrupt = this.synchronized {
      registrations.update(db, conf)
      startDaemonIfNeededLocked()
      daemonThread
    }
    if (threadToInterrupt != null) {
      threadToInterrupt.interrupt()
    }
  }

  def unregister(db: OpenIvmRocksDB): Unit = {
    val threadToInterrupt = this.synchronized {
      registrations.remove(db)
      daemonThread
    }
    if (threadToInterrupt != null) {
      threadToInterrupt.interrupt()
    }
  }

  def shutdown(): Unit = {
    val threadToJoin = this.synchronized {
      running = false
      registrations.clear()
      val current = daemonThread
      daemonThread = null
      current
    }

    if (threadToJoin != null) {
      threadToJoin.interrupt()
      threadToJoin.join(1000L)
    }
  }

  private def startDaemonIfNeededLocked(): Unit =
    if (daemonThread == null || !daemonThread.isAlive) {
      running = true
      daemonThread = new Thread(() => runLoop(), "openivm-rocksdb-maintenance")
      daemonThread.setDaemon(true)
      daemonThread.start()
    }

  private def currentIntervalMsLocked(): Long =
    if (registrations.isEmpty) {
      OpenIvmRocksDBConf.default.maintenanceIntervalMs
    } else {
      registrations.valuesIterator.map(_.maintenanceIntervalMs).min
    }

  private def runLoop(): Unit =
    while (running) {
      val sleepMs = this.synchronized {
        if (!running) 0L else currentIntervalMsLocked()
      }
      if (!running) {
        return
      }

      try {
        Thread.sleep(sleepMs)
      } catch {
        case _: InterruptedException => ()
      }

      if (!running) {
        return
      }

      val snapshot = this.synchronized {
        registrations.toSeq
      }

      snapshot.foreach { case (db, conf) =>
        try {
          if (!db.isClosed) {
            db.withWriteLock {
              if (!db.isClosed) {
                db.cleanup(conf.minVersionsToRetain)
                if (db.sstFileCount > conf.compactionThresholdSstCount) {
                  db.compactRange()
                }
              }
            }
          }
        } catch {
          case NonFatal(error) =>
            log.warn(s"openivm-rocksdb maintenance failed for ${db.path}", error)
        }
      }
    }

  private[rocksdb] def isRunning: Boolean = {
    val current = daemonThread
    current != null && current.isAlive
  }
}
