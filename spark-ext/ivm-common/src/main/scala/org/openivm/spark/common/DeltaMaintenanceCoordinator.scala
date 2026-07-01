package org.openivm.spark.common

import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.{Row, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable

object DeltaMaintenanceCoordinator {
  private val log = LoggerFactory.getLogger(getClass)

  private val inProgressLocations = mutable.HashSet.empty[String]
  private val listenerAppIds      = mutable.HashSet.empty[String]

  @volatile private var running               = false
  @volatile private var shutdownHookInstalled = false
  private var daemonThread: Thread            = _
  private var sparkSession: SparkSession      = _

  def ensureStarted(spark: SparkSession): Unit = {
    if (!FeatureGate.maintenanceEnabled(spark)) return

    this.synchronized {
      sparkSession = spark
      installShutdownHookIfNeededLocked()
      registerSparkListenerIfNeededLocked(spark, spark.sparkContext.applicationId)
      startDaemonIfNeededLocked()
    }
  }

  def markRefreshInProgress(location: String): Unit =
    this.synchronized {
      inProgressLocations += normalizeLocation(location)
    }

  def clearRefreshInProgress(location: String): Unit =
    this.synchronized {
      inProgressLocations -= normalizeLocation(location)
    }

  def withRefreshInProgress[A](location: String)(body: => A): A = {
    markRefreshInProgress(location)
    try body
    finally clearRefreshInProgress(location)
  }

  def shutdown(): Unit = {
    val threadToJoin = this.synchronized {
      running = false
      inProgressLocations.clear()
      listenerAppIds.clear()
      sparkSession = null
      val current = daemonThread
      daemonThread = null
      current
    }

    if (threadToJoin != null) {
      threadToJoin.interrupt()
      threadToJoin.join(1000L)
    }
  }

  private def installShutdownHookIfNeededLocked(): Unit =
    if (!shutdownHookInstalled) {
      Runtime.getRuntime.addShutdownHook(new Thread(() => shutdown()))
      shutdownHookInstalled = true
    }

  private def registerSparkListenerIfNeededLocked(spark: SparkSession, appId: String): Unit =
    if (!listenerAppIds.contains(appId)) {
      listenerAppIds += appId
      spark.sparkContext.addSparkListener(new SparkListener {
        override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit =
          DeltaMaintenanceCoordinator.shutdown()
      })
    }

  private def startDaemonIfNeededLocked(): Unit =
    if (daemonThread == null || !daemonThread.isAlive) {
      running = true
      daemonThread = new Thread(() => runLoop(), "openivm-delta-maintenance")
      daemonThread.setDaemon(true)
      daemonThread.start()
    }

  private def runLoop(): Unit =
    while (running) {
      val sleepMs = this.synchronized {
        val spark = sparkSession
        if (!running || spark == null) 0L else FeatureGate.maintenanceIntervalSeconds(spark).toLong * 1000L
      }
      if (!running) return

      try {
        Thread.sleep(sleepMs)
      } catch {
        case _: InterruptedException => ()
      }

      if (!running) return

      val spark = this.synchronized { sparkSession }
      if (spark != null) {
        runOnce(spark)
      }
    }

  private def runOnce(spark: SparkSession): Unit = {
    if (!FeatureGate.maintenanceEnabled(spark)) return

    val mvs =
      try MvCatalog.list(spark)
      catch {
        case error: Throwable =>
          log.warn("openivm Delta maintenance failed to list materialized views", error)
          Seq.empty[MvMetadata]
      }

    mvs.foreach { mv =>
      val location = normalizeLocation(mv.location)
      if (isInProgress(location)) {
        log.debug(s"openivm Delta maintenance skipped in-progress MV at ${mv.location}")
      } else {
        runOptimizeIfNeeded(spark, mv)
        runVacuumIfNeeded(spark, mv)
      }
    }
  }

  private def runOptimizeIfNeeded(spark: SparkSession, mv: MvMetadata): Unit = {
    val zorderColumns = if (FeatureGate.maintenanceZorderEnabled(spark)) probeKeys(mv) else Seq.empty[String]
    val shouldOptimize = FeatureGate.maintenanceOptimizeEnabled(spark) ||
      (FeatureGate.maintenanceZorderEnabled(spark) && zorderColumns.nonEmpty)
    if (!shouldOptimize) return

    val files = fileCount(spark, mv.location).getOrElse(
      return
    )
    if (files <= FeatureGate.maintenanceOptimizeMinFiles(spark).toLong) return

    val sql = optimizeSql(mv.location, zorderColumns)
    try {
      spark.sql(sql).collect()
      log.info(s"openivm Delta maintenance executed: $sql")
    } catch {
      case error: Throwable =>
        log.warn(s"openivm Delta maintenance OPTIMIZE failed for ${mv.location}", error)
    }
  }

  private def runVacuumIfNeeded(spark: SparkSession, mv: MvMetadata): Unit = {
    if (!FeatureGate.maintenanceVacuumEnabled(spark)) return

    val sql = vacuumSql(mv.location, FeatureGate.maintenanceVacuumRetentionHours(spark))
    try {
      spark.sql(sql).collect()
      log.info(s"openivm Delta maintenance executed: $sql")
    } catch {
      case error: Throwable =>
        log.warn(s"openivm Delta maintenance VACUUM failed for ${mv.location}", error)
    }
  }

  private def fileCount(spark: SparkSession, location: String): Option[Long] =
    try {
      val row = spark.sql(s"DESCRIBE DETAIL delta.`${escapeBackticks(location)}`").select("numFiles").head()
      Some(longFromRow(row, 0))
    } catch {
      case error: Throwable =>
        log.warn(s"openivm Delta maintenance failed to count files for $location", error)
        None
    }

  private def longFromRow(row: Row, ordinal: Int): Long =
    row.get(ordinal) match {
      case n: java.lang.Number => n.longValue()
      case other               => other.toString.toLong
    }

  private def isInProgress(location: String): Boolean =
    this.synchronized {
      inProgressLocations.contains(normalizeLocation(location))
    }

  private def normalizeLocation(location: String): String = location.trim.stripSuffix("/")

  private[common] def probeKeys(mv: MvMetadata): Seq[String] =
    mv.properties
      .get("_ivm_probe_keys")
      .toSeq
      .flatMap(_.split(","))
      .map(_.trim.stripPrefix("`").stripSuffix("`"))
      .filter(_.nonEmpty)
      .distinct
      .take(4)

  private[common] def optimizeSql(location: String, zorderColumns: Seq[String]): String = {
    val base = s"OPTIMIZE delta.`${escapeBackticks(location)}`"
    if (zorderColumns.nonEmpty) s"$base ZORDER BY (${zorderColumns.map(quoteColumn).mkString(", ")})" else base
  }

  private[common] def vacuumSql(location: String, retentionHours: Int): String =
    s"VACUUM delta.`${escapeBackticks(location)}` RETAIN $retentionHours HOURS"

  private[common] def isRunning: Boolean = {
    val current = daemonThread
    current != null && current.isAlive
  }

  private[common] def isMarkedInProgress(location: String): Boolean = isInProgress(location)

  private[common] def runOnceForTesting(spark: SparkSession): Unit = runOnce(spark)

  private def quoteColumn(column: String): String = s"`${escapeBackticks(column)}`"

  private def escapeBackticks(value: String): String = value.replace("`", "``")
}
