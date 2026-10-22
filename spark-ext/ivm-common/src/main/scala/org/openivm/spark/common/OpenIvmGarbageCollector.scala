package org.openivm.spark.common

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.util.concurrent.{ExecutorService, Executors}
import java.util.concurrent.atomic.AtomicLong
import scala.util.control.NonFatal

/**
 * Reclaims OpenIVM-owned warehouse storage that no durable state references.
 *
 * Staging, view-delta and trigger locations hold change rows referenced by staging records.
 * Pruning a fully consumed record tombstones its location in the same RocksDB write. The location
 * is deleted only once the state without the record is durable, so a driver restored from the
 * newest published state snapshot never reads a deleted location: callers [[prepare]] a plan
 * before publishing state and [[collect]] it after the publish returns. Locations no record ever
 * referenced, left by a driver that stopped between writing and recording them, are swept in the
 * background once older than [[FeatureGate.GcOrphanGracePeriodKey]], together with mirrored
 * telemetry spans older than [[FeatureGate.TelemetryOneLakeRetentionKey]]. The sweep works from
 * the plan's snapshot of references and touches only the file system.
 */
object OpenIvmGarbageCollector {
  private val log = LoggerFactory.getLogger(getClass)

  /** Garbage candidates read from the local state before it is published.
    *
    * @param tombstoned locations whose records were pruned
    * @param referenced locations referenced by staging records, MV metadata, or the cached view
    *                   deltas staging records reference
    */
  final case class Plan(tombstoned: Set[String], referenced: Set[String])

  /** Staging-record roots and the depth of their record locations below each root. */
  private val OwnedRoots: Seq[(String, Int)] =
    Seq("_ivm/staging" -> 3, "_ivm/view_deltas" -> 2, "_openivm/triggers" -> 2)

  private final case class Sweep(
      hadoopConf: Configuration,
      warehouse: Path,
      protectedPaths: Set[String],
      cutoffMs: Long,
      telemetry: Option[(Path, Long)]
  )

  private val lastSweepMs = new AtomicLong(0L)
  private val executor: ExecutorService = Executors.newSingleThreadExecutor { runnable =>
    val thread = new Thread(runnable, "openivm-gc")
    thread.setDaemon(true)
    thread
  }

  /** Read the current garbage candidates, or `None` when collection is disabled or unreadable.
    *
    * @param spark active session
    */
  def prepare(spark: SparkSession): Option[Plan] =
    if (!FeatureGate.gcEnabled(spark)) None
    else
      try {
        val metadataReferences = MvCatalog
          .list(spark)
          .iterator
          .flatMap(_.properties.values)
          .filter(value => value.contains("/_ivm/") || value.contains("/_openivm/"))
          .toSet
        val recorded = StagingCatalog.referencedStagingPaths(spark)
        Some(
          Plan(
            StagingCatalog.pendingGarbage(spark),
            recorded ++ metadataReferences ++ StagingDeltaView.CachedViewDeltaRef.inputsOf(recorded)
          )
        )
      } catch {
        case NonFatal(error) =>
          log.warn(s"openivm gc: cannot read garbage candidates: $error")
          None
      }

  /** Delete the tombstoned locations of `plan` that nothing references, then start a background
    * orphan sweep when one is due. Call only after the state `plan` was read from is durable.
    *
    * @param spark active session
    * @param plan  candidates from [[prepare]]
    */
  def collect(spark: SparkSession, plan: Option[Plan]): Unit =
    plan.foreach { candidates =>
      try {
        val removed = candidates.tombstoned.filterNot(candidates.referenced.contains).filter { location =>
          try deleteLocation(spark.sessionState.newHadoopConf(), location)
          catch {
            case NonFatal(error) =>
              log.warn(s"openivm gc: cannot delete $location: $error")
              false
          }
        }
        StagingCatalog.forgetGarbage(spark, removed)
        if (removed.nonEmpty) log.info(s"openivm gc: deleted ${removed.size} consumed staging location(s)")
        val now  = System.currentTimeMillis()
        val last = lastSweepMs.get()
        if (now - last >= FeatureGate.gcOrphanSweepIntervalMs(spark) && lastSweepMs.compareAndSet(last, now)) {
          val sweep = Sweep(
            spark.sessionState.newHadoopConf(),
            new Path(spark.conf.get("spark.sql.warehouse.dir")),
            candidates.referenced ++ candidates.tombstoned,
            now - FeatureGate.gcOrphanGracePeriodMs(spark),
            FeatureGate
              .oneLakeTelemetryDir(spark)
              .map(dir => new Path(dir) -> (now - FeatureGate.telemetryOneLakeRetentionMs(spark)))
          )
          executor.execute(() => runSweep(sweep))
        }
      } catch {
        case NonFatal(error) => log.warn(s"openivm gc: collection failed: $error")
      }
    }

  /** Run an orphan sweep synchronously, as [[collect]] does in the background.
    *
    * @param spark active session
    * @param plan  candidates from [[prepare]], taken after the orphans were abandoned
    * @param nowMs time the grace period and telemetry retention are measured from
    */
  private[openivm] def sweepNow(spark: SparkSession, plan: Plan, nowMs: Long): Unit =
    runSweep(
      Sweep(
        spark.sessionState.newHadoopConf(),
        new Path(spark.conf.get("spark.sql.warehouse.dir")),
        plan.referenced ++ plan.tombstoned,
        nowMs - FeatureGate.gcOrphanGracePeriodMs(spark),
        FeatureGate
          .oneLakeTelemetryDir(spark)
          .map(dir => new Path(dir) -> (nowMs - FeatureGate.telemetryOneLakeRetentionMs(spark)))
      )
    )

  private def runSweep(sweep: Sweep): Unit =
    try {
      val fs             = sweep.warehouse.getFileSystem(sweep.hadoopConf)
      val protectedPaths = sweep.protectedPaths.map(path => qualify(fs, path))
      var removed        = 0
      OwnedRoots.foreach { case (relative, depth) =>
        locations(fs, new Path(sweep.warehouse, relative), depth).foreach { location =>
          val path = fs.makeQualified(location.getPath)
          if (
            !protectedPaths.contains(path.toString) &&
            lastModified(fs, location) < sweep.cutoffMs &&
            fs.delete(path, true)
          ) removed += 1
        }
      }
      if (removed > 0) log.info(s"openivm gc: deleted $removed unreferenced staging location(s)")
      sweep.telemetry.foreach { case (root, expiryMs) =>
        val rootFs = root.getFileSystem(sweep.hadoopConf)
        if (rootFs.exists(root)) {
          val expired = rootFs
            .listStatus(root)
            .count(status =>
              status.isFile && status.getModificationTime < expiryMs && rootFs.delete(status.getPath, false)
            )
          if (expired > 0) log.info(s"openivm gc: deleted $expired expired telemetry span(s) under $root")
        }
      }
    } catch {
      case NonFatal(error) => log.warn(s"openivm gc: orphan sweep failed: $error")
    }

  private def deleteLocation(hadoopConf: Configuration, location: String): Boolean = {
    val path = new Path(location)
    val fs   = path.getFileSystem(hadoopConf)
    !fs.exists(path) || fs.delete(path, true) || !fs.exists(path)
  }

  private def qualify(fs: FileSystem, location: String): String =
    try fs.makeQualified(new Path(location)).toString
    catch { case NonFatal(_) => location }

  private def locations(fs: FileSystem, root: Path, depth: Int): Seq[FileStatus] =
    if (!fs.exists(root)) Seq.empty
    else
      (1 until depth).foldLeft(fs.listStatus(root).toSeq.filter(_.isDirectory)) { (level, _) =>
        level.flatMap(parent => fs.listStatus(parent.getPath).toSeq.filter(_.isDirectory))
      }

  /** Newest modification time of a location and its direct children, which include its Delta log. */
  private def lastModified(fs: FileSystem, location: FileStatus): Long =
    (location.getModificationTime +: fs.listStatus(location.getPath).toSeq.map(_.getModificationTime)).max
}
