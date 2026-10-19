package org.openivm.spark.common

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.{
  Action,
  AddCDCFile,
  AddFile,
  CommitInfo,
  DeletionVectorDescriptor,
  Metadata,
  RemoveFile
}

import java.io.FileNotFoundException
import java.net.URI
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Callable, ExecutionException, ExecutorService, Executors, Future, ThreadFactory}
import scala.util.control.NonFatal

/** Whether Delta can still serve a source's change rows for a version range.
  *
  * VACUUM deletes `_change_data` files, removed data files and superseded deletion vectors once
  * they pass retention, and log cleanup deletes old commits. A change-feed read over such a range
  * fails part-way through a refresh, so REFRESH checks the range before it applies anything and
  * recomputes the view instead.
  */
object CdfChangeAvailability {

  private val ChangeDataFeedKey = "delta.enableChangeDataFeed"

  private val ProbeParallelism = 16

  /** Error classes and message fragments Spark and Delta (3.x and 4.x) raise when change rows or
    * the commits describing them are gone.
    */
  private val UnavailableMarkers: Seq[String] = Seq(
    "DELTA_CHANGE_DATA_FILE_NOT_FOUND",
    "DELTA_FILE_NOT_FOUND",
    "DELTA_FILE_OR_DIR_NOT_FOUND",
    "DELTA_LOG_FILE_NOT_FOUND",
    "DELTA_MISSING_CHANGE_DATA",
    "DELTA_TRUNCATED_TRANSACTION_LOG",
    "DELTA_VERSIONS_NOT_CONTIGUOUS",
    "DELTA_VERSION_NOT_FOUND",
    "DELTA_TIMESTAMP_EARLIER_THAN_COMMIT_RETENTION",
    "DELTA_UNSUPPORTED_TIME_TRAVEL_BEYOND_DELETED_FILE_RETENTION_DURATION",
    "FAILED_READ_FILE.FILE_NOT_EXIST",
    "earliest available version",
    "Available versions:"
  )

  private lazy val probePool: ExecutorService =
    Executors.newFixedThreadPool(
      ProbeParallelism,
      new ThreadFactory {
        private val count = new AtomicInteger()
        override def newThread(runnable: Runnable): Thread = {
          val thread = new Thread(runnable, s"openivm-cdf-availability-${count.incrementAndGet()}")
          thread.setDaemon(true)
          thread
        }
      }
    )

  /** Why the change rows of `(startVersionExclusive, endVersionInclusive]` can no longer be read,
    * or `None` when they can.
    *
    * Mirrors Delta's batch change-feed reader: every commit in the range must still be in the log,
    * the change feed must be enabled from the start of the range, and every file the reader opens
    * must exist — a commit's `_change_data` files when it wrote any, otherwise its data-changing
    * added and removed files and their deletion vectors.
    *
    * @param spark                 active session
    * @param tableNameOrPath       source table name or Delta path
    * @param startVersionExclusive last consumed version
    * @param endVersionInclusive   last version to consume
    */
  def unavailableReason(
      spark: SparkSession,
      tableNameOrPath: String,
      startVersionExclusive: Long,
      endVersionInclusive: Long
  ): Option[String] =
    if (endVersionInclusive <= startVersionExclusive) None
    else
      try inspect(DeltaTableVersion.deltaLogFor(spark, tableNameOrPath), startVersionExclusive, endVersionInclusive)
      catch {
        case NonFatal(failure) if isChangeDataUnavailable(failure) =>
          Some(s"change feed unreadable: ${describe(failure)}")
      }

  /** Whether `failure`, or any of its causes, reports change rows or commits that no longer exist.
    *
    * @param failure exception raised while reading a Delta change feed or its log
    */
  def isChangeDataUnavailable(failure: Throwable): Boolean =
    causes(failure).exists { cause =>
      cause.isInstanceOf[FileNotFoundException] ||
      cause.getClass.getName.contains("VersionNotFound") ||
      Option(cause.getMessage).exists(message => UnavailableMarkers.exists(message.contains))
    }

  private def inspect(deltaLog: DeltaLog, startVersionExclusive: Long, endVersionInclusive: Long): Option[String] = {
    val first    = startVersionExclusive + 1L
    val snapshot = deltaLog.update()
    val commits = deltaLog
      .getChanges(first, failOnDataLoss = false)
      .takeWhile(_._1 <= math.max(snapshot.version, endVersionInclusive))
      .map { case (version, actions) => version -> actions.toVector }
      .toVector
    val range = commits.takeWhile(_._1 <= endVersionInclusive)
    if (range.size.toLong != endVersionInclusive - startVersionExclusive || range.headOption.forall(_._1 != first)) {
      val found = range.headOption.fold("none")(_._1.toString)
      return Some(s"commits $first..$endVersionInclusive are no longer all in the Delta log (first found: $found)")
    }

    val metadataChanges = commits.flatMap { case (version, actions) =>
      actions.collect { case metadata: Metadata => version -> metadata }
    }
    val startMetadata =
      if (metadataChanges.isEmpty) snapshot.metadata else deltaLog.getSnapshotAt(first).metadata
    val disabledAt =
      if (!changeDataFeedEnabled(startMetadata)) Some(first)
      else
        metadataChanges.collectFirst {
          case (version, metadata) if version <= endVersionInclusive && !changeDataFeedEnabled(metadata) => version
        }
    disabledAt match {
      case Some(version) => Some(s"the change data feed is not enabled at version $version")
      case None =>
        val files   = range.flatMap { case (_, actions) => filesRead(actions, deltaLog.dataPath) }.distinct
        val missing = missingFiles(files, deltaLog.newDeltaHadoopConf())
        if (missing.isEmpty) None
        else Some(s"${missing.size} of ${files.size} change data files no longer exist, e.g. ${missing.head}")
    }
  }

  /** Files Delta's change-feed reader opens for one commit. */
  private def filesRead(actions: Seq[Action], dataPath: Path): Seq[Path] = {
    val changeFiles = actions.collect { case change: AddCDCFile => change.path }
    if (changeFiles.nonEmpty) changeFiles.map(absolute(dataPath, _))
    else if (actions.exists { case info: CommitInfo => isMergeWithoutChangedRows(info); case _ => false }) Nil
    else
      actions.flatMap {
        case add: AddFile if add.dataChange =>
          absolute(dataPath, add.path) +: onDiskVector(dataPath, add.deletionVector)
        case remove: RemoveFile if remove.dataChange =>
          absolute(dataPath, remove.path) +: onDiskVector(dataPath, remove.deletionVector)
        case _ => Nil
      }
  }

  private def isMergeWithoutChangedRows(info: CommitInfo): Boolean = {
    val metrics = info.operationMetrics.getOrElse(Map.empty[String, String])
    info.operation == "MERGE" &&
    Seq("numTargetRowsInserted", "numTargetRowsUpdated", "numTargetRowsDeleted").forall(metrics.get(_).contains("0"))
  }

  private def onDiskVector(dataPath: Path, vector: DeletionVectorDescriptor): Seq[Path] =
    Option(vector)
      .filter(_.storageType != DeletionVectorDescriptor.INLINE_DV_MARKER)
      .map(_.absolutePath(dataPath))
      .toSeq

  private def absolute(dataPath: Path, path: String): Path = {
    val parsed = new Path(new URI(path))
    if (parsed.isAbsolute) parsed else new Path(dataPath, parsed)
  }

  private def changeDataFeedEnabled(metadata: Metadata): Boolean =
    metadata.configuration.exists { case (key, value) =>
      key.equalsIgnoreCase(ChangeDataFeedKey) && value.trim.equalsIgnoreCase("true")
    }

  private def missingFiles(paths: Seq[Path], conf: Configuration): Seq[Path] = {
    def absent(path: Path): Boolean = !path.getFileSystem(conf).exists(path)
    if (paths.size <= 1) paths.filter(absent)
    else
      paths
        .map(path => path -> probePool.submit(new Callable[Boolean] { override def call(): Boolean = absent(path) }))
        .collect { case (path, probe) if awaitProbe(probe) => path }
  }

  private def awaitProbe(probe: Future[Boolean]): Boolean =
    try probe.get()
    catch { case failure: ExecutionException if failure.getCause != null => throw failure.getCause }

  private def causes(failure: Throwable): Seq[Throwable] =
    Iterator.iterate(failure)(_.getCause).takeWhile(_ != null).take(32).toSeq

  private def describe(failure: Throwable): String = {
    val cause = causes(failure).reverse.find(isChangeDataUnavailable).getOrElse(failure)
    s"${cause.getClass.getSimpleName}: ${Option(cause.getMessage).getOrElse("").split('\n').headOption.getOrElse("")}"
  }
}
