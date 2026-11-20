package org.openivm.spark.common.rocksdb

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.FeatureGate
import org.openivm.spark.telemetry.OpenIvmExecutionSpan
import org.slf4j.LoggerFactory

import java.io.File
import java.nio.file.Files
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.UUID
import java.util.concurrent.{CompletableFuture, ConcurrentHashMap, ConcurrentLinkedQueue, Executors}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import scala.util.control.NonFatal

/**
 * Mirrors the local RocksDB state tree (`<statePath>/_openivm`) to and from a
 * Hadoop-FS URI (OneLake `abfss://…` on Microsoft Fabric) so OpenIVM's IVM state
 * survives an ephemeral / recycled cloud Spark session.
 *
 * Gated entirely by [[FeatureGate.StateSyncUriKey]]: when the key is unset (every
 * local / HDFS deployment) all methods are no-ops and the on-disk behaviour is
 * unchanged. When set:
 *
 *   - [[maybeRestore]] runs once per Spark application, before any RocksDB opens,
 *     and downloads the remote tree if the local tree is absent/empty.
 *   - [[backupNow]] synchronously publishes a complete immutable generation.
 *     Concurrent requests share a pass only if they arrived before its request
 *     barrier. Failed or interrupted uploads never overwrite earlier snapshots.
 *   - [[backupAsync]] coalesces background requests through the same serialized
 *     generation publisher. Legacy flat-layout snapshots remain readable.
 */
private[rocksdb] final case class StateSyncPhaseStart(
    operation: String,
    workClass: String,
    startNanos: Long,
    startUtc: String
)

object OpenIvmStateSync {
  private val log = LoggerFactory.getLogger(getClass)

  private[rocksdb] final case class BackupStateSnapshot(running: Boolean, requested: Boolean)

  private final case class BackupRequest(
      requestId: String,
      parent: OpenIvmExecutionSpan.StateSyncParentContext,
      span: Option[OpenIvmExecutionSpan],
      requestedAtNanos: Long,
      requestedAtUtc: String,
      mode: String,
      captureTelemetry: Boolean,
      receiptWriter: Option[OpenIvmExecutionSpan.StateSyncReceiptWriter]
  )

  private final case class AsyncRequest(request: BackupRequest, afterPublish: () => Unit)
  private final case class SynchronousRequest(request: BackupRequest, completion: CompletableFuture[Unit])

  private final class BackupState {
    val passLock                    = new ReentrantLock(true)
    val requested                   = new AtomicBoolean(false)
    val running                     = new AtomicBoolean(false)
    private val requests            = new ConcurrentLinkedQueue[AsyncRequest]()
    private val synchronousMonitor  = new Object
    private val synchronousRequests = scala.collection.mutable.ArrayBuffer.empty[SynchronousRequest]
    private var synchronousRunning  = false
    private var submittedRequests   = 0L

    def synchronousRequestCount: Long = synchronousMonitor.synchronized(submittedRequests)

    def backupSynchronous(request: BackupRequest)(pass: Seq[BackupRequest] => Unit): Unit = {
      val completion = new CompletableFuture[Unit]()
      val submitted  = SynchronousRequest(request, completion)
      synchronousMonitor.synchronized {
        submittedRequests += 1L
        synchronousRequests += submitted
      }
      val beforeDrain = beforeSynchronousDrainHookForTesting
      if (beforeDrain != null) beforeDrain()
      try {
        while (!completion.isDone) {
          val batch = synchronousMonitor.synchronized {
            if (completion.isDone) Vector.empty[SynchronousRequest]
            else if (!synchronousRunning) {
              synchronousRunning = true
              val captured = synchronousRequests.toVector
              synchronousRequests.clear()
              captured
            } else {
              synchronousMonitor.wait()
              Vector.empty[SynchronousRequest]
            }
          }
          if (batch.nonEmpty) {
            var failure: Throwable = null
            try pass(batch.map(_.request))
            catch { case error: Throwable => failure = error }
            finally
              synchronousMonitor.synchronized {
                batch.foreach { waiting =>
                  if (failure == null) waiting.completion.complete(())
                  else waiting.completion.completeExceptionally(failure)
                }
                synchronousRunning = false
                synchronousMonitor.notifyAll()
              }
          }
        }
        try completion.get()
        catch { case error: java.util.concurrent.ExecutionException => throw error.getCause }
      } catch {
        case interrupted: InterruptedException =>
          synchronousMonitor.synchronized {
            synchronousRequests -= submitted
          }
          Thread.currentThread().interrupt()
          throw interrupted
      }
    }

    def capture(request: BackupRequest, afterPublish: () => Unit): Unit =
      requests.add(AsyncRequest(request, afterPublish))

    def drainRequests(): Seq[AsyncRequest] = {
      val drained = scala.collection.mutable.ArrayBuffer.empty[AsyncRequest]
      var request = requests.poll()
      while (request != null) {
        drained += request
        request = requests.poll()
      }
      drained.toSeq
    }
  }

  private val MaxPrunedPerPublish = 25

  private val restoreStates = new ConcurrentHashMap[String, CompletableFuture[Unit]]()
  private val backupStates  = new ConcurrentHashMap[String, BackupState]()
  @volatile private var backupPassHookForTesting: (String, SparkSession, String) => Unit = null
  @volatile private var stateSyncKeyHookForTesting: (SparkSession, String) => String     = null
  @volatile private var stateSyncUriHookForTesting: SparkSession => Option[String]       = null
  @volatile private var beforeLocalFileCopyHookForTesting: File => Unit                  = null
  @volatile private var beforeCaptureHookForTesting: () => Unit                          = null
  @volatile private var beforeSynchronousDrainHookForTesting: () => Unit                 = null
  @volatile private var duringCaptureHookForTesting: () => Unit                          = null
  @volatile private var copyLocalFileHookForTesting: (FileSystem, File, Path) => Unit    = null
  @volatile private var beforeSnapshotCommitHookForTesting: () => Unit                   = null

  private final class BackupPassTrace(
      requests: Seq[BackupRequest],
      mode: String,
      publicationId: String
  ) {
    val enabled: Boolean       = requests.exists(_.captureTelemetry)
    private val startedAtNanos = if (enabled) System.nanoTime() else 0L
    private val startedAtUtc   = if (enabled) Instant.now().toString else ""
    private val phases         = scala.collection.mutable.ArrayBuffer.empty[OpenIvmExecutionSpan.StateSyncPhaseEvidence]
    private var admittedAtNanos       = Option.empty[Long]
    private var wasCaptured           = false
    private var wasPublished          = false
    private var snapshotGeneration    = Option.empty[String]
    private var capturedDatabaseCount = Option.empty[Long]
    private var capturedFileCount     = Option.empty[Long]
    private var capturedBytes         = Option.empty[Long]
    private var archiveBytes          = Option.empty[Long]
    private var uploadedBytes         = Option.empty[Long]
    private var prunedPaths           = Option.empty[Long]

    def markAdmitted(atNanos: Long): Unit = if (enabled) admittedAtNanos = Some(atNanos)

    def setSnapshotGeneration(generation: String): Unit = if (enabled) snapshotGeneration = Some(generation)

    def beginPhase(operation: String, workClass: String): StateSyncPhaseStart =
      if (enabled) StateSyncPhaseStart(operation, workClass, System.nanoTime(), Instant.now().toString)
      else StateSyncPhaseStart(operation, workClass, 0L, "")

    def endPhase(
        started: StateSyncPhaseStart,
        status: String,
        counts: Map[String, Long] = Map.empty,
        error: Option[Throwable] = None
    ): Unit = if (enabled) {
      val endedAt = System.nanoTime()
      phases += OpenIvmExecutionSpan.StateSyncPhaseEvidence(
        started.operation,
        status,
        started.workClass,
        started.startNanos,
        elapsedNanos(started.startNanos, endedAt),
        started.startUtc,
        counts,
        error.map(_.getClass.getSimpleName.take(128))
      )
    }

    def measure[A](operation: String, workClass: String)(body: => A): A = {
      if (!enabled) return body
      val started = beginPhase(operation, workClass)
      try {
        val result = body
        endPhase(started, "success")
        result
      } catch {
        case error: Throwable =>
          endPhase(started, spanStatus(error), error = Some(error))
          throw error
      }
    }

    def markCaptured(databaseCount: Int): Unit = if (enabled) {
      wasCaptured = true
      capturedDatabaseCount = Some(databaseCount.toLong)
    }

    def markCapturedInventory(fileCount: Int, bytes: Option[Long]): Unit = if (enabled) {
      capturedFileCount = Some(fileCount.toLong)
      capturedBytes = bytes
    }

    def markArchive(bytes: Long): Unit = if (enabled) archiveBytes = Some(bytes)

    def markUploaded(bytes: Long): Unit = if (enabled) uploadedBytes = Some(bytes)

    def markPublished(): Unit = if (enabled) wasPublished = true

    def markPruned(paths: Long): Unit = if (enabled) prunedPaths = Some(paths)

    def finish(status: String, endNanos: Long, error: Option[Throwable]): Unit = {
      if (!enabled) return
      val admittedAt          = admittedAtNanos.getOrElse(endNanos)
      val committedGeneration = if (wasPublished) snapshotGeneration else None
      val trackedRequests     = requests.filter(_.captureTelemetry)
      val requestEvidence = trackedRequests.map { request =>
        val queueStatus = if (admittedAtNanos.nonEmpty) "success" else spanStatus(error.orNull)
        OpenIvmExecutionSpan.StateSyncRequestEvidence(
          requestId = request.requestId,
          parentRequestId = request.parent.requestId,
          publicationId = publicationId,
          effectiveMode = request.mode,
          scope = if (request.parent.modelUniqueId.nonEmpty) "model" else "run",
          queueWaitNs = elapsedNanos(request.requestedAtNanos, admittedAt),
          status = queueStatus,
          requested = true,
          captured = wasCaptured,
          published = wasPublished,
          snapshotGenerationId = committedGeneration,
          acknowledgedSnapshotGenerationId =
            if (request.mode == "synchronous" && status == "success" && wasPublished) committedGeneration else None,
          requestStartMonotonicNs = request.requestedAtNanos,
          requestStartUtc = request.requestedAtUtc
        )
      }
      val counts = Map(
        "requested_requests" -> Some(requests.size.toLong),
        "captured_requests"  -> Some(if (wasCaptured) requests.size.toLong else 0L),
        "published_requests" -> Some(if (wasPublished) requests.size.toLong else 0L),
        "captured_databases" -> capturedDatabaseCount,
        "captured_files"     -> capturedFileCount,
        "captured_bytes"     -> capturedBytes,
        "archive_bytes"      -> archiveBytes,
        "uploaded_bytes"     -> uploadedBytes,
        "pruned_paths"       -> prunedPaths
      )
      val publication = OpenIvmExecutionSpan.StateSyncPublicationEvidence(
        publicationId = publicationId,
        effectiveMode = mode,
        status = status,
        startMonotonicNs = startedAtNanos,
        durationNs = elapsedNanos(startedAtNanos, endNanos),
        startUtc = startedAtUtc,
        phases = phases.toVector,
        counts = counts,
        requestedRequests = requests.size,
        capturedRequests = if (wasCaptured) requests.size else 0,
        publishedRequests = if (wasPublished) requests.size else 0,
        captured = wasCaptured,
        published = wasPublished,
        snapshotGenerationId = committedGeneration,
        requestIds = trackedRequests.map(_.requestId).take(OpenIvmExecutionSpan.MaxStateSyncRequests),
        parentRequestIds =
          trackedRequests.flatMap(_.parent.requestId).distinct.take(OpenIvmExecutionSpan.MaxStateSyncRequests),
        droppedRequestIds = math.max(0, trackedRequests.size - OpenIvmExecutionSpan.MaxStateSyncRequests).toLong,
        droppedParentRequestIds = math
          .max(
            0,
            trackedRequests.flatMap(_.parent.requestId).distinct.size - OpenIvmExecutionSpan.MaxStateSyncRequests
          )
          .toLong
      )

      val parented = trackedRequests.zip(requestEvidence).flatMap { case (request, evidence) =>
        request.span.filter(_.stateSyncEvidenceEnabled).map(_ -> evidence)
      }
      val parentedRequests     = parented.map(_._2.requestId).toSet
      val unparented           = requestEvidence.filterNot(request => parentedRequests.contains(request.requestId))
      val unparentedRequestIds = unparented.iterator.map(_.requestId).toSet
      val unparentedReceiptWriter = trackedRequests
        .filter(request => unparentedRequestIds.contains(request.requestId))
        .iterator
        .flatMap(_.receiptWriter)
        .take(1)
        .toList
        .headOption
      val sinkBackedLeader    = parented.find(_._1.stateSyncPublicationSinkConfigured).map(_._1)
      val publishToRunReceipt = parented.nonEmpty && sinkBackedLeader.isEmpty && unparentedReceiptWriter.nonEmpty
      val leader = sinkBackedLeader
        .orElse(if (publishToRunReceipt) None else parented.headOption.map(_._1))
      if (parented.isEmpty)
        OpenIvmExecutionSpan.emitRunStateSyncPublication(
          requestEvidence,
          publication,
          trackedRequests.iterator.flatMap(_.receiptWriter).take(1).toList.headOption
        )
      else {
        parented.groupBy(_._1).foreach { case (parent, values) =>
          parent.recordStateSyncEvidence(
            values.map(_._2),
            if (leader.contains(parent)) Seq(publication) else Seq.empty
          )
        }
        if (publishToRunReceipt)
          OpenIvmExecutionSpan.emitRunStateSyncPublication(requestEvidence, publication, unparentedReceiptWriter)
        else if (unparented.nonEmpty)
          OpenIvmExecutionSpan.emitRunStateSyncRequests(unparented, unparentedReceiptWriter)
      }
    }
  }

  private def elapsedNanos(startNanos: Long, endNanos: Long): Long = {
    val elapsed = endNanos - startNanos
    require(elapsed >= 0L, "state-sync duration must be non-negative")
    elapsed
  }

  private def spanStatus(error: Throwable): String =
    if (error == null) "failed"
    else if (error.isInstanceOf[InterruptedException] || error.isInstanceOf[java.io.InterruptedIOException]) "cancelled"
    else "failed"

  private def newBackupRequest(spark: SparkSession, mode: String): BackupRequest = {
    val span             = OpenIvmExecutionSpan.captureCurrent()
    val captureTelemetry = OpenIvmExecutionSpan.stateSyncCaptureEnabled(spark, span)
    val emptyParent = OpenIvmExecutionSpan.StateSyncParentContext(
      None,
      None,
      None,
      None,
      None,
      None,
      None
    )
    val parent =
      if (!captureTelemetry) emptyParent
      else
        try OpenIvmExecutionSpan.captureStateSyncParent(spark, span)
        catch {
          case NonFatal(error) =>
            log.warn(s"openivm state-sync: optional telemetry context capture failed: $error")
            emptyParent
        }
    BackupRequest(
      requestId = if (captureTelemetry) UUID.randomUUID().toString else "",
      parent = parent,
      span = span,
      requestedAtNanos = if (captureTelemetry) System.nanoTime() else 0L,
      requestedAtUtc = if (captureTelemetry) Instant.now().toString else "",
      mode = mode,
      captureTelemetry = captureTelemetry,
      receiptWriter = if (captureTelemetry) OpenIvmExecutionSpan.captureStateSyncReceiptWriter(spark, span) else None
    )
  }

  private val backupExecutor = Executors.newCachedThreadPool { r =>
    val t = new Thread(r, "openivm-state-sync")
    t.setDaemon(true)
    t
  }

  private def localRoot(spark: SparkSession): File =
    OpenIvmStateBarrier.root(spark).toFile

  private def hasLocalStateFiles(root: File): Boolean = {
    if (!root.exists()) return false
    val paths = Files.walk(root.toPath)
    try {
      val iterator = paths.iterator()
      var found    = false
      while (iterator.hasNext && !found) {
        found = Files.isRegularFile(iterator.next())
      }
      found
    } finally paths.close()
  }

  private def canonicalLocalRoot(spark: SparkSession): String =
    try localRoot(spark).getCanonicalFile.getAbsolutePath
    catch {
      case NonFatal(_) => localRoot(spark).getAbsolutePath
    }

  private def stateSyncKey(spark: SparkSession, uri: String): String =
    s"${canonicalLocalRoot(spark)}|$uri"

  private def effectiveStateSyncKey(spark: SparkSession, uri: String): String = {
    val hook = stateSyncKeyHookForTesting
    if (hook == null) stateSyncKey(spark, uri) else hook(spark, uri)
  }

  private def stateSyncUri(spark: SparkSession): Option[String] = {
    val hook = stateSyncUriHookForTesting
    if (hook == null) FeatureGate.stateSyncUri(spark) else hook(spark)
  }

  private def backupStateFor(key: String): BackupState = {
    val fresh    = new BackupState
    val existing = backupStates.putIfAbsent(key, fresh)
    if (existing != null) existing else fresh
  }

  /** Download the remote state tree into the local `_openivm` dir on the first
    * call per Spark application. Restore failures fail all callers closed. */
  def maybeRestore(spark: SparkSession): Unit = {
    val uri = stateSyncUri(spark)
      .getOrElse(
        return
      )
    OpenIvmStateBarrier.register(spark)
    val key    = s"${spark.sparkContext.applicationId}|${effectiveStateSyncKey(spark, uri)}"
    val waiter = new CompletableFuture[Unit]()
    val prior  = restoreStates.putIfAbsent(key, waiter)
    if (prior != null) {
      try prior.join()
      catch { case error: java.util.concurrent.CompletionException => throw error.getCause }
      return
    }

    try {
      OpenIvmStateBarrier.withCapture(spark) {
        restoreNow(spark, uri)
      }
      waiter.complete(())
    } catch {
      case error: Throwable =>
        waiter.completeExceptionally(error)
        throw error
    }
  }

  /** Coalesced, non-blocking incremental backup of the local `_openivm` tree to
    * the remote URI. If a backup is already queued this call is folded into that
    * in-flight work and forces one more pass afterwards. */
  def backupAsync(spark: SparkSession): Unit = backupAsync(spark, () => ())

  /** Select the command's backup policy, then run cleanup only after publication succeeds. */
  def backupForCommand(spark: SparkSession, afterPublish: () => Unit = () => ()): Unit =
    if (FeatureGate.stateSyncAsyncEnabled(spark)) backupAsync(spark, afterPublish)
    else {
      backupNow(spark)
      afterPublish()
    }

  private def backupAsync(spark: SparkSession, afterPublish: () => Unit): Unit = {
    val uri = stateSyncUri(spark)
      .getOrElse {
        afterPublish()
        return
      }
    val key   = effectiveStateSyncKey(spark, uri)
    val state = backupStateFor(key)
    state.capture(newBackupRequest(spark, "asynchronous"), afterPublish)
    state.requested.set(true)
    scheduleBackupIfNeeded(state, spark, uri)
  }

  private def scheduleBackupIfNeeded(state: BackupState, spark: SparkSession, uri: String): Unit =
    if (state.running.compareAndSet(false, true)) {
      val key = effectiveStateSyncKey(spark, uri)
      backupExecutor.execute(() => runBackupLoop(key, state, spark, uri))
    }

  private def runBackupLoop(key: String, state: BackupState, spark: SparkSession, uri: String): Unit =
    try {
      var continue = true
      while (continue) {
        state.requested.set(false)
        // Capture before the snapshot: a later request must not clean up files
        // until a pass that covers its committed state has succeeded.
        val requests  = state.drainRequests()
        val started   = System.nanoTime()
        var succeeded = false
        try {
          runBackupPass(key, spark, uri, requests.map(_.request), "asynchronous")
          succeeded = true
        } catch { case NonFatal(e) => log.warn(s"openivm state-sync: backup failed: $e") }
        finally {
          val durationMs = (System.nanoTime() - started) / 1000000L
          requests.foreach(_.request.span.foreach(_.recordRocksDbBackup(durationMs)))
          log.info(
            s"openivm state-sync: async backup duration_ms=$durationMs succeeded=$succeeded requests=${requests.size}"
          )
        }
        if (succeeded)
          requests.foreach { request =>
            try request.afterPublish()
            catch { case NonFatal(e) => log.warn(s"openivm state-sync: post-backup cleanup failed: $e") }
          }
        continue = state.requested.get()
      }
    } finally {
      state.running.set(false)
      if (state.requested.get() && state.running.compareAndSet(false, true)) {
        backupExecutor.execute(() => runBackupLoop(key, state, spark, uri))
      }
    }

  /** Synchronously publish a snapshot covering this request's committed state. */
  def backupNow(spark: SparkSession): Unit = {
    val uri = stateSyncUri(spark)
      .getOrElse(
        return
      )
    val key     = effectiveStateSyncKey(spark, uri)
    val request = newBackupRequest(spark, "synchronous")
    val started = System.nanoTime()
    try
      backupStateFor(key).backupSynchronous(request) { requests =>
        runBackupPass(key, spark, uri, requests, "synchronous")
      }
    finally {
      val durationMs = (System.nanoTime() - started) / 1000000L
      request.span.foreach(_.recordRocksDbBackup(durationMs))
    }
  }

  private def runBackupPass(
      key: String,
      spark: SparkSession,
      uri: String,
      requests: Seq[BackupRequest],
      mode: String
  ): Unit = {
    val operationId = if (requests.exists(_.captureTelemetry)) UUID.randomUUID().toString else ""
    val trace       = new BackupPassTrace(requests, mode, operationId)
    val lock        = backupStateFor(key).passLock
    var locked      = false
    var failure     = Option.empty[Throwable]
    try {
      lock.lockInterruptibly()
      locked = true
      trace.markAdmitted(System.nanoTime())
      val hook = backupPassHookForTesting
      if (hook == null) backupNowInternal(spark, uri, trace)
      else hook(key, spark, uri)
    } catch {
      case error: Throwable =>
        failure = Some(error)
        throw error
    } finally {
      val ended = if (trace.enabled) System.nanoTime() else 0L
      if (locked) lock.unlock()
      try trace.finish(failure.fold("success")(_ => spanStatus(failure.get)), ended, failure)
      catch {
        case NonFatal(telemetryError) =>
          log.warn(s"openivm state-sync: optional telemetry emission failed: $telemetryError")
      }
    }
  }

  private def restoreNow(spark: SparkSession, uri: String): Unit = {
    val local = localRoot(spark)
    if (hasLocalStateFiles(local)) return

    val hconf      = spark.sessionState.newHadoopConf()
    val remoteRoot = new Path(uri)
    val fs         = remoteRoot.getFileSystem(hconf)
    if (!fs.exists(remoteRoot)) {
      log.info(s"openivm state-sync: no remote state at $uri, starting fresh")
      return
    }

    val committed = latestCommit(fs, remoteRoot)
    committed.flatMap(_.archive).foreach { info =>
      restoreArchive(fs, committed.get.root, info, local)
      return
    }
    val snapshotRoot = committed.map(_.root).getOrElse(remoteRoot)
    local.mkdirs()
    var restored = 0
    val it       = fs.listFiles(snapshotRoot, /* recursive = */ true)
    while (it.hasNext) {
      val remotePath = it.next().getPath
      val rel        = relativize(snapshotRoot, remotePath)
      if (!rel.startsWith(".openivm-state-sync/")) {
        val dst = new File(local, rel)
        Option(dst.getParentFile).foreach(_.mkdirs())
        // useRawLocalFileSystem=true so Hadoop does NOT drop .crc sidecars into
        // the RocksDB directory (RocksDB would choke on the extra files).
        fs.copyToLocalFile(
          /* delSrc = */ false,
          remotePath,
          new Path(dst.getAbsolutePath), /* useRawLocalFileSystem = */ true
        )
        restored += 1
      }
    }
    if (restored == 0 && snapshotRoot == remoteRoot && fs.exists(new Path(remoteRoot, ".openivm-state-sync")))
      throw new java.io.IOException(s"openivm state-sync: no committed snapshot or legacy state at $uri")
    log.info(s"openivm state-sync: restored $restored files from $uri to $local")
  }

  private final case class CapturedState(databaseCount: Int)
  private final case class PackedArchive(checksum: String, archiveBytes: Long)
  private final case class PruneResult(deletedPaths: Long, failure: Option[Throwable])

  private def backupNowInternal(spark: SparkSession, uri: String, trace: BackupPassTrace): Unit = {
    val local = localRoot(spark)
    if (!local.exists()) return

    val hconf         = spark.sessionState.newHadoopConf()
    val namespaceRoot = new Path(uri)
    val fs            = namespaceRoot.getFileSystem(hconf)
    val controlRoot   = new Path(namespaceRoot, ".openivm-state-sync")
    val commitRoot    = new Path(controlRoot, "commits")
    if (!fs.mkdirs(commitRoot))
      throw new java.io.IOException(s"openivm state-sync: cannot create commit directory $commitRoot")
    val previous = fs
      .listStatus(commitRoot)
      .iterator
      .filter(status => status.isFile && status.getPath.getName.endsWith(".commit"))
      .map(_.getPath.getName.takeWhile(_ != '-').toLong)
      .foldLeft(0L)(math.max)
    val stamp      = math.max(System.currentTimeMillis(), previous + 1L)
    val generation = f"$stamp%019d-${UUID.randomUUID()}"
    val remoteRoot = new Path(new Path(controlRoot, "snapshots"), generation)
    if (!fs.mkdirs(remoteRoot))
      throw new java.io.IOException(s"openivm state-sync: cannot create snapshot $remoteRoot")
    trace.setSnapshotGeneration(generation)
    OpenIvmStateBarrier.register(spark)
    val archive = new File(local.getParentFile, s".openivm-state-snapshot-$generation.zip")
    val capture = Files.createTempDirectory(local.getParentFile.toPath, ".openivm-state-capture-").toFile
    try {
      val captureHook = beforeCaptureHookForTesting
      if (captureHook != null) captureHook()
      val barrierStarted  = trace.beginPhase("spark.backup.barrier_wait", "wait")
      var barrierAcquired = false
      try {
        OpenIvmStateBarrier.withCapture(spark) {
          trace.endPhase(barrierStarted, "success")
          barrierAcquired = true
          val captured = trace.measure("spark.backup.capture", "durability") {
            OpenIvmSnapshotArchive.checkInterrupted()
            val captureHook = duringCaptureHookForTesting
            var captureHookInvoked = false
            val localBase = local.toPath
            val files     = backupFiles(local)
            val databases = files
              .filter(file => file.getName == "CURRENT" && file.getParentFile.getName == "rocksdb")
              .map(_.getParentFile.toPath)
              .distinct
              .sortBy(_.toString)
            databases.foreach { db =>
              val target = capture.toPath.resolve(localBase.relativize(db))
              Files.createDirectories(target.getParent)
              OpenIvmRocksDBRegistry.checkpoint(spark, db.toString, target)
              if (!captureHookInvoked && captureHook != null) {
                captureHookInvoked = true
                captureHook()
              }
            }
            files.filterNot(file => databases.exists(file.toPath.startsWith)).foreach { file =>
              OpenIvmSnapshotArchive.checkInterrupted()
              val target = capture.toPath.resolve(localBase.relativize(file.toPath))
              Files.createDirectories(target.getParent)
              Files.copy(file.toPath, target)
            }
            CapturedState(databases.size)
          }
          trace.markCaptured(captured.databaseCount)
        }
      } catch {
        case error: Throwable =>
          if (!barrierAcquired) trace.endPhase(barrierStarted, spanStatus(error), error = Some(error))
          throw error
      }

      val packed = trace.measure("spark.backup.pack_hash", "durability") {
        val files = backupFiles(capture)
        if (trace.enabled) {
          val capturedBytes = files.foldLeft(Option(0L)) { (total, file) =>
            total.flatMap { bytes =>
              try Some(bytes + Files.size(file.toPath))
              catch { case NonFatal(_) => None }
            }
          }
          trace.markCapturedInventory(files.size, capturedBytes)
        }
        val checksum = OpenIvmSnapshotArchive.pack(
          capture,
          files,
          archive,
          file => {
            val hook = beforeLocalFileCopyHookForTesting
            if (hook != null) hook(file)
          }
        )
        val archiveBytes = archive.length()
        trace.markArchive(archiveBytes)
        PackedArchive(checksum, archiveBytes)
      }
      val archiveBytes  = packed.archiveBytes
      val remoteArchive = new Path(remoteRoot, "snapshot.zip")
      val copyHook      = copyLocalFileHookForTesting
      trace.measure("spark.backup.upload", "durability") {
        if (copyHook != null) copyHook(fs, archive, remoteArchive)
        else fs.copyFromLocalFile(false, true, new Path(archive.getAbsolutePath), remoteArchive)
        OpenIvmSnapshotArchive.checkInterrupted()
        val remoteBytes = fs.getFileStatus(remoteArchive).getLen
        if (remoteBytes != archiveBytes)
          throw new java.io.IOException("openivm state-sync: uploaded archive length mismatch")
        trace.markUploaded(remoteBytes)
      }
      val pendingCommit = new Path(commitRoot, s".$generation.pending")
      val committed     = new Path(commitRoot, s"$generation.commit")
      trace.measure("spark.backup.commit", "durability") {
        val output = fs.create(pendingCommit, false)
        try
          output.write(
            s"OPENIVM_STATE_SNAPSHOT_V2\n$generation\n$archiveBytes\n${packed.checksum}\n".getBytes(
              StandardCharsets.UTF_8
            )
          )
        finally output.close()
        val commitHook = beforeSnapshotCommitHookForTesting
        if (commitHook != null) commitHook()
        OpenIvmSnapshotArchive.checkInterrupted()
        if (!fs.rename(pendingCommit, committed))
          throw new java.io.IOException(s"openivm state-sync: cannot publish snapshot commit $committed")
        trace.markPublished()
      }
      log.info(
        s"openivm state-sync: published packed snapshot $generation ($archiveBytes bytes) at $uri"
      )
      val pruneStarted = trace.beginPhase("spark.backup.prune", "lifecycle")
      val pruned       = pruneGenerations(fs, controlRoot, FeatureGate.stateSyncRetainGenerations(spark))
      trace.markPruned(pruned.deletedPaths)
      trace.endPhase(
        pruneStarted,
        if (pruned.failure.isDefined) "failed" else "success",
        counts = Map("deleted_paths" -> pruned.deletedPaths),
        error = pruned.failure
      )
    } finally {
      try Files.deleteIfExists(archive.toPath)
      finally deleteStagedTree(capture)
    }
  }

  /** Delete committed generations older than the newest `retain`, then snapshots and pending
    * commit markers older than every retained generation, which interrupted publishes left behind.
    * A generation is unpublished before its archive is deleted, so restore never selects it. At
    * most [[MaxPrunedPerPublish]] entries go per publish, so a large backlog never stalls one.
    */
  private def pruneGenerations(fs: FileSystem, controlRoot: Path, retain: Int): PruneResult = {
    var deletedPaths = 0L
    try {
      val commitRoot    = new Path(controlRoot, "commits")
      val snapshotsRoot = new Path(controlRoot, "snapshots")
      val committed = fs
        .listStatus(commitRoot)
        .toSeq
        .filter(status => status.isFile && status.getPath.getName.endsWith(".commit"))
        .map(_.getPath.getName.stripSuffix(".commit"))
        .sorted
      val retained = committed.takeRight(retain)
      val expired  = committed.dropRight(retain).take(MaxPrunedPerPublish)
      expired.foreach { generation =>
        if (fs.delete(new Path(commitRoot, s"$generation.commit"), false)) deletedPaths += 1
        if (fs.delete(new Path(snapshotsRoot, generation), true)) deletedPaths += 1
      }
      val budget = MaxPrunedPerPublish - expired.size
      if (budget > 0) retained.headOption.foreach { oldestRetained =>
        val staleSnapshots = fs
          .listStatus(snapshotsRoot)
          .toSeq
          .filter(status => status.isDirectory && status.getPath.getName < oldestRetained)
          .map(_.getPath)
        val stalePending = fs
          .listStatus(commitRoot)
          .toSeq
          .filter { status =>
            val name = status.getPath.getName
            name.startsWith(".") && name.endsWith(".pending") && name.stripPrefix(".") < oldestRetained
          }
          .map(_.getPath)
        (staleSnapshots ++ stalePending).take(budget).foreach { path =>
          if (fs.delete(path, true)) deletedPaths += 1
        }
      }
      PruneResult(deletedPaths, None)
    } catch {
      case NonFatal(error) =>
        log.warn(s"openivm state-sync: cannot prune old snapshot generations: $error")
        PruneResult(deletedPaths, Some(error))
    }
  }

  private def backupFiles(local: File): Seq[File] =
    listLocalFiles(local)
      .filterNot(file => isTransientRocksDbFile(file.getName))
      .sortBy(_.getAbsolutePath)

  private def isTransientRocksDbFile(name: String): Boolean =
    name.endsWith(".crc") ||
      name == "LOCK" ||
      name == "LOG" ||
      name.startsWith("LOG.old.") ||
      name.startsWith("OPTIONS-") ||
      name.endsWith(".dbtmp")

  private[openivm] def setBackupPassHookForTesting(hook: (String, SparkSession, String) => Unit): Unit =
    backupPassHookForTesting = hook

  private[rocksdb] def setStateSyncKeyHookForTesting(hook: (SparkSession, String) => String): Unit =
    stateSyncKeyHookForTesting = hook

  private[openivm] def setStateSyncUriHookForTesting(hook: SparkSession => Option[String]): Unit =
    stateSyncUriHookForTesting = hook

  private[rocksdb] def setBeforeLocalFileCopyHookForTesting(hook: File => Unit): Unit =
    beforeLocalFileCopyHookForTesting = hook

  private[rocksdb] def setBeforeCaptureHookForTesting(hook: () => Unit): Unit =
    beforeCaptureHookForTesting = hook

  private[rocksdb] def setBeforeSynchronousDrainHookForTesting(hook: () => Unit): Unit =
    beforeSynchronousDrainHookForTesting = hook

  private[rocksdb] def setDuringCaptureHookForTesting(hook: () => Unit): Unit =
    duringCaptureHookForTesting = hook

  private[rocksdb] def setBeforeSnapshotCommitHookForTesting(hook: () => Unit): Unit =
    beforeSnapshotCommitHookForTesting = hook

  private[rocksdb] def setCopyLocalFileHookForTesting(hook: (FileSystem, File, Path) => Unit): Unit =
    copyLocalFileHookForTesting = hook

  private final case class CommittedSnapshot(root: Path, archive: Option[(Long, String)])

  private def latestCommit(fs: FileSystem, namespaceRoot: Path): Option[CommittedSnapshot] = {
    val controlRoot = new Path(namespaceRoot, ".openivm-state-sync")
    val commitRoot  = new Path(controlRoot, "commits")
    if (!fs.exists(commitRoot)) return None
    fs.listStatus(commitRoot)
      .toSeq
      .filter(status => status.isFile && status.getPath.getName.endsWith(".commit"))
      .sortBy(_.getPath.getName)
      .lastOption
      .map { commit =>
        if (commit.getLen > 4096) throw new java.io.IOException("openivm state-sync: oversized snapshot commit")
        val input = fs.open(commit.getPath)
        val record =
          try scala.io.Source.fromInputStream(input, "UTF-8").mkString
          finally input.close()
        val generation = commit.getPath.getName.stripSuffix(".commit")
        if (!generation.matches("[0-9]{19}-[0-9a-f-]{36}"))
          throw new java.io.IOException(s"openivm state-sync: invalid snapshot commit ${commit.getPath}")
        val archive =
          if (record == s"OPENIVM_STATE_SNAPSHOT_V1\n$generation\n") None
          else {
            val lines = record.split("\\n", -1)
            if (
              lines.length != 5 || lines(0) != "OPENIVM_STATE_SNAPSHOT_V2" ||
              lines(1) != generation || !lines(2).matches("[0-9]{1,12}") ||
              !lines(3).matches("[0-9a-f]{64}") || lines(4).nonEmpty
            )
              throw new java.io.IOException(s"openivm state-sync: invalid snapshot commit ${commit.getPath}")
            val length = lines(2).toLong
            if (length > OpenIvmSnapshotArchive.MaxBytes)
              throw new java.io.IOException("openivm state-sync: archive size limit exceeded")
            Some(length -> lines(3))
          }
        val snapshot = new Path(new Path(controlRoot, "snapshots"), generation)
        if (!fs.exists(snapshot))
          throw new java.io.IOException(s"openivm state-sync: missing committed snapshot $snapshot")
        CommittedSnapshot(snapshot, archive)
      }
  }

  private[rocksdb] def latestCommittedSnapshot(fs: FileSystem, root: Path): Option[Path] =
    latestCommit(fs, root).map(_.root)

  private def restoreArchive(fs: FileSystem, root: Path, info: (Long, String), local: File): Unit = {
    val remote = new Path(root, "snapshot.zip")
    if (fs.getFileStatus(remote).getLen != info._1)
      throw new java.io.IOException("openivm state-sync: archive length mismatch")
    val suffix  = UUID.randomUUID().toString
    val archive = new File(local.getParentFile, s".openivm-state-restore-$suffix.zip")
    val staged  = new File(local.getParentFile, s".openivm-state-restore-$suffix")
    Files.createDirectories(local.getParentFile.toPath)
    try {
      fs.copyToLocalFile(false, remote, new Path(archive.getAbsolutePath), true)
      if (archive.length() != info._1 || OpenIvmSnapshotArchive.sha256(archive) != info._2)
        throw new java.io.IOException("openivm state-sync: archive checksum mismatch")
      Files.createDirectories(staged.toPath)
      OpenIvmSnapshotArchive.extract(archive, staged)
      if (hasLocalStateFiles(local))
        throw new java.io.IOException("openivm state-sync: local state appeared during restore")
      removeEmptyDirectories(local)
      Files.move(staged.toPath, local.toPath, java.nio.file.StandardCopyOption.ATOMIC_MOVE)
    } finally {
      Files.deleteIfExists(archive.toPath)
      deleteStagedTree(staged)
    }
  }

  private def removeEmptyDirectories(file: File): Unit =
    if (file.isDirectory) {
      Option(file.listFiles()).toSeq.flatten.foreach(removeEmptyDirectories)
      Files.delete(file.toPath)
    }

  private def deleteStagedTree(file: File): Unit =
    if (file.exists()) {
      if (file.isDirectory) Option(file.listFiles()).toSeq.flatten.foreach(deleteStagedTree)
      Files.deleteIfExists(file.toPath)
    }

  private[rocksdb] def queuedPassCountForTesting(spark: SparkSession): Int =
    stateSyncUri(spark)
      .flatMap(uri => Option(backupStates.get(effectiveStateSyncKey(spark, uri))))
      .map(_.passLock.getQueueLength)
      .getOrElse(0)

  private[rocksdb] def synchronousRequestCountForTesting(spark: SparkSession): Long =
    stateSyncUri(spark)
      .flatMap(uri => Option(backupStates.get(effectiveStateSyncKey(spark, uri))))
      .map(_.synchronousRequestCount)
      .getOrElse(0L)

  private[rocksdb] def backupStateSnapshotForTesting(spark: SparkSession): Option[BackupStateSnapshot] =
    stateSyncUri(spark)
      .flatMap(uri => Option(backupStates.get(effectiveStateSyncKey(spark, uri))))
      .map(state => BackupStateSnapshot(running = state.running.get(), requested = state.requested.get()))

  private[rocksdb] def allBackupStatesIdleForTesting: Boolean = {
    val it   = backupStates.values().iterator()
    var idle = true
    while (idle && it.hasNext) {
      val state = it.next()
      idle = !state.running.get() && !state.requested.get()
    }
    idle
  }

  private[rocksdb] def resetForTesting(): Unit = {
    backupPassHookForTesting = null
    stateSyncKeyHookForTesting = null
    stateSyncUriHookForTesting = null
    beforeLocalFileCopyHookForTesting = null
    beforeCaptureHookForTesting = null
    beforeSynchronousDrainHookForTesting = null
    duringCaptureHookForTesting = null
    copyLocalFileHookForTesting = null
    beforeSnapshotCommitHookForTesting = null
    OpenIvmExecutionSpan.resetStateSyncForTesting()
    restoreStates.clear()
    backupStates.clear()
  }

  private def relativize(root: Path, child: Path): String = {
    val r = root.toUri.getPath.stripSuffix("/")
    val c = child.toUri.getPath
    if (c.startsWith(r + "/")) c.substring(r.length + 1) else child.getName
  }

  private def listLocalFiles(dir: File): Seq[File] = {
    val out = scala.collection.mutable.ArrayBuffer.empty[File]
    def walk(f: File): Unit =
      if (Files.isSymbolicLink(f.toPath))
        throw new java.io.IOException("openivm state-sync: symbolic state path")
      else if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(walk))
      else out += f
    walk(dir)
    out.toSeq
  }
}
