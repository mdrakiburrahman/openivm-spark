package org.openivm.spark.common.rocksdb

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.FeatureGate
import org.openivm.spark.telemetry.OpenIvmExecutionSpan
import org.slf4j.LoggerFactory

import java.io.File
import java.nio.file.Files
import java.nio.charset.StandardCharsets
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
object OpenIvmStateSync {
  private val log = LoggerFactory.getLogger(getClass)

  private[rocksdb] final case class BackupStateSnapshot(running: Boolean, requested: Boolean)

  private final case class AsyncRequest(span: Option[OpenIvmExecutionSpan], afterPublish: () => Unit)

  private[rocksdb] final case class StateSyncRequestReceipt(
      requestId: String,
      requestedMutationGeneration: Long,
      capturedMutationGeneration: Option[Long],
      publicationGeneration: Option[String],
      published: Boolean,
      reused: Boolean,
      coverageProven: Boolean,
      acknowledged: Boolean,
      outcome: String
  )

  private final case class SnapshotPublication(generation: String, capturedMutationGeneration: Option[Long])

  private final case class SynchronousCandidateRequest(
      requestId: String,
      requestedMutationGeneration: Long,
      coverageEligible: Boolean,
      completion: CompletableFuture[Unit]
  )

  private final case class PublishedCoverage(generation: String, capturedMutationGeneration: Long)

  private final case class CandidatePassResult(
      publication: Option[SnapshotPublication],
      noState: Boolean
  )

  private final class BackupState {
    val passLock                    = new ReentrantLock(true)
    val requested                   = new AtomicBoolean(false)
    val running                     = new AtomicBoolean(false)
    private val requests            = new ConcurrentLinkedQueue[AsyncRequest]()
    private val synchronousMonitor      = new Object
    private val synchronousRequests     = scala.collection.mutable.ArrayBuffer.empty[CompletableFuture[Unit]]
    private val candidateRequests       = scala.collection.mutable.ArrayBuffer.empty[SynchronousCandidateRequest]
    private var synchronousRunning      = false
    private var candidateRunning        = false
    private var submittedRequests       = 0L
    private var publishedCoverage       = Option.empty[PublishedCoverage]

    def synchronousRequestCount: Long = synchronousMonitor.synchronized(submittedRequests)

    def candidateIdle: Boolean = synchronousMonitor.synchronized(candidateRequests.isEmpty && !candidateRunning)

    def invalidatePublishedCoverage(): Unit = synchronousMonitor.synchronized { publishedCoverage = None }

    def submitCandidate(request: SynchronousCandidateRequest): Unit = synchronousMonitor.synchronized {
      submittedRequests += 1L
      if (!request.coverageEligible) publishedCoverage = None
      candidateRequests += request
      synchronousMonitor.notifyAll()
    }

    def backupSynchronous(pass: () => Unit): Unit = {
      val request = new CompletableFuture[Unit]()
      synchronousMonitor.synchronized {
        submittedRequests += 1L
        synchronousRequests += request
      }
      try {
        while (!request.isDone) {
          val batch = synchronousMonitor.synchronized {
            if (request.isDone) Vector.empty[CompletableFuture[Unit]]
            else if (!synchronousRunning) {
              synchronousRunning = true
              val captured = synchronousRequests.toVector
              synchronousRequests.clear()
              captured
            } else {
              synchronousMonitor.wait()
              Vector.empty[CompletableFuture[Unit]]
            }
          }
          if (batch.nonEmpty) {
            var failure: Throwable = null
            try pass()
            catch { case error: Throwable => failure = error }
            finally
              synchronousMonitor.synchronized {
                batch.foreach { waiting =>
                  if (failure == null) waiting.complete(())
                  else waiting.completeExceptionally(failure)
                }
                synchronousRunning = false
                synchronousMonitor.notifyAll()
              }
          }
        }
        try request.get()
        catch { case error: java.util.concurrent.ExecutionException => throw error.getCause }
      } catch {
        case interrupted: InterruptedException =>
          synchronousMonitor.synchronized {
            synchronousRequests -= request
          }
          Thread.currentThread().interrupt()
          throw interrupted
      }
    }

    def awaitCandidate(
        request: SynchronousCandidateRequest,
        pass: () => CandidatePassResult,
        recordReceipt: StateSyncRequestReceipt => Unit
    ): Unit = {
      try {
        while (!request.completion.isDone) {
          var batch = Vector.empty[SynchronousCandidateRequest]
          synchronousMonitor.synchronized {
            if (!request.completion.isDone && !candidateRunning) {
              publishedCoverage.foreach { coverage =>
                val reusable = candidateRequests.filter(req =>
                  req.coverageEligible && req.requestedMutationGeneration <= coverage.capturedMutationGeneration
                )
                candidateRequests --= reusable
                reusable.foreach { req =>
                  recordReceipt(
                    StateSyncRequestReceipt(
                      req.requestId,
                      req.requestedMutationGeneration,
                      Some(coverage.capturedMutationGeneration),
                      Some(coverage.generation),
                      published = false,
                      reused = true,
                      coverageProven = true,
                      acknowledged = true,
                      outcome = "reused_committed_coverage"
                    )
                  )
                  req.completion.complete(())
                }
              }
              if (!request.completion.isDone && candidateRequests.nonEmpty) {
                candidateRunning = true
                batch = candidateRequests.toVector
                candidateRequests.clear()
              }
              synchronousMonitor.notifyAll()
            } else if (!request.completion.isDone) synchronousMonitor.wait()
          }

          if (batch.nonEmpty) {
            var result: CandidatePassResult = null
            var failure: Throwable           = null
            try result = pass()
            catch { case error: Throwable => failure = error }
            synchronousMonitor.synchronized {
              if (failure != null) {
                batch.foreach { req =>
                  recordReceipt(
                    StateSyncRequestReceipt(
                      req.requestId,
                      req.requestedMutationGeneration,
                      None,
                      None,
                      published = false,
                      reused = false,
                      coverageProven = false,
                      acknowledged = false,
                      outcome = "publication_failed"
                    )
                  )
                  req.completion.completeExceptionally(failure)
                }
              } else if (result.noState) {
                publishedCoverage = None
                batch.foreach { req =>
                  recordReceipt(
                    StateSyncRequestReceipt(
                      req.requestId,
                      req.requestedMutationGeneration,
                      None,
                      None,
                      published = false,
                      reused = false,
                      coverageProven = false,
                      acknowledged = true,
                      outcome = "no_local_state"
                    )
                  )
                  req.completion.complete(())
                }
              } else if (result.publication.isEmpty) {
                val error = new java.io.IOException("openivm state-sync: synchronous pass produced no committed publication")
                publishedCoverage = None
                batch.foreach { req =>
                  recordReceipt(
                    StateSyncRequestReceipt(
                      req.requestId,
                      req.requestedMutationGeneration,
                      None,
                      None,
                      published = false,
                      reused = false,
                      coverageProven = false,
                      acknowledged = false,
                      outcome = "publication_missing"
                    )
                  )
                  req.completion.completeExceptionally(error)
                }
              } else {
                val publication = result.publication.get
                val captured = publication.capturedMutationGeneration
                val proofBatchEligible = batch.forall(_.coverageEligible) && captured.isDefined
                if (proofBatchEligible)
                  publishedCoverage = Some(PublishedCoverage(publication.generation, captured.get))
                else publishedCoverage = None
                val (covered, uncovered) = batch.partition { req =>
                  if (!req.coverageEligible) true
                  else captured.exists(_ >= req.requestedMutationGeneration)
                }
                covered.foreach { req =>
                  val proof = req.coverageEligible && captured.exists(_ >= req.requestedMutationGeneration)
                  recordReceipt(
                    StateSyncRequestReceipt(
                      req.requestId,
                      req.requestedMutationGeneration,
                      captured,
                      Some(publication.generation),
                      published = true,
                      reused = false,
                      coverageProven = proof,
                      acknowledged = true,
                      outcome = if (proof) "published_covered_generation" else "published_fresh_fallback"
                    )
                  )
                  req.completion.complete(())
                }
                uncovered.foreach { req =>
                  recordReceipt(
                    StateSyncRequestReceipt(
                      req.requestId,
                      req.requestedMutationGeneration,
                      captured,
                      Some(publication.generation),
                      published = true,
                      reused = false,
                      coverageProven = false,
                      acknowledged = false,
                      outcome = "later_generation_requires_new_publication"
                    )
                  )
                }
                candidateRequests ++= uncovered
              }
              candidateRunning = false
              synchronousMonitor.notifyAll()
            }
          }
        }
        try request.completion.get()
        catch { case error: java.util.concurrent.ExecutionException => throw error.getCause }
      } catch {
        case interrupted: InterruptedException =>
          synchronousMonitor.synchronized {
            candidateRequests -= request
          }
          Thread.currentThread().interrupt()
          throw interrupted
      }
    }

    def capture(span: Option[OpenIvmExecutionSpan], afterPublish: () => Unit): Unit =
      requests.add(AsyncRequest(span, afterPublish))

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
  private val stateSyncRootsByUri = new ConcurrentHashMap[String, ConcurrentHashMap[String, java.lang.Boolean]]()
  private val sharedStateSyncUris = new ConcurrentHashMap[String, java.lang.Boolean]()
  @volatile private var backupPassHookForTesting: (String, SparkSession, String) => Unit = null
  @volatile private var stateSyncKeyHookForTesting: (SparkSession, String) => String     = null
  @volatile private var stateSyncUriHookForTesting: SparkSession => Option[String]       = null
  @volatile private var beforeLocalFileCopyHookForTesting: File => Unit                  = null
  @volatile private var beforeCaptureHookForTesting: () => Unit                          = null
  @volatile private var copyLocalFileHookForTesting: (FileSystem, File, Path) => Unit    = null
  @volatile private var beforeSnapshotCommitHookForTesting: () => Unit                   = null
  @volatile private var stateSyncRequestReceiptHookForTesting: StateSyncRequestReceipt => Unit = null
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

  private def recordStateSyncReceipt(receipt: StateSyncRequestReceipt): Unit = {
    log.info(
      s"openivm state-sync: sync_request request_id=${receipt.requestId} " +
        s"requested_generation=${receipt.requestedMutationGeneration} " +
        s"captured_generation=${receipt.capturedMutationGeneration.getOrElse("none")} " +
        s"publication_generation=${receipt.publicationGeneration.getOrElse("none")} " +
        s"published=${receipt.published} reused=${receipt.reused} " +
        s"coverage_proven=${receipt.coverageProven} acknowledged=${receipt.acknowledged} " +
        s"outcome=${receipt.outcome}"
    )
    val hook = stateSyncRequestReceiptHookForTesting
    if (hook != null)
      try hook(receipt)
      catch { case NonFatal(error) => log.warn(s"openivm state-sync: receipt hook failed: $error") }
  }

  private def registerRootForUri(uri: String, localRoot: String): Boolean = {
    val roots = stateSyncRootsByUri.computeIfAbsent(uri, _ => new ConcurrentHashMap[String, java.lang.Boolean]())
    roots.putIfAbsent(localRoot, java.lang.Boolean.TRUE)
    if (roots.size() > 1 && sharedStateSyncUris.putIfAbsent(uri, java.lang.Boolean.TRUE) == null) {
      val states = backupStates.entrySet().iterator()
      while (states.hasNext) {
        val entry = states.next()
        if (entry.getKey.endsWith(s"|$uri")) entry.getValue.invalidatePublishedCoverage()
      }
    }
    sharedStateSyncUris.containsKey(uri)
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
    registerRootForUri(uri, canonicalLocalRoot(spark))
    val state = backupStateFor(key)
    state.capture(OpenIvmExecutionSpan.captureCurrent(), afterPublish)
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
          state.invalidatePublishedCoverage()
          runBackupPass(key, spark, uri)
          succeeded = true
        } catch { case NonFatal(e) => log.warn(s"openivm state-sync: backup failed: $e") }
        finally {
          val durationMs = (System.nanoTime() - started) / 1000000L
          requests.foreach(_.span.foreach(_.recordRocksDbBackup(durationMs)))
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
    val sharedDestination = registerRootForUri(uri, canonicalLocalRoot(spark))
    val span    = OpenIvmExecutionSpan.captureCurrent()
    val started = System.nanoTime()
    try {
      val state = backupStateFor(key)
      if (!FeatureGate.stateSyncCheckpointEnabled(spark)) {
        state.invalidatePublishedCoverage()
        state.backupSynchronous(() => runBackupPass(key, spark, uri))
      } else {
        val requestId = Option(spark.sparkContext.getLocalProperty("openivm.request_id"))
          .filter(_.nonEmpty)
          .getOrElse(UUID.randomUUID().toString)
        val coverageEligible = !sharedDestination && OpenIvmStateBarrier.mutationCoverageEnabled(spark)
        var request: SynchronousCandidateRequest = null
        OpenIvmStateBarrier.withCurrentMutationGeneration(spark) { requestedGeneration =>
          request = SynchronousCandidateRequest(
            requestId,
            requestedGeneration,
            coverageEligible,
            new CompletableFuture[Unit]()
          )
          state.submitCandidate(request)
        }
        state.awaitCandidate(
          request,
          () => runCandidateBackupPass(key, spark, uri),
          recordStateSyncReceipt
        )
      }
    } finally {
      val durationMs = (System.nanoTime() - started) / 1000000L
      span.foreach(_.recordRocksDbBackup(durationMs))
    }
  }

  private def runBackupPass(key: String, spark: SparkSession, uri: String): Unit = {
    val lock = backupStateFor(key).passLock
    lock.lockInterruptibly()
    try {
      val hook = backupPassHookForTesting
      if (hook == null) backupNowInternal(spark, uri)
      else hook(key, spark, uri)
    } finally lock.unlock()
  }

  private def runCandidateBackupPass(key: String, spark: SparkSession, uri: String): CandidatePassResult = {
    val lock = backupStateFor(key).passLock
    lock.lockInterruptibly()
    try {
      val hook = backupPassHookForTesting
      if (hook != null) {
        hook(key, spark, uri)
        CandidatePassResult(None, noState = !localRoot(spark).exists())
      } else {
        val publication = backupNowInternalWithResult(spark, uri)
        CandidatePassResult(publication, noState = publication.isEmpty && !localRoot(spark).exists())
      }
    } finally lock.unlock()
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

  private def backupNowInternal(spark: SparkSession, uri: String): Unit = {
    backupNowInternalWithResult(spark, uri)
    ()
  }

  private def backupNowInternalWithResult(spark: SparkSession, uri: String): Option[SnapshotPublication] = {
    val local = localRoot(spark)
    if (!local.exists()) return None

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
    OpenIvmStateBarrier.register(spark)
    val archive = new File(local.getParentFile, s".openivm-state-snapshot-$generation.zip")
    val capture = Files.createTempDirectory(local.getParentFile.toPath, ".openivm-state-capture-").toFile
    var capturedMutationGeneration = Option.empty[Long]
    try {
      val captureHook = beforeCaptureHookForTesting
      if (captureHook != null) captureHook()
      OpenIvmStateBarrier.withCapture(spark) {
        OpenIvmSnapshotArchive.checkInterrupted()
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
        }
        files.filterNot(file => databases.exists(file.toPath.startsWith)).foreach { file =>
          OpenIvmSnapshotArchive.checkInterrupted()
          val target = capture.toPath.resolve(localBase.relativize(file.toPath))
          Files.createDirectories(target.getParent)
          Files.copy(file.toPath, target)
        }
        if (FeatureGate.stateSyncCheckpointEnabled(spark))
          capturedMutationGeneration = Some(OpenIvmStateBarrier.currentMutationGeneration(spark))
      }
      OpenIvmSnapshotArchive.pack(
        capture,
        backupFiles(capture),
        archive,
        file => {
          val hook = beforeLocalFileCopyHookForTesting
          if (hook != null) hook(file)
        }
      )
      val archiveBytes  = archive.length()
      val checksum      = OpenIvmSnapshotArchive.sha256(archive)
      val remoteArchive = new Path(remoteRoot, "snapshot.zip")
      val copyHook      = copyLocalFileHookForTesting
      if (copyHook != null) copyHook(fs, archive, remoteArchive)
      else fs.copyFromLocalFile(false, true, new Path(archive.getAbsolutePath), remoteArchive)
      OpenIvmSnapshotArchive.checkInterrupted()
      if (fs.getFileStatus(remoteArchive).getLen != archiveBytes)
        throw new java.io.IOException("openivm state-sync: uploaded archive length mismatch")
      val pendingCommit = new Path(commitRoot, s".$generation.pending")
      val committed     = new Path(commitRoot, s"$generation.commit")
      val output        = fs.create(pendingCommit, false)
      try
        output.write(
          s"OPENIVM_STATE_SNAPSHOT_V2\n$generation\n$archiveBytes\n$checksum\n".getBytes(StandardCharsets.UTF_8)
        )
      finally output.close()
      val commitHook = beforeSnapshotCommitHookForTesting
      if (commitHook != null) commitHook()
      OpenIvmSnapshotArchive.checkInterrupted()
      if (!fs.rename(pendingCommit, committed))
        throw new java.io.IOException(s"openivm state-sync: cannot publish snapshot commit $committed")
      log.info(
        s"openivm state-sync: published packed snapshot $generation ($archiveBytes bytes) at $uri"
      )
      pruneGenerations(fs, controlRoot, FeatureGate.stateSyncRetainGenerations(spark))
      Some(SnapshotPublication(generation, capturedMutationGeneration))
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
  private def pruneGenerations(fs: FileSystem, controlRoot: Path, retain: Int): Unit =
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
        fs.delete(new Path(commitRoot, s"$generation.commit"), false)
        fs.delete(new Path(snapshotsRoot, generation), true)
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
        (staleSnapshots ++ stalePending).take(budget).foreach(path => fs.delete(path, true))
      }
    } catch {
      case NonFatal(error) => log.warn(s"openivm state-sync: cannot prune old snapshot generations: $error")
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

  private[rocksdb] def setBeforeSnapshotCommitHookForTesting(hook: () => Unit): Unit =
    beforeSnapshotCommitHookForTesting = hook

  private[rocksdb] def setCopyLocalFileHookForTesting(hook: (FileSystem, File, Path) => Unit): Unit =
    copyLocalFileHookForTesting = hook

  private[rocksdb] def setStateSyncRequestReceiptHookForTesting(
      hook: StateSyncRequestReceipt => Unit
  ): Unit = stateSyncRequestReceiptHookForTesting = hook

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
    copyLocalFileHookForTesting = null
    beforeSnapshotCommitHookForTesting = null
    stateSyncRequestReceiptHookForTesting = null
    restoreStates.clear()
    backupStates.clear()
    stateSyncRootsByUri.clear()
    sharedStateSyncUris.clear()
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
