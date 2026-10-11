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
import java.util.concurrent.Callable
import java.util.concurrent.locks.ReentrantLock
import scala.jdk.CollectionConverters._
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

  private final class BackupState {
    val passLock                    = new ReentrantLock(true)
    val requested                   = new AtomicBoolean(false)
    val running                     = new AtomicBoolean(false)
    private val spans               = new ConcurrentLinkedQueue[OpenIvmExecutionSpan]()
    private val synchronousMonitor  = new Object
    private val synchronousRequests = scala.collection.mutable.ArrayBuffer.empty[CompletableFuture[Unit]]
    private var synchronousRunning  = false
    private var submittedRequests   = 0L

    def synchronousRequestCount: Long = synchronousMonitor.synchronized(submittedRequests)

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

    def capture(span: Option[OpenIvmExecutionSpan]): Unit =
      span.foreach(spans.add)

    def drainSpans(): Seq[OpenIvmExecutionSpan] = {
      val drained = scala.collection.mutable.ArrayBuffer.empty[OpenIvmExecutionSpan]
      var span    = spans.poll()
      while (span != null) {
        drained += span
        span = spans.poll()
      }
      drained.toSeq
    }
  }

  private val restoreStates = new ConcurrentHashMap[String, CompletableFuture[Unit]]()
  private val backupStates  = new ConcurrentHashMap[String, BackupState]()
  @volatile private var backupPassHookForTesting: (String, SparkSession, String) => Unit = null
  @volatile private var stateSyncKeyHookForTesting: (SparkSession, String) => String     = null
  @volatile private var stateSyncUriHookForTesting: SparkSession => Option[String]       = null
  @volatile private var beforeLocalFileCopyHookForTesting: File => Unit                  = null
  @volatile private var copyLocalFileHookForTesting: (FileSystem, File, Path) => Unit    = null
  @volatile private var beforeSnapshotCommitHookForTesting: () => Unit                   = null
  private val copyExecutor = Executors.newFixedThreadPool(
    16,
    (r: Runnable) => {
      val thread = new Thread(r, "openivm-state-sync-copy")
      thread.setDaemon(true)
      thread
    }
  )
  private val backupExecutor = Executors.newCachedThreadPool { r =>
    val t = new Thread(r, "openivm-state-sync")
    t.setDaemon(true)
    t
  }

  private def localRoot(spark: SparkSession): File =
    new File(FeatureGate.stateWarehouse(spark).stripSuffix("/") + "/_openivm")

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
    s"${spark.sparkContext.applicationId}|${canonicalLocalRoot(spark)}|$uri"

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
    val key    = effectiveStateSyncKey(spark, uri)
    val waiter = new CompletableFuture[Unit]()
    val prior  = restoreStates.putIfAbsent(key, waiter)
    if (prior != null) {
      try prior.join()
      catch { case error: java.util.concurrent.CompletionException => throw error.getCause }
      return
    }

    try {
      restoreNow(spark, uri)
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
  def backupAsync(spark: SparkSession): Unit = {
    val uri = stateSyncUri(spark)
      .getOrElse(
        return
      )
    val key   = effectiveStateSyncKey(spark, uri)
    val state = backupStateFor(key)
    state.capture(OpenIvmExecutionSpan.captureCurrent())
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
        val spans   = state.drainSpans()
        val started = System.nanoTime()
        try runBackupPass(key, spark, uri)
        catch { case NonFatal(e) => log.warn(s"openivm state-sync: backup failed: $e") }
        finally {
          val durationMs = (System.nanoTime() - started) / 1000000L
          spans.foreach(_.recordRocksDbBackup(durationMs))
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
    val span    = OpenIvmExecutionSpan.captureCurrent()
    val started = System.nanoTime()
    try backupStateFor(key).backupSynchronous(() => runBackupPass(key, spark, uri))
    finally {
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

    val snapshotRoot = latestCommittedSnapshot(fs, remoteRoot).getOrElse(remoteRoot)
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
    val localBase   = local.toPath
    var uploaded    = 0
    var attempts    = 0
    var stablePass  = false
    var stablePaths = Set.empty[String]

    while (!stablePass && attempts < 20) {
      attempts += 1
      var localFileVanished = false
      val files             = backupFiles(local)
      val initialSignature  = backupSignature(localBase, files)

      def copyFiles(selected: Seq[File]): Unit =
        selected.grouped(16).foreach { group =>
          val tasks = group.map { file =>
            new Callable[Boolean] {
              override def call(): Boolean = {
                val hook = beforeLocalFileCopyHookForTesting
                if (hook != null) hook(file)
                if (!file.isFile) false
                else {
                  val remote = new Path(remoteRoot, localBase.relativize(file.toPath).toString)
                  try {
                    val copyHook = copyLocalFileHookForTesting
                    if (copyHook != null) copyHook(fs, file, remote)
                    else fs.copyFromLocalFile(false, true, new Path(file.getAbsolutePath), remote)
                    true
                  } catch {
                    case NonFatal(_) if !file.isFile => false
                  }
                }
              }
            }
          }
          val completed          = copyExecutor.invokeAll(tasks.asJava)
          var failure: Throwable = null
          completed.asScala.foreach { result =>
            try {
              if (result.get()) uploaded += 1
              else localFileVanished = true
            } catch {
              case error: java.util.concurrent.ExecutionException =>
                if (failure == null) failure = error.getCause
            }
          }
          if (failure != null) throw failure
        }
      copyFiles(files.filterNot(_.getName == "CURRENT"))
      copyFiles(files.filter(_.getName == "CURRENT"))

      val finalSignature = backupSignature(localBase, backupFiles(local))
      if (localFileVanished || initialSignature != finalSignature) Thread.sleep(50L)
      else {
        stablePass = true
        stablePaths = finalSignature.iterator.map(_._1).toSet
      }
    }
    if (!stablePass)
      throw new java.io.IOException(
        s"openivm state-sync: local RocksDB files kept changing while backing up '$local' to '$uri'"
      )
    val pruned        = pruneRemoteFiles(fs, remoteRoot, stablePaths)
    val pendingCommit = new Path(commitRoot, s".$generation.pending")
    val committed     = new Path(commitRoot, s"$generation.commit")
    val output        = fs.create(pendingCommit, false)
    try output.write(s"OPENIVM_STATE_SNAPSHOT_V1\n$generation\n".getBytes(StandardCharsets.UTF_8))
    finally output.close()
    val commitHook = beforeSnapshotCommitHookForTesting
    if (commitHook != null) commitHook()
    if (!fs.rename(pendingCommit, committed))
      throw new java.io.IOException(s"openivm state-sync: cannot publish snapshot commit $committed")
    if (uploaded > 0 || pruned > 0)
      log.info(s"openivm state-sync: backed up $uploaded files and pruned $pruned stale files at $uri")
  }

  private def backupFiles(local: File): Seq[File] =
    listLocalFiles(local)
      .filterNot(file => isTransientRocksDbFile(file.getName))
      .sortBy(file => if (file.getName == "CURRENT") 1 else 0)

  private def backupSignature(localBase: java.nio.file.Path, files: Seq[File]): Seq[(String, Long, Long)] =
    files.map { file =>
      (
        localBase.relativize(file.toPath).toString,
        file.length(),
        file.lastModified()
      )
    }

  private def pruneRemoteFiles(fs: FileSystem, remoteRoot: Path, localPaths: Set[String]): Int = {
    if (!fs.exists(remoteRoot)) return 0
    val remoteFiles = fs.listFiles(remoteRoot, /* recursive = */ true)
    var pruned      = 0
    while (remoteFiles.hasNext) {
      val remote = remoteFiles.next().getPath
      val rel    = relativize(remoteRoot, remote)
      if (!localPaths.contains(rel)) {
        if (!fs.delete(remote, /* recursive = */ false) && fs.exists(remote))
          throw new java.io.IOException(s"openivm state-sync: failed to prune stale remote file '$remote'")
        pruned += 1
      }
    }
    pruned
  }

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

  private[rocksdb] def setBeforeSnapshotCommitHookForTesting(hook: () => Unit): Unit =
    beforeSnapshotCommitHookForTesting = hook

  private[rocksdb] def setCopyLocalFileHookForTesting(hook: (FileSystem, File, Path) => Unit): Unit =
    copyLocalFileHookForTesting = hook

  private[rocksdb] def latestCommittedSnapshot(fs: FileSystem, namespaceRoot: Path): Option[Path] = {
    val controlRoot = new Path(namespaceRoot, ".openivm-state-sync")
    val commitRoot  = new Path(controlRoot, "commits")
    if (!fs.exists(commitRoot)) return None
    val latest = fs
      .listStatus(commitRoot)
      .toSeq
      .filter(status => status.isFile && status.getPath.getName.endsWith(".commit"))
      .sortBy(_.getPath.getName)
      .lastOption
    latest.map { commit =>
      val input = fs.open(commit.getPath)
      val record =
        try scala.io.Source.fromInputStream(input, "UTF-8").mkString
        finally input.close()
      val generation = commit.getPath.getName.stripSuffix(".commit")
      if (
        !generation.matches("[0-9]{19}-[0-9a-f-]{36}") ||
        record != s"OPENIVM_STATE_SNAPSHOT_V1\n$generation\n"
      )
        throw new java.io.IOException(s"openivm state-sync: invalid snapshot commit ${commit.getPath}")
      val snapshot = new Path(new Path(controlRoot, "snapshots"), generation)
      if (!fs.exists(snapshot))
        throw new java.io.IOException(s"openivm state-sync: missing committed snapshot $snapshot")
      snapshot
    }
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
    copyLocalFileHookForTesting = null
    beforeSnapshotCommitHookForTesting = null
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
      if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(walk))
      else out += f
    walk(dir)
    out.toSeq
  }
}
