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
 *     generation publisher. Legacy flat-layout and V1/V2 snapshots remain readable.
 *     Component commits may reference older immutable archives; retention must
 *     preserve those references rather than deleting every older generation.
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

    // Immutable remote objects, updated only after commit under passLock.
    private[OpenIvmStateSync] var lastParts = Vector.empty[CachedPart]

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
    val state = backupStateFor(key)
    val lock  = state.passLock
    lock.lockInterruptibly()
    try {
      val hook = backupPassHookForTesting
      if (hook == null) backupNowInternal(spark, uri, state)
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

    val committed = latestCommit(fs, remoteRoot)
    committed.filter(_.parts.nonEmpty).foreach { snapshot =>
      restoreParts(fs, snapshot.parts, local)
      return
    }
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

  private def backupNowInternal(spark: SparkSession, uri: String, state: BackupState): Unit = {
    val local = localRoot(spark)
    if (!local.exists()) return

    val fs          = new Path(uri).getFileSystem(spark.sessionState.newHadoopConf())
    val controlRoot = new Path(new Path(uri), ".openivm-state-sync")
    val commitRoot  = new Path(controlRoot, "commits")
    if (!fs.mkdirs(commitRoot))
      throw new java.io.IOException(s"openivm state-sync: cannot create commit directory $commitRoot")
    val previous = fs
      .listStatus(commitRoot)
      .iterator
      .filter(status => status.isFile && status.getPath.getName.endsWith(".commit"))
      .map(_.getPath.getName.takeWhile(_ != '-').toLong)
      .foldLeft(0L)(math.max)
    val generation = f"${math.max(System.currentTimeMillis(), previous + 1L)}%019d-${UUID.randomUUID()}"
    val remoteRoot = new Path(new Path(controlRoot, "snapshots"), generation)
    if (!fs.mkdirs(remoteRoot))
      throw new java.io.IOException(s"openivm state-sync: cannot create snapshot $remoteRoot")
    val archive       = new File(local.getParentFile, s".openivm-state-snapshot-$generation.zip")
    val remoteArchive = new Path(remoteRoot, "snapshot.zip")
    var attempts      = 0
    var stablePass    = false
    var archiveBytes  = 0L
    var checksum      = ""
    var parts         = Vector.empty[CachedPart]
    try {
      while (!stablePass && attempts < 20) {
        attempts += 1
        OpenIvmSnapshotArchive.checkInterrupted()
        val files   = backupFiles(local)
        val initial = backupSignature(local.toPath, files)
        if (files.size > OpenIvmSnapshotArchive.MaxEntries)
          throw new java.io.IOException("openivm state-sync: archive entry limit exceeded")
        initial.foldLeft(0L) { case (used, (_, length, _)) =>
          if (length < 0L || length > OpenIvmSnapshotArchive.MaxBytes - used)
            throw new java.io.IOException("openivm state-sync: archive size limit exceeded")
          used + length
        }
        val groups        = files.groupBy(file => component(local.toPath.relativize(file.toPath).toString))
        val signatures    = initial.groupBy(entry => component(entry._1))
        val componentMode = files.nonEmpty && (groups.contains("telemetry") || state.lastParts.nonEmpty)
        var vanished      = false
        var reused        = Vector.empty[CachedPart]
        var hashes        = Map.empty[String, String]
        var changed       = files
        try {
          if (componentMode) {
            val available = scala.collection.mutable.Map.empty[Path, Boolean]
            reused = groups.keys.toVector.flatMap { name =>
              state.lastParts.find { cached =>
                val part = cached.part
                // Metadata can rule reuse out, but only exact content can authorize it.
                part.component == name && cached.signature == signatures(name) &&
                part.contentHash == OpenIvmSnapshotArchive.contentHash(local, groups(name)) &&
                available.getOrElseUpdate(
                  part.remote,
                  try fs.getFileStatus(part.remote).getLen == part.length
                  catch { case _: java.io.FileNotFoundException => false }
                )
              }
            }
            val reusable = reused.map(_.part.component).toSet
            changed = files.filterNot(file => reusable(component(local.toPath.relativize(file.toPath).toString)))
          }
          if (!componentMode || changed.nonEmpty) {
            hashes = OpenIvmSnapshotArchive.pack(
              local,
              changed,
              archive,
              file => {
                val hook = beforeLocalFileCopyHookForTesting
                if (hook != null) hook(file)
              },
              if (componentMode) Some(component _) else None
            )
            archiveBytes = archive.length()
            checksum = OpenIvmSnapshotArchive.sha256(archive)
          } else archiveBytes = 0L
        } catch {
          case NonFatal(error) if !Thread.currentThread().isInterrupted && files.exists(file => !file.isFile) =>
            vanished = true
        }
        stablePass = !vanished && initial == backupSignature(local.toPath, backupFiles(local))
        if (stablePass) {
          if (!componentMode || changed.nonEmpty) {
            val copyHook = copyLocalFileHookForTesting
            if (copyHook != null) copyHook(fs, archive, remoteArchive)
            else fs.copyFromLocalFile(false, true, new Path(archive.getAbsolutePath), remoteArchive)
            OpenIvmSnapshotArchive.checkInterrupted()
            if (fs.getFileStatus(remoteArchive).getLen != archiveBytes)
              throw new java.io.IOException("openivm state-sync: uploaded archive length mismatch")
          }
          parts =
            if (!componentMode) Vector.empty
            else
              (reused ++ hashes.toVector.map { case (name, hash) =>
                CachedPart(SnapshotPart(name, hash, remoteArchive, archiveBytes, checksum), signatures(name))
              }).sortBy(_.part.component)
          stablePass = initial == backupSignature(local.toPath, backupFiles(local))
        }
        if (!stablePass) Thread.sleep(50L)
      }
      if (!stablePass)
        throw new java.io.IOException(s"openivm state-sync: local RocksDB files kept changing while packing '$local'")
      val record =
        if (parts.isEmpty)
          s"OPENIVM_STATE_SNAPSHOT_V2\n$generation\n$archiveBytes\n$checksum\n"
        else {
          val entries = parts
            .map { cached =>
              val part = cached.part
              s"${part.component}\t${part.contentHash}\t${part.remote.getParent.getName}\t${part.length}\t${part.checksum}"
            }
            .mkString("\n")
          s"OPENIVM_STATE_SNAPSHOT_V3\n$generation\n${parts.size}\n$entries\n"
        }
      val pendingCommit = new Path(commitRoot, s".$generation.pending")
      val committed     = new Path(commitRoot, s"$generation.commit")
      val output        = fs.create(pendingCommit, false)
      try output.write(record.getBytes(StandardCharsets.UTF_8))
      finally output.close()
      val commitHook = beforeSnapshotCommitHookForTesting
      if (commitHook != null) commitHook()
      OpenIvmSnapshotArchive.checkInterrupted()
      if (!fs.rename(pendingCommit, committed))
        throw new java.io.IOException(s"openivm state-sync: cannot publish snapshot commit $committed")
      state.lastParts = parts
      log.info(
        s"openivm state-sync: published packed snapshot $generation ($archiveBytes uploaded bytes, $attempts passes) at $uri"
      )
    } finally Files.deleteIfExists(archive.toPath)
  }

  private def component(relative: String): String =
    relative.replace('\\', '/').takeWhile(_ != '/') match {
      case "refresh_profile" | "refresh_sql_log" => "telemetry"
      case _                                     => "state"
    }

  private final case class CachedPart(part: SnapshotPart, signature: Seq[(String, Long, Long)])

  private def backupFiles(local: File): Seq[File] =
    listLocalFiles(local)
      .filterNot(file => isTransientRocksDbFile(file.getName))
      .sortBy(_.getAbsolutePath)

  private def backupSignature(localBase: java.nio.file.Path, files: Seq[File]): Seq[(String, Long, Long)] =
    files.map { file =>
      (
        localBase.relativize(file.toPath).toString,
        file.length(),
        file.lastModified()
      )
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

  private final case class SnapshotPart(
      component: String,
      contentHash: String,
      remote: Path,
      length: Long,
      checksum: String
  )
  private final case class CommittedSnapshot(
      root: Path,
      archive: Option[(Long, String)],
      parts: Vector[SnapshotPart] = Vector.empty
  )

  private def parseParts(record: String, generation: String, snapshots: Path): Vector[SnapshotPart] = {
    def invalid(): Nothing = throw new java.io.IOException("openivm state-sync: invalid component snapshot commit")
    val lines              = record.split("\n", -1)
    if (lines.length < 5 || lines(1) != generation || !lines(2).matches("[12]") || lines.last.nonEmpty) invalid()
    val count = lines(2).toInt
    if (lines.length != count + 4) invalid()
    val parts = lines.slice(3, 3 + count).toVector.map { line =>
      val fields = line.split("\t", -1)
      if (
        fields.length != 5 || !Set("state", "telemetry")(fields(0)) ||
        !fields(1).matches("[0-9a-f]{64}") || !fields(2).matches("[0-9]{19}-[0-9a-f-]{36}") ||
        !fields(3).matches("[0-9]{1,12}") || !fields(4).matches("[0-9a-f]{64}")
      ) invalid()
      val length = fields(3).toLong
      if (length > OpenIvmSnapshotArchive.MaxBytes) invalid()
      SnapshotPart(fields(0), fields(1), new Path(new Path(snapshots, fields(2)), "snapshot.zip"), length, fields(4))
    }
    if (parts.map(_.component).distinct.size != parts.size) invalid()
    parts.groupBy(_.remote).values.foreach { references =>
      if (references.map(part => part.length -> part.checksum).distinct.size != 1) invalid()
    }
    parts
  }

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
        val parts =
          if (record.startsWith("OPENIVM_STATE_SNAPSHOT_V3\n"))
            parseParts(record, generation, new Path(controlRoot, "snapshots"))
          else Vector.empty
        val archive =
          if (parts.nonEmpty || record == s"OPENIVM_STATE_SNAPSHOT_V1\n$generation\n") None
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
        CommittedSnapshot(snapshot, archive, parts)
      }
  }

  private[rocksdb] def latestCommittedSnapshot(fs: FileSystem, root: Path): Option[Path] =
    latestCommit(fs, root).map(_.root)

  private def restoreArchive(fs: FileSystem, root: Path, info: (Long, String), local: File): Unit = {
    restoreParts(fs, Vector(SnapshotPart("full", "", new Path(root, "snapshot.zip"), info._1, info._2)), local)
  }

  private def restoreParts(fs: FileSystem, parts: Vector[SnapshotPart], local: File): Unit = {
    val suffix  = UUID.randomUUID().toString
    val archive = new File(local.getParentFile, s".openivm-state-restore-$suffix.zip")
    val staged  = new File(local.getParentFile, s".openivm-state-restore-$suffix")
    Files.createDirectories(local.getParentFile.toPath)
    try {
      Files.createDirectories(staged.toPath)
      var remainingEntries = OpenIvmSnapshotArchive.MaxEntries
      var remainingBytes   = OpenIvmSnapshotArchive.MaxBytes
      parts.groupBy(_.remote).toVector.sortBy(_._1.toString).foreach { case (remote, references) =>
        val info = references.head
        if (fs.getFileStatus(remote).getLen != info.length)
          throw new java.io.IOException("openivm state-sync: archive length mismatch")
        fs.copyToLocalFile(false, remote, new Path(archive.getAbsolutePath), true)
        if (archive.length() != info.length || OpenIvmSnapshotArchive.sha256(archive) != info.checksum)
          throw new java.io.IOException("openivm state-sync: archive checksum mismatch")
        val selected = references.map(_.component).toSet
        val (entries, bytes) = OpenIvmSnapshotArchive.extract(
          archive,
          staged,
          name => selected("full") || selected(component(name)),
          remainingEntries,
          remainingBytes
        )
        remainingEntries -= entries
        remainingBytes -= bytes
        Files.deleteIfExists(archive.toPath)
      }
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
      if (Files.isSymbolicLink(f.toPath))
        throw new java.io.IOException("openivm state-sync: symbolic state path")
      else if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(walk))
      else out += f
    walk(dir)
    out.toSeq
  }
}
