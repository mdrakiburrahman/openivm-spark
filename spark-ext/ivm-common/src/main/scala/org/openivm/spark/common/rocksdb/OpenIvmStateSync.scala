package org.openivm.spark.common.rocksdb

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.FeatureGate
import org.openivm.spark.telemetry.OpenIvmExecutionSpan
import org.slf4j.LoggerFactory

import java.io.File
import java.util.concurrent.{CompletableFuture, ConcurrentHashMap, ConcurrentLinkedQueue, Executors}
import java.util.concurrent.atomic.AtomicBoolean
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
 *   - [[backupAsync]] is fired after each CREATE / REFRESH MATERIALIZED VIEW and
 *     incrementally uploads the local tree. Immutable `*.sst` files are skipped
 *     when a same-length copy already exists remotely; the (small, mutable)
 *     RocksDB metadata files — `CURRENT`, `MANIFEST-*`, `OPTIONS-*`, `*.log`, and
 *     OpenIVM's own manifests under `openivm-manifests` — are always re-uploaded
 *     so a restore reflects the latest committed version.
 */
object OpenIvmStateSync {
  private val log = LoggerFactory.getLogger(getClass)

  private[rocksdb] final case class BackupStateSnapshot(running: Boolean, requested: Boolean)

  private final class BackupState {
    val requested     = new AtomicBoolean(false)
    val running       = new AtomicBoolean(false)
    private val spans = new ConcurrentLinkedQueue[OpenIvmExecutionSpan]()

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
  private val backupExecutor = Executors.newCachedThreadPool { r =>
    val t = new Thread(r, "openivm-state-sync")
    t.setDaemon(true)
    t
  }

  private def localRoot(spark: SparkSession): File =
    new File(FeatureGate.stateWarehouse(spark).stripSuffix("/") + "/_openivm")

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

  private def backupStateFor(key: String): BackupState = {
    val fresh    = new BackupState
    val existing = backupStates.putIfAbsent(key, fresh)
    if (existing != null) existing else fresh
  }

  /** Download the remote state tree into the local `_openivm` dir on the first
    * call per Spark application. Best-effort: any failure logs a warning and
    * leaves the local tree empty (openivm then rebuilds state from scratch). */
  def maybeRestore(spark: SparkSession): Unit = {
    val uri = FeatureGate
      .stateSyncUri(spark)
      .getOrElse(
        return
      )
    val key    = effectiveStateSyncKey(spark, uri)
    val waiter = new CompletableFuture[Unit]()
    val prior  = restoreStates.putIfAbsent(key, waiter)
    if (prior != null) {
      prior.join()
      return
    }

    try {
      restoreNow(spark, uri)
    } catch {
      case NonFatal(e) =>
        log.warn(s"openivm state-sync: restore from $uri failed, continuing with empty state: $e")
    } finally {
      waiter.complete(())
    }
  }

  /** Coalesced, non-blocking incremental backup of the local `_openivm` tree to
    * the remote URI. If a backup is already queued this call is folded into that
    * in-flight work and forces one more pass afterwards. */
  def backupAsync(spark: SparkSession): Unit = {
    val uri = FeatureGate
      .stateSyncUri(spark)
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

  /** Synchronous incremental backup. Public for tests / explicit checkpoints. */
  def backupNow(spark: SparkSession): Unit = {
    val uri = FeatureGate
      .stateSyncUri(spark)
      .getOrElse(
        return
      )
    val span    = OpenIvmExecutionSpan.captureCurrent()
    val started = System.nanoTime()
    try backupNowInternal(spark, uri)
    finally {
      val durationMs = (System.nanoTime() - started) / 1000000L
      span.foreach(_.recordRocksDbBackup(durationMs))
    }
  }

  private def runBackupPass(key: String, spark: SparkSession, uri: String): Unit = {
    val hook = backupPassHookForTesting
    if (hook == null) backupNowInternal(spark, uri)
    else hook(key, spark, uri)
  }

  private def restoreNow(spark: SparkSession, uri: String): Unit = {
    val local = localRoot(spark)
    if (local.exists() && Option(local.list()).exists(_.nonEmpty)) return

    val hconf      = spark.sessionState.newHadoopConf()
    val remoteRoot = new Path(uri)
    val fs         = remoteRoot.getFileSystem(hconf)
    if (!fs.exists(remoteRoot)) {
      log.info(s"openivm state-sync: no remote state at $uri, starting fresh")
      return
    }

    local.mkdirs()
    var restored = 0
    val it       = fs.listFiles(remoteRoot, /* recursive = */ true)
    while (it.hasNext) {
      val remotePath = it.next().getPath
      val rel        = relativize(remoteRoot, remotePath)
      val dst        = new File(local, rel)
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
    log.info(s"openivm state-sync: restored $restored files from $uri to $local")
  }

  private def backupNowInternal(spark: SparkSession, uri: String): Unit = {
    val local = localRoot(spark)
    if (!local.exists()) return

    val hconf      = spark.sessionState.newHadoopConf()
    val remoteRoot = new Path(uri)
    val fs         = remoteRoot.getFileSystem(hconf)
    val localBase  = local.toPath
    var uploaded   = 0

    listLocalFiles(local).foreach { f =>
      val name = f.getName
      if (!name.endsWith(".crc")) {
        val rel    = localBase.relativize(f.toPath).toString
        val remote = new Path(remoteRoot, rel)
        val len    = f.length()
        // Immutable SST files are content-addressed by RocksDB, so a same-length
        // remote copy is identical and can be skipped. Everything else (mutable
        // metadata) is always re-uploaded so the remote reflects the newest
        // committed manifest.
        val skip =
          name.endsWith(".sst") && {
            try fs.exists(remote) && fs.getFileStatus(remote).getLen == len
            catch { case NonFatal(_) => false }
          }
        if (!skip) {
          fs.copyFromLocalFile( /* delSrc = */ false, /* overwrite = */ true, new Path(f.getAbsolutePath), remote)
          uploaded += 1
        }
      }
    }
    if (uploaded > 0) log.info(s"openivm state-sync: backed up $uploaded files to $uri")
  }

  private[rocksdb] def setBackupPassHookForTesting(hook: (String, SparkSession, String) => Unit): Unit =
    backupPassHookForTesting = hook

  private[rocksdb] def setStateSyncKeyHookForTesting(hook: (SparkSession, String) => String): Unit =
    stateSyncKeyHookForTesting = hook

  private[rocksdb] def backupStateSnapshotForTesting(spark: SparkSession): Option[BackupStateSnapshot] =
    FeatureGate
      .stateSyncUri(spark)
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
