package org.openivm.spark.common.rocksdb

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.FeatureGate
import org.slf4j.LoggerFactory

import java.io.File
import java.util.concurrent.{ConcurrentHashMap, Executors}
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

  private val restoredApps  = ConcurrentHashMap.newKeySet[String]()
  private val backupPending = new AtomicBoolean(false)
  private val backupExecutor = Executors.newSingleThreadExecutor { r =>
    val t = new Thread(r, "openivm-state-sync")
    t.setDaemon(true)
    t
  }

  private def localRoot(spark: SparkSession): File =
    new File(FeatureGate.stateWarehouse(spark).stripSuffix("/") + "/_openivm")

  /** Download the remote state tree into the local `_openivm` dir on the first
    * call per Spark application. Best-effort: any failure logs a warning and
    * leaves the local tree empty (openivm then rebuilds state from scratch). */
  def maybeRestore(spark: SparkSession): Unit = {
    val uri = FeatureGate
      .stateSyncUri(spark)
      .getOrElse(
        return
      )
    val appId = spark.sparkContext.applicationId
    if (!restoredApps.add(appId)) return
    try {
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
    } catch {
      case NonFatal(e) =>
        log.warn(s"openivm state-sync: restore from $uri failed, continuing with empty state: $e")
    }
  }

  /** Coalesced, non-blocking incremental backup of the local `_openivm` tree to
    * the remote URI. If a backup is already queued this call is dropped (the
    * queued one will capture the latest on-disk state). */
  def backupAsync(spark: SparkSession): Unit = {
    if (FeatureGate.stateSyncUri(spark).isEmpty) return
    if (!backupPending.compareAndSet(false, true)) return
    backupExecutor.execute(() => {
      backupPending.set(false)
      try backupNow(spark)
      catch { case NonFatal(e) => log.warn(s"openivm state-sync: backup failed: $e") }
    })
  }

  /** Synchronous incremental backup. Public for tests / explicit checkpoints. */
  def backupNow(spark: SparkSession): Unit = {
    val uri = FeatureGate
      .stateSyncUri(spark)
      .getOrElse(
        return
      )
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
