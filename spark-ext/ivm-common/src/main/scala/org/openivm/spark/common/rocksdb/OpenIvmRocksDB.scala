package org.openivm.spark.common.rocksdb

import org.rocksdb.{
  ColumnFamilyDescriptor,
  ColumnFamilyHandle,
  ColumnFamilyOptions,
  DBOptions,
  FlushOptions,
  Options,
  RocksDB,
  WriteBatch,
  WriteOptions
}

import java.nio.channels.{FileChannel, FileLock, OverlappingFileLockException}
import java.nio.file.{Files, Paths, StandardCopyOption, StandardOpenOption}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import scala.collection.JavaConverters._
import scala.collection.mutable
import org.openivm.spark.telemetry.metrics.OpenIvmMetrics

object OpenIvmRocksDB {
  RocksDB.loadLibrary()

  private val ManifestPrefix     = "MANIFEST-"
  private val ManifestTxnPattern = """"txnId"\s*:\s*"([^"]+)"""".r

  private[rocksdb] val DefaultColumnFamilyName: String =
    RocksDBCodec.fromUtf8(RocksDB.DEFAULT_COLUMN_FAMILY)
  private[rocksdb] val InternalTxnColumnFamilyName: String = "__openivm_txn"

  private[rocksdb] def parseManifestVersion(fileName: String): Option[Long] =
    if (fileName.startsWith(ManifestPrefix)) {
      scala.util.Try(fileName.stripPrefix(ManifestPrefix).toLong).toOption
    } else {
      None
    }

  private[rocksdb] def closeQuietly(closeable: AutoCloseable): Unit =
    if (closeable != null) {
      try closeable.close()
      catch {
        case _: Throwable => ()
      }
    }
}

final class OpenIvmRocksDB(dbPath: String, val conf: OpenIvmRocksDBConf, columnFamilies: Seq[String]) {
  import OpenIvmRocksDB._

  private val writeMutex = new ReentrantLock()
  private final class BatchStats {
    var activeKeys: Long                                    = 0L
    var activeBytes: Long                                   = 0L
    var logicalKeys: Long                                   = 0L
    var logicalBytes: Long                                  = 0L
    val activeColumnFamilies: mutable.LinkedHashSet[String] = mutable.LinkedHashSet.empty[String]
    val txnId: String                                       = s"${Thread.currentThread().getId}-${System.nanoTime()}"
    var undoSeq: Long                                       = 0L
    var manifestWritten                                     = false

    def add(bytes: Long): Unit = {
      activeKeys += 1L
      activeBytes += bytes
      logicalKeys += 1L
      logicalBytes += bytes
    }

    def touch(columnFamily: String): Unit = {
      activeColumnFamilies += columnFamily
    }

    def resetActive(): Unit = {
      activeKeys = 0L
      activeBytes = 0L
      activeColumnFamilies.clear()
    }

    def addPhysical(bytes: Long): Unit = {
      activeKeys += 1L
      activeBytes += bytes
    }
  }
  private val activeBatchStats = new ThreadLocal[BatchStats]()

  private val declaredColumnFamilies: Seq[String] =
    (DefaultColumnFamilyName +: InternalTxnColumnFamilyName +: columnFamilies.filterNot(
      _ == DefaultColumnFamilyName
    )).distinct

  private val normalizedDbPath = RocksDBCodec.requireLocalPath(dbPath)
  private val dbDir            = Paths.get(normalizedDbPath)
  private val manifestsDir     = dbDir.resolve("openivm-manifests")

  @volatile private var dbHandle: RocksDB                                    = _
  @volatile private var columnFamilyHandles: Map[String, ColumnFamilyHandle] = Map.empty
  @volatile private var versionValue: Long                                   = 0L
  @volatile private var closed                                               = false
  @volatile private var dirtySinceFlush                                      = false
  @volatile private var dirtyColumnFamilies                                  = Set.empty[String]

  private val compactCalls                                             = new AtomicLong(0L)
  @volatile private var beforeFlushHookForTesting: Seq[String] => Unit = (_: Seq[String]) => ()
  @volatile private var beforeSyncWalHookForTesting: () => Unit        = () => ()
  @volatile private var afterManifestHookForTesting: () => Unit        = () => ()
  @volatile private var beforeCloseHookForTesting: () => Unit          = () => ()

  private def cf(name: String): ColumnFamilyHandle =
    columnFamilyHandles.getOrElse(
      name,
      throw new IllegalArgumentException(
        s"OpenIVM RocksDB at $normalizedDbPath was not opened with column family '$name'. " +
          s"Declared column families: ${columnFamilyHandles.keys.toSeq.sorted.mkString(", ")}"
      )
    )

  private def ensureLoaded(): RocksDB = {
    if (closed) {
      throw new IllegalStateException(s"OpenIVM RocksDB at $normalizedDbPath is already closed.")
    }
    if (dbHandle == null) {
      load()
    }
    val current = dbHandle
    if (current == null) {
      throw new IllegalStateException(s"OpenIVM RocksDB at $normalizedDbPath is not loaded.")
    }
    current
  }

  private def listExistingColumnFamilies(): Seq[String] = {
    if (!Files.exists(dbDir.resolve("CURRENT"))) {
      Seq(DefaultColumnFamilyName)
    } else {
      val options = new Options()
      try {
        val names = RocksDB.listColumnFamilies(options, normalizedDbPath).asScala.map(RocksDBCodec.fromUtf8).toSeq
        if (names.isEmpty) Seq(DefaultColumnFamilyName) else names
      } finally {
        options.close()
      }
    }
  }

  private def flushColumnFamilies(localDb: RocksDB, columnFamilies: Iterable[String]): Unit = {
    val targetColumnFamilies = columnFamilies.iterator.filter(columnFamilyHandles.contains).toSeq.distinct match {
      case Seq() => columnFamilyHandles.keys.toSeq.sorted
      case names => names
    }
    beforeFlushHookForTesting(targetColumnFamilies)
    val flushOptions = new FlushOptions().setWaitForFlush(true)
    val started      = System.nanoTime()
    var failed       = true
    try {
      localDb.flush(flushOptions, targetColumnFamilies.map(cf).asJava)
      failed = false
    } finally {
      flushOptions.close()
      OpenIvmMetrics.recordRocksDbFlush(
        OpenIvmRocksDBTelemetry.scopeForPath(normalizedDbPath),
        System.nanoTime() - started,
        targetColumnFamilies.size,
        failed
      )
    }
    dirtySinceFlush = false
    dirtyColumnFamilies = Set.empty
  }

  private def flushDirtyColumnFamilies(localDb: RocksDB): Unit =
    flushColumnFamilies(localDb, dirtyColumnFamilies)

  private def sstFileCountInternal(localDb: RocksDB): Int =
    localDb.getLiveFilesMetaData.size()

  private def writeManifest(newVersion: Long, sstCount: Int, txnId: String): Unit = {
    Files.createDirectories(manifestsDir)
    val manifestPath = manifestsDir.resolve(s"$ManifestPrefix$newVersion")
    val tmpPath = manifestsDir.resolve(
      s".$ManifestPrefix$newVersion.${Thread.currentThread().getId}.${System.nanoTime()}.tmp"
    )
    val payload =
      s"""{"version":$newVersion,"timestampMs":${System.currentTimeMillis()},"sstCount":$sstCount,"txnId":"$txnId"}"""
    Files.write(tmpPath, RocksDBCodec.utf8(payload))
    try {
      Files.move(tmpPath, manifestPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
    } finally {
      Files.deleteIfExists(tmpPath)
    }
  }

  private def readManifestTxnIds(): Set[String] = {
    Files.createDirectories(manifestsDir)
    val stream = Files.newDirectoryStream(manifestsDir, s"$ManifestPrefix*")
    try {
      stream
        .iterator()
        .asScala
        .flatMap { path =>
          scala.util.Try(RocksDBCodec.fromUtf8(Files.readAllBytes(path))).toOption
        }
        .flatMap(payload => ManifestTxnPattern.findFirstMatchIn(payload).map(_.group(1)))
        .toSet
    } finally {
      stream.close()
    }
  }

  private def writePhysicalBatch(
      batch: WriteBatch,
      stats: BatchStats,
      syncWalWrite: Boolean = conf.walEnabled
  ): Unit = {
    val started = System.nanoTime()
    val localDb = ensureLoaded()
    val writeOptions = new WriteOptions()
      .setSync(conf.walEnabled && syncWalWrite)
      .setDisableWAL(!conf.walEnabled)
    var failed = true
    try {
      localDb.write(writeOptions, batch)
      dirtySinceFlush = true
      dirtyColumnFamilies = dirtyColumnFamilies ++ stats.activeColumnFamilies
      failed = false
    } finally {
      writeOptions.close()
      OpenIvmMetrics.RocksDbCommitBatchLastBytes.set(stats.logicalBytes)
      OpenIvmMetrics.RocksDbCommitBatchActiveBytes.set(0L)
      OpenIvmMetrics.recordRocksDbWrite(
        OpenIvmRocksDBTelemetry.scopeForPath(normalizedDbPath),
        System.nanoTime() - started,
        stats.activeKeys,
        stats.activeBytes,
        failed
      )
    }
  }

  private def syncWal(localDb: RocksDB): Unit = {
    val started = System.nanoTime()
    var failed  = true
    try {
      beforeSyncWalHookForTesting()
      localDb.syncWal()
      failed = false
    } finally {
      OpenIvmMetrics.recordRocksDbWalSync(
        OpenIvmRocksDBTelemetry.scopeForPath(normalizedDbPath),
        System.nanoTime() - started,
        failed
      )
    }
  }

  private def commitLogicalBatch(batch: WriteBatch, stats: BatchStats): Long = {
    val started  = System.nanoTime()
    var sstCount = 0
    var failed   = true
    try {
      if (stats.activeKeys > 0L) {
        writePhysicalBatch(batch, stats, syncWalWrite = false)
        batch.clear()
        stats.resetActive()
      }

      val localDb = ensureLoaded()
      // Keep every pre-manifest write in the WAL, then publish one durability
      // barrier per logical commit. This avoids fsync amplification from
      // syncing each internal bookkeeping batch independently while still
      // guaranteeing the undo log and data mutations are durable before the
      // manifest makes the new version visible.
      if (conf.walEnabled) {
        syncWal(localDb)
      } else {
        flushDirtyColumnFamilies(localDb)
      }
      sstCount = sstFileCountInternal(localDb)

      val nextVersion = versionValue + 1L
      writeManifest(nextVersion, sstCount, stats.txnId)
      stats.manifestWritten = true
      afterManifestHookForTesting()
      versionValue = nextVersion
      // Manifest publication is the durable commit marker. Cleanup can stay on
      // the regular WAL/flush path because recovery replays or removes any
      // leftover undo rows by consulting the manifest set on reopen.
      cleanupTxn(stats.txnId, syncWalWrite = false)
      failed = false
      nextVersion
    } finally {
      OpenIvmMetrics.RocksDbCommitBatchLastBytes.set(stats.logicalBytes)
      OpenIvmMetrics.recordRocksDbCommit(
        OpenIvmRocksDBTelemetry.scopeForPath(normalizedDbPath),
        System.nanoTime() - started,
        stats.logicalKeys,
        stats.logicalBytes,
        sstCount,
        failed
      )
    }
  }

  private def cleanupTxn(txnId: String, syncWalWrite: Boolean = conf.walEnabled): Unit = {
    val batch = new WriteBatch()
    try {
      batch.deleteRange(cf(InternalTxnColumnFamilyName), txnPrefix(txnId), txnPrefixEnd(txnId))
      val stats = new BatchStats
      stats.touch(InternalTxnColumnFamilyName)
      stats.addPhysical(txnPrefix(txnId).length.toLong + txnPrefixEnd(txnId).length.toLong)
      writePhysicalBatch(batch, stats, syncWalWrite = syncWalWrite)
    } finally {
      batch.close()
    }
  }

  private def maybeWritePhysicalBatch(batch: WriteBatch, stats: BatchStats): Unit = {
    OpenIvmMetrics.RocksDbCommitBatchActiveBytes.set(stats.activeBytes)
    if (conf.maxWriteBatchBytes > 0L && stats.activeBytes >= conf.maxWriteBatchBytes) {
      writePhysicalBatch(batch, stats, syncWalWrite = false)
      batch.clear()
      stats.resetActive()
      OpenIvmMetrics.RocksDbCommitBatchActiveBytes.set(0L)
    }
  }

  private def txnPrefix(txnId: String): Array[Byte] =
    RocksDBCodec.compositeKey(Seq(RocksDBCodec.utf8(txnId)))

  private def txnPrefixEnd(txnId: String): Array[Byte] =
    txnPrefix(txnId) ++ Array(0xff.toByte)

  private def undoKey(txnId: String, sequence: Long, columnFamily: String, key: Array[Byte]): Array[Byte] =
    RocksDBCodec.compositeKey(
      Seq(
        RocksDBCodec.utf8(txnId),
        RocksDBCodec.encodeLongBE(Long.MaxValue - sequence),
        RocksDBCodec.utf8(columnFamily),
        key
      )
    )

  private def recordUndo(batch: WriteBatch, stats: BatchStats, columnFamily: String, key: Array[Byte]): Unit = {
    if (columnFamily == InternalTxnColumnFamilyName) return
    val localDb = ensureLoaded()
    val old     = Option(localDb.get(cf(columnFamily), key))
    val value = old match {
      case Some(bytes) => RocksDBCodec.compositeKey(Seq(RocksDBCodec.utf8("put"), bytes))
      case None        => RocksDBCodec.compositeKey(Seq(RocksDBCodec.utf8("delete")))
    }
    val logKey = undoKey(stats.txnId, stats.undoSeq, columnFamily, key)
    stats.undoSeq += 1L
    stats.touch(InternalTxnColumnFamilyName)
    stats.addPhysical(logKey.length.toLong + value.length.toLong)
    batch.put(cf(InternalTxnColumnFamilyName), logKey, value)
  }

  private def recordRangeUndo(
      batch: WriteBatch,
      stats: BatchStats,
      columnFamily: String,
      startKey: Array[Byte],
      endKey: Array[Byte]
  ): Unit = {
    if (columnFamily == InternalTxnColumnFamilyName) return
    val localDb = ensureLoaded()
    val it      = localDb.newIterator(cf(columnFamily))
    try {
      it.seek(startKey)
      while (it.isValid && java.util.Arrays.compareUnsigned(it.key(), endKey) < 0) {
        recordUndo(batch, stats, columnFamily, it.key())
        it.next()
        maybeWritePhysicalBatch(batch, stats)
      }
    } finally {
      it.close()
    }
  }

  private def rollbackTxn(txnId: String): Unit = {
    val localDb = ensureLoaded()
    val it      = localDb.newIterator(cf(InternalTxnColumnFamilyName))
    val batch   = new WriteBatch()
    val stats   = new BatchStats
    try {
      it.seek(txnPrefix(txnId))
      while (it.isValid && startsWith(it.key(), txnPrefix(txnId))) {
        val parts = RocksDBCodec.splitComposite(it.key(), maxParts = 4)
        if (parts.length == 4) {
          val columnFamily = RocksDBCodec.fromUtf8(parts(2))
          val key          = parts(3)
          val valueParts   = RocksDBCodec.splitComposite(it.value(), maxParts = 2)
          RocksDBCodec.fromUtf8(valueParts.headOption.getOrElse(Array.emptyByteArray)) match {
            case "put" if valueParts.length == 2 =>
              stats.touch(columnFamily)
              batch.put(cf(columnFamily), key, valueParts(1))
              stats.addPhysical(key.length.toLong + valueParts(1).length.toLong)
            case "delete" =>
              stats.touch(columnFamily)
              batch.delete(cf(columnFamily), key)
              stats.addPhysical(key.length.toLong)
            case _ => ()
          }
          maybeWritePhysicalBatch(batch, stats)
        }
        it.next()
      }
      if (stats.activeKeys > 0L) {
        writePhysicalBatch(batch, stats)
        batch.clear()
        stats.resetActive()
      }
    } finally {
      it.close()
      batch.close()
    }
    cleanupTxn(txnId)
  }

  private def recoverLogicalTransactions(): Unit = {
    if (!columnFamilyHandles.contains(InternalTxnColumnFamilyName)) return
    val manifested = readManifestTxnIds()
    val localDb    = ensureLoaded()
    val it         = localDb.newIterator(cf(InternalTxnColumnFamilyName))
    val txns       = scala.collection.mutable.Set.empty[String]
    val committed  = scala.collection.mutable.Set.empty[String]
    try {
      it.seekToFirst()
      while (it.isValid) {
        val parts = RocksDBCodec.splitComposite(it.key(), maxParts = 2)
        if (parts.nonEmpty) {
          val txnId = RocksDBCodec.fromUtf8(parts.head)
          txns += txnId
          if (parts.length == 2 && RocksDBCodec.fromUtf8(parts(1)) == "committed") committed += txnId
        }
        it.next()
      }
    } finally {
      it.close()
    }
    txns.foreach { txnId =>
      if (manifested.contains(txnId) || committed.contains(txnId)) cleanupTxn(txnId)
      else rollbackTxn(txnId)
    }
  }

  private def startsWith(bytes: Array[Byte], prefix: Array[Byte]): Boolean = {
    if (prefix.length > bytes.length) return false
    var index = 0
    while (index < prefix.length) {
      if (bytes(index) != prefix(index)) {
        return false
      }
      index += 1
    }
    true
  }

  private final class PrefixScanIterator(
      localDb: RocksDB,
      handle: ColumnFamilyHandle,
      columnFamily: String,
      prefix: Array[Byte]
  ) extends Iterator[(Array[Byte], Array[Byte])]
      with AutoCloseable {

    private val iterator = localDb.newIterator(handle)
    private var done     = false

    iterator.seek(prefix)

    override def hasNext: Boolean = {
      if (done) {
        false
      } else {
        val valid = iterator.isValid && startsWith(iterator.key(), prefix)
        if (!valid) {
          close()
        }
        valid
      }
    }

    override def next(): (Array[Byte], Array[Byte]) = {
      if (!hasNext) {
        throw new NoSuchElementException("prefixScan exhausted")
      }
      val key   = iterator.key()
      val value = iterator.value()
      OpenIvmMetrics.recordColumnFamilyRead(columnFamily, key.length.toLong + value.length.toLong)
      iterator.next()
      if (!(iterator.isValid && startsWith(iterator.key(), prefix))) {
        close()
      }
      key -> value
    }

    override def close(): Unit =
      if (!done) {
        done = true
        iterator.close()
      }
  }

  private def readCurrentVersion(): Long =
    manifestVersions.lastOption.getOrElse(0L)

  private[rocksdb] def withWriteLock[A](body: => A): A = {
    writeMutex.lock()
    try body
    finally writeMutex.unlock()
  }

  private[rocksdb] def isClosed: Boolean = closed

  private[rocksdb] def path: String = normalizedDbPath

  private[common] def setBeforeFlushHookForTesting(hook: Seq[String] => Unit): Unit =
    beforeFlushHookForTesting = hook

  private[common] def setBeforeSyncWalHookForTesting(hook: () => Unit): Unit =
    beforeSyncWalHookForTesting = hook

  private[common] def setAfterManifestHookForTesting(hook: () => Unit): Unit =
    afterManifestHookForTesting = hook

  private[common] def setBeforeCloseHookForTesting(hook: () => Unit): Unit =
    beforeCloseHookForTesting = hook

  private[rocksdb] def markDirtyForTesting(columnFamilies: Seq[String]): Unit = withWriteLock {
    dirtySinceFlush = true
    dirtyColumnFamilies = dirtyColumnFamilies ++ columnFamilies
  }

  private[common] def put(batch: WriteBatch, columnFamily: String, key: Array[Byte], value: Array[Byte]): Unit = {
    val stats = activeBatchStats.get()
    stats match {
      case null => ()
      case s =>
        recordUndo(batch, s, columnFamily, key)
        s.touch(columnFamily)
        s.add(key.length.toLong + value.length.toLong)
    }
    OpenIvmMetrics.recordColumnFamilyWrite(columnFamily, key.length.toLong + value.length.toLong)
    batch.put(cf(columnFamily), key, value)
    if (stats != null) maybeWritePhysicalBatch(batch, stats)
  }

  private[common] def delete(batch: WriteBatch, columnFamily: String, key: Array[Byte]): Unit = {
    val stats = activeBatchStats.get()
    stats match {
      case null => ()
      case s =>
        recordUndo(batch, s, columnFamily, key)
        s.touch(columnFamily)
        s.add(key.length.toLong)
    }
    OpenIvmMetrics.recordColumnFamilyWrite(columnFamily, key.length.toLong)
    batch.delete(cf(columnFamily), key)
    if (stats != null) maybeWritePhysicalBatch(batch, stats)
  }

  private[common] def deleteRange(
      batch: WriteBatch,
      columnFamily: String,
      startKey: Array[Byte],
      endKey: Array[Byte]
  ): Unit = {
    val stats = activeBatchStats.get()
    stats match {
      case null => ()
      case s =>
        recordRangeUndo(batch, s, columnFamily, startKey, endKey)
        s.touch(columnFamily)
        s.add(startKey.length.toLong + endKey.length.toLong)
    }
    OpenIvmMetrics.recordColumnFamilyWrite(columnFamily, startKey.length.toLong + endKey.length.toLong)
    batch.deleteRange(cf(columnFamily), startKey, endKey)
    if (stats != null) maybeWritePhysicalBatch(batch, stats)
  }

  private[rocksdb] def manifestVersions: Seq[Long] = {
    Files.createDirectories(manifestsDir)
    val stream = Files.newDirectoryStream(manifestsDir, s"$ManifestPrefix*")
    try {
      stream.iterator().asScala.flatMap(path => parseManifestVersion(path.getFileName.toString)).toSeq.sorted
    } finally {
      stream.close()
    }
  }

  private[rocksdb] def compactCallCount: Long = compactCalls.get()

  /** Cross-JVM POSIX file lock at `<dbPath>/openivm-jvm.lock`. Used only in
    * multi-process mode to mutex other JVMs out before opening the RocksDB
    * directory (which RocksDB itself enforces with `<dbPath>/LOCK` —
    * concurrent multi-process opens fail with `Resource temporarily
    * unavailable`).
    */
  private def externalLockPath = dbDir.resolve("openivm-jvm.lock")

  // Reentrancy guard for the cross-JVM external lock. `FileChannel.tryLock()`
  // throws `OverlappingFileLockException` if the same JVM already holds an
  // overlapping region of the same file, so we cannot naively re-acquire
  // from within a callback (e.g. `MvCatalog.rewriteProperties` calls
  // `collectPrefix` from inside `withBatch`). The outer `writeMutex`
  // (ReentrantLock) serialises threads within this JVM, so a single-thread
  // "owner + depth" pattern is sufficient.
  @volatile private var externalLockOwner: Thread = _
  private var externalLockDepth: Int              = 0

  private def withExternalLock[A](measureWait: Boolean)(recordWaitNanos: Long => Unit)(body: => A): A = {
    val current = Thread.currentThread()
    if (externalLockOwner eq current) {
      if (measureWait) recordWaitNanos(0L)
      externalLockDepth += 1
      try body
      finally externalLockDepth -= 1
    } else {
      Files.createDirectories(dbDir)
      val ch = FileChannel.open(externalLockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)
      try {
        val waitStarted          = if (measureWait) System.nanoTime() else 0L
        val deadline             = System.currentTimeMillis() + conf.lockTimeoutMs
        var acquired: FileLock   = null
        var lastError: Throwable = null
        while (acquired == null && System.currentTimeMillis() < deadline) {
          try {
            acquired = ch.tryLock()
          } catch {
            case e: OverlappingFileLockException =>
              lastError = e
              acquired = null
            case e: java.io.IOException =>
              lastError = e
              acquired = null
          }
          if (acquired == null) {
            try Thread.sleep(50)
            catch { case _: InterruptedException => () }
          }
        }
        if (measureWait) recordWaitNanos(System.nanoTime() - waitStarted)
        if (acquired == null) {
          throw new RuntimeException(
            s"OpenIVM RocksDB external lock acquisition timed out after ${conf.lockTimeoutMs}ms on $externalLockPath",
            lastError
          )
        }
        externalLockOwner = current
        externalLockDepth = 1
        try body
        finally {
          externalLockDepth = 0
          externalLockOwner = null
          try acquired.release()
          catch { case _: Throwable => () }
        }
      } finally {
        closeQuietly(ch)
      }
    }
  }

  private def openInternal(): Unit = {
    Files.createDirectories(dbDir)
    Files.createDirectories(manifestsDir)

    val allColumnFamilies = (listExistingColumnFamilies() ++ declaredColumnFamilies).distinct
    val descriptors = allColumnFamilies.map { name =>
      new ColumnFamilyDescriptor(RocksDBCodec.utf8(name), new ColumnFamilyOptions())
    }
    val handles = new java.util.ArrayList[ColumnFamilyHandle]()
    val options = new DBOptions()
      .setCreateIfMissing(true)
      .setCreateMissingColumnFamilies(true)
      .setAtomicFlush(true)
    var openedDb: RocksDB = null
    try {
      openedDb = RocksDB.open(options, normalizedDbPath, descriptors.asJava, handles)
      dbHandle = openedDb
      columnFamilyHandles = allColumnFamilies.zip(handles.asScala).toMap
      dirtySinceFlush = false
      dirtyColumnFamilies = Set.empty
      recoverLogicalTransactions()
      versionValue = readCurrentVersion()
    } catch {
      case t: Throwable =>
        handles.asScala.foreach(closeQuietly)
        closeQuietly(openedDb)
        throw t
    } finally {
      descriptors.foreach(descriptor => closeQuietly(descriptor.getOptions))
      options.close()
    }
  }

  private def closeInternal(): Unit = {
    val localDb               = dbHandle
    val localHandles          = columnFamilyHandles.values.toSeq
    var firstError: Throwable = null

    try {
      if (localDb != null) {
        beforeCloseHookForTesting()
      }
      if (localDb != null && dirtySinceFlush) flushDirtyColumnFamilies(localDb)
    } catch { case t: Throwable => firstError = t }

    try localHandles.foreach(closeQuietly)
    finally columnFamilyHandles = Map.empty

    try {
      if (localDb != null) localDb.close()
    } catch {
      case t: Throwable =>
        if (firstError == null) firstError = t
        else firstError.addSuppressed(t)
    } finally {
      dbHandle = null
      dirtySinceFlush = false
      dirtyColumnFamilies = Set.empty
    }

    if (firstError != null) throw firstError
  }

  /** Wrap a body that needs the native RocksDB handle live for its duration.
    *
    * In single-process mode this just ensures the cached handle is open;
    * the handle is NOT closed afterwards (kept hot for the lifetime of the
    * Spark application).
    *
    * In multi-process mode this:
    *   1. acquires the external POSIX lock,
    *   2. opens a fresh RocksDB,
    *   3. runs `body`,
    *   4. closes RocksDB (releasing `<dbPath>/LOCK`),
    *   5. releases the external lock.
    *
    * Always holds the in-JVM `writeMutex` so within one JVM concurrent
    * threads serialise; the external lock then mutexes other JVMs.
    */
  private[rocksdb] def withNativeHandle[A](operation: String)(body: => A): A = {
    if (!OpenIvmRocksDBTelemetry.isActive && !OpenIvmMetrics.enabled) {
      return withWriteLock {
        if (closed) {
          throw new IllegalStateException(s"OpenIVM RocksDB at $normalizedDbPath is already closed.")
        }
        if (conf.multiProcess) {
          withExternalLock(measureWait = false)(_ => ()) {
            if (dbHandle != null) {
              body
            } else {
              openInternal()
              try body
              finally closeInternal()
            }
          }
        } else {
          if (dbHandle == null) openInternal()
          body
        }
      }
    }

    val totalStarted = System.nanoTime()
    val lockStarted  = System.nanoTime()
    writeMutex.lock()
    val lockAcquired = System.nanoTime()

    var externalLockWaitNanos = 0L
    var nativeOpenNanos       = 0L
    var nativeCloseNanos      = 0L
    var bodyNanos             = 0L
    var failed                = true

    def timedBody(): A = {
      val started = System.nanoTime()
      try body
      finally bodyNanos += System.nanoTime() - started
    }

    def timedOpen(): Unit = {
      val started = System.nanoTime()
      try openInternal()
      finally nativeOpenNanos += System.nanoTime() - started
    }

    def timedClose(): Unit = {
      val started = System.nanoTime()
      try closeInternal()
      finally nativeCloseNanos += System.nanoTime() - started
    }

    try {
      if (closed) {
        throw new IllegalStateException(s"OpenIVM RocksDB at $normalizedDbPath is already closed.")
      }
      val result =
        if (conf.multiProcess) {
          withExternalLock(measureWait = true)(waitNanos => externalLockWaitNanos += waitNanos) {
            if (dbHandle != null) {
              // Reentry from a callback running inside an outer withNativeHandle
              // (e.g. MvCatalog.rewriteProperties calls collectPrefix inside
              // withBatch). The outer frame owns the open/close; we just run.
              timedBody()
            } else {
              timedOpen()
              try timedBody()
              finally timedClose()
            }
          }
        } else {
          if (dbHandle == null) timedOpen()
          timedBody()
        }
      failed = false
      result
    } finally {
      val lockReleased = System.nanoTime()
      writeMutex.unlock()
      OpenIvmRocksDBTelemetry.record(
        dbPath = normalizedDbPath,
        operation = operation,
        multiProcess = conf.multiProcess,
        failed = failed,
        totalNanos = lockReleased - totalStarted,
        jvmLockWaitNanos = lockAcquired - lockStarted,
        jvmLockHeldNanos = lockReleased - lockAcquired,
        externalLockWaitNanos = externalLockWaitNanos,
        nativeOpenNanos = nativeOpenNanos,
        nativeCloseNanos = nativeCloseNanos,
        bodyNanos = bodyNanos
      )
      OpenIvmMetrics.recordRocksDbOperation(
        dbScope = OpenIvmRocksDBTelemetry.scopeForPath(normalizedDbPath),
        operation = operation,
        multiProcess = conf.multiProcess,
        failed = failed,
        totalNanos = lockReleased - totalStarted,
        jvmLockWaitNanos = lockAcquired - lockStarted,
        jvmLockHeldNanos = lockReleased - lockAcquired,
        externalLockWaitNanos = externalLockWaitNanos,
        nativeOpenNanos = nativeOpenNanos,
        nativeCloseNanos = nativeCloseNanos,
        bodyNanos = bodyNanos
      )
    }
  }

  def withBatch[A](f: WriteBatch => A): Long = withNativeHandle("with_batch") {
    val batch = new WriteBatch()
    val stats = new BatchStats
    activeBatchStats.set(stats)
    try {
      f(batch)
      if (stats.logicalKeys > 0L) commitLogicalBatch(batch, stats)
      else versionValue
    } catch {
      case t: Throwable =>
        if (stats.logicalKeys > 0L && !stats.manifestWritten) {
          try rollbackTxn(stats.txnId)
          catch {
            case rollbackError: Throwable => t.addSuppressed(rollbackError)
          }
        }
        throw t
    } finally {
      activeBatchStats.remove()
      batch.close()
    }
  }

  /** Keep this database's native handle open for one short, logical catalog
    * operation. Nested reads, scans, and batches reuse the handle and the
    * cross-process lock instead of reopening RocksDB for every key.
    *
    * Callers must not run Spark jobs or acquire sessions for other databases
    * inside this scope. A session deliberately serializes access to this one
    * RocksDB shard in multi-process mode.
    */
  def withSession[A](body: => A): A = withNativeHandle("session")(body)

  def get(columnFamily: String, key: Array[Byte]): Option[Array[Byte]] = withNativeHandle("get") {
    val value = Option(dbHandle.get(cf(columnFamily), key))
    value.foreach(bytes => OpenIvmMetrics.recordColumnFamilyRead(columnFamily, key.length.toLong + bytes.length.toLong))
    value
  }

  /** Fetch several keys from one column family under one native-handle scope.
    * The result preserves input order and represents missing keys as `None`.
    */
  def multiGet(columnFamily: String, keys: Seq[Array[Byte]]): Seq[Option[Array[Byte]]] = {
    if (keys.isEmpty) return Seq.empty
    withNativeHandle("multi_get") {
      val handles = java.util.Collections.nCopies(keys.size, cf(columnFamily))
      dbHandle
        .multiGetAsList(handles, keys.map(_.clone()).asJava)
        .asScala
        .map { value =>
          Option(value).foreach(bytes => OpenIvmMetrics.recordColumnFamilyRead(columnFamily, bytes.length.toLong))
          Option(value)
        }
        .toVector
    }
  }

  /** Returns a snapshot iterator of `(key,value)` pairs for the prefix.
    *
    * In single-process mode this is a lazy iterator over the live RocksDB
    * `newIterator(...)`. In multi-process mode we cannot return a live
    * iterator (the RocksDB handle is closed after this method returns), so
    * we materialise the full result into a Vector inside the lock. The
    * caller observes the same `Iterator[(Array[Byte], Array[Byte])]` shape.
    */
  def prefixScan(columnFamily: String, prefix: Array[Byte]): Iterator[(Array[Byte], Array[Byte])] =
    if (conf.multiProcess) {
      withNativeHandle("prefix_scan") {
        val it = new PrefixScanIterator(dbHandle, cf(columnFamily), columnFamily, prefix.clone())
        try {
          val buffer = scala.collection.mutable.ArrayBuffer.empty[(Array[Byte], Array[Byte])]
          while (it.hasNext) buffer += it.next()
          buffer.iterator
        } finally {
          it.close()
        }
      }
    } else {
      withNativeHandle("prefix_scan") {
        new PrefixScanIterator(dbHandle, cf(columnFamily), columnFamily, prefix.clone())
      }
    }

  def currentVersion: Long = withNativeHandle("current_version") {
    versionValue
  }

  def load(): Long = withNativeHandle("load") {
    versionValue
  }

  def cleanup(minVersionsToRetain: Int): Unit = withWriteLock {
    if (closed) return
    require(minVersionsToRetain >= 0, s"minVersionsToRetain must be >= 0, found $minVersionsToRetain")
    def body(): Unit = {
      val threshold = readCurrentVersion() - minVersionsToRetain
      manifestVersions.filter(_ < threshold).foreach { version =>
        Files.deleteIfExists(manifestsDir.resolve(s"$ManifestPrefix$version"))
      }
    }
    if (conf.multiProcess) withExternalLock(measureWait = false)(_ => ())(body()) else body()
  }

  def compactRange(): Unit = withNativeHandle("compact_range") {
    dbHandle.compactRange()
    compactCalls.incrementAndGet()
    ()
  }

  def close(): Unit = withWriteLock {
    if (closed) return
    closed = true

    if (dbHandle != null) {
      closeInternal()
    }
  }

  def sstFileCount: Int = {
    if (closed) 0
    else if (conf.multiProcess) {
      withNativeHandle("sst_file_count")(sstFileCountInternal(dbHandle))
    } else {
      sstFileCountInternal(ensureLoaded())
    }
  }
}
