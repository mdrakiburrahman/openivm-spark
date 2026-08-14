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

object OpenIvmRocksDB {
  RocksDB.loadLibrary()

  private val ManifestPrefix = "MANIFEST-"

  private[rocksdb] val DefaultColumnFamilyName: String =
    RocksDBCodec.fromUtf8(RocksDB.DEFAULT_COLUMN_FAMILY)

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

  private val declaredColumnFamilies: Seq[String] =
    (DefaultColumnFamilyName +: columnFamilies.filterNot(_ == DefaultColumnFamilyName)).distinct

  private val normalizedDbPath = RocksDBCodec.requireLocalPath(dbPath)
  private val dbDir            = Paths.get(normalizedDbPath)
  private val manifestsDir     = dbDir.resolve("openivm-manifests")

  @volatile private var dbHandle: RocksDB                                    = _
  @volatile private var columnFamilyHandles: Map[String, ColumnFamilyHandle] = Map.empty
  @volatile private var versionValue: Long                                   = 0L
  @volatile private var closed                                               = false
  @volatile private var dirtySinceFlush                                      = false

  private val compactCalls = new AtomicLong(0L)

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

  private def flushAll(localDb: RocksDB): Unit = {
    val flushOptions = new FlushOptions().setWaitForFlush(true)
    try {
      columnFamilyHandles.values.foreach(handle => localDb.flush(flushOptions, handle))
    } finally {
      flushOptions.close()
    }
    dirtySinceFlush = false
  }

  private def sstFileCountInternal(localDb: RocksDB): Int =
    localDb.getLiveFilesMetaData.size()

  private def writeManifest(newVersion: Long, sstCount: Int): Unit = {
    Files.createDirectories(manifestsDir)
    val manifestPath = manifestsDir.resolve(s"$ManifestPrefix$newVersion")
    val tmpPath = manifestsDir.resolve(
      s".$ManifestPrefix$newVersion.${Thread.currentThread().getId}.${System.nanoTime()}.tmp"
    )
    val payload =
      s"""{"version":$newVersion,"timestampMs":${System.currentTimeMillis()},"sstCount":$sstCount}"""
    Files.write(tmpPath, RocksDBCodec.utf8(payload))
    try {
      Files.move(tmpPath, manifestPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
    } finally {
      Files.deleteIfExists(tmpPath)
    }
  }

  private def commitBatch(batch: WriteBatch): Long = {
    val localDb      = ensureLoaded()
    val writeOptions = new WriteOptions().setSync(false).setDisableWAL(!conf.walEnabled)
    try {
      localDb.write(writeOptions, batch)
      dirtySinceFlush = true
    } finally {
      writeOptions.close()
    }

    flushAll(localDb)

    val nextVersion = versionValue + 1L
    writeManifest(nextVersion, sstFileCountInternal(localDb))
    versionValue = nextVersion
    nextVersion
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

  private final class PrefixScanIterator(localDb: RocksDB, handle: ColumnFamilyHandle, prefix: Array[Byte])
      extends Iterator[(Array[Byte], Array[Byte])]
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

  private[common] def put(batch: WriteBatch, columnFamily: String, key: Array[Byte], value: Array[Byte]): Unit =
    batch.put(cf(columnFamily), key, value)

  private[common] def delete(batch: WriteBatch, columnFamily: String, key: Array[Byte]): Unit =
    batch.delete(cf(columnFamily), key)

  private[common] def deleteRange(
      batch: WriteBatch,
      columnFamily: String,
      startKey: Array[Byte],
      endKey: Array[Byte]
  ): Unit =
    batch.deleteRange(cf(columnFamily), startKey, endKey)

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
    val handles           = new java.util.ArrayList[ColumnFamilyHandle]()
    val options           = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true)
    var openedDb: RocksDB = null
    try {
      openedDb = RocksDB.open(options, normalizedDbPath, descriptors.asJava, handles)
      dbHandle = openedDb
      columnFamilyHandles = allColumnFamilies.zip(handles.asScala).toMap
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
      if (localDb != null && dirtySinceFlush) flushAll(localDb)
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
    if (!OpenIvmRocksDBTelemetry.isActive) {
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
    }
  }

  def withBatch[A](f: WriteBatch => A): Long = withNativeHandle("with_batch") {
    val batch = new WriteBatch()
    try {
      f(batch)
      commitBatch(batch)
    } finally {
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
    Option(dbHandle.get(cf(columnFamily), key))
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
        .map(value => Option(value))
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
        val it = new PrefixScanIterator(dbHandle, cf(columnFamily), prefix.clone())
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
        new PrefixScanIterator(dbHandle, cf(columnFamily), prefix.clone())
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
      try closeInternal()
      catch { case _: Throwable => () }
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
