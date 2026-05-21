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

import java.nio.file.{Files, Paths, StandardCopyOption}
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

  def withBatch[A](f: WriteBatch => A): Long = withWriteLock {
    if (closed) {
      throw new IllegalStateException(s"OpenIVM RocksDB at $normalizedDbPath is already closed.")
    }
    load()
    val batch = new WriteBatch()
    try {
      f(batch)
      commitBatch(batch)
    } finally {
      batch.close()
    }
  }

  def get(columnFamily: String, key: Array[Byte]): Option[Array[Byte]] =
    Option(ensureLoaded().get(cf(columnFamily), key))

  def prefixScan(columnFamily: String, prefix: Array[Byte]): Iterator[(Array[Byte], Array[Byte])] =
    new PrefixScanIterator(ensureLoaded(), cf(columnFamily), prefix.clone())

  def currentVersion: Long = {
    ensureLoaded()
    versionValue
  }

  def load(): Long = withWriteLock {
    if (closed) {
      throw new IllegalStateException(s"OpenIVM RocksDB at $normalizedDbPath is already closed.")
    }
    if (dbHandle != null) {
      versionValue
    } else {
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
        versionValue
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
  }

  def cleanup(minVersionsToRetain: Int): Unit = withWriteLock {
    if (closed) return
    require(minVersionsToRetain >= 0, s"minVersionsToRetain must be >= 0, found $minVersionsToRetain")
    val threshold = currentVersion - minVersionsToRetain
    manifestVersions.filter(_ < threshold).foreach { version =>
      Files.deleteIfExists(manifestsDir.resolve(s"$ManifestPrefix$version"))
    }
  }

  def compactRange(): Unit = withWriteLock {
    if (closed) return
    val localDb = ensureLoaded()
    localDb.compactRange()
    compactCalls.incrementAndGet()
    ()
  }

  def close(): Unit = withWriteLock {
    if (closed) return
    closed = true

    val localDb               = dbHandle
    val localHandles          = columnFamilyHandles.values.toSeq
    var firstError: Throwable = null

    try {
      if (localDb != null) {
        flushAll(localDb)
      }
    } catch {
      case t: Throwable => firstError = t
    }

    try {
      localHandles.foreach(closeQuietly)
    } finally {
      columnFamilyHandles = Map.empty
    }

    try {
      if (localDb != null) {
        localDb.close()
      }
    } catch {
      case t: Throwable =>
        if (firstError == null) {
          firstError = t
        } else {
          firstError.addSuppressed(t)
        }
    } finally {
      dbHandle = null
    }

    if (firstError != null) {
      throw firstError
    }
  }

  def sstFileCount: Int = {
    if (closed) 0 else sstFileCountInternal(ensureLoaded())
  }
}
