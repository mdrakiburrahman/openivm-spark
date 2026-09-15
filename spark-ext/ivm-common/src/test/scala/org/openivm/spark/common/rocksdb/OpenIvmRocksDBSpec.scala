package org.openivm.spark.common.rocksdb

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.Paths
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object OpenIvmRocksDBCrashWriter {
  def main(args: Array[String]): Unit = {
    val conf = OpenIvmRocksDBConf.default.copy(walEnabled = args.lift(1).forall(_.toBoolean))
    val db   = new OpenIvmRocksDB(args(0), conf, Seq("meta"))
    db.load()
    db.withBatch { batch =>
      db.put(batch, "meta", RocksDBCodec.utf8("crash-key"), RocksDBCodec.utf8("crash-value"))
    }
    Runtime.getRuntime.halt(0)
  }
}

object OpenIvmRocksDBPostManifestCrashWriter {
  def main(args: Array[String]): Unit = {
    val conf = OpenIvmRocksDBConf.default.copy(walEnabled = args.lift(1).forall(_.toBoolean))
    val db   = new OpenIvmRocksDB(args(0), conf, Seq("meta"))
    db.setAfterManifestHookForTesting(() => Runtime.getRuntime.halt(0))
    db.load()
    db.withBatch { batch =>
      db.put(batch, "meta", RocksDBCodec.utf8("manifest-key"), RocksDBCodec.utf8("manifest-value"))
    }
    Runtime.getRuntime.halt(1)
  }
}

/** Stand-alone main used by the multi-process test below. Each invocation:
  *   1. opens the DB at `args(0)` in multi-process mode,
  *   2. writes `<args(1)> = <args(2)>` to the `meta` CF in one withBatch,
  *   3. cleanly closes and exits.
  *
  * Two concurrent invocations against the same DB path must both succeed
  * because every public op grabs the cross-JVM POSIX file lock at
  * `<dbPath>/openivm-jvm.lock`, opens RocksDB, runs the op, then closes
  * RocksDB (releasing `<dbPath>/LOCK`) before releasing the file lock.
  */
object OpenIvmRocksDBMultiProcessWriter {
  def main(args: Array[String]): Unit = {
    val conf = OpenIvmRocksDBConf.default.copy(multiProcess = true, lockTimeoutMs = 60000L)
    val db   = new OpenIvmRocksDB(args(0), conf, Seq("meta"))
    try {
      db.withBatch { batch =>
        db.put(batch, "meta", RocksDBCodec.utf8(args(1)), RocksDBCodec.utf8(args(2)))
      }
    } finally {
      db.close()
    }
  }
}

class OpenIvmRocksDBSpec extends AnyFunSpec with Matchers {

  private def newDbDir(prefix: String): File = {
    val dir = new File(s"target/test-rocksdb-$prefix-${UUID.randomUUID().toString.take(8)}")
    dir.mkdirs()
    dir
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
    }
    file.delete()
    ()
  }

  private def closeQuietly(db: OpenIvmRocksDB): Unit =
    try db.close()
    catch {
      case _: Throwable => ()
    }

  private def runCrashWriter(dir: File, walEnabled: Boolean): Unit = {
    val javaHome  = Paths.get(System.getProperty("java.home"), "bin", "java").toString
    val classpath = System.getProperty("java.class.path")
    val logFile   = new File(dir, "crash-writer.log")
    val process = new ProcessBuilder(
      javaHome,
      "-cp",
      classpath,
      "org.openivm.spark.common.rocksdb.OpenIvmRocksDBCrashWriter",
      dir.getAbsolutePath,
      walEnabled.toString
    )
      .redirectErrorStream(true)
      .redirectOutput(ProcessBuilder.Redirect.appendTo(logFile))
      .start()

    process.waitFor(120, TimeUnit.SECONDS) shouldBe true
    process.exitValue() shouldBe 0
  }

  private def runPostManifestCrashWriter(dir: File, walEnabled: Boolean): Unit = {
    val javaHome  = Paths.get(System.getProperty("java.home"), "bin", "java").toString
    val classpath = System.getProperty("java.class.path")
    val logFile   = new File(dir, "post-manifest-crash-writer.log")
    val process = new ProcessBuilder(
      javaHome,
      "-cp",
      classpath,
      "org.openivm.spark.common.rocksdb.OpenIvmRocksDBPostManifestCrashWriter",
      dir.getAbsolutePath,
      walEnabled.toString
    )
      .redirectErrorStream(true)
      .redirectOutput(ProcessBuilder.Redirect.appendTo(logFile))
      .start()

    process.waitFor(120, TimeUnit.SECONDS) shouldBe true
    process.exitValue() shouldBe 0
  }

  private def assertCrashRecovery(walEnabled: Boolean): Unit = {
    val dir      = newDbDir(s"crash-wal-$walEnabled")
    val conf     = OpenIvmRocksDBConf.default.copy(walEnabled = walEnabled)
    val reopened = new OpenIvmRocksDB(dir.getAbsolutePath, conf, Seq("meta"))

    try {
      runCrashWriter(dir, walEnabled)
      reopened.load()
      reopened.get("meta", RocksDBCodec.utf8("crash-key")).map(RocksDBCodec.fromUtf8) shouldBe Some("crash-value")
      reopened.prefixScan(OpenIvmRocksDB.InternalTxnColumnFamilyName, Array.emptyByteArray).toList shouldBe empty
    } finally {
      closeQuietly(reopened)
      deleteRecursively(dir)
    }
  }

  private def assertPostManifestCrashRecovery(walEnabled: Boolean): Unit = {
    val dir      = newDbDir(s"post-manifest-crash-wal-$walEnabled")
    val conf     = OpenIvmRocksDBConf.default.copy(walEnabled = walEnabled)
    val reopened = new OpenIvmRocksDB(dir.getAbsolutePath, conf, Seq("meta"))

    try {
      runPostManifestCrashWriter(dir, walEnabled)
      reopened.load()
      reopened.get("meta", RocksDBCodec.utf8("manifest-key")).map(RocksDBCodec.fromUtf8) shouldBe Some("manifest-value")
      reopened.prefixScan(OpenIvmRocksDB.InternalTxnColumnFamilyName, Array.emptyByteArray).toList shouldBe empty
    } finally {
      closeQuietly(reopened)
      deleteRecursively(dir)
    }
  }

  describe("OpenIvmRocksDB") {
    it("opens and closes cleanly without leaving a stale lock") {
      val dir = newDbDir("open-close")
      val db1 = new OpenIvmRocksDB(dir.getAbsolutePath, OpenIvmRocksDBConf.default, Seq("meta"))
      val db2 = new OpenIvmRocksDB(dir.getAbsolutePath, OpenIvmRocksDBConf.default, Seq("meta"))

      try {
        db1.load()
        db1.close()
        noException should be thrownBy db2.load()
        db2.close()
      } finally {
        closeQuietly(db1)
        closeQuietly(db2)
        deleteRecursively(dir)
      }
    }

    it("resets state and releases the native lock when an open fails after the lock is acquired") {
      val dir = newDbDir("open-failure-retry")
      val db  = new OpenIvmRocksDB(dir.getAbsolutePath, OpenIvmRocksDBConf.default, Seq("meta"))

      try {
        // Simulate a post-`RocksDB.open` failure (as `recoverLogicalTransactions`
        // / `readCurrentVersion` can throw) that happens AFTER `<dbPath>/LOCK` is
        // already held.
        db.setAfterRawOpenHookForTesting(() => throw new RuntimeException("open boom"))
        intercept[RuntimeException](db.load()).getMessage shouldBe "open boom"

        // The failed open must leave NO stale handle and NO held lock: the very
        // next operation in the SAME process must re-open cleanly. A stale closed
        // handle would run against a dead DB; a leaked lock would fail the reopen
        // with "lock hold by current process".
        db.setAfterRawOpenHookForTesting(() => ())
        db.withBatch { batch =>
          db.put(batch, "meta", RocksDBCodec.utf8("k"), RocksDBCodec.utf8("v"))
        }
        db.get("meta", RocksDBCodec.utf8("k")).map(RocksDBCodec.fromUtf8) shouldBe Some("v")
        db.close()

        // A brand-new instance on the same path also opens, proving the failed
        // open leaked no OS-level RocksDB lock.
        val reopened = new OpenIvmRocksDB(dir.getAbsolutePath, OpenIvmRocksDBConf.default, Seq("meta"))
        try {
          reopened.load()
          reopened.get("meta", RocksDBCodec.utf8("k")).map(RocksDBCodec.fromUtf8) shouldBe Some("v")
        } finally closeQuietly(reopened)
      } finally {
        db.setAfterRawOpenHookForTesting(() => ())
        closeQuietly(db)
        deleteRecursively(dir)
      }
    }

    it("round-trips puts and gets across multiple column families") {
      val dir = newDbDir("round-trip")
      val db  = new OpenIvmRocksDB(dir.getAbsolutePath, OpenIvmRocksDBConf.default, Seq("meta", "props"))

      try {
        db.load()
        db.withBatch { batch =>
          db.put(batch, "meta", RocksDBCodec.utf8("mv-a"), RocksDBCodec.utf8("v1"))
          db.put(batch, "props", RocksDBCodec.utf8("mv-a.owner"), RocksDBCodec.utf8("alice"))
        }

        db.get("meta", RocksDBCodec.utf8("mv-a")).map(RocksDBCodec.fromUtf8) shouldBe Some("v1")
        db.get("props", RocksDBCodec.utf8("mv-a.owner")).map(RocksDBCodec.fromUtf8) shouldBe Some("alice")
      } finally {
        closeQuietly(db)
        deleteRecursively(dir)
      }
    }

    it("flushes only the column families touched by the batch") {
      val dir  = newDbDir("flush-targets")
      val conf = OpenIvmRocksDBConf.default.copy(walEnabled = false)
      val db   = new OpenIvmRocksDB(dir.getAbsolutePath, conf, Seq("meta", "props"))

      try {
        val flushed = ArrayBuffer.empty[Seq[String]]
        db.setBeforeFlushHookForTesting(columnFamilies => flushed.synchronized { flushed += columnFamilies.sorted })
        db.load()
        db.withBatch { batch =>
          db.put(batch, "meta", RocksDBCodec.utf8("k"), RocksDBCodec.utf8("v"))
        }

        flushed should have size 1
        flushed.exists(_.contains("props")) shouldBe false
        flushed.head.toSet shouldBe Set("meta", OpenIvmRocksDB.InternalTxnColumnFamilyName)
      } finally {
        closeQuietly(db)
        deleteRecursively(dir)
      }
    }

    it("propagates a flush failure and rolls back the uncommitted batch") {
      val dir  = newDbDir("flush-failure")
      val conf = OpenIvmRocksDBConf.default.copy(walEnabled = false)
      val db   = new OpenIvmRocksDB(dir.getAbsolutePath, conf, Seq("meta"))

      try {
        db.load()
        db.setBeforeFlushHookForTesting(_ => throw new RuntimeException("flush boom"))

        val thrown = intercept[RuntimeException] {
          db.withBatch { batch =>
            db.put(batch, "meta", RocksDBCodec.utf8("k"), RocksDBCodec.utf8("v"))
          }
        }

        thrown.getMessage shouldBe "flush boom"
        db.currentVersion shouldBe 0L
        db.get("meta", RocksDBCodec.utf8("k")) shouldBe None
      } finally {
        db.setBeforeFlushHookForTesting(_ => ())
        closeQuietly(db)
        deleteRecursively(dir)
      }
    }

    it("propagates close failures instead of swallowing them") {
      val dir = newDbDir("close-propagation")
      val db  = new OpenIvmRocksDB(dir.getAbsolutePath, OpenIvmRocksDBConf.default, Seq("meta"))

      try {
        db.load()
        db.setBeforeCloseHookForTesting(() => throw new RuntimeException("close boom"))

        val thrown = intercept[RuntimeException] {
          db.close()
        }

        thrown.getMessage shouldBe "close boom"
        an[IllegalStateException] should be thrownBy db.currentVersion
      } finally {
        closeQuietly(db)
        deleteRecursively(dir)
      }
    }

    it("streams oversized write batches into bounded commits") {
      val dir  = newDbDir("bounded-batch")
      val conf = OpenIvmRocksDBConf.default.copy(walEnabled = false, maxWriteBatchBytes = 32L)
      val db   = new OpenIvmRocksDB(dir.getAbsolutePath, conf, Seq("meta"))

      try {
        val flushed = ArrayBuffer.empty[Seq[String]]
        db.setBeforeFlushHookForTesting(columnFamilies => flushed.synchronized { flushed += columnFamilies.sorted })
        db.load()
        db.withBatch { batch =>
          (1 to 5).foreach { idx =>
            db.put(batch, "meta", RocksDBCodec.utf8(s"k$idx"), RocksDBCodec.utf8("v" * 32))
          }
        }

        (1 to 5).foreach { idx =>
          db.get("meta", RocksDBCodec.utf8(s"k$idx")).map(RocksDBCodec.fromUtf8) shouldBe Some("v" * 32)
        }
        db.currentVersion shouldBe 1L
        flushed should have size 1
        flushed.head.toSet shouldBe Set("meta", OpenIvmRocksDB.InternalTxnColumnFamilyName)
      } finally {
        closeQuietly(db)
        deleteRecursively(dir)
      }
    }

    it("uses synchronous WAL commits without flushing each batch") {
      val dir = newDbDir("wal-commit")
      val db  = new OpenIvmRocksDB(dir.getAbsolutePath, OpenIvmRocksDBConf.default, Seq("meta"))

      try {
        val flushed = ArrayBuffer.empty[Seq[String]]
        db.setBeforeFlushHookForTesting(columnFamilies => flushed.synchronized { flushed += columnFamilies.sorted })
        db.load()
        db.withBatch { batch =>
          db.put(batch, "meta", RocksDBCodec.utf8("k"), RocksDBCodec.utf8("v"))
        }

        db.get("meta", RocksDBCodec.utf8("k")).map(RocksDBCodec.fromUtf8) shouldBe Some("v")
        db.currentVersion shouldBe 1L
        flushed shouldBe empty
      } finally {
        closeQuietly(db)
        deleteRecursively(dir)
      }
    }

    it("syncs WAL once for a streamed logical commit") {
      val dir  = newDbDir("wal-group-commit")
      val conf = OpenIvmRocksDBConf.default.copy(maxWriteBatchBytes = 32L)
      val db   = new OpenIvmRocksDB(dir.getAbsolutePath, conf, Seq("meta"))

      try {
        val syncCount = new AtomicInteger(0)
        db.setBeforeSyncWalHookForTesting(() => syncCount.incrementAndGet())
        db.load()
        db.withBatch { batch =>
          (1 to 5).foreach { idx =>
            db.put(batch, "meta", RocksDBCodec.utf8(s"k$idx"), RocksDBCodec.utf8("v" * 32))
          }
        }

        (1 to 5).foreach { idx =>
          db.get("meta", RocksDBCodec.utf8(s"k$idx")).map(RocksDBCodec.fromUtf8) shouldBe Some("v" * 32)
        }
        syncCount.get() shouldBe 1
        db.currentVersion shouldBe 1L
      } finally {
        closeQuietly(db)
        deleteRecursively(dir)
      }
    }

    it("rolls back streamed writes that fail before the logical manifest") {
      val dir  = newDbDir("bounded-rollback")
      val conf = OpenIvmRocksDBConf.default.copy(maxWriteBatchBytes = 32L)
      val db   = new OpenIvmRocksDB(dir.getAbsolutePath, conf, Seq("meta"))

      try {
        db.load()
        val thrown = intercept[RuntimeException] {
          db.withBatch { batch =>
            (1 to 5).foreach { idx =>
              db.put(batch, "meta", RocksDBCodec.utf8(s"k$idx"), RocksDBCodec.utf8("v" * 32))
            }
            throw new RuntimeException("abort before manifest")
          }
        }
        thrown.getMessage shouldBe "abort before manifest"
        (1 to 5).foreach { idx =>
          db.get("meta", RocksDBCodec.utf8(s"k$idx")) shouldBe None
        }
        closeQuietly(db)

        val reopened = new OpenIvmRocksDB(dir.getAbsolutePath, conf, Seq("meta"))
        try {
          reopened.load() shouldBe 0L
          (1 to 5).foreach { idx =>
            reopened.get("meta", RocksDBCodec.utf8(s"k$idx")) shouldBe None
          }
        } finally {
          closeQuietly(reopened)
        }
      } finally {
        closeQuietly(db)
        deleteRecursively(dir)
      }
    }

    it("returns prefix scans in lexicographic key order") {
      val dir = newDbDir("prefix-scan")
      val db  = new OpenIvmRocksDB(dir.getAbsolutePath, OpenIvmRocksDBConf.default, Seq("scan"))

      try {
        db.load()
        db.withBatch { batch =>
          val rows = Seq(
            "b"  -> "value-b",
            "a"  -> "value-a",
            "aa" -> "value-aa",
            "z"  -> "other-prefix"
          )
          rows.foreach { case (suffix, value) =>
            val group = if (suffix == "z") "other" else "group"
            val key   = RocksDBCodec.compositeKey(Seq(RocksDBCodec.utf8(group), RocksDBCodec.utf8(suffix)))
            db.put(batch, "scan", key, RocksDBCodec.utf8(value))
          }
        }

        val prefix  = RocksDBCodec.compositeKey(Seq(RocksDBCodec.utf8("group")))
        val scanned = db.prefixScan("scan", prefix).toList
        val suffixes = scanned.map { case (key, _) =>
          RocksDBCodec.splitComposite(key).map(RocksDBCodec.fromUtf8).last
        }

        suffixes shouldBe Seq("a", "aa", "b")
        scanned.map { case (_, value) => RocksDBCodec.fromUtf8(value) } shouldBe Seq("value-a", "value-aa", "value-b")
      } finally {
        closeQuietly(db)
        deleteRecursively(dir)
      }
    }

    it("keeps commit versions monotonic") {
      val dir = newDbDir("versions")
      val db  = new OpenIvmRocksDB(dir.getAbsolutePath, OpenIvmRocksDBConf.default, Seq("meta"))

      try {
        db.load()
        (1 to 10).foreach { version =>
          db.withBatch { batch =>
            db.put(batch, "meta", RocksDBCodec.utf8(s"k$version"), RocksDBCodec.utf8(s"v$version"))
          }
        }

        db.currentVersion shouldBe 10L
      } finally {
        closeQuietly(db)
        deleteRecursively(dir)
      }
    }

    it("recovers committed values after an unclean process exit") {
      assertCrashRecovery(walEnabled = true)
    }

    it("recovers committed values without WAL after an unclean process exit") {
      assertCrashRecovery(walEnabled = false)
    }

    it("recovers manifest-published values after a WAL crash before cleanup") {
      assertPostManifestCrashRecovery(walEnabled = true)
    }

    it("recovers manifest-published values without WAL after a crash before cleanup") {
      assertPostManifestCrashRecovery(walEnabled = false)
    }

    it("retains only the configured recent manifests during cleanup") {
      val dir = newDbDir("cleanup")
      val db  = new OpenIvmRocksDB(dir.getAbsolutePath, OpenIvmRocksDBConf.default, Seq("meta"))

      try {
        db.load()
        (1 to 5).foreach { version =>
          db.withBatch { batch =>
            db.put(batch, "meta", RocksDBCodec.utf8(s"k$version"), RocksDBCodec.utf8(s"v$version"))
          }
        }

        db.cleanup(minVersionsToRetain = 2)
        db.manifestVersions shouldBe Seq(3L, 4L, 5L)
      } finally {
        closeQuietly(db)
        deleteRecursively(dir)
      }
    }

    it("compacts without throwing") {
      val dir = newDbDir("compact")
      val db  = new OpenIvmRocksDB(dir.getAbsolutePath, OpenIvmRocksDBConf.default, Seq("meta"))

      try {
        db.load()
        db.withBatch { batch =>
          db.put(batch, "meta", RocksDBCodec.utf8("k"), RocksDBCodec.utf8("v"))
        }
        noException should be thrownBy db.compactRange()
      } finally {
        closeQuietly(db)
        deleteRecursively(dir)
      }
    }

    it("serializes concurrent withBatch writers behind the per-db mutex") {
      val dir = newDbDir("concurrent")
      val db  = new OpenIvmRocksDB(dir.getAbsolutePath, OpenIvmRocksDBConf.default, Seq("meta"))

      val ready     = new CountDownLatch(2)
      val start     = new CountDownLatch(1)
      val active    = new AtomicInteger(0)
      val maxActive = new AtomicInteger(0)

      def write(key: String, value: String): Future[Long] = Future {
        ready.countDown()
        start.await(5, TimeUnit.SECONDS) shouldBe true
        db.withBatch { batch =>
          val current = active.incrementAndGet()
          maxActive.updateAndGet(existing => math.max(existing, current))
          try {
            Thread.sleep(100L)
            db.put(batch, "meta", RocksDBCodec.utf8(key), RocksDBCodec.utf8(value))
          } finally {
            active.decrementAndGet()
          }
        }
      }

      try {
        db.load()
        val first  = write("k1", "v1")
        val second = write("k2", "v2")
        ready.await(5, TimeUnit.SECONDS) shouldBe true
        start.countDown()

        val committedVersions = Seq(Await.result(first, 10.seconds), Await.result(second, 10.seconds)).toSet

        committedVersions shouldBe Set(1L, 2L)
        maxActive.get() shouldBe 1
        db.currentVersion shouldBe 2L
        db.get("meta", RocksDBCodec.utf8("k1")).map(RocksDBCodec.fromUtf8) shouldBe Some("v1")
        db.get("meta", RocksDBCodec.utf8("k2")).map(RocksDBCodec.fromUtf8) shouldBe Some("v2")
      } finally {
        closeQuietly(db)
        deleteRecursively(dir)
      }
    }

    it("allows coalesced flushes for unrelated shards to overlap") {
      val dbCount   = 8
      val dirs      = (1 to dbCount).map(index => newDbDir(s"parallel-flush-$index"))
      val conf      = OpenIvmRocksDBConf.default.copy(walEnabled = false)
      val dbs       = dirs.map(dir => new OpenIvmRocksDB(dir.getAbsolutePath, conf, Seq("meta")))
      val ready     = new CountDownLatch(dbCount)
      val release   = new CountDownLatch(1)
      val active    = new AtomicInteger(0)
      val maxActive = new AtomicInteger(0)
      val executor  = java.util.concurrent.Executors.newFixedThreadPool(dbCount)
      val ec        = scala.concurrent.ExecutionContext.fromExecutorService(executor)

      dbs.foreach { db =>
        db.setBeforeFlushHookForTesting { _ =>
          val current = active.incrementAndGet()
          maxActive.updateAndGet(existing => math.max(existing, current))
          ready.countDown()
          try release.await(10, TimeUnit.SECONDS) shouldBe true
          finally active.decrementAndGet()
        }
      }

      try {
        dbs.foreach(_.load())
        val writes = dbs.zipWithIndex.map { case (db, index) =>
          Future {
            db.withBatch { batch =>
              db.put(batch, "meta", RocksDBCodec.utf8(s"k$index"), RocksDBCodec.utf8(s"v$index"))
            }
          }(ec)
        }

        ready.await(10, TimeUnit.SECONDS) shouldBe true
        maxActive.get() shouldBe dbCount
        release.countDown()
        writes.foreach(write => Await.result(write, 30.seconds))

        dbs.zipWithIndex.foreach { case (db, index) =>
          db.currentVersion shouldBe 1L
          db.get("meta", RocksDBCodec.utf8(s"k$index")).map(RocksDBCodec.fromUtf8) shouldBe Some(s"v$index")
        }
      } finally {
        release.countDown()
        executor.shutdownNow()
        dbs.foreach(closeQuietly)
        dirs.foreach(deleteRecursively)
      }
    }

    it("reports per-operation JVM lock contention to the active telemetry session") {
      val dir = newDbDir("telemetry-contention")
      val db  = new OpenIvmRocksDB(dir.getAbsolutePath, OpenIvmRocksDBConf.default, Seq("meta"))

      val holderEntered = new CountDownLatch(1)
      val releaseHolder = new CountDownLatch(1)

      try {
        db.load()
        val holder = Future {
          db.withBatch { batch =>
            holderEntered.countDown()
            releaseHolder.await(5, TimeUnit.SECONDS) shouldBe true
            db.put(batch, "meta", RocksDBCodec.utf8("held"), RocksDBCodec.utf8("value"))
          }
        }

        holderEntered.await(5, TimeUnit.SECONDS) shouldBe true
        val waiter = Future {
          val telemetry = OpenIvmRocksDBTelemetry.start()
          db.get("meta", RocksDBCodec.utf8("held"))
          telemetry.finish()
        }

        // Keep the holder in its batch long enough that the waiter must park
        // on writeMutex. The assertion leaves a wide margin for scheduler
        // jitter while still distinguishing real contention from lock-call
        // bookkeeping overhead.
        Thread.sleep(100L)
        releaseHolder.countDown()

        Await.result(holder, 10.seconds)
        val summaries  = Await.result(waiter, 10.seconds)
        val getSummary = summaries.find(_.operation == "get").getOrElse(fail("missing get telemetry"))
        getSummary.operationCount shouldBe 1L
        getSummary.failedCount shouldBe 0L
        getSummary.jvmLockWaitNanos should be >= 50000000L
        getSummary.jvmLockHeldNanos should be > 0L
      } finally {
        releaseHolder.countDown()
        closeQuietly(db)
        deleteRecursively(dir)
      }
    }

    it("reports multi-process native open and close costs without exposing the DB path") {
      val root = newDbDir("telemetry-multi-process")
      val dir  = new File(root, "_openivm/index/rocksdb")
      val conf = OpenIvmRocksDBConf.default.copy(multiProcess = true, lockTimeoutMs = 60000L)
      val db   = new OpenIvmRocksDB(dir.getAbsolutePath, conf, Seq("meta"))

      try {
        val telemetry = OpenIvmRocksDBTelemetry.start()
        db.get("meta", RocksDBCodec.utf8("missing")) shouldBe None
        val summaries  = telemetry.finish()
        val getSummary = summaries.find(_.operation == "get").getOrElse(fail("missing get telemetry"))

        getSummary.dbScope shouldBe "index"
        getSummary.multiProcess shouldBe true
        getSummary.operationCount shouldBe 1L
        getSummary.nativeOpenNanos should be > 0L
        getSummary.nativeCloseNanos should be > 0L
        getSummary.externalLockWaitNanos should be >= 0L
      } finally {
        closeQuietly(db)
        deleteRecursively(root)
      }
    }

    it("reuses one native open and close for a multi-process catalog session") {
      val root = newDbDir("catalog-session")
      val dir  = new File(root, "_openivm/mvs/test/rocksdb")
      val conf = OpenIvmRocksDBConf.default.copy(multiProcess = true, lockTimeoutMs = 60000L)
      val db   = new OpenIvmRocksDB(dir.getAbsolutePath, conf, Seq("meta"))

      try {
        db.withBatch { batch =>
          db.put(batch, "meta", RocksDBCodec.utf8("one"), RocksDBCodec.utf8("1"))
          db.put(batch, "meta", RocksDBCodec.utf8("two"), RocksDBCodec.utf8("2"))
        }

        val telemetry = OpenIvmRocksDBTelemetry.start()
        val values = db.withSession {
          val first = db.multiGet(
            "meta",
            Seq(RocksDBCodec.utf8("one"), RocksDBCodec.utf8("missing"), RocksDBCodec.utf8("two"))
          )
          db.get("meta", RocksDBCodec.utf8("one")) shouldBe defined
          first
        }
        val summaries = telemetry.finish()

        values.map(_.map(RocksDBCodec.fromUtf8)) shouldBe Seq(Some("1"), None, Some("2"))
        val session = summaries.find(_.operation == "session").getOrElse(fail("missing session telemetry"))
        session.operationCount shouldBe 1L
        session.nativeOpenNanos should be > 0L
        session.nativeCloseNanos should be > 0L

        val nested = summaries.filter(summary => summary.operation == "get" || summary.operation == "multi_get")
        nested.map(_.operation).toSet shouldBe Set("get", "multi_get")
        nested.map(_.nativeOpenNanos).sum shouldBe 0L
        nested.map(_.nativeCloseNanos).sum shouldBe 0L
        nested.map(_.externalLockWaitNanos).sum shouldBe 0L
      } finally {
        closeQuietly(db)
        deleteRecursively(root)
      }
    }

    it("allows reentrant catalog ops within withBatch under multiProcess=true") {
      val dir  = newDbDir("mp-reentrant")
      val conf = OpenIvmRocksDBConf.default.copy(multiProcess = true, lockTimeoutMs = 60000L)
      val db   = new OpenIvmRocksDB(dir.getAbsolutePath, conf, Seq("meta"))

      try {
        // Seed two pre-existing entries to make the prefix scan non-trivial.
        db.withBatch { batch =>
          db.put(batch, "meta", RocksDBCodec.utf8("seed-1"), RocksDBCodec.utf8("v1"))
          db.put(batch, "meta", RocksDBCodec.utf8("seed-2"), RocksDBCodec.utf8("v2"))
        }

        // Mimic MvCatalog.rewriteProperties: call prefixScan + get from inside
        // withBatch. Both nested calls must succeed without throwing
        // OverlappingFileLockException.
        db.withBatch { batch =>
          val seeded = db.prefixScan("meta", Array.emptyByteArray).toList
          seeded.size shouldBe 2
          val existingV1 = db.get("meta", RocksDBCodec.utf8("seed-1")).map(RocksDBCodec.fromUtf8)
          existingV1 shouldBe Some("v1")
          db.put(batch, "meta", RocksDBCodec.utf8("new"), RocksDBCodec.utf8("v3"))
        }

        db.get("meta", RocksDBCodec.utf8("new")).map(RocksDBCodec.fromUtf8) shouldBe Some("v3")
      } finally {
        closeQuietly(db)
        deleteRecursively(dir)
      }
    }

    it("labels sharded telemetry paths without exposing identifiers") {
      OpenIvmRocksDBTelemetry.scopeForPath("/tmp/warehouse/_openivm/mvs/private-mv/rocksdb") shouldBe "mv"
      OpenIvmRocksDBTelemetry.scopeForPath("/tmp/warehouse/_openivm/tables/private-table/rocksdb") shouldBe "table"
      OpenIvmRocksDBTelemetry.scopeForPath("/tmp/warehouse/_openivm/sources/private-source/rocksdb") shouldBe "source"
    }

    it("allows concurrent multi-process writers with multiProcess=true") {
      val dir       = newDbDir("multi-process")
      val javaHome  = Paths.get(System.getProperty("java.home"), "bin", "java").toString
      val classpath = System.getProperty("java.class.path")
      val procCount = 4

      val processes = (1 to procCount).map { idx =>
        val logFile = new File(dir, s"mp-writer-$idx.log")
        new ProcessBuilder(
          javaHome,
          "-cp",
          classpath,
          "org.openivm.spark.common.rocksdb.OpenIvmRocksDBMultiProcessWriter",
          dir.getAbsolutePath,
          s"mp-key-$idx",
          s"mp-value-$idx"
        )
          .redirectErrorStream(true)
          .redirectOutput(ProcessBuilder.Redirect.appendTo(logFile))
          .start()
      }

      val reopened = new OpenIvmRocksDB(
        dir.getAbsolutePath,
        OpenIvmRocksDBConf.default.copy(multiProcess = true, lockTimeoutMs = 60000L),
        Seq("meta")
      )

      try {
        processes.foreach { p =>
          p.waitFor(60, TimeUnit.SECONDS) shouldBe true
          p.exitValue() shouldBe 0
        }

        (1 to procCount).foreach { idx =>
          reopened
            .get("meta", RocksDBCodec.utf8(s"mp-key-$idx"))
            .map(RocksDBCodec.fromUtf8) shouldBe Some(s"mp-value-$idx")
        }
        reopened.currentVersion shouldBe procCount.toLong
      } finally {
        closeQuietly(reopened)
        deleteRecursively(dir)
      }
    }
  }
}
