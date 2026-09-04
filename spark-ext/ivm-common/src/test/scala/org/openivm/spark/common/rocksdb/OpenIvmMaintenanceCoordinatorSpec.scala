package org.openivm.spark.common.rocksdb

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

class OpenIvmMaintenanceCoordinatorSpec extends AnyFunSpec with BeforeAndAfterEach with Matchers {

  override def afterEach(): Unit = {
    OpenIvmMaintenanceCoordinator.shutdown()
    super.afterEach()
  }

  private def newDbDir(prefix: String): File = {
    val dir = new File(s"target/test-rocksdb-maint-$prefix-${UUID.randomUUID().toString.take(8)}")
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

  private def awaitCondition(timeoutMs: Long = 2000L, pollMs: Long = 25L)(condition: => Boolean): Unit = {
    val deadline = System.nanoTime() + timeoutMs * 1000000L
    while (System.nanoTime() < deadline && !condition) {
      Thread.sleep(pollMs)
    }
    condition shouldBe true
  }

  private def populateUntilSst(db: OpenIvmRocksDB): Unit = {
    var commit = 0
    while (commit < 64 && db.sstFileCount == 0) {
      commit += 1
      db.withBatch { batch =>
        db.put(batch, "meta", RocksDBCodec.utf8(s"k$commit"), RocksDBCodec.utf8(s"v$commit"))
      }
    }
    db.sstFileCount should be > 0
  }

  describe("OpenIvmMaintenanceCoordinator") {
    it("starts the daemon on first register") {
      val dir = newDbDir("start")
      val db  = new OpenIvmRocksDB(dir.getAbsolutePath, OpenIvmRocksDBConf.default, Seq("meta"))

      try {
        db.load()
        OpenIvmMaintenanceCoordinator.register(db, OpenIvmRocksDBConf.default.copy(maintenanceIntervalMs = 50L))

        awaitCondition() {
          OpenIvmMaintenanceCoordinator.isRunning
        }
      } finally {
        closeQuietly(db)
        deleteRecursively(dir)
      }
    }

    it("cleans up old manifests on the maintenance interval") {
      val dir = newDbDir("cleanup")
      val db  = new OpenIvmRocksDB(dir.getAbsolutePath, OpenIvmRocksDBConf.default, Seq("meta"))
      val conf = OpenIvmRocksDBConf.default.copy(
        minVersionsToRetain = 1,
        maintenanceIntervalMs = 50L,
        compactionThresholdSstCount = Int.MaxValue
      )

      try {
        db.load()
        (1 to 3).foreach { version =>
          db.withBatch { batch =>
            db.put(batch, "meta", RocksDBCodec.utf8(s"k$version"), RocksDBCodec.utf8(s"v$version"))
          }
        }

        db.manifestVersions shouldBe Seq(1L, 2L, 3L)
        OpenIvmMaintenanceCoordinator.register(db, conf)

        awaitCondition() {
          db.manifestVersions.size <= 2
        }
        db.manifestVersions shouldBe Seq(2L, 3L)
      } finally {
        closeQuietly(db)
        deleteRecursively(dir)
      }
    }

    it("triggers compaction when the SST count exceeds the configured threshold") {
      val dir = newDbDir("compact")
      val conf = OpenIvmRocksDBConf.default.copy(
        minVersionsToRetain = 64,
        maintenanceIntervalMs = 50L,
        compactionThresholdSstCount = 0,
        walEnabled = false
      )
      val db = new OpenIvmRocksDB(dir.getAbsolutePath, conf, Seq("meta"))

      try {
        db.load()
        populateUntilSst(db)
        OpenIvmMaintenanceCoordinator.register(db, conf)

        awaitCondition() {
          db.compactCallCount > 0L
        }
        db.compactCallCount should be > 0L
      } finally {
        closeQuietly(db)
        deleteRecursively(dir)
      }
    }

    it("stops cleanly and idempotently") {
      val dir = newDbDir("shutdown")
      val db  = new OpenIvmRocksDB(dir.getAbsolutePath, OpenIvmRocksDBConf.default, Seq("meta"))

      try {
        db.load()
        OpenIvmMaintenanceCoordinator.register(db, OpenIvmRocksDBConf.default.copy(maintenanceIntervalMs = 50L))
        awaitCondition() {
          OpenIvmMaintenanceCoordinator.isRunning
        }

        OpenIvmMaintenanceCoordinator.shutdown()
        OpenIvmMaintenanceCoordinator.shutdown()

        Thread.sleep(100L)
        OpenIvmMaintenanceCoordinator.isRunning shouldBe false
      } finally {
        closeQuietly(db)
        deleteRecursively(dir)
      }
    }
  }
}
