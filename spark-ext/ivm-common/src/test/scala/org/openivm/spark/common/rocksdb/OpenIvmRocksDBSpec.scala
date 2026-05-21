package org.openivm.spark.common.rocksdb

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.Paths
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object OpenIvmRocksDBCrashWriter {
  def main(args: Array[String]): Unit = {
    val db = new OpenIvmRocksDB(args(0), OpenIvmRocksDBConf.default, Seq("meta"))
    db.load()
    db.withBatch { batch =>
      db.put(batch, "meta", RocksDBCodec.utf8("crash-key"), RocksDBCodec.utf8("crash-value"))
    }
    Runtime.getRuntime.halt(0)
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
      val dir       = newDbDir("crash")
      val javaHome  = Paths.get(System.getProperty("java.home"), "bin", "java").toString
      val classpath = System.getProperty("java.class.path")
      val logFile   = new File(dir, "crash-writer.log")
      val process = new ProcessBuilder(
        javaHome,
        "-cp",
        classpath,
        "org.openivm.spark.common.rocksdb.OpenIvmRocksDBCrashWriter",
        dir.getAbsolutePath
      )
        .redirectErrorStream(true)
        .redirectOutput(ProcessBuilder.Redirect.appendTo(logFile))
        .start()
      val reopened = new OpenIvmRocksDB(dir.getAbsolutePath, OpenIvmRocksDBConf.default, Seq("meta"))

      try {
        process.waitFor(10, TimeUnit.SECONDS) shouldBe true
        process.exitValue() shouldBe 0

        reopened.load()
        reopened.get("meta", RocksDBCodec.utf8("crash-key")).map(RocksDBCodec.fromUtf8) shouldBe Some("crash-value")
      } finally {
        closeQuietly(reopened)
        deleteRecursively(dir)
      }
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
  }
}
