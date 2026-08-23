package org.openivm.spark.common.rocksdb

import org.openivm.spark.common.FeatureGate
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.UUID

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class OpenIvmRocksDBRegistrySpec extends AnyFunSpec with BeforeAndAfterEach with Matchers {
  private val TestStateSyncKey = "spark.openivm.test.stateSync.key"

  private val sparks = ArrayBuffer.empty[SparkSession]
  private val dirs   = ArrayBuffer.empty[File]

  override def afterEach(): Unit = {
    OpenIvmRocksDBRegistry.closeAll()
    sparks.reverse.foreach { spark =>
      try spark.stop()
      catch {
        case _: Throwable => ()
      }
    }
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    OpenIvmStateSync.resetForTesting()
    sparks.clear()
    dirs.reverse.foreach(deleteRecursively)
    dirs.clear()
    super.afterEach()
  }

  private def newDir(prefix: String): File = {
    val dir = new File(s"target/test-rocksdb-registry-$prefix-${UUID.randomUUID().toString.take(8)}")
    dir.mkdirs()
    dirs += dir
    dir
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
    }
    file.delete()
    ()
  }

  private def newSpark(appName: String, extraConf: Seq[(String, String)] = Seq.empty): SparkSession = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()

    val warehouse = newDir(s"$appName-warehouse")
    val builder = SparkSession
      .builder()
      .master("local[1]")
      .appName(appName)
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "localhost")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.warehouse.dir", warehouse.getAbsolutePath)
    extraConf.foreach { case (key, value) => builder.config(key, value) }
    val spark = builder.getOrCreate()

    sparks += spark
    spark
  }

  private def newStateSyncSession(base: SparkSession, key: String): SparkSession = {
    val session = base.newSession()
    session.conf.set(TestStateSyncKey, key)
    session
  }

  private def waitUntil(timeout: FiniteDuration = 10.seconds)(condition: => Boolean): Unit = {
    val deadline = timeout.fromNow
    while (!condition && deadline.hasTimeLeft()) {
      Thread.sleep(25L)
    }
    condition shouldBe true
  }

  private def waitForStateSyncIdle(session: SparkSession, timeout: FiniteDuration = 10.seconds): Unit =
    waitUntil(timeout) {
      OpenIvmStateSync
        .backupStateSnapshotForTesting(session)
        .contains(
          OpenIvmStateSync.BackupStateSnapshot(running = false, requested = false)
        )
    }

  describe("OpenIvmRocksDBRegistry") {
    it("returns the same instance for repeated opens of the same path") {
      val spark = newSpark("registry-same-instance")
      val dbDir = newDir("same-instance-db")

      val first  = OpenIvmRocksDBRegistry.getOrOpen(spark, dbDir.getAbsolutePath, Seq("meta", "props"))
      val second = OpenIvmRocksDBRegistry.getOrOpen(spark, dbDir.getAbsolutePath, Seq("meta"))

      first should be theSameInstanceAs second
    }

    it("allows column-family subsets and rejects missing column families with a clear message") {
      val spark = newSpark("registry-cf-subset")
      val dbDir = newDir("cf-subset-db")

      val first  = OpenIvmRocksDBRegistry.getOrOpen(spark, dbDir.getAbsolutePath, Seq("meta", "props"))
      val second = OpenIvmRocksDBRegistry.getOrOpen(spark, dbDir.getAbsolutePath, Seq("meta"))

      first should be theSameInstanceAs second

      val error = intercept[IllegalArgumentException] {
        OpenIvmRocksDBRegistry.getOrOpen(spark, dbDir.getAbsolutePath, Seq("meta", "staging"))
      }
      error.getMessage should include("staging")
      error.getMessage should include("Registered column families")
    }

    it("closes only the databases associated with the given Spark application id") {
      val spark = newSpark("registry-app-id")
      val appId = spark.sparkContext.applicationId

      val dbOneDir = newDir("app-one-db")
      val dbTwoDir = newDir("app-two-db")

      val dbOne = OpenIvmRocksDBRegistry.getOrOpen(spark, dbOneDir.getAbsolutePath, Seq("meta"))
      val dbTwo = OpenIvmRocksDBRegistry.getOrOpen(spark, dbTwoDir.getAbsolutePath, Seq("meta"))

      OpenIvmRocksDBRegistry.overrideAppIdsForTesting(dbTwoDir.getAbsolutePath, Set("other-app"))
      OpenIvmRocksDBRegistry.closeAllForSparkContext(appId)

      an[IllegalStateException] should be thrownBy dbOne.currentVersion
      noException should be thrownBy dbTwo.currentVersion
    }

    it("closes everything idempotently") {
      val spark = newSpark("registry-close-all")
      val dbDir = newDir("close-all-db")
      val db    = OpenIvmRocksDBRegistry.getOrOpen(spark, dbDir.getAbsolutePath, Seq("meta"))

      OpenIvmRocksDBRegistry.closeAll()
      OpenIvmRocksDBRegistry.closeAll()

      an[IllegalStateException] should be thrownBy db.currentVersion
    }

    it("waits for same-path reopen while allowing other paths to open") {
      val spark    = newSpark("registry-reopen-race")
      val dbDir    = newDir("reopen-db")
      val otherDir = newDir("reopen-other-db")
      val db       = OpenIvmRocksDBRegistry.getOrOpen(spark, dbDir.getAbsolutePath, Seq("meta"))

      val closeEntered = new CountDownLatch(1)
      val releaseClose = new CountDownLatch(1)
      db.setBeforeCloseHookForTesting(() => {
        closeEntered.countDown()
        releaseClose.await(5, TimeUnit.SECONDS) shouldBe true
      })

      val closeFuture = Future {
        OpenIvmRocksDBRegistry.close(dbDir.getAbsolutePath)
      }
      closeEntered.await(5, TimeUnit.SECONDS) shouldBe true

      val reopened = new AtomicBoolean(false)
      val reopenFuture = Future {
        val reopenedDb = OpenIvmRocksDBRegistry.getOrOpen(spark, dbDir.getAbsolutePath, Seq("meta"))
        reopened.set(true)
        reopenedDb
      }

      Thread.sleep(100L)
      reopened.get() shouldBe false

      val otherDb = OpenIvmRocksDBRegistry.getOrOpen(spark, otherDir.getAbsolutePath, Seq("meta"))
      noException should be thrownBy otherDb.currentVersion

      releaseClose.countDown()
      Await.result(closeFuture, 10.seconds)
      val reopenedDb = Await.result(reopenFuture, 10.seconds)

      reopenedDb should not be theSameInstanceAs(db)
      noException should be thrownBy reopenedDb.currentVersion
    }

    it("does not shut down active users opened while closeAll is in progress") {
      val spark       = newSpark("registry-close-all-race")
      val closingDir  = newDir("close-all-race-db")
      val survivorDir = newDir("close-all-survivor-db")
      val closingDb   = OpenIvmRocksDBRegistry.getOrOpen(spark, closingDir.getAbsolutePath, Seq("meta"))

      val closeEntered = new CountDownLatch(1)
      val releaseClose = new CountDownLatch(1)
      closingDb.setBeforeCloseHookForTesting(() => {
        closeEntered.countDown()
        releaseClose.await(5, TimeUnit.SECONDS) shouldBe true
      })

      val closeAllFuture = Future {
        OpenIvmRocksDBRegistry.closeAll()
      }
      closeEntered.await(5, TimeUnit.SECONDS) shouldBe true

      val survivor = OpenIvmRocksDBRegistry.getOrOpen(spark, survivorDir.getAbsolutePath, Seq("meta"))
      survivor.withBatch { batch =>
        survivor.put(batch, "meta", RocksDBCodec.utf8("k"), RocksDBCodec.utf8("v"))
      }

      releaseClose.countDown()
      Await.result(closeAllFuture, 10.seconds)

      OpenIvmMaintenanceCoordinator.isRunning shouldBe true
      noException should be thrownBy survivor.currentVersion
    }
  }

  describe("OpenIvmStateSync") {
    it("restores a committed batch backed up before deferred transaction cleanup is flushed") {
      val stateRoot  = newDir("state-sync-local")
      val remoteRoot = newDir("state-sync-remote")
      val spark = newSpark(
        "state-sync-restore",
        Seq(
          FeatureGate.StatePathKey    -> stateRoot.getAbsolutePath,
          FeatureGate.StateSyncUriKey -> remoteRoot.toURI.toString
        )
      )
      val dbDir  = new File(stateRoot, "_openivm/mvs/restore-test/rocksdb")
      val dbPath = dbDir.getAbsolutePath
      val db     = OpenIvmRocksDBRegistry.getOrOpen(spark, dbPath, Seq("meta"))

      db.withBatch { batch =>
        db.put(batch, "meta", RocksDBCodec.utf8("delta-key"), RocksDBCodec.utf8("delta-value"))
      }
      OpenIvmStateSync.backupNow(spark)
      OpenIvmRocksDBRegistry.close(dbPath)
      deleteRecursively(new File(stateRoot, "_openivm"))
      OpenIvmStateSync.resetForTesting()

      val restored = OpenIvmRocksDBRegistry.getOrOpen(spark, dbPath, Seq("meta"))
      restored.currentVersion shouldBe 1L
      restored.get("meta", RocksDBCodec.utf8("delta-key")).map(RocksDBCodec.fromUtf8) shouldBe Some("delta-value")
      restored.prefixScan(OpenIvmRocksDB.InternalTxnColumnFamilyName, Array.emptyByteArray).toList shouldBe empty
    }

    it("coalesces in-flight requests into one follow-up pass and stays bounded while eventually becoming idle") {
      val spark = newSpark(
        "state-sync-coalesce",
        Seq(FeatureGate.StateSyncUriKey -> "abfss://state-sync@onelake/lh/_openivm")
      )
      val stateSession = newStateSyncSession(spark, "coalesce")
      val passCount    = new AtomicInteger(0)
      val entered      = Array.fill(3)(new CountDownLatch(1))
      val release      = Array.fill(3)(new CountDownLatch(1))

      OpenIvmStateSync.setStateSyncKeyHookForTesting((session, _) => session.conf.get(TestStateSyncKey))
      OpenIvmStateSync.setBackupPassHookForTesting { (_, _, _) =>
        val pass = passCount.incrementAndGet()
        if (pass <= entered.length) {
          entered(pass - 1).countDown()
          release(pass - 1).await(10, TimeUnit.SECONDS)
        }
      }

      try {
        OpenIvmStateSync.backupAsync(stateSession)
        entered(0).await(5, TimeUnit.SECONDS) shouldBe true

        OpenIvmStateSync.backupAsync(stateSession)
        release(0).countDown()

        entered(1).await(5, TimeUnit.SECONDS) shouldBe true
        passCount.get() shouldBe 2

        (1 to 64).foreach(_ => OpenIvmStateSync.backupAsync(stateSession))
        release(1).countDown()

        entered(2).await(5, TimeUnit.SECONDS) shouldBe true
        passCount.get() shouldBe 3

        release(2).countDown()
        waitForStateSyncIdle(stateSession)
        OpenIvmStateSync.allBackupStatesIdleForTesting shouldBe true
        passCount.get() shouldBe 3
      } finally {
        release.foreach(_.countDown())
        waitUntil(10.seconds) { OpenIvmStateSync.allBackupStatesIdleForTesting }
      }
    }

    it("allows different state-sync keys to overlap and eventually returns both to idle") {
      val spark = newSpark(
        "state-sync-overlap",
        Seq(FeatureGate.StateSyncUriKey -> "abfss://state-sync@onelake/lh/_openivm")
      )
      val leftSession  = newStateSyncSession(spark, "overlap-left")
      val rightSession = newStateSyncSession(spark, "overlap-right")
      val leftEntered  = new CountDownLatch(1)
      val rightEntered = new CountDownLatch(1)
      val releaseLeft  = new CountDownLatch(1)
      val releaseRight = new CountDownLatch(1)
      val active       = new AtomicInteger(0)
      val maxActive    = new AtomicInteger(0)

      OpenIvmStateSync.setStateSyncKeyHookForTesting((session, _) => session.conf.get(TestStateSyncKey))
      OpenIvmStateSync.setBackupPassHookForTesting { (key, _, _) =>
        val current = active.incrementAndGet()
        maxActive.updateAndGet(existing => math.max(existing, current))
        try {
          key match {
            case "overlap-left" =>
              leftEntered.countDown()
              releaseLeft.await(10, TimeUnit.SECONDS)
            case "overlap-right" =>
              rightEntered.countDown()
              releaseRight.await(10, TimeUnit.SECONDS)
            case _ => ()
          }
        } finally {
          active.decrementAndGet()
        }
      }

      try {
        OpenIvmStateSync.backupAsync(leftSession)
        OpenIvmStateSync.backupAsync(rightSession)

        leftEntered.await(5, TimeUnit.SECONDS) shouldBe true
        rightEntered.await(5, TimeUnit.SECONDS) shouldBe true
        maxActive.get() shouldBe 2

        releaseLeft.countDown()
        releaseRight.countDown()

        waitForStateSyncIdle(leftSession)
        waitForStateSyncIdle(rightSession)
        OpenIvmStateSync.allBackupStatesIdleForTesting shouldBe true
      } finally {
        releaseLeft.countDown()
        releaseRight.countDown()
        waitUntil(10.seconds) { OpenIvmStateSync.allBackupStatesIdleForTesting }
      }
    }
  }
}
