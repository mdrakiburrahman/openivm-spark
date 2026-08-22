package org.openivm.spark.common.rocksdb

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.UUID

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class OpenIvmRocksDBRegistrySpec extends AnyFunSpec with BeforeAndAfterEach with Matchers {

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

  private def newSpark(appName: String): SparkSession = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()

    val warehouse = newDir(s"$appName-warehouse")
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName(appName)
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "localhost")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.warehouse.dir", warehouse.getAbsolutePath)
      .getOrCreate()

    sparks += spark
    spark
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
}
