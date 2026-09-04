package org.openivm.spark.common.rocksdb

import org.openivm.spark.common.{FeatureGate, MvCatalog, MvMetadata, OpenIvmStatePaths}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.sql.Timestamp
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import java.util.UUID

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class OpenIvmRocksDBRegistrySpec extends AnyFunSpec with BeforeAndAfterEach with Matchers {
  private val TestStateSyncKey                = "spark.openivm.test.stateSync.key"
  private val RegistryRaceTimeout             = 30.seconds
  private val SourceSharingAdmissionWidth     = 18
  private val SourceSharingSource             = "source_sharing_base"
  private val NoLiveOrReachableRocksDbHandles = OpenIvmRocksDBRegistry.HandleSnapshot(0, 0, 0)

  private val sparks = ArrayBuffer.empty[SparkSession]
  private val dirs   = ArrayBuffer.empty[File]

  override def afterEach(): Unit = {
    OpenIvmRocksDBRegistry.clearAfterSlotSelectionHookForTesting()
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

  private def awaitLatch(latch: CountDownLatch, description: String): Unit =
    withClue(s"$description within $RegistryRaceTimeout: ") {
      latch.await(RegistryRaceTimeout.toMillis, TimeUnit.MILLISECONDS) shouldBe true
    }

  private def withPool[A](parallelism: Int)(body: ExecutionContext => A): A = {
    val pool = Executors.newFixedThreadPool(parallelism)
    try body(ExecutionContext.fromExecutorService(pool))
    finally {
      pool.shutdownNow()
      pool.awaitTermination(RegistryRaceTimeout.toMillis, TimeUnit.MILLISECONDS)
      ()
    }
  }

  private def withParkedSlotSelections[A](dbPaths: Set[String], expectedSelections: Int = -1)(
      body: (CountDownLatch, CountDownLatch) => A
  ): A = {
    val selected = new CountDownLatch(if (expectedSelections < 0) dbPaths.size else expectedSelections)
    val release  = new CountDownLatch(1)
    OpenIvmRocksDBRegistry.setAfterSlotSelectionHookForTesting { selectedPath =>
      if (dbPaths.contains(selectedPath)) {
        selected.countDown()
        if (!release.await(RegistryRaceTimeout.toMillis, TimeUnit.MILLISECONDS)) {
          throw new AssertionError(s"slot selection was not released for $selectedPath")
        }
      }
    }
    try body(selected, release)
    finally {
      release.countDown()
      OpenIvmRocksDBRegistry.clearAfterSlotSelectionHookForTesting()
    }
  }

  private def sourceSharingMeta(index: Int, generation: String): MvMetadata =
    MvMetadata(
      name = TableIdentifier(s"source_sharing_mv_$index", Some("registry_source_sharing")),
      querySql = s"SELECT id FROM $SourceSharingSource WHERE generation = '$generation'",
      refreshType = 0,
      refreshTypeName = "SIMPLE_PROJECTION",
      lastVersion = 0L,
      sourceTables = Seq(SourceSharingSource),
      sourceSchemaFingerprint = MvCatalog.schemaFingerprint(
        Map(SourceSharingSource -> StructType(Seq(StructField("id", StringType))))
      ),
      location = s"source-sharing-$generation-$index",
      createdAt = new Timestamp(1700000000000L),
      properties = Map("generation" -> generation)
    )

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

    it("keeps one canonical slot when a getOrOpen races a concurrent close+prune") {
      val spark = newSpark("registry-orphan-race")
      val dbDir = newDir("orphan-race-db")
      val db1   = OpenIvmRocksDBRegistry.getOrOpen(spark, dbDir.getAbsolutePath, Seq("meta"))

      val closeEntered = new CountDownLatch(1)
      val releaseClose = new CountDownLatch(1)
      db1.setBeforeCloseHookForTesting(() => {
        closeEntered.countDown()
        releaseClose.await(5, TimeUnit.SECONDS) shouldBe true
      })

      // Close pauses mid-flight while a concurrent getOrOpen fetches the slot
      // that is about to be pruned and blocks on the slot lock.
      val closeFuture = Future(OpenIvmRocksDBRegistry.close(dbDir.getAbsolutePath))
      closeEntered.await(5, TimeUnit.SECONDS) shouldBe true
      val reopenFuture =
        Future(OpenIvmRocksDBRegistry.getOrOpen(spark, dbDir.getAbsolutePath, Seq("meta")))
      Thread.sleep(100L)

      releaseClose.countDown()
      Await.result(closeFuture, 10.seconds)
      val reopenedDb = Await.result(reopenFuture, 10.seconds)

      // A later getOrOpen must return the SAME instance as the raced reopen:
      // the retired slot was never re-populated into an orphan holding the
      // native LOCK (which a fresh slot would then fail to acquire with
      // "lock hold by current process").
      val afterDb = OpenIvmRocksDBRegistry.getOrOpen(spark, dbDir.getAbsolutePath, Seq("meta"))
      afterDb should be theSameInstanceAs reopenedDb
      reopenedDb should not be theSameInstanceAs(db1)
      noException should be thrownBy afterDb.currentVersion
    }

    it("never orphans a per-path RocksDB lock under concurrent getOrOpen + close") {
      // Reproduces the DROP-cleanup race: many threads open + write + close the
      // same small set of paths (as concurrent DROP MATERIALIZED VIEW / DROP
      // TABLE cleanups do). Before the retire-on-prune fix a pruned slot could
      // be re-populated into an orphan, and the next open of that path failed
      // with `RocksDBException: lock hold by current process`.
      //
      // `close(path)` invalidates the shared registry handle for ALL current
      // holders. A concurrent close racing another thread's getOrOpen+write
      // therefore produces IllegalStateException("already closed") — a
      // test-induced shared-handle use-after-close that is tolerated here as
      // distinct from an orphaned-lock failure. RocksDBException "lock hold by
      // current process" / "No locks available" and all other exceptions are
      // real failures and are surfaced unconditionally.
      //
      // After the stress: closeAll() drains remaining handles, the registry must
      // quiesce to HandleSnapshot(0,0,0), and a fresh getOrOpen of every path
      // must succeed — positively proving no native LOCK file is orphaned.
      val spark      = newSpark("registry-concurrent-cleanup")
      val paths      = (0 until 4).map(i => newDir(s"concurrent-$i").getAbsolutePath)
      val errors     = new java.util.concurrent.ConcurrentLinkedQueue[Throwable]()
      val iterations = 300
      val workers    = 8
      val start      = new CountDownLatch(1)

      val futures = (0 until workers).map { w =>
        Future {
          start.await(5, TimeUnit.SECONDS)
          var i = 0
          while (i < iterations) {
            val p = paths((i + w) % paths.size)
            try {
              val db = OpenIvmRocksDBRegistry.getOrOpen(spark, p, Seq("meta"))
              db.withBatch(batch => db.put(batch, "meta", RocksDBCodec.utf8(s"k$w"), RocksDBCodec.utf8("v")))
              if ((i + w) % 3 == 0) OpenIvmRocksDBRegistry.close(p)
            } catch {
              case e: IllegalStateException if Option(e.getMessage).exists(_.contains("already closed")) =>
                // A concurrent close invalidated the shared handle between
                // getOrOpen and withBatch: tolerated, not an orphaned-lock signal.
                ()
              case t: Throwable =>
                errors.add(t)
            }
            i += 1
          }
        }
      }

      start.countDown()
      futures.foreach(f => Await.result(f, 60.seconds))

      val errList = errors.asScala.toList
      withClue(errList.map(e => s"${e.getClass.getName}: ${e.getMessage}").mkString("\n")) {
        errList shouldBe empty
      }

      // Drain any live handles; registry must reach full quiescence.
      OpenIvmRocksDBRegistry.closeAll()
      OpenIvmRocksDBRegistry.handleSnapshotForTesting shouldBe NoLiveOrReachableRocksDbHandles

      // Positive orphan-lock probe: every path must reopen cleanly. An orphaned
      // native LOCK would cause RocksDBException("lock hold by current process")
      // or RocksDBException("No locks available") on the native open below.
      paths.foreach { p =>
        val db = OpenIvmRocksDBRegistry.getOrOpen(spark, p, Seq("meta"))
        noException should be thrownBy db.currentVersion
      }
    }

    it("does not orphan a live handle when a slot is retired between selection and open") {
      val spark = newSpark("registry-toctou-orphan")
      val dbDir = newDir("toctou-orphan-db")
      val path  = dbDir.getAbsolutePath
      val canon = new File(path).getCanonicalPath

      // Force the exact defect interleaving: an opener installs a fresh (empty)
      // slot via slotFor and parks BEFORE acquiring the slot lock; concurrently
      // close() retires and evicts that very slot; then the opener is released.
      val parked      = new CountDownLatch(1)
      val releaseOpen = new CountDownLatch(1)
      val armed       = new AtomicBoolean(true)
      OpenIvmRocksDBRegistry.setAfterSlotSelectionHookForTesting { hookedPath =>
        if (hookedPath == canon && armed.compareAndSet(true, false)) {
          parked.countDown()
          releaseOpen.await(10, TimeUnit.SECONDS)
          ()
        }
      }

      val openFuture = Future {
        OpenIvmRocksDBRegistry.getOrOpen(spark, path, Seq("meta"))
      }

      // The opener has published an empty slot and parked before opening it.
      parked.await(10, TimeUnit.SECONDS) shouldBe true
      OpenIvmRocksDBRegistry.liveHandleForTesting(path) shouldBe None

      // Concurrent close retires and evicts the empty slot the opener is holding.
      OpenIvmRocksDBRegistry.close(path)
      OpenIvmRocksDBRegistry.mappedSlotCountForTesting shouldBe 0

      // Release the opener: it must detect the slot was retired and retry into a
      // fresh, reachable slot instead of stranding a native handle.
      releaseOpen.countDown()
      val opened = Await.result(openFuture, 10.seconds)

      // The opened handle is live, reachable and unique — no orphan holding LOCK.
      opened.isClosed shouldBe false
      OpenIvmRocksDBRegistry.liveHandleForTesting(path) shouldBe Some(opened)
      OpenIvmRocksDBRegistry.mappedSlotCountForTesting shouldBe 1
      OpenIvmRocksDBRegistry.openEntryCountForTesting shouldBe 1

      // Same-process reuse hits the reachable slot (no second native open, so no
      // "lock hold by current process"): it is the very same instance.
      OpenIvmRocksDBRegistry.getOrOpen(spark, path, Seq("meta")) should be theSameInstanceAs opened

      // Closing releases the LOCK; a subsequent fresh native open of the SAME
      // path then succeeds, proving nothing still holds <path>/LOCK.
      OpenIvmRocksDBRegistry.close(path)
      OpenIvmRocksDBRegistry.mappedSlotCountForTesting shouldBe 0
      OpenIvmRocksDBRegistry.openEntryCountForTesting shouldBe 0
      opened.isClosed shouldBe true

      val reopened = OpenIvmRocksDBRegistry.getOrOpen(spark, path, Seq("meta"))
      reopened should not be theSameInstanceAs(opened)
      noException should be thrownBy reopened.currentVersion
    }

    it("holds per-path exclusion across close and a caller-supplied delete") {
      val spark = newSpark("registry-close-and-delete")
      val dbDir = newDir("close-and-delete-db")
      val path  = dbDir.getAbsolutePath

      val original = OpenIvmRocksDBRegistry.getOrOpen(spark, path, Seq("meta"))
      original.withBatch { batch =>
        original.put(batch, "meta", RocksDBCodec.utf8("k"), RocksDBCodec.utf8("v"))
      }

      val deleteEntered         = new CountDownLatch(1)
      val releaseDelete         = new CountDownLatch(1)
      val deleteRan             = new AtomicBoolean(false)
      val closedDuringDelete    = new AtomicBoolean(false)
      val reachableDuringDelete = new AtomicBoolean(true)

      val deleteFuture = Future {
        OpenIvmRocksDBRegistry.closeAndDelete(path) { canonical =>
          deleteRan.set(true)
          // The handle is closed (LOCK released) and unreachable before delete runs.
          closedDuringDelete.set(original.isClosed)
          reachableDuringDelete.set(OpenIvmRocksDBRegistry.liveHandleForTesting(path).isDefined)
          deleteEntered.countDown()
          releaseDelete.await(10, TimeUnit.SECONDS)
          deleteRecursively(new File(canonical))
        }
      }

      deleteEntered.await(10, TimeUnit.SECONDS) shouldBe true

      // Exclusion: a concurrent open must NOT proceed while the delete is in-flight.
      val reopenObserved = new AtomicBoolean(false)
      val reopenFuture = Future {
        val db = OpenIvmRocksDBRegistry.getOrOpen(spark, path, Seq("meta"))
        reopenObserved.set(true)
        db
      }
      Thread.sleep(200L)
      reopenObserved.get() shouldBe false

      releaseDelete.countDown()
      Await.result(deleteFuture, 10.seconds)
      val reopened = Await.result(reopenFuture, 10.seconds)

      deleteRan.get() shouldBe true
      closedDuringDelete.get() shouldBe true
      reachableDuringDelete.get() shouldBe false
      original.isClosed shouldBe true

      // The reopen produced a distinct, working handle over a freshly recreated DB.
      reopened should not be theSameInstanceAs(original)
      reopened.currentVersion shouldBe 0L
      OpenIvmRocksDBRegistry.liveHandleForTesting(path) shouldBe Some(reopened)
    }

    it("surfaces a caller delete failure while keeping the handle closed and the path retryable") {
      val spark = newSpark("registry-close-and-delete-failure")
      val dbDir = newDir("close-and-delete-failure-db")
      val path  = dbDir.getAbsolutePath

      val original = OpenIvmRocksDBRegistry.getOrOpen(spark, path, Seq("meta"))
      original.withBatch { batch =>
        original.put(batch, "meta", RocksDBCodec.utf8("k"), RocksDBCodec.utf8("v"))
      }

      // The DB directory is still present when the caller-supplied delete fails.
      val boom = new RuntimeException("delete failed while DB present")
      val thrown = intercept[RuntimeException] {
        OpenIvmRocksDBRegistry.closeAndDelete(path)(_ => throw boom)
      }
      thrown should be theSameInstanceAs boom

      // No silent partial success: the handle is closed (LOCK released) and the
      // dead slot is evicted, so nothing is orphaned even though delete failed.
      original.isClosed shouldBe true
      OpenIvmRocksDBRegistry.liveHandleForTesting(path) shouldBe None
      OpenIvmRocksDBRegistry.handleSnapshotForTesting shouldBe NoLiveOrReachableRocksDbHandles
      OpenIvmStatePaths.isExistingDb(new File(path).getCanonicalPath) shouldBe true

      // The path stays retryable: a fresh getOrOpen reopens the still-present DB
      // as a new, working handle (no "lock hold by current process").
      val reopened = OpenIvmRocksDBRegistry.getOrOpen(spark, path, Seq("meta"))
      reopened should not be theSameInstanceAs(original)
      noException should be thrownBy reopened.currentVersion
    }

    it("keeps the first open reachable when DROP cleanup retires its selected empty slot") {
      val spark  = newSpark("registry-drop-open-race")
      val dbPath = newDir("drop-open-race-db").getCanonicalPath

      withParkedSlotSelections(Set(dbPath)) { (selected, release) =>
        val opening = Future {
          OpenIvmRocksDBRegistry.getOrOpen(spark, dbPath, Seq("meta"))
        }
        awaitLatch(selected, "opener never selected its registry slot")

        OpenIvmRocksDBRegistry.close(dbPath)
        release.countDown()

        val opened = Await.result(opening, RegistryRaceTimeout)
        val retry  = OpenIvmRocksDBRegistry.getOrOpen(spark, dbPath, Seq("meta"))
        retry should be theSameInstanceAs opened

        OpenIvmRocksDBRegistry.close(dbPath)
        OpenIvmRocksDBRegistry.handleSnapshotForTesting shouldBe NoLiveOrReachableRocksDbHandles
      }
    }

    it("retries in the same process after closeAll races the first open and quiesces cleanly") {
      val spark  = newSpark("registry-close-all-open-race")
      val dbPath = newDir("close-all-open-race-db").getCanonicalPath

      withParkedSlotSelections(Set(dbPath)) { (selected, release) =>
        val opening = Future {
          OpenIvmRocksDBRegistry.getOrOpen(spark, dbPath, Seq("meta"))
        }
        awaitLatch(selected, "opener never selected its registry slot")

        OpenIvmRocksDBRegistry.closeAll()
        release.countDown()

        val opened = Await.result(opening, RegistryRaceTimeout)
        val retry  = OpenIvmRocksDBRegistry.getOrOpen(spark, dbPath, Seq("meta"))
        retry should be theSameInstanceAs opened

        OpenIvmRocksDBRegistry.closeAll()
        OpenIvmRocksDBRegistry.handleSnapshotForTesting shouldBe NoLiveOrReachableRocksDbHandles
      }
    }

    it("keeps 18 source-sharing DROP and CREATE replacements lock-free and consistent") {
      val spark          = newSpark("registry-source-sharing-fanout")
      val originalMvs    = (1 to SourceSharingAdmissionWidth).map(sourceSharingMeta(_, "before"))
      val replacementMvs = (1 to SourceSharingAdmissionWidth).map(sourceSharingMeta(_, "after"))
      MvCatalog.ensureTables(spark)
      originalMvs.foreach(MvCatalog.upsert(spark, _))
      val sharedSourceDbPath = new File(
        OpenIvmStatePaths.sourceDependencyDbPath(spark, SourceSharingSource)
      ).getCanonicalPath
      OpenIvmRocksDBRegistry.closeAll()

      withParkedSlotSelections(Set(sharedSourceDbPath), SourceSharingAdmissionWidth) { (selected, release) =>
        withPool(SourceSharingAdmissionWidth) { implicit ec =>
          val drops = originalMvs.map { meta =>
            Future {
              MvCatalog.remove(spark, meta.name)
            }(ec)
          }
          awaitLatch(selected, s"not all $SourceSharingAdmissionWidth DROP cleanups selected the shared source slot")

          OpenIvmRocksDBRegistry.close(sharedSourceDbPath)
          release.countDown()
          drops.foreach(drop => Await.result(drop, RegistryRaceTimeout))
          OpenIvmRocksDBRegistry.clearAfterSlotSelectionHookForTesting()

          val creates = replacementMvs.map { meta =>
            Future {
              MvCatalog.upsert(spark, meta)
            }(ec)
          }
          creates.foreach(create => Await.result(create, RegistryRaceTimeout))
        }
      }

      MvCatalog.viewsForSource(spark, SourceSharingSource).map(_.name).toSet shouldBe replacementMvs.map(_.name).toSet
      MvCatalog.list(spark).map(_.properties("generation")).toSet shouldBe Set("after")
      OpenIvmRocksDBRegistry.closeAll()
      OpenIvmRocksDBRegistry.handleSnapshotForTesting shouldBe NoLiveOrReachableRocksDbHandles
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
