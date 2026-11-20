package org.openivm.spark.common.rocksdb

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.{
  FeatureGate,
  OpenIvmGarbageCollector,
  OpenIvmStatePaths,
  RefreshProfileCatalog,
  RefreshProfileRow
}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.lang.management.ManagementFactory
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.security.MessageDigest
import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.{CountDownLatch, CopyOnWriteArrayList, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.io.Source

object OpenIvmStateSyncFreshRestoreProbe {
  def main(args: Array[String]): Unit = {
    val warehouse = new File(args(0))
    val uri       = args(1)
    val deleted   = args(2)
    val retained  = args(3)
    val refreshId = args(4)
    val evidence  = new File(args(5))
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-checkpoint-canary-fresh-restore")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "localhost")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.warehouse.dir", warehouse.getAbsolutePath)
      .config(FeatureGate.StateSyncUriKey, uri)
      .getOrCreate()
    try {
      OpenIvmStateSync.maybeRestore(spark)
      val root = OpenIvmStatePaths.openIvmRoot(spark)
      require(!Files.exists(root.resolve(deleted)), s"deleted trigger was restored: $deleted")
      require(Files.exists(root.resolve(retained)), s"unrelated trigger was lost: $retained")
      val rows = RefreshProfileCatalog.scanAll(spark)
      require(rows.exists(_.refreshId == refreshId), s"profile-only row $refreshId was not restored")
      Files.createDirectories(evidence.toPath.getParent)
      Files.write(
        evidence.toPath,
        s"{\"fresh_jvm\":true,\"deleted_absent\":true,\"retained_present\":true,\"profile_row\":\"$refreshId\"}"
          .getBytes(
            StandardCharsets.UTF_8
          )
      )
    } finally {
      OpenIvmRocksDBRegistry.closeAll()
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }
}

class OpenIvmStateSyncCheckpointCanarySpec extends AnyFunSpec with BeforeAndAfterEach with Matchers {
  private val dirs     = scala.collection.mutable.ArrayBuffer.empty[File]
  private val sparks   = scala.collection.mutable.ArrayBuffer.empty[SparkSession]
  private val receipts = new CopyOnWriteArrayList[OpenIvmStateSync.StateSyncRequestReceipt]()

  override def beforeEach(): Unit = {
    OpenIvmStateSync.resetForTesting()
    OpenIvmGarbageCollector.setAfterSweepDeleteHookForTesting(_ => ())
    receipts.clear()
    OpenIvmStateSync.setStateSyncRequestReceiptHookForTesting(receipt => { receipts.add(receipt); () })
    super.beforeEach()
  }

  override def afterEach(): Unit = {
    OpenIvmGarbageCollector.setAfterSweepDeleteHookForTesting(_ => ())
    OpenIvmRocksDBRegistry.closeAll()
    sparks.reverse.foreach { spark =>
      try spark.stop()
      catch { case _: Throwable => () }
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
    val dir = new File(s"target/test-checkpoint-canary-$prefix-${UUID.randomUUID().toString.take(8)}")
    dir.mkdirs()
    dirs += dir
    dir
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
    file.delete()
    ()
  }

  private def newSpark(appName: String, uri: File, extra: Seq[(String, String)] = Seq.empty): SparkSession = {
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
      .config(FeatureGate.StateSyncUriKey, uri.toURI.toString)
      .config(FeatureGate.StateSyncCheckpointEnabledKey, "true")
      .config(FeatureGate.StateSyncAsyncEnabledKey, "false")
      .config(FeatureGate.GcOrphanGracePeriodKey, "0ms")
    extra.foreach { case (key, value) => builder.config(key, value) }
    val spark = builder.getOrCreate()
    sparks += spark
    spark
  }

  private def stopSpark(spark: SparkSession): Unit = {
    OpenIvmRocksDBRegistry.closeAll()
    spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    sparks -= spark
  }

  private def recordProfile(spark: SparkSession, refreshId: String): Unit = {
    RefreshProfileCatalog.ensureTables(spark)
    RefreshProfileCatalog.record(
      spark,
      Seq(
        RefreshProfileRow(
          refreshId = refreshId,
          viewName = "canary.profile_only",
          profileTimestamp = Timestamp.valueOf("2025-01-01 00:00:00"),
          stepOrder = 0,
          stepName = "profile_only_fixture",
          durationMs = 7L,
          detail = s"operation_id=$refreshId"
        )
      )
    )
  }

  private def setRequestId(spark: SparkSession, requestId: String): Unit =
    spark.sparkContext.setLocalProperty("openivm.request_id", requestId)

  private def commitDetails(spark: SparkSession, uri: String): (String, String, Long, String) = {
    val remoteRoot = new Path(uri)
    val fs         = remoteRoot.getFileSystem(spark.sessionState.newHadoopConf())
    val commits    = new Path(new Path(remoteRoot, ".openivm-state-sync"), "commits")
    val latest = fs
      .listStatus(commits)
      .filter(status => status.isFile && status.getPath.getName.endsWith(".commit"))
      .sortBy(_.getPath.getName)
      .last
    val input = fs.open(latest.getPath)
    val text =
      try Source.fromInputStream(input, "UTF-8").mkString
      finally input.close()
    val lines = text.split("\\n", -1)
    lines.length shouldBe 5
    val generation = lines(1)
    val archive =
      new Path(new Path(new Path(new Path(remoteRoot, ".openivm-state-sync"), "snapshots"), generation), "snapshot.zip")
    (generation, lines(3), lines(2).toLong, sha256(fs, archive))
  }

  private def sha256(fs: org.apache.hadoop.fs.FileSystem, path: Path): String = {
    val input  = fs.open(path)
    val digest = MessageDigest.getInstance("SHA-256")
    try {
      val bytes = new Array[Byte](8192)
      var count = input.read(bytes)
      while (count >= 0) {
        if (count > 0) digest.update(bytes, 0, count)
        count = input.read(bytes)
      }
    } finally input.close()
    digest.digest().map(byte => f"${byte & 0xff}%02x").mkString
  }

  private def repoRoot: File = {
    var current = new File(".").getCanonicalFile
    while (current.getParentFile != null && !new File(current, ".git").exists()) current = current.getParentFile
    current
  }

  private def writeReceiptEvidence(
      fixture: String,
      expectedIds: Seq[String],
      latest: (String, String, Long, String),
      extra: String
  ): Unit = {
    val receiptJson = receipts.asScala
      .map { receipt =>
        s"{\"request_id\":\"${receipt.requestId}\",\"requested_generation\":${receipt.requestedMutationGeneration}," +
          s"\"captured_generation\":${receipt.capturedMutationGeneration.map(_.toString).getOrElse("null")}," +
          s"\"publication_generation\":\"${receipt.publicationGeneration.getOrElse("")}\"," +
          s"\"published\":${receipt.published},\"reused\":${receipt.reused}," +
          s"\"archive_sha256\":\"${receipt.archiveSha256.getOrElse("")}\"," +
          s"\"archive_bytes\":${receipt.archiveBytes.getOrElse(0L)}," +
          s"\"coverage_proven\":${receipt.coverageProven},\"acknowledged\":${receipt.acknowledged}," +
          s"\"outcome\":\"${receipt.outcome}\"}"
      }
      .mkString("[", ",", "]")
    val output = new File(repoRoot, s".logs/checkpoint-only-canary/$fixture.json")
    Files.createDirectories(output.toPath.getParent)
    val json =
      s"{\"fixture\":\"$fixture\",\"request_ids\":[${expectedIds.map(id => s"\"$id\"").mkString(",")}]" +
        s",\"latest_generation\":\"${latest._1}\",\"manifest_sha256\":\"${latest._2}\"," +
        s"\"archive_bytes\":${latest._3},\"archive_sha256\":\"${latest._4}\",\"receipts\":$receiptJson,$extra}"
    Files.write(output.toPath, json.getBytes(StandardCharsets.UTF_8))
  }

  private def runFreshRestoreProbe(
      uri: String,
      deletedRelative: String,
      retainedRelative: String,
      refreshId: String,
      evidenceFile: File
  ): Unit = {
    val javaHome  = new File(System.getProperty("java.home"), "bin/java").getAbsolutePath
    val classpath = System.getProperty("java.class.path")
    val jvmArgs = ManagementFactory.getRuntimeMXBean.getInputArguments.asScala
      .filter(arg => arg.startsWith("--add-opens=") || arg.startsWith("--add-exports="))
    val childWarehouse = newDir("fresh-child-warehouse")
    val logFile        = new File(childWarehouse, "fresh-restore.log")
    val process = new ProcessBuilder(
      (Seq(javaHome) ++ jvmArgs ++ Seq(
        "-cp",
        classpath,
        "org.openivm.spark.common.rocksdb.OpenIvmStateSyncFreshRestoreProbe",
        childWarehouse.getAbsolutePath,
        uri,
        deletedRelative,
        retainedRelative,
        refreshId,
        evidenceFile.getAbsolutePath
      )): _*
    )
      .redirectErrorStream(true)
      .redirectOutput(ProcessBuilder.Redirect.appendTo(logFile))
      .start()
    process.waitFor(180, TimeUnit.SECONDS) shouldBe true
    if (process.exitValue() != 0) {
      val input = Source.fromFile(logFile)
      val output =
        try input.mkString
        finally input.close()
      fail(s"Fresh restore JVM exited ${process.exitValue()}:\n$output")
    }
    evidenceFile.isFile shouldBe true
  }

  describe("synchronous checkpoint canary") {
    it("publishes a post-cleanup archive after partial trigger deletion and restores it in a fresh JVM") {
      val fixtureRoot = newDir("partial-sweep")
      val remote      = new File(fixtureRoot, "remote")
      remote.mkdirs()
      val spark     = newSpark("checkpoint-partial-sweep", remote)
      val warehouse = OpenIvmStatePaths.warehouseRoot(spark).toFile
      val refreshId = s"profile-only-${UUID.randomUUID()}"
      recordProfile(spark, refreshId)

      val triggerRoot     = new File(warehouse, "_openivm/triggers")
      val firstTrigger    = new File(triggerRoot, "db-a/operation-a/marker")
      val secondTrigger   = new File(triggerRoot, "db-b/operation-b/marker")
      val currentVersions = org.openivm.spark.common.MvCatalog.list(spark).map(_.lastVersion)
      currentVersions shouldBe empty
      firstTrigger.getParentFile.mkdirs()
      secondTrigger.getParentFile.mkdirs()
      Files.write(firstTrigger.toPath, Array[Byte](1, 2, 3))
      Files.write(secondTrigger.toPath, Array[Byte](4, 5, 6))
      firstTrigger.setLastModified(1L)
      firstTrigger.getParentFile.setLastModified(1L)
      secondTrigger.setLastModified(1L)
      secondTrigger.getParentFile.setLastModified(1L)

      val cleanupMarker = new File(triggerRoot, "command-cleanup/operation/marker")
      cleanupMarker.getParentFile.mkdirs()
      Files.write(cleanupMarker.toPath, Array[Byte](9))
      cleanupMarker.setLastModified(System.currentTimeMillis() + 60000L)
      var cleanupCallbackRan = false
      OpenIvmStateSync.setBeforeSnapshotCommitHookForTesting(() =>
        throw new java.io.IOException("injected commit failure")
      )
      setRequestId(spark, "failed-before-cleanup")
      intercept[java.io.IOException] {
        OpenIvmStateSync.backupForCommand(
          spark,
          () => {
            cleanupCallbackRan = true
            Files.deleteIfExists(cleanupMarker.toPath)
            ()
          }
        )
      }
      cleanupCallbackRan shouldBe false
      cleanupMarker.isFile shouldBe true
      OpenIvmStateSync.setBeforeSnapshotCommitHookForTesting(null)

      setRequestId(spark, "before-partial-cleanup")
      OpenIvmStateSync.backupNow(spark)
      val initial = receipts.asScala.find(_.requestId == "before-partial-cleanup").get
      initial.published shouldBe true
      initial.acknowledged shouldBe true
      initial.reused shouldBe false

      val deletedPath = new java.util.concurrent.atomic.AtomicReference[String]()
      OpenIvmGarbageCollector.setAfterSweepDeleteHookForTesting { path =>
        if (deletedPath.compareAndSet(null, path.toString)) throw new java.io.IOException("injected after delete")
      }
      OpenIvmGarbageCollector.sweepNow(
        spark,
        OpenIvmGarbageCollector.Plan(Set.empty, Set.empty),
        System.currentTimeMillis() + 1L
      )
      deletedPath.get() should not be null
      val deletedDirectory = new Path(deletedPath.get()).toUri.getPath
      val deletedFile      = new File(deletedDirectory, "marker")
      deletedFile.isFile shouldBe false
      val remaining = Seq(firstTrigger, secondTrigger).find(file =>
        file.getParentFile.getAbsolutePath != new File(deletedDirectory).getAbsolutePath
      )
      remaining.isDefined shouldBe true
      remaining.get.isFile shouldBe true

      OpenIvmGarbageCollector.setAfterSweepDeleteHookForTesting(_ => ())
      setRequestId(spark, "after-partial-cleanup")
      OpenIvmStateSync.backupNow(spark)
      val afterCleanup = receipts.asScala.find(_.requestId == "after-partial-cleanup").get
      afterCleanup.published shouldBe true
      afterCleanup.acknowledged shouldBe true
      afterCleanup.reused shouldBe false
      afterCleanup.publicationGeneration should not be initial.publicationGeneration
      afterCleanup.coverageProven shouldBe false

      val latest = commitDetails(spark, remote.toURI.toString)
      latest._1 shouldBe afterCleanup.publicationGeneration.get
      latest._2 shouldBe latest._4
      latest._3 should be > 0L
      afterCleanup.archiveSha256 shouldBe Some(latest._2)
      afterCleanup.archiveBytes shouldBe Some(latest._3)
      val childEvidence = new File(repoRoot, ".logs/checkpoint-only-canary/partial-trigger-cleanup-fresh-jvm.json")
      val deletedRelative =
        new File(warehouse, "_openivm/triggers").toPath.relativize(new File(deletedDirectory).toPath)
      val retainedRelative =
        new File(warehouse, "_openivm/triggers").toPath.relativize(remaining.get.getParentFile.toPath)
      val deletedRel  = s"triggers/${deletedRelative.toString}/marker"
      val retainedRel = s"triggers/${retainedRelative.toString}/marker"
      org.openivm.spark.common.MvCatalog.list(spark).map(_.lastVersion) shouldBe currentVersions
      stopSpark(spark)
      runFreshRestoreProbe(remote.toURI.toString, deletedRel, retainedRel, refreshId, childEvidence)

      writeReceiptEvidence(
        "partial-trigger-cleanup-spark-4.1",
        Seq("before-partial-cleanup", "after-partial-cleanup"),
        latest,
        s"\"deleted_trigger_absent_in_fresh_jvm\":true,\"unrelated_profile_row_preserved\":true," +
          s"\"mv_versions_unchanged\":true,\"partial_cleanup_failure_injected\":true," +
          s"\"precommit_cleanup_callback_suppressed\":${!cleanupCallbackRan}," +
          s"\"fresh_jvm_evidence\":\"${childEvidence.getAbsolutePath}\""
      )
    }

    it("defers a profile-only mutation after capture until its exact follow-up archive commits") {
      implicit val executionContext: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
      val fixtureRoot                                                  = newDir("late-profile")
      val remote                                                       = new File(fixtureRoot, "remote")
      val stateRoot                                                    = new File(fixtureRoot, "state")
      remote.mkdirs()
      stateRoot.mkdirs()
      val spark = newSpark(
        "checkpoint-late-profile",
        remote,
        Seq(FeatureGate.StatePathKey -> stateRoot.getAbsolutePath)
      )
      OpenIvmStateBarrier.mutationCoverageEnabled(spark) shouldBe true
      val ivmRoot  = new File(stateRoot, "_openivm")
      val retained = new File(ivmRoot, "triggers/late-profile/retained/marker")
      retained.getParentFile.mkdirs()
      Files.write(retained.toPath, Array[Byte](7, 8, 9))
      val versionsBefore = org.openivm.spark.common.MvCatalog.list(spark).map(_.lastVersion)
      versionsBefore shouldBe empty
      recordProfile(spark, s"profile-before-${UUID.randomUUID()}")

      def request(requestId: String): Future[Unit] = Future {
        setRequestId(spark, requestId)
        try OpenIvmStateSync.backupNow(spark)
        finally setRequestId(spark, null)
      }

      Await.result(request("late-profile-baseline"), 60.seconds)
      val baseline = receipts.asScala.find(_.requestId == "late-profile-baseline").get
      baseline.acknowledged shouldBe true
      baseline.archiveSha256 should not be empty

      recordProfile(spark, s"profile-before-capture-${UUID.randomUUID()}")
      val captureStarted       = new CountDownLatch(1)
      val releaseFirstPack     = new CountDownLatch(1)
      val secondCaptureStarted = new CountDownLatch(1)
      val releaseSecondCapture = new CountDownLatch(1)
      val holdPack             = new AtomicBoolean(false)
      val captureNumber        = new AtomicInteger(0)
      OpenIvmStateSync.setBeforeLocalFileCopyHookForTesting { _ =>
        if (holdPack.compareAndSet(false, true)) {
          captureStarted.countDown()
          releaseFirstPack.await(30, TimeUnit.SECONDS) shouldBe true
        }
      }
      OpenIvmStateSync.setBeforeCaptureHookForTesting { () =>
        if (captureNumber.incrementAndGet() == 2) {
          secondCaptureStarted.countDown()
          releaseSecondCapture.await(30, TimeUnit.SECONDS) shouldBe true
        }
      }
      def awaitRequestCount(expected: Long): Unit = {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
        while (OpenIvmStateSync.synchronousRequestCountForTesting(spark) != expected && System.nanoTime() < deadline)
          Thread.sleep(25L)
        OpenIvmStateSync.synchronousRequestCountForTesting(spark) shouldBe expected
      }
      val countBeforeCapture  = OpenIvmStateSync.synchronousRequestCountForTesting(spark)
      val firstCaptureRequest = request("capture-before-late-profile")
      awaitRequestCount(countBeforeCapture + 1)
      captureStarted.await(10, TimeUnit.SECONDS) shouldBe true
      val capturedGeneration = OpenIvmStateBarrier.currentMutationGeneration(spark)

      val lateProfileId = s"profile-late-${UUID.randomUUID()}"
      recordProfile(spark, lateProfileId)
      val lateRequestedGeneration = OpenIvmStateBarrier.currentMutationGeneration(spark)
      lateRequestedGeneration should be > capturedGeneration
      val lateRequest = request("late-profile-during-capture")
      awaitRequestCount(countBeforeCapture + 2)
      lateRequest.isCompleted shouldBe false

      releaseFirstPack.countDown()
      Await.result(firstCaptureRequest, 60.seconds)
      secondCaptureStarted.await(10, TimeUnit.SECONDS) shouldBe true
      val olderCommitDidNotAckLateRequest = !lateRequest.isCompleted
      olderCommitDidNotAckLateRequest shouldBe true
      val firstPublication = receipts.asScala.find(_.requestId == "capture-before-late-profile").get
      firstPublication.acknowledged shouldBe true
      firstPublication.capturedMutationGeneration shouldBe Some(capturedGeneration)
      firstPublication.capturedMutationGeneration.get should be < lateRequestedGeneration
      val firstCommit = commitDetails(spark, remote.toURI.toString)
      firstCommit._1 shouldBe firstPublication.publicationGeneration.get
      firstPublication.archiveSha256 shouldBe Some(firstCommit._2)
      firstPublication.archiveBytes shouldBe Some(firstCommit._3)

      releaseSecondCapture.countDown()
      Await.result(lateRequest, 60.seconds)
      OpenIvmStateSync.setBeforeLocalFileCopyHookForTesting(null)
      OpenIvmStateSync.setBeforeCaptureHookForTesting(null)

      val latePublication =
        receipts.asScala.find(receipt => receipt.requestId == "late-profile-during-capture" && receipt.acknowledged).get
      latePublication.published shouldBe true
      latePublication.reused shouldBe false
      latePublication.coverageProven shouldBe true
      latePublication.requestedMutationGeneration shouldBe lateRequestedGeneration
      latePublication.capturedMutationGeneration shouldBe Some(lateRequestedGeneration)
      latePublication.publicationGeneration should not be firstPublication.publicationGeneration
      val latest = commitDetails(spark, remote.toURI.toString)
      latest._1 shouldBe latePublication.publicationGeneration.get
      latePublication.archiveSha256 shouldBe Some(latest._2)
      latePublication.archiveBytes shouldBe Some(latest._3)
      latest._2 should not be firstCommit._2
      org.openivm.spark.common.MvCatalog.list(spark).map(_.lastVersion) shouldBe versionsBefore

      val childEvidence = new File(repoRoot, ".logs/checkpoint-only-canary/late-profile-fresh-jvm.json")
      val missingRel    = "triggers/late-profile/missing/marker"
      val retainedRel   = "triggers/late-profile/retained/marker"
      stopSpark(spark)
      runFreshRestoreProbe(remote.toURI.toString, missingRel, retainedRel, lateProfileId, childEvidence)
      writeReceiptEvidence(
        "late-profile-inflight-spark-4.1",
        Seq("late-profile-baseline", "capture-before-late-profile", "late-profile-during-capture"),
        latest,
        s"\"captured_generation_before_late_write\":$capturedGeneration," +
          s"\"late_requested_generation\":$lateRequestedGeneration," +
          s"\"older_commit_did_not_ack_late_request\":$olderCommitDidNotAckLateRequest," +
          s"\"late_archive_differs_from_older_commit\":true," +
          s"\"mv_versions_unchanged\":true,\"fresh_jvm_profile_restored\":true," +
          s"\"fresh_jvm_evidence\":\"${childEvidence.getAbsolutePath}\""
      )
    }

    it("isolates cached coverage when distinct state roots share one publication destination") {
      val fixtureRoot = newDir("shared-destination")
      val remote      = new File(fixtureRoot, "remote")
      remote.mkdirs()
      val stateA = new File(fixtureRoot, "state-a")
      val stateB = new File(fixtureRoot, "state-b")
      stateA.mkdirs()
      stateB.mkdirs()

      val a1 = newSpark(
        "checkpoint-root-a-first",
        remote,
        Seq(FeatureGate.StatePathKey -> stateA.getAbsolutePath)
      )
      recordProfile(a1, "root-a-before")
      setRequestId(a1, "root-a-before")
      OpenIvmStateSync.backupNow(a1)
      val first = receipts.asScala.find(_.requestId == "root-a-before").get
      first.published shouldBe true
      first.coverageProven shouldBe true
      setRequestId(a1, "root-a-no-change")
      OpenIvmStateSync.backupNow(a1)
      val noChange = receipts.asScala.find(_.requestId == "root-a-no-change").get
      noChange.published shouldBe false
      noChange.reused shouldBe true
      noChange.coverageProven shouldBe true
      noChange.publicationGeneration shouldBe first.publicationGeneration
      noChange.archiveSha256 shouldBe first.archiveSha256
      noChange.archiveBytes shouldBe first.archiveBytes
      stopSpark(a1)

      val sentinelDbPath = new File(stateB, "_openivm/sentinel/rocksdb").getAbsolutePath
      val sentinelDb     = new OpenIvmRocksDB(sentinelDbPath, OpenIvmRocksDBConf.default, Seq("meta"))
      sentinelDb.load()
      sentinelDb.withBatch { batch =>
        sentinelDb.put(batch, "meta", RocksDBCodec.utf8("state-b"), RocksDBCodec.utf8("local"))
      }
      sentinelDb.close()

      val b = newSpark(
        "checkpoint-root-b",
        remote,
        Seq(FeatureGate.StatePathKey -> stateB.getAbsolutePath)
      )
      RefreshProfileCatalog.ensureTables(b)
      RefreshProfileCatalog.record(
        b,
        Seq(
          RefreshProfileRow(
            "root-b-only",
            "canary.profile_only",
            Timestamp.valueOf("2025-01-01 00:00:00"),
            0,
            "profile_only_fixture",
            7L,
            "operation_id=root-b-only"
          )
        )
      )
      RefreshProfileCatalog.scanAll(b).map(_.refreshId) shouldBe Seq("root-b-only")
      setRequestId(b, "root-b-only")
      OpenIvmStateSync.backupNow(b)
      val second = receipts.asScala.find(_.requestId == "root-b-only").get
      second.published shouldBe true
      second.reused shouldBe false
      stopSpark(b)

      val a2 = newSpark(
        "checkpoint-root-a-after-sharing",
        remote,
        Seq(FeatureGate.StatePathKey -> stateA.getAbsolutePath)
      )
      recordProfile(a2, "root-a-after-sharing")
      setRequestId(a2, "root-a-after-sharing")
      OpenIvmStateSync.backupNow(a2)
      val third = receipts.asScala.find(_.requestId == "root-a-after-sharing").get
      third.published shouldBe true
      third.reused shouldBe false
      third.acknowledged shouldBe true
      third.publicationGeneration should not be second.publicationGeneration
      val latest = commitDetails(a2, remote.toURI.toString)
      latest._1 shouldBe third.publicationGeneration.get
      third.archiveSha256 shouldBe Some(latest._2)
      third.archiveBytes shouldBe Some(latest._3)
      val publishedIds = receipts.asScala.filter(receipt =>
        receipt.requestId == "root-a-before" || receipt.requestId == "root-a-no-change" ||
          receipt.requestId == "root-b-only" || receipt.requestId == "root-a-after-sharing"
      )
      publishedIds.count(_.published) shouldBe 3
      publishedIds.count(_.reused) shouldBe 1
      publishedIds.find(_.requestId == "root-a-after-sharing").get.reused shouldBe false

      writeReceiptEvidence(
        "shared-state-roots",
        Seq("root-a-before", "root-a-no-change", "root-b-only", "root-a-after-sharing"),
        latest,
        "\"distinct_roots\":2,\"shared_destination_fallback\":true," +
          "\"cross_root_reuse\":false,\"same_root_no_change_reuse\":true"
      )
    }
  }
}
