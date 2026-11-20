package org.openivm.spark.common.rocksdb

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.openivm.spark.common.{
  CdfWatermarkCatalog,
  FeatureGate,
  MvCatalog,
  MvMetadata,
  OpenIvmStatePaths,
  RefreshProfileCatalog,
  RefreshProfileRow
}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.security.MessageDigest
import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.{CountDownLatch, CopyOnWriteArrayList, Executors, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}

import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.io.Source

object OpenIvmStateSyncMatched200FreshRestoreProbe {
  private val ViewCount      = 200
  private val SourceName     = "bench.shared_source"
  private val InitialVersion = 10000L
  private val InitialOffset  = 50000L

  def main(args: Array[String]): Unit = {
    val stateRoot           = new File(args(0))
    val warehouse           = new File(args(1))
    val uri                 = args(2)
    val expectedGeneration  = args(3)
    val expectedArchiveHash = args(4)
    val baselineProfileId   = args(5)
    val lateProfileId       = args(6)
    val changedVersion      = args(7).toLong
    val changedOffset       = args(8).toLong
    val evidence            = new File(args(9))
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-checkpoint-matched-200-fresh-restore")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "localhost")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.warehouse.dir", warehouse.getAbsolutePath)
      .config(FeatureGate.StatePathKey, stateRoot.getAbsolutePath)
      .config(FeatureGate.StateSyncUriKey, uri)
      .config(FeatureGate.StateSyncCheckpointEnabledKey, "false")
      .getOrCreate()
    try {
      val remoteRoot  = new Path(uri)
      val fs          = remoteRoot.getFileSystem(spark.sessionState.newHadoopConf())
      val commitsRoot = new Path(new Path(remoteRoot, ".openivm-state-sync"), "commits")
      val latestCommit = fs
        .listStatus(commitsRoot)
        .filter(status => status.isFile && status.getPath.getName.endsWith(".commit"))
        .sortBy(_.getPath.getName)
        .last
      val input = fs.open(latestCommit.getPath)
      val manifest =
        try Source.fromInputStream(input, "UTF-8").mkString
        finally input.close()
      val lines = manifest.split("\\n", -1)
      require(lines.length == 5 && lines(1) == expectedGeneration, "fresh JVM did not select the expected commit")
      val archive = new Path(
        new Path(new Path(new Path(remoteRoot, ".openivm-state-sync"), "snapshots"), expectedGeneration),
        "snapshot.zip"
      )
      require(
        sha256(fs, archive) == expectedArchiveHash && lines(3) == expectedArchiveHash,
        "archive hash differs from expected commit"
      )
      OpenIvmStateSync.maybeRestore(spark)
      val all = MvCatalog.list(spark)
      require(all.size == ViewCount, s"expected $ViewCount restored MVs, found ${all.size}")
      require(
        OpenIvmStatePaths.existingMvDbPaths(spark).size == ViewCount,
        "expected 200 restored per-view RocksDB shards"
      )
      (0 until ViewCount).foreach { index =>
        val name            = viewName(index)
        val expectedVersion = if (index == 0) changedVersion else InitialVersion + index
        val expectedOffset  = if (index == 0) changedOffset else InitialOffset + index
        val id              = CatalystSqlParser.parseTableIdentifier(name)
        val metadata        = MvCatalog.lookup(spark, id).getOrElse(sys.error(s"missing restored MV $name"))
        require(metadata.lastVersion == expectedVersion, s"$name version ${metadata.lastVersion} != $expectedVersion")
        require(
          CdfWatermarkCatalog.get(spark, name, SourceName).contains(expectedOffset),
          s"$name CDF offset did not restore to $expectedOffset"
        )
      }
      val profiles = RefreshProfileCatalog.scanAll(spark)
      require(
        profiles.exists(row => row.refreshId == baselineProfileId && row.detail == s"operation_id=$baselineProfileId"),
        s"missing exact baseline profile row $baselineProfileId"
      )
      require(
        profiles.exists(row => row.refreshId == lateProfileId && row.detail == s"operation_id=$lateProfileId"),
        s"missing exact late profile row $lateProfileId"
      )
      Files.createDirectories(evidence.toPath.getParent)
      Files.write(
        evidence.toPath,
        s"{\"fresh_jvm\":true,\"selected_generation\":\"$expectedGeneration\",\"archive_sha256\":\"$expectedArchiveHash\",\"restored_mvs\":${all.size},\"all_versions_and_offsets_verified\":true,\"baseline_profile_id\":\"$baselineProfileId\",\"late_profile_id\":\"$lateProfileId\"}"
          .getBytes(StandardCharsets.UTF_8)
      )
    } finally {
      OpenIvmRocksDBRegistry.closeAll()
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
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
    digest.digest().map(b => f"${b & 0xff}%02x").mkString
  }

  private def viewName(index: Int): String = f"bench.mv_$index%03d"
}

class OpenIvmStateSyncMatched200Spec extends AnyFunSpec with BeforeAndAfterEach with Matchers {
  private val ViewCount      = 200
  private val SourceName     = "bench.shared_source"
  private val InitialVersion = 10000L
  private val InitialOffset  = 50000L
  private val BeforeCount    = 12
  private val DuringCount    = 18
  private val AfterCount     = 18
  private val CohortCount    = BeforeCount + DuringCount + AfterCount
  private val dirs           = scala.collection.mutable.ArrayBuffer.empty[File]
  private val sparks         = scala.collection.mutable.ArrayBuffer.empty[SparkSession]
  private val receipts       = new CopyOnWriteArrayList[OpenIvmStateSync.StateSyncRequestReceipt]()

  private case class RequestTiming(
      requestId: String,
      phase: String,
      startMonotonicNanos: Long,
      endMonotonicNanos: Long,
      latencyMs: Long,
      succeeded: Boolean
  )
  private case class PublicationEvent(requestId: String, generation: String, archiveBytes: Long, archiveSha256: String)
  private case class CommitEvidence(
      generation: String,
      manifestSha256: String,
      archiveBytes: Long,
      archiveSha256: String
  )

  private val requestExecutor = Executors.newFixedThreadPool(64)
  private implicit val executionContext: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(requestExecutor)

  override def beforeEach(): Unit = {
    OpenIvmStateSync.resetForTesting()
    receipts.clear()
    OpenIvmStateSync.setStateSyncRequestReceiptHookForTesting(receipt => { receipts.add(receipt); () })
    super.beforeEach()
  }

  override def afterEach(): Unit = {
    OpenIvmStateSync.resetForTesting()
    requestExecutor.shutdownNow()
    OpenIvmRocksDBRegistry.closeAll()
    sparks.reverse.foreach(spark =>
      try spark.stop()
      catch { case _: Throwable => () }
    )
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    sparks.clear()
    dirs.reverse.foreach(deleteRecursively)
    dirs.clear()
    super.afterEach()
  }

  private def newDir(prefix: String): File = {
    val dir = new File(s"target/test-checkpoint-matched-200-$prefix-${UUID.randomUUID().toString.take(8)}")
    dir.mkdirs()
    dirs += dir
    dir
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
    file.delete()
    ()
  }

  private def viewName(index: Int): String = f"bench.mv_$index%03d"

  private def newSpark(appName: String, stateRoot: File, remoteRoot: File, checkpointEnabled: Boolean): SparkSession = {
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
      .config(FeatureGate.StatePathKey, stateRoot.getAbsolutePath)
      .config(FeatureGate.StateSyncUriKey, remoteRoot.toURI.toString)
      .config(FeatureGate.StateSyncCheckpointEnabledKey, checkpointEnabled.toString)
      .config(FeatureGate.StateSyncAsyncEnabledKey, "false")
      .getOrCreate()
    sparks += spark
    spark
  }

  private def recordProfile(spark: SparkSession, refreshId: String, detail: String): Unit = {
    RefreshProfileCatalog.ensureTables(spark)
    RefreshProfileCatalog.record(
      spark,
      Seq(
        RefreshProfileRow(
          refreshId = refreshId,
          viewName = "bench.mv_000",
          profileTimestamp = new Timestamp(System.currentTimeMillis()),
          stepOrder = 0,
          stepName = "matched_200_fixture",
          durationMs = 7L,
          detail = detail
        )
      )
    )
  }

  private def populate(spark: SparkSession, arm: String): String = {
    val schema      = StructType(Seq(StructField("id", LongType, nullable = false)))
    val fingerprint = MvCatalog.schemaFingerprint(Map(SourceName -> schema))
    MvCatalog.ensureTables(spark)
    CdfWatermarkCatalog.ensureTables(spark)
    RefreshProfileCatalog.ensureTables(spark)
    (0 until ViewCount).foreach { index =>
      val name = viewName(index)
      val metadata = MvMetadata(
        name = CatalystSqlParser.parseTableIdentifier(name),
        querySql = s"SELECT id FROM $SourceName",
        refreshType = 0,
        refreshTypeName = "SIMPLE_PROJECTION",
        lastVersion = InitialVersion + index,
        sourceTables = Seq(SourceName),
        sourceSchemaFingerprint = fingerprint,
        location = s"${OpenIvmStatePaths.openIvmRoot(spark)}/fixture-data/$name",
        createdAt = new Timestamp(1700000000000L + index),
        properties = Map("fixture_arm" -> arm, "fixture_index" -> index.toString)
      )
      MvCatalog.upsert(spark, metadata)
      CdfWatermarkCatalog.put(spark, name, SourceName, InitialOffset + index)
    }
    val baselineProfile = s"matched-200-baseline-$arm-${UUID.randomUUID()}"
    recordProfile(spark, baselineProfile, s"operation_id=$baselineProfile")
    MvCatalog.list(spark).size shouldBe ViewCount
    OpenIvmStatePaths.existingMvDbPaths(spark).size shouldBe ViewCount
    baselineProfile
  }

  private def withRequestId[A](spark: SparkSession, requestId: String)(body: => A): A = {
    spark.sparkContext.setLocalProperty("openivm.request_id", requestId)
    try body
    finally spark.sparkContext.setLocalProperty("openivm.request_id", null)
  }

  private def launch(
      spark: SparkSession,
      requestId: String,
      phase: String,
      startGate: Option[CountDownLatch] = None,
      readyGate: Option[CountDownLatch] = None
  ): Future[RequestTiming] = Future {
    startGate.foreach { gate =>
      readyGate.foreach(_.countDown())
      gate.await(30, TimeUnit.SECONDS) shouldBe true
    }
    val started = System.nanoTime()
    try {
      withRequestId(spark, requestId) { OpenIvmStateSync.backupNow(spark) }
      val ended = System.nanoTime()
      RequestTiming(requestId, phase, started, ended, (ended - started) / 1000000L, succeeded = true)
    } catch {
      case _: Throwable =>
        val ended = System.nanoTime()
        RequestTiming(requestId, phase, started, ended, (ended - started) / 1000000L, succeeded = false)
    }
  }

  private def awaitRequestCount(spark: SparkSession, expected: Long): Unit = {
    val deadline = System.nanoTime() + 30.seconds.toNanos
    while (OpenIvmStateSync.synchronousRequestCountForTesting(spark) < expected && System.nanoTime() < deadline)
      Thread.sleep(5L)
    OpenIvmStateSync.synchronousRequestCountForTesting(spark) should be >= expected
  }

  private def sha256(bytes: Array[Byte]): String =
    MessageDigest.getInstance("SHA-256").digest(bytes).map(b => f"${b & 0xff}%02x").mkString

  private def sha256(file: File): String = sha256(Files.readAllBytes(file.toPath))

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
    digest.digest().map(b => f"${b & 0xff}%02x").mkString
  }

  private def commits(spark: SparkSession, uri: String): Seq[CommitEvidence] = {
    val root       = new Path(uri)
    val fs         = root.getFileSystem(spark.sessionState.newHadoopConf())
    val commitRoot = new Path(new Path(root, ".openivm-state-sync"), "commits")
    if (!fs.exists(commitRoot)) return Seq.empty
    fs.listStatus(commitRoot)
      .filter(status => status.isFile && status.getPath.getName.endsWith(".commit"))
      .sortBy(_.getPath.getName)
      .map { status =>
        val input = fs.open(status.getPath)
        val text =
          try Source.fromInputStream(input, "UTF-8").mkString
          finally input.close()
        val lines = text.split("\\n", -1)
        lines.length shouldBe 5
        val generation = lines(1)
        val archive =
          new Path(new Path(new Path(new Path(root, ".openivm-state-sync"), "snapshots"), generation), "snapshot.zip")
        val actualHash = sha256(fs, archive)
        actualHash shouldBe lines(3)
        fs.getFileStatus(archive).getLen shouldBe lines(2).toLong
        val manifestHash = sha256(text.getBytes(StandardCharsets.UTF_8))
        CommitEvidence(generation, manifestHash, lines(2).toLong, actualHash)
      }
      .toSeq
  }

  private def writeArmEvidence(
      arm: String,
      checkpointEnabled: Boolean,
      timings: Seq[RequestTiming],
      publicationEvents: Seq[PublicationEvent],
      committed: Seq[CommitEvidence],
      practicalTimings: Seq[RequestTiming],
      practicalBurstWallMs: Long,
      practicalCommitDelta: Int,
      barrierWaitMs: Long,
      receiptsForArm: Seq[OpenIvmStateSync.StateSyncRequestReceipt],
      baselineProfileId: String,
      lateProfileId: String,
      firstCapturedGeneration: Long,
      lateRequestedGeneration: Long,
      secondCapturedGeneration: Long,
      olderCommitExcludedLate: Option[Boolean],
      failureCommitsUnchanged: Boolean,
      replayReceipt: Option[OpenIvmStateSync.StateSyncRequestReceipt],
      childEvidence: File,
      burstStartNanos: Long,
      burstEndNanos: Long
  ): Unit = {
    val result = timings
      .map(t =>
        s"{\"request_id\":\"${t.requestId}\",\"phase\":\"${t.phase}\",\"start_monotonic_ns\":${t.startMonotonicNanos},\"end_monotonic_ns\":${t.endMonotonicNanos},\"latency_ms\":${t.latencyMs},\"succeeded\":${t.succeeded}}"
      )
      .mkString("[", ",", "]")
    val practical = practicalTimings
      .map(t =>
        s"{\"request_id\":\"${t.requestId}\",\"phase\":\"${t.phase}\",\"start_monotonic_ns\":${t.startMonotonicNanos},\"end_monotonic_ns\":${t.endMonotonicNanos},\"latency_ms\":${t.latencyMs},\"succeeded\":${t.succeeded}}"
      )
      .mkString("[", ",", "]")
    val uploads = publicationEvents
      .map(e =>
        s"{\"request_id\":\"${e.requestId}\",\"generation\":\"${e.generation}\",\"archive_bytes\":${e.archiveBytes},\"archive_sha256\":\"${e.archiveSha256}\"}"
      )
      .mkString("[", ",", "]")
    val commitJson = committed
      .map(c =>
        s"{\"generation\":\"${c.generation}\",\"manifest_sha256\":\"${c.manifestSha256}\",\"archive_bytes\":${c.archiveBytes},\"archive_sha256\":\"${c.archiveSha256}\"}"
      )
      .mkString("[", ",", "]")
    val receiptJson = receiptsForArm
      .map(r =>
        s"{\"request_id\":\"${r.requestId}\",\"requested_generation\":${r.requestedMutationGeneration},\"captured_generation\":${r.capturedMutationGeneration
            .map(_.toString)
            .getOrElse("null")},\"publication_generation\":\"${r.publicationGeneration
            .getOrElse("")}\",\"published\":${r.published},\"reused\":${r.reused},\"coverage_proven\":${r.coverageProven},\"acknowledged\":${r.acknowledged},\"outcome\":\"${r.outcome}\",\"archive_bytes\":${r.archiveBytes
            .getOrElse(0L)},\"archive_sha256\":\"${r.archiveSha256.getOrElse("")}\"}"
      )
      .mkString("[", ",", "]")
    val replay = replayReceipt
      .map(r =>
        s"{\"publication_generation\":\"${r.publicationGeneration
            .getOrElse("")}\",\"reused\":${r.reused},\"coverage_proven\":${r.coverageProven},\"acknowledged\":${r.acknowledged}}"
      )
      .getOrElse("null")
    val json =
      s"{\"arm\":\"$arm\",\"checkpoint_enabled\":$checkpointEnabled,\"coverage_eligible\":$checkpointEnabled,\"fixture_scale\":{\"populated_native_view_shards\":$ViewCount,\"cdf_offsets\":$ViewCount,\"mv_catalog_rows\":$ViewCount}," +
        s"\"cohort_size\":$CohortCount,\"cohort_wall_start_monotonic_ns\":$burstStartNanos,\"cohort_wall_end_monotonic_ns\":$burstEndNanos,\"cohort_wall_ms\":${(burstEndNanos - burstStartNanos) / 1000000L},\"barrier_wait_ms\":$barrierWaitMs,\"cohort_wall_excluding_controlled_barriers_ms\":${((burstEndNanos - burstStartNanos) / 1000000L - barrierWaitMs)
            .max(0L)},\"commit_count\":${committed.size},\"total_archive_bytes\":${committed.map(_.archiveBytes).sum},\"first_captured_generation\":$firstCapturedGeneration,\"late_requested_generation\":$lateRequestedGeneration,\"second_captured_generation\":$secondCapturedGeneration,\"older_commit_excluded_late\":${olderCommitExcludedLate
            .map(_.toString)
            .getOrElse("null")},\"failure_left_commit_count_unchanged\":$failureCommitsUnchanged,\"baseline_profile_id\":\"$baselineProfileId\",\"late_profile_id\":\"$lateProfileId\",\"unchanged_replay\":$replay,\"practical_burst_wall_ms\":$practicalBurstWallMs,\"practical_burst_commit_delta\":$practicalCommitDelta,\"practical_burst_request_timings\":$practical,\"request_timings\":$result,\"upload_events\":$uploads,\"committed_archives\":$commitJson,\"receipts\":$receiptJson,\"fresh_restore_evidence\":\"${childEvidence.getAbsolutePath
            .replace("\\", "\\\\")}\"}"
    val output = new File(repoRoot, s".logs/checkpoint-matched-200/arm-$arm.json")
    Files.createDirectories(output.toPath.getParent)
    Files.write(output.toPath, json.getBytes(StandardCharsets.UTF_8))
  }

  private def repoRoot: File = {
    var current = new File(".").getCanonicalFile
    while (current.getParentFile != null && !new File(current, ".git").exists()) current = current.getParentFile
    current
  }

  describe("matched 200-shard checkpoint qualification") {
    it("proves generation coverage and measures a 48-request local cohort") {
      val arm = sys.env.getOrElse("OPENIVM_CHECKPOINT_MATCH_ARM", "A1")
      arm should (startWith("A") or startWith("B"))
      val checkpointEnabled = arm.startsWith("B")
      val fixture           = newDir(s"arm-$arm")
      val stateRoot         = new File(fixture, "state")
      val remoteRoot        = new File(fixture, "remote")
      stateRoot.mkdirs()
      remoteRoot.mkdirs()
      val spark             = newSpark(s"checkpoint-matched-200-$arm", stateRoot, remoteRoot, checkpointEnabled)
      val remoteUri         = remoteRoot.toURI.toString
      val baselineProfileId = populate(spark, arm)
      if (checkpointEnabled) OpenIvmStateBarrier.mutationCoverageEnabled(spark) shouldBe true

      val cleanupMarker = new File(fixture, "post-commit-cleanup-marker")
      Files.write(cleanupMarker.toPath, Array[Byte](1, 2, 3))
      val countBeforeFailure = commits(spark, remoteUri).size
      OpenIvmStateSync.setCopyLocalFileHookForTesting((_, _, _) =>
        throw new java.io.IOException("injected 200-shard publish failure")
      )
      val failedRequestId = s"$arm-failure-${UUID.randomUUID()}"
      val failed =
        try {
          withRequestId(spark, failedRequestId) {
            OpenIvmStateSync.backupForCommand(spark, () => { Files.deleteIfExists(cleanupMarker.toPath); () })
          }
          false
        } catch { case _: java.io.IOException => true }
      failed shouldBe true
      cleanupMarker.isFile shouldBe true
      val failureCommitsUnchanged = commits(spark, remoteUri).size == countBeforeFailure
      failureCommitsUnchanged shouldBe true
      if (checkpointEnabled) {
        val failedReceipt = receipts.asScala.find(_.requestId == failedRequestId).get
        failedReceipt.published shouldBe false
        failedReceipt.acknowledged shouldBe false
        failedReceipt.outcome shouldBe "publication_failed"
      }
      OpenIvmStateSync.setCopyLocalFileHookForTesting(null)
      val retryRequestId = s"$arm-retry-${UUID.randomUUID()}"
      withRequestId(spark, retryRequestId) {
        OpenIvmStateSync.backupForCommand(spark, () => { Files.deleteIfExists(cleanupMarker.toPath); () })
      }
      cleanupMarker.isFile shouldBe false
      val retryCommits = commits(spark, remoteUri)
      retryCommits.size shouldBe countBeforeFailure + 1
      if (checkpointEnabled) {
        val retryReceipt = receipts.asScala.find(r => r.requestId == retryRequestId && r.acknowledged).get
        retryReceipt.publicationGeneration shouldBe Some(retryCommits.last.generation)
        retryReceipt.archiveSha256 shouldBe Some(retryCommits.last.archiveSha256)
      }
      val preCohortProfileId = s"matched-200-pre-cohort-$arm-${UUID.randomUUID()}"
      recordProfile(spark, preCohortProfileId, s"operation_id=$preCohortProfileId")

      val captureNumber           = new AtomicInteger(0)
      val firstCopyEntered        = new CountDownLatch(1)
      val releaseFirstCopy        = new CountDownLatch(1)
      val secondCopyEntered       = new CountDownLatch(1)
      val releaseSecondCopy       = new CountDownLatch(1)
      val heldFirst               = new AtomicBoolean(false)
      val heldSecond              = new AtomicBoolean(false)
      val firstCaptureStarted     = new CountDownLatch(1)
      val releaseFirstCapture     = new CountDownLatch(1)
      val controlledGateWaitNanos = new AtomicLong(0L)
      def awaitTestGate(gate: CountDownLatch, timeoutSeconds: Long): Unit = {
        val started  = System.nanoTime()
        val released = gate.await(timeoutSeconds, TimeUnit.SECONDS)
        controlledGateWaitNanos.addAndGet(System.nanoTime() - started)
        released shouldBe true
      }
      OpenIvmStateSync.setBeforeCaptureHookForTesting(() => {
        captureNumber.incrementAndGet()
        if (captureNumber.get() == 1) {
          firstCaptureStarted.countDown()
          awaitTestGate(releaseFirstCapture, 30L)
        }
      })
      OpenIvmStateSync.setBeforeLocalFileCopyHookForTesting { _ =>
        if (captureNumber.get() == 1 && heldFirst.compareAndSet(false, true)) {
          firstCopyEntered.countDown()
          awaitTestGate(releaseFirstCopy, 60L)
        } else if (captureNumber.get() == 2 && heldSecond.compareAndSet(false, true)) {
          secondCopyEntered.countDown()
          awaitTestGate(releaseSecondCopy, 60L)
        }
      }
      val uploads = new CopyOnWriteArrayList[PublicationEvent]()
      OpenIvmStateSync.setCopyLocalFileHookForTesting { (fs, file, remote) =>
        fs.copyFromLocalFile(false, true, new Path(file.getAbsolutePath), remote)
        uploads.add(
          PublicationEvent(
            Option(spark.sparkContext.getLocalProperty("openivm.request_id")).getOrElse("unknown"),
            remote.getParent.getName,
            file.length(),
            sha256(file)
          )
        )
      }

      val requestCountBeforeWave = OpenIvmStateSync.synchronousRequestCountForTesting(spark)
      val burstStart             = System.nanoTime()
      val beforeRequests         = (0 until BeforeCount).map(i => launch(spark, s"$arm-before-$i", "before"))
      firstCaptureStarted.await(30, TimeUnit.SECONDS) shouldBe true
      awaitRequestCount(spark, requestCountBeforeWave + BeforeCount)
      releaseFirstCapture.countDown()
      firstCopyEntered.await(60, TimeUnit.SECONDS) shouldBe true
      val firstCapturedGeneration = OpenIvmStateBarrier.currentMutationGeneration(spark)

      val lateProfileId = s"matched-200-late-$arm-${UUID.randomUUID()}"
      recordProfile(spark, lateProfileId, s"operation_id=$lateProfileId")
      val changedView = CatalystSqlParser.parseTableIdentifier(viewName(0))
      MvCatalog.advance(spark, changedView, InitialVersion + 1L)
      CdfWatermarkCatalog.put(spark, viewName(0), SourceName, InitialOffset + 1L)
      val lateRequestedGeneration = OpenIvmStateBarrier.currentMutationGeneration(spark)
      if (checkpointEnabled) lateRequestedGeneration should be > firstCapturedGeneration
      val duringRequests = (0 until DuringCount).map(i => launch(spark, s"$arm-during-$i", "during"))
      awaitRequestCount(spark, requestCountBeforeWave + BeforeCount + DuringCount)
      releaseFirstCopy.countDown()

      secondCopyEntered.await(90, TimeUnit.SECONDS) shouldBe true
      val firstCommit              = commits(spark, remoteUri).last
      val secondCapturedGeneration = OpenIvmStateBarrier.currentMutationGeneration(spark)
      if (checkpointEnabled) secondCapturedGeneration should be >= lateRequestedGeneration
      val lateIds = (0 until DuringCount).map(i => s"$arm-during-$i").toSet
      val olderCommitExcludedLate =
        if (checkpointEnabled)
          Some(
            !receipts.asScala.exists(r =>
              lateIds.contains(r.requestId) && r.publicationGeneration.contains(
                firstCommit.generation
              ) && r.acknowledged
            )
          )
        else None
      if (checkpointEnabled) olderCommitExcludedLate shouldBe Some(true)

      val afterRequests = (0 until AfterCount).map(i => launch(spark, s"$arm-after-$i", "after_capture"))
      awaitRequestCount(spark, requestCountBeforeWave + CohortCount)
      releaseSecondCopy.countDown()
      val timings  = Await.result(Future.sequence(beforeRequests ++ duringRequests ++ afterRequests), 300.seconds)
      val burstEnd = System.nanoTime()
      timings.size shouldBe CohortCount
      timings.forall(_.succeeded) shouldBe true
      OpenIvmStateSync.setBeforeCaptureHookForTesting(null)
      OpenIvmStateSync.setBeforeLocalFileCopyHookForTesting(null)
      OpenIvmStateSync.setCopyLocalFileHookForTesting(null)

      val cohortCommits = commits(spark, remoteUri)
      cohortCommits.size should be >= countBeforeFailure + 2
      val candidateReceipts = receipts.asScala.toSeq.filterNot(_.requestId == failedRequestId)
      if (checkpointEnabled) {
        val acked = candidateReceipts
          .filter(r =>
            r.requestId.startsWith(s"$arm-before-") ||
              r.requestId.startsWith(s"$arm-during-") ||
              r.requestId.startsWith(s"$arm-after-")
          )
          .filter(_.acknowledged)
        acked.map(_.requestId).distinct.size shouldBe CohortCount
        acked.foreach { receipt =>
          val committedArchive = cohortCommits.find(_.generation == receipt.publicationGeneration.get).get
          receipt.archiveSha256 shouldBe Some(committedArchive.archiveSha256)
          receipt.archiveBytes shouldBe Some(committedArchive.archiveBytes)
        }
        acked.filter(_.requestId.startsWith(s"$arm-before-")).foreach(_.coverageProven shouldBe true)
        acked.filter(_.requestId.startsWith(s"$arm-during-")).foreach { receipt =>
          receipt.coverageProven shouldBe true
          receipt.capturedMutationGeneration.get should be >= lateRequestedGeneration
        }
        acked.filter(_.requestId.startsWith(s"$arm-after-")).foreach { receipt =>
          receipt.coverageProven shouldBe true
          receipt.capturedMutationGeneration.get should be >= receipt.requestedMutationGeneration
        }
      }

      val noChangeId          = s"$arm-no-change-${UUID.randomUUID()}"
      val commitsBeforeReplay = cohortCommits.size
      withRequestId(spark, noChangeId) { OpenIvmStateSync.backupNow(spark) }
      val replayReceipt      = receipts.asScala.find(r => r.requestId == noChangeId && r.acknowledged)
      val commitsAfterReplay = commits(spark, remoteUri)
      if (checkpointEnabled) {
        replayReceipt should not be empty
        replayReceipt.get.reused shouldBe true
        replayReceipt.get.coverageProven shouldBe true
        commitsAfterReplay.size shouldBe commitsBeforeReplay
      } else {
        commitsAfterReplay.size shouldBe commitsBeforeReplay + 1
      }

      val practicalGate   = new CountDownLatch(1)
      val practicalReady  = new CountDownLatch(CohortCount)
      val practicalBefore = commitsAfterReplay.size
      val practicalRequests = (0 until CohortCount).map(i =>
        launch(spark, s"$arm-practical-$i", "practical_no_change", Some(practicalGate), Some(practicalReady))
      )
      practicalReady.await(30, TimeUnit.SECONDS) shouldBe true
      val practicalStart = System.nanoTime()
      practicalGate.countDown()
      val practicalTimings = Await.result(Future.sequence(practicalRequests), 300.seconds)
      val practicalEnd     = System.nanoTime()
      practicalTimings.size shouldBe CohortCount
      practicalTimings.forall(_.succeeded) shouldBe true
      val finalCommits         = commits(spark, remoteUri)
      val practicalCommitDelta = finalCommits.size - practicalBefore
      if (checkpointEnabled) {
        val practicalReceipts =
          receipts.asScala.filter(_.requestId.startsWith(s"$arm-practical-")).filter(_.acknowledged)
        practicalReceipts.map(_.requestId).distinct.size shouldBe CohortCount
        practicalReceipts.foreach(r => { r.reused shouldBe true; r.coverageProven shouldBe true })
        practicalCommitDelta shouldBe 0
      } else {
        practicalCommitDelta should be > 0
      }

      val finalCommit   = finalCommits.last
      val childEvidence = new File(repoRoot, s".logs/checkpoint-matched-200/arm-$arm-fresh-restore.json")
      Files.createDirectories(childEvidence.toPath.getParent)
      val childWarehouse = newDir(s"$arm-fresh-child-warehouse")
      val javaExecutable = new File(System.getProperty("java.home"), "bin/java").getAbsolutePath
      val classpath      = System.getProperty("java.class.path")
      val childLog       = new File(repoRoot, s".logs/checkpoint-matched-200/arm-$arm-fresh-restore.log")
      val process = new ProcessBuilder(
        javaExecutable,
        "-cp",
        classpath,
        "org.openivm.spark.common.rocksdb.OpenIvmStateSyncMatched200FreshRestoreProbe",
        newDir(s"$arm-fresh-child-state").getAbsolutePath,
        childWarehouse.getAbsolutePath,
        remoteUri,
        finalCommit.generation,
        finalCommit.archiveSha256,
        baselineProfileId,
        lateProfileId,
        (InitialVersion + 1L).toString,
        (InitialOffset + 1L).toString,
        childEvidence.getAbsolutePath
      ).redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.appendTo(childLog)).start()
      process.waitFor(180, TimeUnit.SECONDS) shouldBe true
      if (process.exitValue() != 0) {
        val log = Source.fromFile(childLog)
        val output =
          try log.mkString
          finally log.close()
        fail(s"Fresh restore JVM exited ${process.exitValue()}:\n$output")
      }
      childEvidence.isFile shouldBe true
      val lateRow = RefreshProfileCatalog.scanAll(spark).find(_.refreshId == lateProfileId)
      lateRow should not be empty
      MvCatalog.lookup(spark, changedView).get.lastVersion shouldBe InitialVersion + 1L
      CdfWatermarkCatalog.get(spark, viewName(0), SourceName) shouldBe Some(InitialOffset + 1L)
      val allMvs = MvCatalog.list(spark)
      allMvs.size shouldBe ViewCount
      val outputPath  = new File(repoRoot, s".logs/checkpoint-matched-200/arm-$arm.json")
      val armReceipts = receipts.asScala.toSeq
      writeArmEvidence(
        arm,
        checkpointEnabled,
        timings,
        uploads.asScala.toSeq,
        finalCommits,
        practicalTimings,
        (practicalEnd - practicalStart) / 1000000L,
        practicalCommitDelta,
        controlledGateWaitNanos.get() / 1000000L,
        armReceipts,
        baselineProfileId,
        lateProfileId,
        firstCapturedGeneration,
        lateRequestedGeneration,
        secondCapturedGeneration,
        olderCommitExcludedLate,
        failureCommitsUnchanged,
        replayReceipt,
        childEvidence,
        burstStart,
        burstEnd
      )
      outputPath.isFile shouldBe true
      baselineProfileId.nonEmpty shouldBe true
    }
  }
}
