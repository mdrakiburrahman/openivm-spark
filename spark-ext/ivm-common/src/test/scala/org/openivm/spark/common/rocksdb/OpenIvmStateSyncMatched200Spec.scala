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
import java.lang.management.ManagementFactory
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
  private case class Mutation(
      requestId: String,
      phase: String,
      shard: Int,
      version: Long,
      offset: Long,
      stage: Int,
      detail: String
  )

  def main(args: Array[String]): Unit = {
    val stateRoot         = new File(args(0))
    val warehouse         = new File(args(1))
    val uri               = args(2)
    val generation        = args(3)
    val archiveHash       = args(4)
    val baselineProfileId = args(5)
    val planFile          = new File(args(6))
    val captureStage      = args(7).toInt
    val evidence          = new File(args(8))
    val mutations = Files
      .readAllLines(planFile.toPath, StandardCharsets.UTF_8)
      .asScala
      .filter(_.nonEmpty)
      .map { line =>
        val f = line.split("\\t", -1)
        require(f.length == 7, s"invalid mutation plan row: $line")
        Mutation(f(0), f(1), f(2).toInt, f(3).toLong, f(4).toLong, f(5).toInt, f(6))
      }
      .toSeq
    require(mutations.map(_.requestId).distinct.size == 48, "expected 48 unique waiter IDs")
    require(mutations.map(_.shard).distinct.size == 48, "expected 48 distinct mutated shards")
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
      val root       = new Path(uri)
      val fs         = root.getFileSystem(spark.sessionState.newHadoopConf())
      val commitRoot = new Path(new Path(root, ".openivm-state-sync"), "commits")
      val commits    = fs.listStatus(commitRoot).filter(s => s.isFile && s.getPath.getName.endsWith(".commit"))
      require(commits.length == 1, s"expected one staged generation, got ${commits.length}")
      val commitPath = commits.head.getPath
      require(commitPath.getName == s"$generation.commit", "staged commit filename mismatch")
      val in = fs.open(commitPath)
      val manifest =
        try Source.fromInputStream(in, "UTF-8").mkString
        finally in.close()
      val lines = manifest.split("\\n", -1)
      require(lines.length == 5 && lines(1) == generation, "fresh JVM selected a different generation")
      val archive =
        new Path(new Path(new Path(new Path(root, ".openivm-state-sync"), "snapshots"), generation), "snapshot.zip")
      require(sha256(fs, archive) == archiveHash && lines(3) == archiveHash, "archive hash differs from commit")
      require(fs.getFileStatus(archive).getLen == lines(2).toLong, "archive length differs from commit")
      OpenIvmStateSync.maybeRestore(spark)
      val all = MvCatalog.list(spark)
      require(all.size == ViewCount, s"expected $ViewCount restored MVs, found ${all.size}")
      require(OpenIvmStatePaths.existingMvDbPaths(spark).size == ViewCount, "expected 200 restored per-view shards")
      val included = mutations.filter(_.stage <= captureStage)
      val byShard  = included.map(m => m.shard -> m).toMap
      (0 until ViewCount).foreach { index =>
        val name            = viewName(index)
        val expectedVersion = byShard.get(index).map(_.version).getOrElse(InitialVersion + index)
        val expectedOffset  = byShard.get(index).map(_.offset).getOrElse(InitialOffset + index)
        val id              = CatalystSqlParser.parseTableIdentifier(name)
        val metadata        = MvCatalog.lookup(spark, id).getOrElse(sys.error(s"missing restored MV $name"))
        require(metadata.lastVersion == expectedVersion, s"$name version ${metadata.lastVersion} != $expectedVersion")
        require(CdfWatermarkCatalog.get(spark, name, SourceName).contains(expectedOffset), s"$name offset mismatch")
      }
      val profiles = RefreshProfileCatalog.scanAll(spark)
      require(
        profiles.exists(r => r.refreshId == baselineProfileId && r.detail == s"operation_id=$baselineProfileId"),
        "baseline profile missing"
      )
      included.foreach(m =>
        require(profiles.exists(r => r.refreshId == m.requestId && r.detail == m.detail), s"missing ${m.requestId}")
      )
      mutations
        .filter(_.stage > captureStage)
        .foreach(m =>
          require(!profiles.exists(_.refreshId == m.requestId), s"later waiter ${m.requestId} appeared in old archive")
        )
      def renderIds(values: Seq[String]): String =
        values.sorted.map(id => "\"" + id + "\"").mkString("[", ",", "]")
      val includedIds = renderIds(included.map(_.requestId).toVector)
      val excludedIds = renderIds(mutations.filter(_.stage > captureStage).map(_.requestId).toVector)
      Files.createDirectories(evidence.toPath.getParent)
      Files.write(
        evidence.toPath,
        s"""{"fresh_jvm":true,"selected_generation":"$generation","archive_sha256":"$archiveHash","capture_ordinal":$captureStage,"restored_mvs":${all.size},"all_200_versions_and_offsets_verified":true,"baseline_profile_id":"$baselineProfileId","included_waiter_ids":$includedIds,"excluded_later_waiter_ids":$excludedIds}"""
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
    val in     = fs.open(path)
    val digest = MessageDigest.getInstance("SHA-256")
    try {
      val bytes = new Array[Byte](8192); var n = in.read(bytes);
      while (n >= 0) { if (n > 0) digest.update(bytes, 0, n); n = in.read(bytes) }
    } finally in.close()
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
      shardIndex: Option[Int],
      expectedVersion: Option[Long],
      expectedOffset: Option[Long],
      startMonotonicNanos: Long,
      endMonotonicNanos: Long,
      latencyMs: Long,
      succeeded: Boolean,
      completedGeneration: Option[String]
  )
  private case class MutationEvidence(
      requestId: String,
      phase: String,
      captureStage: Int,
      shardIndex: Int,
      expectedVersion: Long,
      expectedOffset: Long,
      detail: String
  )
  private case class PublicationEvent(
      requestId: String,
      captureOrdinal: Int,
      capturedMutationGeneration: Option[Long],
      generation: String,
      archiveBytes: Long,
      archiveSha256: String
  )
  private case class CommitEvidence(
      generation: String,
      manifestSha256: String,
      archiveBytes: Long,
      archiveSha256: String
  )
  private case class WaiterArchiveBinding(
      mutation: MutationEvidence,
      generation: String,
      manifestSha256: String,
      archiveBytes: Long,
      archiveSha256: String,
      captureOrdinal: Int,
      requestedMutationGeneration: Option[Long],
      capturedMutationGeneration: Option[Long],
      coverageProven: Option[Boolean]
  )
  private case class GenerationRestoreEvidence(
      generation: String,
      captureOrdinal: Int,
      archiveSha256: String,
      evidencePath: String,
      evidenceSha256: String
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
      .config(
        OpenIvmStateBarrier.ParallelCaptureKey,
        sys.env.getOrElse("OPENIVM_CHECKPOINT_MATCH_PARALLEL_CAPTURE", "false")
      )
      .config(FeatureGate.StateSyncAsyncEnabledKey, "false")
      .getOrCreate()
    sparks += spark
    spark
  }

  private def recordProfile(
      spark: SparkSession,
      refreshId: String,
      detail: String,
      profileViewName: String = viewName(0)
  ): Unit = {
    RefreshProfileCatalog.ensureTables(spark)
    RefreshProfileCatalog.record(
      spark,
      Seq(
        RefreshProfileRow(
          refreshId = refreshId,
          viewName = profileViewName,
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

  private def stageMutation(
      spark: SparkSession,
      arm: String,
      phase: String,
      captureStage: Int,
      shardIndex: Int
  ): MutationEvidence = {
    val requestId       = s"$arm-$phase-$shardIndex"
    val name            = viewName(shardIndex)
    val expectedVersion = InitialVersion + 100L + shardIndex
    val expectedOffset  = InitialOffset + 1000L + shardIndex
    MvCatalog.advance(spark, CatalystSqlParser.parseTableIdentifier(name), expectedVersion)
    CdfWatermarkCatalog.put(spark, name, SourceName, expectedOffset)
    val detail = s"operation_id=$requestId; shard=$name; version=$expectedVersion; offset=$expectedOffset"
    recordProfile(spark, requestId, detail, name)
    MutationEvidence(requestId, phase, captureStage, shardIndex, expectedVersion, expectedOffset, detail)
  }

  private def launch(
      spark: SparkSession,
      uri: String,
      requestId: String,
      phase: String,
      mutation: Option[MutationEvidence],
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
      RequestTiming(
        requestId,
        phase,
        mutation.map(_.shardIndex),
        mutation.map(_.expectedVersion),
        mutation.map(_.expectedOffset),
        started,
        ended,
        (ended - started) / 1000000L,
        succeeded = true,
        Some(latestCommitGeneration(spark, uri))
      )
    } catch {
      case _: Throwable =>
        val ended = System.nanoTime()
        RequestTiming(
          requestId,
          phase,
          mutation.map(_.shardIndex),
          mutation.map(_.expectedVersion),
          mutation.map(_.expectedOffset),
          started,
          ended,
          (ended - started) / 1000000L,
          succeeded = false,
          None
        )
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

  private def latestCommitGeneration(spark: SparkSession, uri: String): String = {
    val root       = new Path(uri)
    val fs         = root.getFileSystem(spark.sessionState.newHadoopConf())
    val commitRoot = new Path(new Path(root, ".openivm-state-sync"), "commits")
    fs.listStatus(commitRoot)
      .filter(status => status.isFile && status.getPath.getName.endsWith(".commit"))
      .sortBy(_.getPath.getName)
      .last
      .getPath
      .getName
      .stripSuffix(".commit")
  }

  private def stageOneGeneration(
      spark: SparkSession,
      sourceUri: String,
      generation: String,
      destination: File,
      expectedArchiveHash: String
  ): String = {
    val sourceRoot    = new Path(sourceUri)
    val fs            = sourceRoot.getFileSystem(spark.sessionState.newHadoopConf())
    val sourceControl = new Path(sourceRoot, ".openivm-state-sync")
    val sourceCommit  = new Path(new Path(sourceControl, "commits"), s"$generation.commit")
    val sourceArchive = new Path(new Path(new Path(sourceControl, "snapshots"), generation), "snapshot.zip")
    val targetControl = new File(destination, ".openivm-state-sync")
    val targetCommit  = new File(new File(targetControl, "commits"), s"$generation.commit")
    val targetArchive = new File(new File(new File(targetControl, "snapshots"), generation), "snapshot.zip")
    targetCommit.getParentFile.mkdirs()
    targetArchive.getParentFile.mkdirs()
    fs.copyToLocalFile(false, sourceCommit, new Path(targetCommit.getAbsolutePath), true)
    fs.copyToLocalFile(false, sourceArchive, new Path(targetArchive.getAbsolutePath), true)
    sha256(fs, sourceArchive) shouldBe expectedArchiveHash
    sha256(targetArchive) shouldBe expectedArchiveHash
    val stagedUri = destination.toURI.toString
    val stagedFs  = new Path(stagedUri).getFileSystem(spark.sessionState.newHadoopConf())
    val stagedCommits = stagedFs
      .listStatus(new Path(new Path(stagedUri), ".openivm-state-sync/commits"))
      .filter(status => status.isFile && status.getPath.getName.endsWith(".commit"))
    stagedCommits.map(_.getPath.getName).toSeq shouldBe Seq(s"$generation.commit")
    stagedUri
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
      secondCommitExcludedAfter: Boolean,
      failureCommitsUnchanged: Boolean,
      replayReceipt: Option[OpenIvmStateSync.StateSyncRequestReceipt],
      bindings: Seq[WaiterArchiveBinding],
      restoreEvidence: Seq[GenerationRestoreEvidence],
      burstStartNanos: Long,
      burstEndNanos: Long
  ): Unit = {
    def optionalLong(value: Option[Long]): String       = value.map(_.toString).getOrElse("null")
    def optionalBoolean(value: Option[Boolean]): String = value.map(_.toString).getOrElse("null")
    val result = timings
      .map { t =>
        s"""{"request_id":"${t.requestId}","phase":"${t.phase}","shard_index":${t.shardIndex
            .map(_.toString)
            .getOrElse("null")},"expected_version":${optionalLong(
            t.expectedVersion
          )},"expected_offset":${optionalLong(t.expectedOffset)},"completed_generation":"${t.completedGeneration
            .getOrElse(
              ""
            )}","start_monotonic_ns":${t.startMonotonicNanos},"end_monotonic_ns":${t.endMonotonicNanos},"latency_ms":${t.latencyMs},"succeeded":${t.succeeded}}"""
      }
      .mkString("[", ",", "]")
    val practical = practicalTimings
      .map { t =>
        s"""{"request_id":"${t.requestId}","phase":"${t.phase}","start_monotonic_ns":${t.startMonotonicNanos},"end_monotonic_ns":${t.endMonotonicNanos},"latency_ms":${t.latencyMs},"succeeded":${t.succeeded}}"""
      }
      .mkString("[", ",", "]")
    val uploads = publicationEvents
      .map { e =>
        s"""{"request_id":"${e.requestId}","capture_ordinal":${e.captureOrdinal},"captured_mutation_generation":${optionalLong(
            e.capturedMutationGeneration
          )},"generation":"${e.generation}","archive_bytes":${e.archiveBytes},"archive_sha256":"${e.archiveSha256}"}"""
      }
      .mkString("[", ",", "]")
    val commitJson = committed
      .map { c =>
        s"""{"generation":"${c.generation}","manifest_sha256":"${c.manifestSha256}","archive_bytes":${c.archiveBytes},"archive_sha256":"${c.archiveSha256}"}"""
      }
      .mkString("[", ",", "]")
    val receiptJson = receiptsForArm
      .map { r =>
        s"""{"request_id":"${r.requestId}","requested_generation":${r.requestedMutationGeneration},"captured_generation":${optionalLong(
            r.capturedMutationGeneration
          )},"publication_generation":"${r.publicationGeneration
            .getOrElse(
              ""
            )}","published":${r.published},"reused":${r.reused},"coverage_proven":${r.coverageProven},"acknowledged":${r.acknowledged},"outcome":"${r.outcome}","archive_bytes":${r.archiveBytes
            .getOrElse(0L)},"archive_sha256":"${r.archiveSha256.getOrElse("")}"}"""
      }
      .mkString("[", ",", "]")
    val replay = replayReceipt
      .map(r =>
        s"""{"publication_generation":"${r.publicationGeneration
            .getOrElse(
              ""
            )}","reused":${r.reused},"coverage_proven":${r.coverageProven},"acknowledged":${r.acknowledged}}"""
      )
      .getOrElse("null")
    val bindingJson = bindings
      .map { b =>
        val kind = if (checkpointEnabled) "state_sync_receipt" else "test_only_completion_to_single_commit"
        s"""{"request_id":"${b.mutation.requestId}","phase":"${b.mutation.phase}","shard_index":${b.mutation.shardIndex},"expected_version":${b.mutation.expectedVersion},"expected_offset":${b.mutation.expectedOffset},"mutation_capture_stage":${b.mutation.captureStage},"capture_ordinal":${b.captureOrdinal},"generation":"${b.generation}","manifest_sha256":"${b.manifestSha256}","archive_bytes":${b.archiveBytes},"archive_sha256":"${b.archiveSha256}","requested_generation":${optionalLong(
            b.requestedMutationGeneration
          )},"captured_generation":${optionalLong(
            b.capturedMutationGeneration
          )},"coverage_proven":${b.coverageProven.map(_.toString).getOrElse("null")},"linkage_kind":"$kind"}"""
      }
      .mkString("[", ",", "]")
    val restoreJson = restoreEvidence
      .map { e =>
        val path = e.evidencePath.replace("\\", "\\\\")
        s"""{"generation":"${e.generation}","capture_ordinal":${e.captureOrdinal},"archive_sha256":"${e.archiveSha256}","evidence_path":"$path","evidence_sha256":"${e.evidenceSha256}"}"""
      }
      .mkString("[", ",", "]")
    val json =
      s"""{"arm":"$arm","checkpoint_enabled":$checkpointEnabled,"coverage_eligible":$checkpointEnabled,""" +
        s""""fixture_scale":{"populated_native_view_shards":$ViewCount,"cdf_offsets":$ViewCount,"mv_catalog_rows":$ViewCount},""" +
        s""""cohort_size":$CohortCount,"cohort_wall_start_monotonic_ns":$burstStartNanos,"cohort_wall_end_monotonic_ns":$burstEndNanos,"cohort_wall_ms":${(burstEndNanos - burstStartNanos) / 1000000L},"barrier_wait_ms":$barrierWaitMs,"cohort_wall_excluding_controlled_barriers_ms":${((burstEndNanos - burstStartNanos) / 1000000L - barrierWaitMs)
            .max(0L)},""" +
        s""""commit_count":${committed.size},"total_archive_bytes":${committed
            .map(_.archiveBytes)
            .sum},"first_captured_generation":$firstCapturedGeneration,"late_requested_generation":$lateRequestedGeneration,"second_captured_generation":$secondCapturedGeneration,"older_commit_excluded_late":${optionalBoolean(
            olderCommitExcludedLate
          )},"second_commit_excluded_after":$secondCommitExcludedAfter,"failure_left_commit_count_unchanged":$failureCommitsUnchanged,"baseline_profile_id":"$baselineProfileId","late_profile_id":"$lateProfileId","unchanged_replay":$replay,"practical_burst_wall_ms":$practicalBurstWallMs,"practical_burst_commit_delta":$practicalCommitDelta,""" +
        s""""practical_burst_request_timings":$practical,"request_timings":$result,"upload_events":$uploads,"committed_archives":$commitJson,"receipts":$receiptJson,"waiter_archive_bindings":$bindingJson,"generation_restore_evidence":$restoreJson}"""
    val output = new File(repoRoot, s".logs/checkpoint-matched-200-per-waiter/arm-$arm.json")
    Files.createDirectories(output.toPath.getParent)
    Files.write(output.toPath, json.getBytes(StandardCharsets.UTF_8))
  }

  private def repoRoot: File = {
    var current = new File(".").getCanonicalFile
    while (current.getParentFile != null && !new File(current, ".git").exists()) current = current.getParentFile
    current
  }

  describe("matched 200-shard checkpoint qualification") {
    it("proves per-waiter generation coverage across 200 native shards") {
      val arm = sys.env.getOrElse("OPENIVM_CHECKPOINT_MATCH_ARM", "A1")
      arm should (startWith("A") or startWith("B"))
      val checkpointEnabled = arm.startsWith("B")
      val fixture           = newDir(s"arm-$arm-per-waiter")
      val stateRoot         = new File(fixture, "state")
      val remoteRoot        = new File(fixture, "remote")
      stateRoot.mkdirs()
      remoteRoot.mkdirs()
      val spark     = newSpark(s"checkpoint-matched-200-$arm-per-waiter", stateRoot, remoteRoot, checkpointEnabled)
      val remoteUri = remoteRoot.toURI.toString
      val baselineProfileId = populate(spark, arm)
      if (checkpointEnabled) OpenIvmStateBarrier.mutationCoverageEnabled(spark) shouldBe true

      val cleanupMarker = new File(fixture, "post-commit-cleanup-marker")
      Files.write(cleanupMarker.toPath, Array[Byte](1, 2, 3))
      val countBeforeFailure = commits(spark, remoteUri).size
      OpenIvmStateSync.setCopyLocalFileHookForTesting((_, _, _) =>
        throw new java.io.IOException("injected per-waiter publish failure")
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

      val captureNumber           = new AtomicInteger(0)
      val firstCopyEntered        = new CountDownLatch(1)
      val releaseFirstCopy        = new CountDownLatch(1)
      val secondCopyEntered       = new CountDownLatch(1)
      val releaseSecondCopy       = new CountDownLatch(1)
      val thirdCopyEntered        = new CountDownLatch(1)
      val releaseThirdCopy        = new CountDownLatch(1)
      val heldFirst               = new AtomicBoolean(false)
      val heldSecond              = new AtomicBoolean(false)
      val heldThird               = new AtomicBoolean(false)
      val firstCaptureStarted     = new CountDownLatch(1)
      val releaseFirstCapture     = new CountDownLatch(1)
      val controlledGateWaitNanos = new AtomicLong(0L)
      val captureCuts             = new java.util.concurrent.ConcurrentHashMap[Integer, java.lang.Long]()
      def awaitTestGate(gate: CountDownLatch, timeoutSeconds: Long): Unit = {
        val started  = System.nanoTime()
        val released = gate.await(timeoutSeconds, TimeUnit.SECONDS)
        controlledGateWaitNanos.addAndGet(System.nanoTime() - started)
        released shouldBe true
      }
      OpenIvmStateSync.setBeforeCaptureHookForTesting(() => {
        val ordinal = captureNumber.incrementAndGet()
        if (ordinal == 1) {
          firstCaptureStarted.countDown()
          awaitTestGate(releaseFirstCapture, 60L)
        }
      })
      OpenIvmStateSync.setBeforeLocalFileCopyHookForTesting { _ =>
        val ordinal = captureNumber.get()
        if (checkpointEnabled) captureCuts.putIfAbsent(ordinal, OpenIvmStateBarrier.currentMutationGeneration(spark))
        if (ordinal == 1 && heldFirst.compareAndSet(false, true)) {
          firstCopyEntered.countDown()
          awaitTestGate(releaseFirstCopy, 90L)
        } else if (ordinal == 2 && heldSecond.compareAndSet(false, true)) {
          secondCopyEntered.countDown()
          awaitTestGate(releaseSecondCopy, 90L)
        } else if (ordinal == 3 && heldThird.compareAndSet(false, true)) {
          thirdCopyEntered.countDown()
          awaitTestGate(releaseThirdCopy, 90L)
        }
      }
      val uploads = new CopyOnWriteArrayList[PublicationEvent]()
      OpenIvmStateSync.setCopyLocalFileHookForTesting { (fs, file, remote) =>
        fs.copyFromLocalFile(false, true, new Path(file.getAbsolutePath), remote)
        val ordinal = captureNumber.get()
        uploads.add(
          PublicationEvent(
            Option(spark.sparkContext.getLocalProperty("openivm.request_id")).getOrElse("unknown"),
            ordinal,
            Option(captureCuts.get(ordinal)).map(_.longValue()),
            remote.getParent.getName,
            file.length(),
            sha256(file)
          )
        )
      }

      val requestCountBeforeWave = OpenIvmStateSync.synchronousRequestCountForTesting(spark)
      val burstStart             = System.nanoTime()
      val mutations              = scala.collection.mutable.ArrayBuffer.empty[MutationEvidence]
      val requestFutures         = scala.collection.mutable.ArrayBuffer.empty[Future[RequestTiming]]
      def stageAndLaunch(index: Int, phase: String, stage: Int): Unit = {
        val mutation = stageMutation(spark, arm, phase, stage, index)
        mutations += mutation
        requestFutures += launch(spark, remoteUri, mutation.requestId, phase, Some(mutation))
      }

      stageAndLaunch(0, "before", 1)
      firstCaptureStarted.await(60, TimeUnit.SECONDS) shouldBe true
      awaitRequestCount(spark, requestCountBeforeWave + 1L)
      (1 until BeforeCount).foreach(index => stageAndLaunch(index, "before", 1))
      awaitRequestCount(spark, requestCountBeforeWave + BeforeCount)
      releaseFirstCapture.countDown()
      firstCopyEntered.await(90, TimeUnit.SECONDS) shouldBe true

      (BeforeCount until BeforeCount + DuringCount).foreach(index => stageAndLaunch(index, "during", 2))
      awaitRequestCount(spark, requestCountBeforeWave + BeforeCount + DuringCount)
      releaseFirstCopy.countDown()
      secondCopyEntered.await(120, TimeUnit.SECONDS) shouldBe true

      (BeforeCount + DuringCount until CohortCount).foreach(index => stageAndLaunch(index, "after_capture", 3))
      awaitRequestCount(spark, requestCountBeforeWave + CohortCount)
      releaseSecondCopy.countDown()
      thirdCopyEntered.await(120, TimeUnit.SECONDS) shouldBe true
      releaseThirdCopy.countDown()

      val timings  = Await.result(Future.sequence(requestFutures), 420.seconds).toVector
      val burstEnd = System.nanoTime()
      timings.size shouldBe CohortCount
      timings.forall(_.succeeded) shouldBe true
      mutations.size shouldBe CohortCount
      mutations.map(_.requestId).distinct.size shouldBe CohortCount
      mutations.map(_.shardIndex).distinct.size shouldBe CohortCount
      mutations.map(_.shardIndex).sorted shouldBe (0 until CohortCount)
      OpenIvmStateSync.setBeforeCaptureHookForTesting(null)
      OpenIvmStateSync.setBeforeLocalFileCopyHookForTesting(null)
      OpenIvmStateSync.setCopyLocalFileHookForTesting(null)

      val cohortCommits     = commits(spark, remoteUri)
      val publicationEvents = uploads.asScala.toSeq.sortBy(_.captureOrdinal)
      publicationEvents.map(_.captureOrdinal) shouldBe Seq(1, 2, 3)
      publicationEvents.map(_.generation).distinct.size shouldBe 3
      publicationEvents.foreach(event => cohortCommits.exists(_.generation == event.generation) shouldBe true)
      val eventByGeneration  = publicationEvents.map(event => event.generation -> event).toMap
      val commitByGeneration = cohortCommits.map(commit => commit.generation -> commit).toMap
      val timingById         = timings.map(timing => timing.requestId -> timing).toMap
      val cohortIds          = mutations.map(_.requestId).toSet
      val cohortReceipts     = receipts.asScala.filter(receipt => cohortIds.contains(receipt.requestId)).toSeq
      if (checkpointEnabled) {
        cohortReceipts.filter(_.acknowledged).map(_.requestId).distinct.size shouldBe CohortCount
        cohortReceipts.filter(_.acknowledged).foreach(_.coverageProven shouldBe true)
      } else cohortReceipts shouldBe empty
      val receiptById = cohortReceipts.filter(_.acknowledged).map(receipt => receipt.requestId -> receipt).toMap

      val bindings = mutations.map { mutation =>
        val timing = timingById(mutation.requestId)
        val generation =
          if (checkpointEnabled) receiptById(mutation.requestId).publicationGeneration.get
          else
            timing.completedGeneration.getOrElse(fail(s"${mutation.requestId} returned without a committed manifest"))
        timing.completedGeneration shouldBe Some(generation)
        val event = eventByGeneration.getOrElse(
          generation,
          fail(s"${mutation.requestId} mapped to non-cohort archive $generation")
        )
        event.captureOrdinal should be >= mutation.captureStage
        val commit  = commitByGeneration.getOrElse(generation, fail(s"missing manifest for $generation"))
        val receipt = if (checkpointEnabled) Some(receiptById(mutation.requestId)) else None
        receipt.foreach { proof =>
          proof.requestedMutationGeneration should be <= proof.capturedMutationGeneration.get
          proof.capturedMutationGeneration shouldBe event.capturedMutationGeneration
          proof.archiveSha256 shouldBe Some(commit.archiveSha256)
          proof.archiveBytes shouldBe Some(commit.archiveBytes)
        }
        WaiterArchiveBinding(
          mutation,
          generation,
          commit.manifestSha256,
          commit.archiveBytes,
          commit.archiveSha256,
          event.captureOrdinal,
          receipt.map(_.requestedMutationGeneration),
          receipt.flatMap(_.capturedMutationGeneration),
          receipt.map(_.coverageProven)
        )
      }.toSeq
      bindings.size shouldBe CohortCount
      bindings.map(_.mutation.requestId).distinct.size shouldBe CohortCount

      val firstEvent   = publicationEvents.find(_.captureOrdinal == 1).get
      val secondEvent  = publicationEvents.find(_.captureOrdinal == 2).get
      val lateMutation = mutations.find(_.phase == "during").get
      val lateBinding  = bindings.find(_.mutation.requestId == lateMutation.requestId).get
      lateBinding.captureOrdinal should be >= 2
      val lateReceipt = receiptById.get(lateMutation.requestId)
      val olderCommitExcludedLate =
        if (checkpointEnabled) {
          val excluded = lateReceipt.exists(_.publicationGeneration.contains(firstEvent.generation) == false)
          excluded shouldBe true
          Some(excluded)
        } else None
      if (!checkpointEnabled) lateBinding.generation should not be firstEvent.generation
      val afterIds = mutations.filter(_.phase == "after_capture").map(_.requestId).toSet
      val secondCommitExcludedAfter = !bindings.exists(binding =>
        afterIds.contains(binding.mutation.requestId) && binding.generation == secondEvent.generation
      )
      secondCommitExcludedAfter shouldBe true
      if (checkpointEnabled) {
        bindings.filter(_.mutation.phase == "during").foreach { binding =>
          val proof = receiptById(binding.mutation.requestId)
          proof.capturedMutationGeneration.get should be >= proof.requestedMutationGeneration
        }
        bindings.filter(_.mutation.phase == "after_capture").foreach { binding =>
          val proof = receiptById(binding.mutation.requestId)
          proof.publicationGeneration should not be Some(secondEvent.generation)
          proof.capturedMutationGeneration.get should be >= proof.requestedMutationGeneration
        }
        firstEvent.capturedMutationGeneration.get should be < receiptById(
          lateMutation.requestId
        ).requestedMutationGeneration
        secondEvent.capturedMutationGeneration.get should be < receiptById(
          mutations.find(_.phase == "after_capture").get.requestId
        ).requestedMutationGeneration
        val thirdEvent = publicationEvents.find(_.captureOrdinal == 3).get
        thirdEvent.capturedMutationGeneration.get should be >= receiptById(
          mutations.last.requestId
        ).requestedMutationGeneration
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
      } else commitsAfterReplay.size shouldBe commitsBeforeReplay + 1

      val practicalGate   = new CountDownLatch(1)
      val practicalReady  = new CountDownLatch(CohortCount)
      val practicalBefore = commitsAfterReplay.size
      val practicalRequests = (0 until CohortCount).map(index =>
        launch(
          spark,
          remoteUri,
          s"$arm-practical-$index",
          "practical_no_change",
          None,
          Some(practicalGate),
          Some(practicalReady)
        )
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
      } else practicalCommitDelta should be > 0

      val planFile = new File(fixture, "cohort-mutations.tsv")
      val planLines = mutations.map(m =>
        Seq(
          m.requestId,
          m.phase,
          m.shardIndex.toString,
          m.expectedVersion.toString,
          m.expectedOffset.toString,
          m.captureStage.toString,
          m.detail
        ).mkString("\t")
      )
      Files.write(planFile.toPath, planLines.mkString("\n").getBytes(StandardCharsets.UTF_8))
      val restoreEvidence = bindings
        .map(_.generation)
        .distinct
        .sorted
        .map { generation =>
          val event     = eventByGeneration(generation)
          val commit    = commitByGeneration(generation)
          val staged    = newDir(s"$arm-one-generation-$generation")
          val stagedUri = stageOneGeneration(spark, remoteUri, generation, staged, commit.archiveSha256)
          val dir       = new File(repoRoot, ".logs/checkpoint-matched-200-per-waiter")
          val evidence  = new File(dir, s"arm-$arm-generation-$generation-fresh-restore.json")
          val log       = new File(dir, s"arm-$arm-generation-$generation-fresh-restore.log")
          Files.createDirectories(dir.toPath)
          val childState     = newDir(s"$arm-child-$generation-state")
          val childWarehouse = newDir(s"$arm-child-$generation-warehouse")
          val java           = new File(System.getProperty("java.home"), "bin/java").getAbsolutePath
          val jvmArgs = ManagementFactory.getRuntimeMXBean.getInputArguments.asScala
            .filter(arg => arg.startsWith("--add-opens=") || arg.startsWith("--add-exports="))
          val process = new ProcessBuilder(
            (Seq(java) ++ jvmArgs ++ Seq(
              "-cp",
              System.getProperty("java.class.path"),
              "org.openivm.spark.common.rocksdb.OpenIvmStateSyncMatched200FreshRestoreProbe",
              childState.getAbsolutePath,
              childWarehouse.getAbsolutePath,
              stagedUri,
              generation,
              commit.archiveSha256,
              baselineProfileId,
              planFile.getAbsolutePath,
              event.captureOrdinal.toString,
              evidence.getAbsolutePath
            )): _*
          ).redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.appendTo(log)).start()
          process.waitFor(180, TimeUnit.SECONDS) shouldBe true
          if (process.exitValue() != 0) {
            val childOutput = Source.fromFile(log)
            val message =
              try childOutput.mkString
              finally childOutput.close()
            fail(s"Fresh restore JVM for $generation exited ${process.exitValue()}:\n$message")
          }
          evidence.isFile shouldBe true
          val childProof = Source.fromFile(evidence)
          val proofText =
            try childProof.mkString
            finally childProof.close()
          proofText.contains(s""""selected_generation":"$generation"""") shouldBe true
          proofText.contains("\"all_200_versions_and_offsets_verified\":true") shouldBe true
          GenerationRestoreEvidence(
            generation,
            event.captureOrdinal,
            commit.archiveSha256,
            evidence.getAbsolutePath,
            sha256(evidence)
          )
        }
        .toSeq
      restoreEvidence.size shouldBe bindings.map(_.generation).distinct.size
      val firstProof       = restoreEvidence.find(_.generation == firstEvent.generation).get
      val firstProofSource = Source.fromFile(new File(firstProof.evidencePath))
      val firstProofText =
        try firstProofSource.mkString
        finally firstProofSource.close()
      firstProofText.contains(s""""${lateMutation.requestId}"""") shouldBe true

      val lateRequestedGeneration = lateReceipt.map(_.requestedMutationGeneration).getOrElse(0L)
      val thirdEvent              = publicationEvents.find(_.captureOrdinal == 3).get
      writeArmEvidence(
        arm,
        checkpointEnabled,
        timings,
        publicationEvents,
        finalCommits,
        practicalTimings,
        (practicalEnd - practicalStart) / 1000000L,
        practicalCommitDelta,
        controlledGateWaitNanos.get() / 1000000L,
        receipts.asScala.toSeq,
        baselineProfileId,
        lateMutation.requestId,
        firstEvent.capturedMutationGeneration.getOrElse(0L),
        lateRequestedGeneration,
        secondEvent.capturedMutationGeneration.getOrElse(0L),
        olderCommitExcludedLate,
        secondCommitExcludedAfter,
        failureCommitsUnchanged,
        replayReceipt,
        bindings,
        restoreEvidence,
        burstStart,
        burstEnd
      )
      new File(repoRoot, s".logs/checkpoint-matched-200-per-waiter/arm-$arm.json").isFile shouldBe true
      baselineProfileId.nonEmpty shouldBe true
    }
  }

}
