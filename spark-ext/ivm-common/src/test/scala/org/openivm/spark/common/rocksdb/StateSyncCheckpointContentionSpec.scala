package org.openivm.spark.common.rocksdb

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.FeatureGate
import org.openivm.spark.telemetry.OpenIvmExecutionSpan
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.{File, IOException}
import java.nio.file.Files
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{
  Callable,
  CopyOnWriteArrayList,
  CountDownLatch,
  ExecutorService,
  Executors,
  Future,
  TimeUnit
}

private[rocksdb] object StateSyncCheckpointContentionSpec {
  final case class RootContext(round: String, scale: Int, index: Int, local: File, remote: File)
  final case class CallResult(
      requestId: String,
      round: String,
      scale: Int,
      rootIndex: Int,
      kind: String,
      elapsedNs: Long,
      outcome: String,
      errorClass: Option[String],
      commitsBefore: Int,
      commitsAfter: Int
  )
}

class StateSyncCheckpointContentionSpec extends AnyFunSuite with Matchers {
  import StateSyncCheckpointContentionSpec._
  private val currentRoot  = new ThreadLocal[RootContext]()
  private val currentPhase = new ThreadLocal[String]()
  private val output       = new File("../../.logs/state-sync-checkpoint-contention").getCanonicalFile

  private def await(latch: CountDownLatch): Unit =
    if (!latch.await(60, TimeUnit.SECONDS)) throw new IllegalStateException("checkpoint trial timed out")

  private def toVector[A](values: java.util.Collection[A]): Vector[A] = {
    val builder  = Vector.newBuilder[A]
    val iterator = values.iterator()
    while (iterator.hasNext) builder += iterator.next()
    builder.result()
  }

  private def deleteTree(file: File): Unit =
    if (file.exists()) {
      if (file.isDirectory) Option(file.listFiles()).foreach(_.foreach(deleteTree))
      Files.deleteIfExists(file.toPath)
    }

  private def withRoot[A](context: RootContext, phase: String)(body: => A): A = {
    val oldRoot  = currentRoot.get()
    val oldPhase = currentPhase.get()
    currentRoot.set(context)
    currentPhase.set(phase)
    try OpenIvmStateBarrier.withRootForTesting(context.local.toPath)(body)
    finally {
      if (oldRoot == null) currentRoot.remove() else currentRoot.set(oldRoot)
      if (oldPhase == null) currentPhase.remove() else currentPhase.set(oldPhase)
    }
  }

  private def writeDatabase(spark: SparkSession, context: RootContext, key: String, value: String): Unit =
    withRoot(context, "mutation") {
      OpenIvmStateBarrier.withMutation(spark) {
        val db = OpenIvmRocksDBRegistry.getOrOpen(
          spark,
          new File(context.local, "rocksdb").getAbsolutePath,
          Seq("meta")
        )
        db.withBatch { batch =>
          db.put(batch, "meta", RocksDBCodec.utf8(key), RocksDBCodec.utf8(value))
        }
      }
    }

  private def commitFiles(remote: File): Vector[File] = {
    val dir = new File(remote, ".openivm-state-sync/commits")
    Option(dir.listFiles()).toVector.flatten.filter(file => file.isFile && file.getName.endsWith(".commit")).toVector
  }

  private def committedArchive(context: RootContext, generation: String): File =
    new File(context.remote, s".openivm-state-sync/snapshots/$generation/snapshot.zip")

  private def submitCall(
      spark: SparkSession,
      executor: ExecutorService,
      context: RootContext,
      requestId: String,
      kind: String,
      started: CountDownLatch = new CountDownLatch(0)
  ): Future[CallResult] = executor.submit(new Callable[CallResult] {
    override def call(): CallResult = withRoot(context, kind) {
      val oldRequest = spark.sparkContext.getLocalProperty("openivm.request_id")
      spark.sparkContext.setLocalProperty("openivm.request_id", requestId)
      started.countDown()
      val commitsBefore = commitFiles(context.remote).size
      val start         = System.nanoTime()
      try {
        val outcome = OpenIvmStateSync.backupNowWithOutcome(spark)
        CallResult(
          requestId,
          context.round,
          context.scale,
          context.index,
          kind,
          System.nanoTime() - start,
          outcome.toString,
          None,
          commitsBefore,
          commitFiles(context.remote).size
        )
      } catch {
        case error: Throwable =>
          CallResult(
            requestId,
            context.round,
            context.scale,
            context.index,
            kind,
            System.nanoTime() - start,
            "failed",
            Some(error.getClass.getName),
            commitsBefore,
            commitFiles(context.remote).size
          )
      } finally spark.sparkContext.setLocalProperty("openivm.request_id", oldRequest)
    }
  })

  private def matchingRequest(
      events: Seq[OpenIvmExecutionSpan.StateSyncEvidence],
      requestId: String
  ): OpenIvmExecutionSpan.StateSyncRequestEvidence =
    events.iterator
      .flatMap(_.requests)
      .find(_.parentRequestId.contains(requestId))
      .getOrElse(
        fail(s"missing request evidence for $requestId")
      )

  private def assertDurablyCovered(
      context: RootContext,
      request: OpenIvmExecutionSpan.StateSyncRequestEvidence
  ): Unit = {
    request.status shouldBe "success"
    request.published shouldBe true
    request.acknowledgedSnapshotGenerationId shouldBe request.snapshotGenerationId
    val requested = request.requestedMutationGeneration.getOrElse(fail("missing requested generation"))
    val captured  = request.capturedMutationGeneration.getOrElse(fail("missing captured generation"))
    captured should be >= requested
    val generation = request.acknowledgedSnapshotGenerationId.getOrElse(fail("missing acknowledged generation"))
    commitFiles(context.remote).map(_.getName.stripSuffix(".commit")) should contain(generation)
    val archive = committedArchive(context, generation)
    archive.isFile shouldBe true
    archive.length() should be > 0L
  }

  private def percentile(values: Seq[Double], p: Double): Double =
    if (values.isEmpty) 0.0
    else {
      val sorted = values.sorted
      sorted(math.min(values.size - 1, math.ceil(p * values.size).toInt - 1))
    }

  test("counterbalanced 1/200/428-root synchronous backup and checkpoint contention") {
    deleteTree(output)
    val inputs = new File(output, "inputs")
    Files.createDirectories(inputs.toPath)
    val fallbackRemote            = new File(output, "fallback-remote")
    val evidence                  = new CopyOnWriteArrayList[OpenIvmExecutionSpan.StateSyncEvidence]()
    val calls                     = new CopyOnWriteArrayList[CallResult]()
    val uploadCount               = new AtomicInteger(0)
    val rootHook                  = (_: SparkSession) => Option(currentRoot.get()).map(_.remote.toURI.toString)
    var spark: SparkSession       = null
    var executor: ExecutorService = null
    val allRuns = scala.collection.mutable.ArrayBuffer.empty[
      (
          String,
          Int,
          Int,
          Int,
          Int,
          Vector[CallResult],
          Vector[OpenIvmExecutionSpan.StateSyncPublicationEvidence],
          Vector[OpenIvmExecutionSpan.StateSyncEvidence]
      )
    ]

    try {
      spark = SparkSession
        .builder()
        .master("local[4]")
        .appName("openivm-checkpoint-contention-qualification")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "localhost")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.warehouse.dir", new File(output, "warehouse").getAbsolutePath)
        .config(FeatureGate.StateSyncUriKey, fallbackRemote.toURI.toString)
        .config(FeatureGate.StateSyncRetainGenerationsKey, "10")
        .config(FeatureGate.ProfileRefreshKey, "true")
        .config("spark.openivm.rocksdb.multiProcess", "false")
        .getOrCreate()
      executor = Executors.newFixedThreadPool(48)
      OpenIvmExecutionSpan.setStateSyncEvidenceHookForTesting(value => evidence.add(value))
      OpenIvmStateSync.setStateSyncUriHookForTesting(rootHook)

      OpenIvmStateSync.setCopyLocalFileHookForTesting((fs: FileSystem, archive: File, path: Path) => {
        uploadCount.incrementAndGet()
        fs.copyFromLocalFile(false, true, new Path(archive.getAbsolutePath), path)
      })

      val counterbalanced = Seq("ascending" -> Seq(1, 200, 428), "descending" -> Seq(428, 200, 1))
      counterbalanced.foreach { case (round, scales) =>
        scales.foreach { scale =>
          val runDir = new File(inputs, s"$round-n$scale")
          val contexts = (0 until scale).map { index =>
            val root   = new File(runDir, f"root-$index%04d")
            val local  = new File(root, "local")
            val remote = new File(root, "remote")
            Files.createDirectories(local.toPath)
            Files.createDirectories(remote.toPath)
            RootContext(round, scale, index, local, remote)
          }.toVector

          contexts.foreach(context => writeDatabase(spark, context, "seed", s"${round}-${scale}-${context.index}"))
          OpenIvmRocksDBRegistry.closeAll()
          val evidenceStart = evidence.size()
          val callStart     = calls.size()
          uploadCount.set(0)

          val baseline = contexts
            .map { context =>
              val id = s"$round-n$scale-r${context.index}-baseline"
              submitCall(spark, executor, context, id, "baseline")
            }
            .map(_.get(180, TimeUnit.SECONDS))
          baseline.foreach(calls.add)
          baseline.foreach(_.outcome shouldBe OpenIvmStateSync.BackupDurablyPublished.toString)
          contexts.foreach(context => commitFiles(context.remote) should have size 1)

          val noChange = contexts
            .map { context =>
              val id = s"$round-n$scale-r${context.index}-nochange"
              submitCall(spark, executor, context, id, "no_change")
            }
            .map(_.get(180, TimeUnit.SECONDS))
          noChange.foreach(calls.add)
          noChange.foreach(_.outcome shouldBe OpenIvmStateSync.BackupDurablyPublished.toString)
          contexts.foreach(context => commitFiles(context.remote) should have size 1)

          val target = contexts.head
          writeDatabase(spark, target, "target-mutation", "included-by-captured-checkpoint")
          val captureEntered      = new CountDownLatch(1)
          val releaseCapture      = new CountDownLatch(1)
          val uploadEntered       = new CountDownLatch(1)
          val releaseUpload       = new CountDownLatch(1)
          val duringCount         = 1
          val afterCount          = 1
          val duringQueued        = new CountDownLatch(duringCount)
          val afterQueued         = new CountDownLatch(afterCount)
          val targetUploadBlocked = new AtomicBoolean(false)

          OpenIvmStateSync.setDuringCaptureHookForTesting(() => {
            captureEntered.countDown()
            await(releaseCapture)
          })
          OpenIvmStateSync.setCopyLocalFileHookForTesting((fs: FileSystem, archive: File, path: Path) => {
            uploadCount.incrementAndGet()
            if (targetUploadBlocked.compareAndSet(false, true)) {
              uploadEntered.countDown()
              await(releaseUpload)
            }
            fs.copyFromLocalFile(false, true, new Path(archive.getAbsolutePath), path)
          })
          OpenIvmStateSync.setBeforeSynchronousDrainHookForTesting(() =>
            currentPhase.get() match {
              case "target_during_capture" => duringQueued.countDown()
              case "target_after_capture"  => afterQueued.countDown()
              case _                       => ()
            }
          )

          val beforeId      = s"$round-n$scale-r0-target-before"
          val beforeStarted = new CountDownLatch(1)
          val beforeFuture  = submitCall(spark, executor, target, beforeId, "target_before_capture", beforeStarted)
          await(beforeStarted)
          await(captureEntered)
          val duringId      = s"$round-n$scale-r0-target-during"
          val duringStarted = new CountDownLatch(1)
          val duringFuture  = submitCall(spark, executor, target, duringId, "target_during_capture", duringStarted)
          await(duringStarted)
          releaseCapture.countDown()
          await(uploadEntered)
          val afterId      = s"$round-n$scale-r0-target-after"
          val afterStarted = new CountDownLatch(1)
          val afterFuture  = submitCall(spark, executor, target, afterId, "target_after_capture", afterStarted)
          await(afterStarted)
          await(duringQueued)
          await(afterQueued)
          releaseUpload.countDown()
          val arrivalCalls = Vector(
            beforeFuture.get(120, TimeUnit.SECONDS),
            duringFuture.get(120, TimeUnit.SECONDS),
            afterFuture.get(120, TimeUnit.SECONDS)
          )
          val targetRequestIds = Vector(beforeId, duringId, afterId)
          arrivalCalls.foreach { result =>
            calls.add(result)
            result.outcome shouldBe OpenIvmStateSync.BackupDurablyPublished.toString
          }
          OpenIvmStateSync.setDuringCaptureHookForTesting(null)
          OpenIvmStateSync.setBeforeSynchronousDrainHookForTesting(null)
          commitFiles(target.remote) should have size 2

          writeDatabase(spark, target, "same-destination-burst", "covered-by-one-capture")
          val burstCount   = 48
          val burstReady   = new CountDownLatch(burstCount)
          val releaseBurst = new CountDownLatch(1)
          OpenIvmStateSync.setBeforeSynchronousDrainHookForTesting(() =>
            if (currentPhase.get() == "same_destination_burst") {
              burstReady.countDown()
              await(releaseBurst)
            }
          )
          val burstRequestIds = (0 until burstCount).map(index => f"$round-n$scale-r0-burst-$index%02d").toVector
          val burstFutures =
            burstRequestIds.map(id => submitCall(spark, executor, target, id, "same_destination_burst"))
          await(burstReady)
          releaseBurst.countDown()
          val burstCalls = burstFutures.map(_.get(120, TimeUnit.SECONDS))
          burstCalls.foreach { result =>
            calls.add(result)
            result.outcome shouldBe OpenIvmStateSync.BackupDurablyPublished.toString
          }
          OpenIvmStateSync.setBeforeSynchronousDrainHookForTesting(null)
          commitFiles(target.remote) should have size 3

          val mutationBeforeFailure = commitFiles(target.remote).size
          writeDatabase(spark, target, "failure-mutation", "must-survive-failed-publication")
          OpenIvmStateSync.setBeforeSnapshotCommitHookForTesting(() =>
            throw new IOException("checkpoint-trial-injected-commit-failure")
          )
          val failureId = s"$round-n$scale-r0-failure"
          val failed    = submitCall(spark, executor, target, failureId, "injected_failure").get(120, TimeUnit.SECONDS)
          calls.add(failed)
          failed.outcome shouldBe "failed"
          failed.errorClass shouldBe Some(classOf[IOException].getName)
          commitFiles(target.remote).size shouldBe mutationBeforeFailure
          OpenIvmStateSync.setBeforeSnapshotCommitHookForTesting(null)

          val retryId = s"$round-n$scale-r0-retry"
          val retry   = submitCall(spark, executor, target, retryId, "retry_same_mutation").get(120, TimeUnit.SECONDS)
          calls.add(retry)
          retry.outcome shouldBe OpenIvmStateSync.BackupDurablyPublished.toString
          commitFiles(target.remote).size shouldBe mutationBeforeFailure + 1

          val runEvidence = toVector(evidence).drop(evidenceStart)
          val runCalls    = toVector(calls).drop(callStart)
          runCalls.size shouldBe 2 * scale + 53
          runCalls.filter(_.outcome == "failed") should have size 1
          val runPublications = runEvidence.flatMap(_.publications)
          runPublications should have size scale + 4
          runPublications.foreach(_.counts.getOrElse("captured_databases", None) shouldBe Some(1L))
          val successfulPublications = runPublications.count(_.published)
          successfulPublications shouldBe scale + 3
          val checkpoints = runPublications.map(_.counts.getOrElse("captured_databases", None).getOrElse(0L)).sum
          checkpoints shouldBe scale + 4
          uploadCount.get() shouldBe scale + 4
          contexts.map(context => commitFiles(context.remote).size).sum shouldBe scale + 3

          runCalls.foreach { result =>
            val context = contexts(result.rootIndex)
            val request = matchingRequest(runEvidence, result.requestId)
            if (result.outcome == "failed") {
              request.status shouldBe "failed"
              request.published shouldBe false
              request.acknowledgedSnapshotGenerationId shouldBe empty
              result.commitsBefore shouldBe mutationBeforeFailure
              result.commitsAfter shouldBe mutationBeforeFailure
            } else assertDurablyCovered(context, request)
          }
          val targetRequests = targetRequestIds.map(matchingRequest(runEvidence, _))
          targetRequests.map(_.publicationId).distinct should have length 1
          targetRequests.map(_.capturedMutationGeneration).distinct should have length 1
          targetRequests.map(_.acknowledgedSnapshotGenerationId).distinct should have length 1
          targetRequests.foreach { request =>
            request.capturedMutationGeneration shouldBe request.requestedMutationGeneration
            request.published shouldBe true
          }
          val burstRequests = burstRequestIds.map(matchingRequest(runEvidence, _))
          burstRequests.map(_.publicationId).distinct should have length 1
          burstRequests.map(_.capturedMutationGeneration).distinct should have length 1
          burstRequests.map(_.acknowledgedSnapshotGenerationId).distinct should have length 1
          burstRequests.foreach { request =>
            request.capturedMutationGeneration shouldBe request.requestedMutationGeneration
            request.published shouldBe true
          }
          val burstPublicationId = burstRequests.head.publicationId
          runPublications.count(_.publicationId == burstPublicationId) shouldBe 1
          val targetPublicationId = matchingRequest(runEvidence, beforeId).publicationId
          val targetPublication   = runPublications.find(_.publicationId == targetPublicationId).get
          targetPublication.phases.find(_.operation == "spark.backup.capture").get.durationNs should be > 0L
          targetPublication.phases.find(_.operation == "spark.backup.pack_hash").get.durationNs should be > 0L
          targetPublication.phases.find(_.operation == "spark.backup.upload").get.durationNs should be > 0L
          targetPublication.phases.find(_.operation == "spark.backup.commit").get.durationNs should be > 0L

          allRuns += ((
            round,
            scale,
            checkpoints.toInt,
            successfulPublications,
            uploadCount.get(),
            runCalls,
            runPublications,
            runEvidence
          ))
          OpenIvmRocksDBRegistry.closeAll()
          deleteTree(runDir)
        }
      }

      val finalEvents = toVector(evidence)
      val mapper      = new ObjectMapper()
      val root        = mapper.createObjectNode()
      root.put("schema_version", 1)
      root.put("source_head", "6bb1353a62ef43b4490aae68067c4fbe17bfab58")
      root.put("environment", "pinned devcontainer; Spark 3.5; local filesystem only")
      root.put("counterbalance", "ascending 1/200/428 then descending 428/200/1; fresh real RocksDB roots each round")
      root.put("max_parallel_root_calls", 48)
      root.put("remote_latency_measured", false)
      val runs = root.putArray("runs")
      allRuns.foreach { case (round, scale, checkpoints, publications, uploads, runCalls, runPubs, runEvidence) =>
        val run = runs.addObject()
        run.put("round", round)
        run.put("root_count", scale)
        run.put("actual_checkpoint_attempts", checkpoints)
        run.put("committed_publications", publications)
        run.put("archive_upload_calls", uploads)
        run.put("no_change_calls", scale)
        val requestRows = run.putArray("requests")
        runCalls.foreach { result =>
          val request = matchingRequest(runEvidence, result.requestId)
          val row     = requestRows.addObject()
          row.put("request_id", result.requestId)
          row.put("root_index", result.rootIndex)
          row.put("classification", result.kind)
          row.put("elapsed_ms", result.elapsedNs / 1000000.0)
          row.put("outcome", result.outcome)
          row.put("committed_before", result.commitsBefore)
          row.put("committed_after", result.commitsAfter)
          result.errorClass.foreach(row.put("error_class", _))
          row.put("requested_generation", request.requestedMutationGeneration.getOrElse(-1L))
          row.put("captured_generation", request.capturedMutationGeneration.getOrElse(-1L))
          row.put("acknowledged_generation", request.acknowledgedSnapshotGenerationId.getOrElse(""))
          row.put("publication_id", request.publicationId)
          row.put("status", request.status)
          row.put("published", request.published)
        }
        val publicationRows = run.putArray("publication_phases")
        runPubs.foreach { publication =>
          val row = publicationRows.addObject()
          row.put("publication_id", publication.publicationId)
          row.put("status", publication.status)
          row.put("captured_databases", publication.counts.getOrElse("captured_databases", None).getOrElse(0L))
          row.put("archive_bytes", publication.counts.getOrElse("archive_bytes", None).getOrElse(0L))
          row.put("uploaded_bytes", publication.counts.getOrElse("uploaded_bytes", None).getOrElse(0L))
          val phases = row.putArray("phases")
          publication.phases.foreach { phase =>
            val phaseRow = phases.addObject()
            phaseRow.put("name", phase.operation)
            phaseRow.put("duration_ms", phase.durationNs / 1000000.0)
            phaseRow.put("status", phase.status)
          }
        }
        val successfulMs = runCalls.filter(_.outcome != "failed").map(_.elapsedNs / 1000000.0)
        run.put("request_p50_ms", percentile(successfulMs, 0.50))
        run.put("request_p95_ms", percentile(successfulMs, 0.95))
      }
      Files.createDirectories(output.toPath)
      Files.write(
        new File(output, "checkpoint-contention-results.json").toPath,
        mapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(root)
      )
      assert(finalEvents.nonEmpty)
    } finally {
      OpenIvmStateSync.setDuringCaptureHookForTesting(null)
      OpenIvmStateSync.setBeforeSynchronousDrainHookForTesting(null)
      OpenIvmStateSync.setBeforeSnapshotCommitHookForTesting(null)
      OpenIvmStateSync.setCopyLocalFileHookForTesting(null)
      OpenIvmStateSync.setStateSyncUriHookForTesting(null)
      OpenIvmExecutionSpan.setStateSyncEvidenceHookForTesting(null)
      if (executor != null) {
        executor.shutdownNow()
        executor.awaitTermination(30, TimeUnit.SECONDS)
      }
      OpenIvmRocksDBRegistry.closeAll()
      OpenIvmStateSync.resetForTesting()
      if (spark != null) spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      deleteTree(inputs)
    }
  }
}
