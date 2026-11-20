package org.openivm.spark.common.rocksdb

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.FeatureGate
import org.openivm.spark.telemetry.OpenIvmExecutionSpan
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.{File, IOException}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
import java.util.concurrent.{Callable, CopyOnWriteArrayList, CountDownLatch, ExecutorService, Executors, Future, TimeUnit}
import java.util.zip.ZipFile
import scala.collection.JavaConverters._

private[rocksdb] object StateSyncSingleDestinationExperimentSpec {
  final case class Context(label: String, scale: Int, local: File, remote: File)
  final case class Result(
      requestId: String,
      kind: String,
      invokedAtNs: Long,
      elapsedNs: Long,
      outcome: String,
      errorClass: Option[String],
      commitsBefore: Vector[String],
      commitsAfter: Vector[String],
      archivesAfter: Vector[String]
  )
  final class Gate(count: Int) {
    val entered = new CountDownLatch(count)
    val release = new CountDownLatch(1)
  }
}

class StateSyncSingleDestinationExperimentSpec extends AnyFunSuite with Matchers {
  import StateSyncSingleDestinationExperimentSpec._

  private val currentContext = new ThreadLocal[Context]()
  private val currentKind    = new ThreadLocal[String]()
  private val drainGates     = new java.util.concurrent.ConcurrentHashMap[String, Gate]()
  private val repositoryRoot = {
    var current = new File(sys.props("user.dir")).getCanonicalFile
    while (current != null && !new File(current, "nx.json").isFile) current = current.getParentFile
    Option(current).getOrElse(new File(sys.props("user.dir")).getCanonicalFile)
  }
  private val output = new File(repositoryRoot, ".logs/state-sync-single-destination")

  private def await(latch: CountDownLatch): Unit =
    if (!latch.await(120, TimeUnit.SECONDS)) throw new IllegalStateException("single-destination experiment timed out")

  private def deleteTree(file: File): Unit =
    if (file.exists()) {
      if (file.isDirectory) Option(file.listFiles()).foreach(_.foreach(deleteTree))
      Files.deleteIfExists(file.toPath)
    }

  private def withContext[A](context: Context, kind: String)(body: => A): A = {
    val oldContext = currentContext.get()
    val oldKind    = currentKind.get()
    currentContext.set(context)
    currentKind.set(kind)
    try OpenIvmStateBarrier.withRootForTesting(context.local.toPath)(body)
    finally {
      if (oldContext == null) currentContext.remove() else currentContext.set(oldContext)
      if (oldKind == null) currentKind.remove() else currentKind.set(oldKind)
    }
  }

  private def writeDatabase(
      spark: SparkSession,
      context: Context,
      database: Int,
      key: String,
      value: String
  ): Unit = withContext(context, "mutation") {
    OpenIvmStateBarrier.withMutation(spark) {
      val dbPath = new File(new File(context.local, f"db-$database%04d"), "rocksdb")
      Files.createDirectories(dbPath.getParentFile.toPath)
      val db = OpenIvmRocksDBRegistry.getOrOpen(spark, dbPath.getAbsolutePath, Seq("meta"))
      db.withBatch { batch =>
        db.put(batch, "meta", RocksDBCodec.utf8(key), RocksDBCodec.utf8(value))
      }
    }
  }

  private def readValue(spark: SparkSession, context: Context, database: Int, key: String): Option[String] =
    withContext(context, "restore-verify") {
      val dbPath = new File(new File(context.local, f"db-$database%04d"), "rocksdb")
      OpenIvmRocksDBRegistry
        .getOrOpen(spark, dbPath.getAbsolutePath, Seq("meta"))
        .get("meta", RocksDBCodec.utf8(key))
        .map(bytes => new String(bytes, StandardCharsets.UTF_8))
    }

  private def commitFiles(remote: File): Vector[File] = {
    val dir = new File(remote, ".openivm-state-sync/commits")
    Option(dir.listFiles()).toVector.flatten
      .filter(file => file.isFile && file.getName.endsWith(".commit"))
      .sortBy(_.getName)
  }

  private def generation(file: File): String = file.getName.stripSuffix(".commit")

  private def archive(remote: File, snapshotGeneration: String): File =
    new File(remote, s".openivm-state-sync/snapshots/$snapshotGeneration/snapshot.zip")

  private def awaitResults(futures: Vector[Future[Result]]): Vector[Result] =
    futures.map(_.get(300, TimeUnit.SECONDS))

  private def startCohort(
      spark: SparkSession,
      executor: ExecutorService,
      context: Context,
      kind: String,
      count: Int
  ): Vector[Future[Result]] = {
    val ready   = new CountDownLatch(count)
    val release = new CountDownLatch(1)
    val entered = new CountDownLatch(count)
    val futures = (0 until count).map { index =>
      val requestId = f"${context.label}-n${context.scale}-$kind-$index%02d"
      executor.submit(new Callable[Result] {
        override def call(): Result = withContext(context, kind) {
          val oldRequest = spark.sparkContext.getLocalProperty("openivm.request_id")
          spark.sparkContext.setLocalProperty("openivm.request_id", requestId)
          try {
            ready.countDown()
            await(release)
            val invokedAt = System.nanoTime()
            entered.countDown()
            val before = commitFiles(context.remote).map(generation)
            val started = System.nanoTime()
            try {
              OpenIvmStateSync.backupNow(spark)
              val after = commitFiles(context.remote).map(generation)
              Result(
                requestId,
                kind,
                invokedAt,
                System.nanoTime() - started,
                "success",
                None,
                before,
                after,
                after.filter(id => archive(context.remote, id).isFile)
              )
            } catch {
              case error: Throwable =>
                val after = commitFiles(context.remote).map(generation)
                Result(
                  requestId,
                  kind,
                  invokedAt,
                  System.nanoTime() - started,
                  "failed",
                  Some(error.getClass.getName),
                  before,
                  after,
                  after.filter(id => archive(context.remote, id).isFile)
                )
            }
          } finally spark.sparkContext.setLocalProperty("openivm.request_id", oldRequest)
        }
      })
    }.toVector
    await(ready)
    release.countDown()
    await(entered)
    futures
  }

  private def awaitDrainGate(kind: String, gate: Gate): Unit = {
    await(gate.entered)
    gate.release.countDown()
    drainGates.remove(kind)
  }

  private def matchingRequest(
      events: Seq[OpenIvmExecutionSpan.StateSyncEvidence],
      requestId: String
  ): OpenIvmExecutionSpan.StateSyncRequestEvidence =
    events.iterator
      .flatMap(_.requests)
      .find(request => request.parentRequestId.contains(requestId) || request.requestId == requestId)
      .getOrElse(fail(s"missing state-sync request receipt for $requestId"))

  private def reflectedGeneration(value: AnyRef, method: String): Option[Long] =
    try value.getClass.getMethod(method).invoke(value).asInstanceOf[Option[Long]]
    catch { case _: NoSuchMethodException => None }

  private def assertDurablyCovered(
      request: OpenIvmExecutionSpan.StateSyncRequestEvidence,
      result: Result
  ): Unit = {
    request.status shouldBe "success"
    request.published shouldBe true
    request.acknowledgedSnapshotGenerationId shouldBe request.snapshotGenerationId
    val snapshot = request.acknowledgedSnapshotGenerationId.getOrElse(fail("missing acknowledged snapshot generation"))
    result.commitsAfter should contain(snapshot)
    result.archivesAfter should contain(snapshot)
    val requested = reflectedGeneration(request, "requestedMutationGeneration")
    val captured  = reflectedGeneration(request, "capturedMutationGeneration")
    requested.foreach { required =>
      val actual = captured.getOrElse(fail("candidate omitted captured mutation generation"))
      actual should be >= required
    }
  }

  private def addOption(node: com.fasterxml.jackson.databind.node.ObjectNode, key: String, value: Option[String]): Unit =
    value match {
      case Some(found) => node.put(key, found)
      case None        => node.putNull(key)
    }

  test("single-destination original-versus-candidate checkpoint workload with mixed synchronous requests") {
    val variant = sys.env.getOrElse("OPENIVM_EXPERIMENT_VARIANT", "candidate")
    val order   = sys.env.getOrElse("OPENIVM_EXPERIMENT_ORDER", "ascending")
    val label   = sys.env.getOrElse("OPENIVM_EXPERIMENT_LABEL", s"$variant-$order")
    val source  = sys.env.getOrElse("OPENIVM_SOURCE_HEAD", "unknown")
    variant should (be("original") or be("candidate"))
    order should (be("ascending") or be("descending"))
    label.matches("[A-Za-z0-9._-]{1,80}") shouldBe true

    val evidence = new CopyOnWriteArrayList[OpenIvmExecutionSpan.StateSyncEvidence]()
    val mapper   = new ObjectMapper()
    var spark: SparkSession       = null
    var executor: ExecutorService = null
    var completed                 = false
    val allRuns                   = scala.collection.mutable.ArrayBuffer.empty[com.fasterxml.jackson.databind.node.ObjectNode]

    try {
      Files.createDirectories(output.toPath)
      val localInputRoot = new File(output, s"inputs/$label")
      Files.createDirectories(localInputRoot.toPath)
      spark = SparkSession
        .builder()
        .master("local[4]")
        .appName("openivm-single-destination-state-sync-ablation")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "localhost")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.warehouse.dir", new File(output, s"warehouse-$label").getAbsolutePath)
        .config(FeatureGate.StateSyncUriKey, new File(output, "unused-fallback").toURI.toString)
        .config(FeatureGate.StateSyncRetainGenerationsKey, "2")
        .config(FeatureGate.ProfileRefreshKey, "true")
        .config("spark.openivm.rocksdb.multiProcess", "false")
        .getOrCreate()
      executor = Executors.newFixedThreadPool(48)
      OpenIvmExecutionSpan.setStateSyncEvidenceHookForTesting(value => evidence.add(value))
      OpenIvmStateSync.setStateSyncUriHookForTesting(_ => Option(currentContext.get()).map(_.remote.toURI.toString))
      OpenIvmStateSync.setBeforeSynchronousDrainHookForTesting(() => {
        val kind = currentKind.get()
        if (kind != null) {
          val gate = drainGates.get(kind)
          if (gate != null) {
            gate.entered.countDown()
            await(gate.release)
          }
        }
      })

      val scales = if (order == "ascending") Seq(1, 200, 428) else Seq(428, 200, 1)
      val remote = new File(output, s"destinations/$label/single")
      if (remote.exists() && Option(remote.listFiles()).exists(_.nonEmpty))
        fail(s"refusing to reuse a non-empty remote destination: $remote")
      scales.zipWithIndex.foreach { case (scale, scaleIndex) =>
        val local = new File(localInputRoot, s"n$scale/local")
        if (scaleIndex > 0) deleteTree(remote)
        Files.createDirectories(local.toPath)
        Files.createDirectories(remote.toPath)
        val context     = Context(label, scale, local, remote)
        val eventStart  = evidence.size()
        val uploads     = new AtomicInteger(0)
        val calls       = scala.collection.mutable.ArrayBuffer.empty[Result]
        (0 until scale).foreach(index => writeDatabase(spark, context, index, "seed", f"seed-$index%04d"))
        OpenIvmRocksDBRegistry.closeAll()

        val initial = awaitResults(startCohort(spark, executor, context, "initial", 1))
        calls ++= initial
        initial.head.outcome shouldBe "success"
        commitFiles(remote) should have size 1

        val beforeGate       = new Gate(8)
        val preCaptureEntered = new CountDownLatch(1)
        val releasePreCapture = new CountDownLatch(1)
        val firstPreCapture  = new AtomicBoolean(false)
        OpenIvmStateSync.setBeforeCaptureHookForTesting(() =>
          if (firstPreCapture.compareAndSet(false, true)) {
            preCaptureEntered.countDown()
            await(releasePreCapture)
          }
        )
        val captureEntered = new CountDownLatch(1)
        val releaseCapture = new CountDownLatch(1)
        val firstCapture   = new AtomicBoolean(false)
        val captureHoldNs  = new AtomicLong(0L)
        OpenIvmStateSync.setDuringCaptureHookForTesting(() =>
          if (firstCapture.compareAndSet(false, true)) {
            val started = System.nanoTime()
            captureEntered.countDown()
            await(releaseCapture)
            captureHoldNs.set(System.nanoTime() - started)
          }
        )
        val uploadEntered = new CountDownLatch(1)
        val releaseUpload = new CountDownLatch(1)
        val firstUpload   = new AtomicBoolean(false)
        val uploadHoldNs  = new AtomicLong(0L)
        OpenIvmStateSync.setCopyLocalFileHookForTesting((fs: FileSystem, file: File, path: Path) => {
          uploads.incrementAndGet()
          if (firstUpload.compareAndSet(false, true)) {
            val started = System.nanoTime()
            uploadEntered.countDown()
            await(releaseUpload)
            uploadHoldNs.set(System.nanoTime() - started)
          }
          fs.copyFromLocalFile(false, true, new Path(file.getAbsolutePath), path)
        })

        writeDatabase(spark, context, 0, "before-capture", "must-be-covered-by-first-mixed-publication")
        OpenIvmRocksDBRegistry.closeAll()
        drainGates.put("before", beforeGate)
        val beforeFutures = startCohort(spark, executor, context, "before", 8)
        awaitDrainGate("before", beforeGate)
        await(preCaptureEntered)
        OpenIvmStateSync.setBeforeCaptureHookForTesting(null)
        releasePreCapture.countDown()
        await(captureEntered)
        OpenIvmStateSync.setDuringCaptureHookForTesting(null)
        val mutationStarted = new CountDownLatch(1)
        val mutationStartedAt = new AtomicLong(0L)
        val blockedMutation = executor.submit(new Callable[Long] {
          override def call(): Long = withContext(context, "blocked-during-capture") {
            mutationStartedAt.set(System.nanoTime())
            mutationStarted.countDown()
            val started = System.nanoTime()
            writeDatabase(spark, context, 0, "mutation-during-capture", "must-run-after-capture-barrier")
            System.nanoTime() - started
          }
        })
        await(mutationStarted)
        Thread.sleep(100)
        blockedMutation.isDone shouldBe false

        val duringGate    = new Gate(8)
        drainGates.put("during", duringGate)
        val duringFutures = startCohort(spark, executor, context, "during", 8)
        releaseCapture.countDown()
        awaitDrainGate("during", duringGate)
        val blockedMutationElapsedNs = blockedMutation.get(120, TimeUnit.SECONDS)
        await(uploadEntered)
        OpenIvmStateSync.setCopyLocalFileHookForTesting(null)

        val afterGate    = new Gate(8)
        drainGates.put("after", afterGate)
        val afterFutures = startCohort(spark, executor, context, "after", 8)
        awaitDrainGate("after", afterGate)
        writeDatabase(spark, context, 0, "after-capture", "must-force-a-later-publication")
        OpenIvmRocksDBRegistry.closeAll()
        val changedGate    = new Gate(8)
        drainGates.put("changed", changedGate)
        val changedFutures = startCohort(spark, executor, context, "changed", 8)
        awaitDrainGate("changed", changedGate)
        releaseUpload.countDown()
        val mixedResults = awaitResults(beforeFutures ++ duringFutures ++ afterFutures ++ changedFutures)
        calls ++= mixedResults
        mixedResults.foreach(_.outcome shouldBe "success")
        OpenIvmStateSync.setBeforeSynchronousDrainHookForTesting(null)
        OpenIvmStateSync.setDuringCaptureHookForTesting(null)
        OpenIvmStateSync.setCopyLocalFileHookForTesting(null)

        val beforeNoChangePublications = evidence.asScala.toVector.drop(eventStart).flatMap(_.publications).size
        val noChangeFutures = startCohort(spark, executor, context, "no_change", 16)
        val noChangeResults = awaitResults(noChangeFutures)
        calls ++= noChangeResults
        noChangeResults.foreach(_.outcome shouldBe "success")
        val afterNoChangePublications = evidence.asScala.toVector.drop(eventStart).flatMap(_.publications).size
        val noChangeNewPublications   = afterNoChangePublications - beforeNoChangePublications
        if (variant == "candidate") noChangeNewPublications shouldBe 0

        val oldCommits = commitFiles(remote).map(generation)
        oldCommits should not be empty
        oldCommits.foreach(id => archive(remote, id).isFile shouldBe true)
        writeDatabase(spark, context, 0, "failure-mutation", "must-survive-failed-publication-and-retry")
        OpenIvmRocksDBRegistry.closeAll()
        OpenIvmStateSync.setBeforeSnapshotCommitHookForTesting(() => {
          commitFiles(remote).map(generation) shouldBe oldCommits
          oldCommits.foreach(id => archive(remote, id).isFile shouldBe true)
          throw new IOException("single-destination-injected-commit-failure")
        })
        val failure = awaitResults(startCohort(spark, executor, context, "injected_failure", 1)).head
        calls += failure
        failure.outcome shouldBe "failed"
        failure.errorClass shouldBe Some(classOf[IOException].getName)
        commitFiles(remote).map(generation) shouldBe oldCommits
        oldCommits.foreach(id => archive(remote, id).isFile shouldBe true)

        val retryCommitEntered = new CountDownLatch(1)
        val releaseRetryCommit = new CountDownLatch(1)
        OpenIvmStateSync.setBeforeSnapshotCommitHookForTesting(() => {
          commitFiles(remote).map(generation) shouldBe oldCommits
          oldCommits.foreach(id => archive(remote, id).isFile shouldBe true)
          retryCommitEntered.countDown()
          await(releaseRetryCommit)
        })
        val retryFutures = startCohort(spark, executor, context, "retry", 1)
        await(retryCommitEntered)
        commitFiles(remote).map(generation) shouldBe oldCommits
        oldCommits.foreach(id => archive(remote, id).isFile shouldBe true)
        releaseRetryCommit.countDown()
        val retry = awaitResults(retryFutures).head
        calls += retry
        retry.outcome shouldBe "success"
        OpenIvmStateSync.setBeforeSnapshotCommitHookForTesting(null)
        val retryRequest = matchingRequest(evidence.asScala.toVector.drop(eventStart), retry.requestId)
        assertDurablyCovered(retryRequest, retry)
        commitFiles(remote).size should be <= 2

        val runEvidence = evidence.asScala.toVector.drop(eventStart)
        val publications = runEvidence.flatMap(_.publications)
        publications.foreach { publication =>
          publication.counts.getOrElse("captured_databases", None) shouldBe Some(scale.toLong)
        }
        publications.count(_.published) shouldBe publications.size - 1
        val capturedDatabaseAttempts = publications.map(_.counts.getOrElse("captured_databases", None).getOrElse(0L)).sum
        uploads.get() shouldBe publications.size
        calls.size shouldBe 51
        val requestById = calls.map(result => result.requestId -> matchingRequest(runEvidence, result.requestId)).toMap
        requestById.values.count(_.status == "failed") shouldBe 1
        val failedReceipt = requestById(failure.requestId)
        failedReceipt.published shouldBe false
        failedReceipt.acknowledgedSnapshotGenerationId shouldBe empty
        failedReceipt.status shouldBe "failed"
        calls.filter(_.outcome == "success").foreach(result => assertDurablyCovered(requestById(result.requestId), result))

        val stableCalls = calls.filter(_.kind == "before").toVector
        val stable = stableCalls.map(call => requestById(call.requestId))
        stable should have size 8
        val duringCalls = calls.filter(_.kind == "during").toVector
        val during = duringCalls.map(call => requestById(call.requestId))
        during should have size 8
        val afterCalls = calls.filter(_.kind == "after").toVector
        val afterCapture = afterCalls.map(call => requestById(call.requestId))
        afterCapture should have size 8
        val changedCalls = calls.filter(_.kind == "changed").toVector
        val changed = changedCalls.map(call => requestById(call.requestId))
        changed should have size 8
        val noChangeCalls = calls.filter(_.kind == "no_change").toVector
        val noChange = noChangeCalls.map(call => requestById(call.requestId))
        noChange should have size 16
        val generationProof = stable.headOption.flatMap(reflectedGeneration(_, "requestedMutationGeneration")).isDefined
        if (variant == "candidate") {
          generationProof shouldBe true
          stable.map(_.publicationId).distinct should have length 1
          afterCapture.map(_.publicationId).distinct should have length 1
          changed.map(_.publicationId).distinct should have length 1
          afterCapture.head.publicationId shouldBe changed.head.publicationId
          afterCapture.head.publicationId should not be stable.head.publicationId
          noChange.map(_.publicationId).distinct should have length 1
          val capturedStable = reflectedGeneration(stable.head, "capturedMutationGeneration").get
          during.foreach { request =>
            val requested = reflectedGeneration(request, "requestedMutationGeneration").get
            if (requested <= capturedStable) request.publicationId shouldBe stable.head.publicationId
            else request.publicationId should not be stable.head.publicationId
            reflectedGeneration(request, "capturedMutationGeneration").get should be >= requested
          }
          during.map(_.publicationId).distinct.size should be <= 2
          val requestedLate = (afterCapture ++ changed).map(reflectedGeneration(_, "requestedMutationGeneration").get)
          requestedLate.foreach(_ should be > capturedStable)
          val capturedLate = (afterCapture ++ changed).map(reflectedGeneration(_, "capturedMutationGeneration").get)
          capturedLate.foreach(_ should be >= capturedStable)
          (afterCapture ++ changed).map(_.acknowledgedSnapshotGenerationId).distinct should have length 1
          noChange.map(_.acknowledgedSnapshotGenerationId).distinct shouldBe
            (afterCapture ++ changed).map(_.acknowledgedSnapshotGenerationId).distinct
        }

        val firstStablePublication = publications.find(_.publicationId == stable.head.publicationId).get
        val capturePhase = firstStablePublication.phases.find(_.operation == "spark.backup.capture").get
        val uploadPhase = firstStablePublication.phases.find(_.operation == "spark.backup.upload").get
        val captureEnd = capturePhase.startMonotonicNs + capturePhase.durationNs
        val uploadEnd  = uploadPhase.startMonotonicNs + uploadPhase.durationNs
        duringCalls.foreach { call =>
          call.invokedAtNs should be >= capturePhase.startMonotonicNs
          call.invokedAtNs should be < captureEnd
        }
        afterCalls.foreach { call =>
          call.invokedAtNs should be >= captureEnd
          call.invokedAtNs should be < uploadEnd
        }
        changedCalls.foreach { call =>
          call.invokedAtNs should be >= captureEnd
          call.invokedAtNs should be < uploadEnd
        }

        val latestCommit = commitFiles(remote).last
        val latestGeneration = generation(latestCommit)
        val markerLines = Files.readAllLines(latestCommit.toPath, StandardCharsets.UTF_8).asScala
        markerLines.head shouldBe "OPENIVM_STATE_SNAPSHOT_V2"
        val latestArchive = archive(remote, latestGeneration)
        latestArchive.isFile shouldBe true
        val zip = new ZipFile(latestArchive)
        val archiveDbCount = try {
          val entries = zip.entries()
          var count = 0
          while (entries.hasMoreElements) {
            if (entries.nextElement().getName.endsWith("/rocksdb/CURRENT")) count += 1
          }
          count
        } finally zip.close()
        archiveDbCount shouldBe scale

        OpenIvmRocksDBRegistry.closeAll()
        deleteTree(local)
        OpenIvmStateSync.resetForTesting()
        OpenIvmStateSync.setStateSyncUriHookForTesting(_ => Option(currentContext.get()).map(_.remote.toURI.toString))
        OpenIvmExecutionSpan.setStateSyncEvidenceHookForTesting(value => evidence.add(value))
        withContext(context, "restore") { OpenIvmStateSync.maybeRestore(spark) }
        (0 until scale).foreach { index =>
          readValue(spark, context, index, "seed") shouldBe Some(f"seed-$index%04d")
        }
        readValue(spark, context, 0, "before-capture") shouldBe Some("must-be-covered-by-first-mixed-publication")
        readValue(spark, context, 0, "mutation-during-capture") shouldBe Some("must-run-after-capture-barrier")
        readValue(spark, context, 0, "after-capture") shouldBe Some("must-force-a-later-publication")
        readValue(spark, context, 0, "failure-mutation") shouldBe Some("must-survive-failed-publication-and-retry")
        OpenIvmRocksDBRegistry.closeAll()

        val run = mapper.createObjectNode()
        run.put("order", order)
        run.put("scale_roots", scale)
        run.put("populated_database_roots", scale)
        run.put("remote_destination", remote.toURI.toString)
        run.put("requests", calls.size)
        run.put("actual_checkpoint_attempts", capturedDatabaseAttempts)
        run.put("publication_attempts", publications.size)
        run.put("committed_publications", publications.count(_.published))
        run.put("archive_upload_calls", uploads.get())
        run.put("no_change_calls", 16)
        run.put("no_change_new_publications", noChangeNewPublications)
        run.put("final_committed_generations_after_retention", commitFiles(remote).size)
        run.put("retention_generations", 2)
        run.put("v2_archive_database_roots", archiveDbCount)
        run.put("restored_database_roots", scale)
        run.put("capture_test_gate_hold_ms", captureHoldNs.get() / 1000000.0)
        run.put("upload_test_gate_hold_ms", uploadHoldNs.get() / 1000000.0)
        run.put("mutation_attempted_during_capture_blocked_until_capture_finished", true)
        run.put("blocked_mutation_elapsed_ms", blockedMutationElapsedNs / 1000000.0)
        run.put("coverage_generation_proof_available", generationProof)
        run.put("failure_old_commits_preserved_before_and_after_failure", true)
        run.put("old_commits_present_at_retry_precommit_gate", true)
        val requestRows = run.putArray("request_results")
        calls.foreach { result =>
          val receipt = matchingRequest(runEvidence, result.requestId)
          val row = requestRows.addObject()
          row.put("request_id", result.requestId)
          row.put("class", result.kind)
          row.put("invoked_monotonic_ns", result.invokedAtNs)
          row.put("elapsed_ms", result.elapsedNs / 1000000.0)
          row.put("outcome", result.outcome)
          result.errorClass.foreach(row.put("error_class", _))
          row.put("commits_before", result.commitsBefore.size)
          row.put("commits_after", result.commitsAfter.size)
          row.put("acknowledged_archive_present_at_return", result.archivesAfter.contains(receipt.acknowledgedSnapshotGenerationId.getOrElse("")))
          row.put("publication_id", receipt.publicationId)
          row.put("status", receipt.status)
          row.put("published", receipt.published)
          addOption(row, "snapshot_generation_id", receipt.snapshotGenerationId)
          addOption(row, "acknowledged_generation_id", receipt.acknowledgedSnapshotGenerationId)
          reflectedGeneration(receipt, "requestedMutationGeneration") match {
            case Some(value) => row.put("requested_mutation_generation", value)
            case None        => row.putNull("requested_mutation_generation")
          }
          reflectedGeneration(receipt, "capturedMutationGeneration") match {
            case Some(value) => row.put("captured_mutation_generation", value)
            case None        => row.putNull("captured_mutation_generation")
          }
        }
        val publicationRows = run.putArray("publications")
        publications.foreach { publication =>
          val row = publicationRows.addObject()
          row.put("publication_id", publication.publicationId)
          row.put("status", publication.status)
          row.put("published", publication.published)
          row.put("captured_databases", publication.counts.getOrElse("captured_databases", None).getOrElse(0L))
          row.put("archive_bytes", publication.counts.getOrElse("archive_bytes", None).getOrElse(0L))
          row.put("uploaded_bytes", publication.counts.getOrElse("uploaded_bytes", None).getOrElse(0L))
          addOption(row, "snapshot_generation_id", publication.snapshotGenerationId)
          reflectedGeneration(publication, "requestedMutationGeneration") match {
            case Some(value) => row.put("requested_mutation_generation", value)
            case None        => row.putNull("requested_mutation_generation")
          }
          reflectedGeneration(publication, "capturedMutationGeneration") match {
            case Some(value) => row.put("captured_mutation_generation", value)
            case None        => row.putNull("captured_mutation_generation")
          }
          val phaseRows = row.putArray("phases")
          publication.phases.foreach { phase =>
            val phaseRow = phaseRows.addObject()
            phaseRow.put("name", phase.operation)
            phaseRow.put("status", phase.status)
            phaseRow.put("duration_ms", phase.durationNs / 1000000.0)
            phaseRow.put("start_monotonic_ns", phase.startMonotonicNs)
            phase.counts.foreach { case (key, value) => phaseRow.put(key, value) }
          }
        }
        allRuns += run
      }

      val root = mapper.createObjectNode()
      root.put("schema_version", 1)
      root.put("source_head", source)
      root.put("variant", variant)
      root.put("counterbalance_order", order)
      root.put("fixture", "one local state root and one file destination containing 1/200/428 populated RocksDB directories")
      root.put("remote_latency_measured", false)
      root.put("spark_sql_version", spark.version)
      val runs = root.putArray("runs")
      allRuns.foreach(runs.add)
      val resultFile = new File(output, s"$label.json")
      Files.write(resultFile.toPath, mapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(root))
      completed = true
    } finally {
      OpenIvmStateSync.setBeforeCaptureHookForTesting(null)
      OpenIvmStateSync.setDuringCaptureHookForTesting(null)
      OpenIvmStateSync.setBeforeSynchronousDrainHookForTesting(null)
      OpenIvmStateSync.setBeforeSnapshotCommitHookForTesting(null)
      OpenIvmStateSync.setCopyLocalFileHookForTesting(null)
      OpenIvmStateSync.setStateSyncUriHookForTesting(null)
      OpenIvmExecutionSpan.setStateSyncEvidenceHookForTesting(null)
      drainGates.clear()
      if (executor != null) {
        executor.shutdownNow()
        executor.awaitTermination(30, TimeUnit.SECONDS)
      }
      OpenIvmRocksDBRegistry.closeAll()
      OpenIvmStateSync.resetForTesting()
      if (spark != null) spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      if (completed) deleteTree(new File(output, s"inputs/$label"))
    }
  }
}
