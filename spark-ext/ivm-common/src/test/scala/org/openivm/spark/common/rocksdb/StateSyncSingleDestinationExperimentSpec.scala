package org.openivm.spark.common.rocksdb

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.openivm.spark.common.{
  ChangeWatermark,
  FeatureGate,
  MvCatalog,
  MvMetadata,
  RefreshProfileCatalog,
  RefreshProfileRow
}
import org.openivm.spark.telemetry.OpenIvmExecutionSpan
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.{File, IOException}
import java.lang.management.ManagementFactory
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.security.MessageDigest
import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
import java.util.concurrent.{
  Callable,
  CopyOnWriteArrayList,
  CountDownLatch,
  ExecutorService,
  Executors,
  Future,
  TimeUnit
}
import java.util.zip.ZipFile

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

private[rocksdb] object StateSyncFreshProfileRestoreProbe {
  def main(args: Array[String]): Unit = {
    require(args.length == 10, s"expected 10 arguments, found ${args.length}")
    val statePath       = args(0)
    val remoteUri       = args(1)
    val refreshId       = args(2)
    val operationId     = args(3)
    val expectedTs      = args(4)
    val database        = args(5)
    val mvName          = args(6)
    val expectedVersion = args(7).toLong
    val source          = args(8)
    val expectedOffset  = args(9).toLong
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-fresh-profile-restore-probe")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "localhost")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.warehouse.dir", statePath)
      .config(FeatureGate.StatePathKey, statePath)
      .config(FeatureGate.StateSyncUriKey, remoteUri)
      .config("spark.openivm.rocksdb.multiProcess", "false")
      .getOrCreate()
    try {
      OpenIvmStateSync.maybeRestore(spark)
      val matchingRows = RefreshProfileCatalog.scanAll(spark).filter(_.refreshId == refreshId)
      require(matchingRows.size == 1, s"expected one restored profile row for $refreshId, found ${matchingRows.size}")
      val row = matchingRows.head
      require(row.viewName == s"$database.$mvName", s"unexpected restored view name: ${row.viewName}")
      require(row.profileTimestamp.toString == expectedTs, s"unexpected restored timestamp: ${row.profileTimestamp}")
      require(row.stepOrder == 0 && row.stepName == "profile_only_probe" && row.durationMs == 17L)
      require(row.detail == s"operation_id=$operationId", s"unexpected restored operation detail: ${row.detail}")

      val mv = MvCatalog
        .list(spark)
        .find(_.name == TableIdentifier(mvName, Some(database)))
        .getOrElse(throw new AssertionError(s"missing restored MV metadata $database.$mvName"))
      require(mv.lastVersion == expectedVersion, s"unexpected restored MV version ${mv.lastVersion}")
      require(
        mv.changeWatermarks.get(source).contains(ChangeWatermark.DeltaVersion(expectedOffset)),
        s"unexpected restored source offset ${mv.changeWatermarks}"
      )
    } finally {
      OpenIvmRocksDBRegistry.closeAll()
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
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
            val before  = commitFiles(context.remote).map(generation)
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

  private def installDrainGateHook(): Unit =
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

  private def addOption(
      node: com.fasterxml.jackson.databind.node.ObjectNode,
      key: String,
      value: Option[String]
  ): Unit =
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

    val evidence                  = new CopyOnWriteArrayList[OpenIvmExecutionSpan.StateSyncEvidence]()
    val mapper                    = new ObjectMapper()
    var spark: SparkSession       = null
    var executor: ExecutorService = null
    var completed                 = false
    val allRuns = scala.collection.mutable.ArrayBuffer.empty[com.fasterxml.jackson.databind.node.ObjectNode]

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
      installDrainGateHook()

      val scales = if (order == "ascending") Seq(1, 200, 428) else Seq(428, 200, 1)
      val remote = new File(output, s"destinations/$label/single")
      if (remote.exists() && Option(remote.listFiles()).exists(_.nonEmpty))
        fail(s"refusing to reuse a non-empty remote destination: $remote")
      scales.zipWithIndex.foreach { case (scale, scaleIndex) =>
        val local = new File(localInputRoot, s"n$scale/local")
        if (scaleIndex > 0) deleteTree(remote)
        Files.createDirectories(local.toPath)
        Files.createDirectories(remote.toPath)
        val context           = Context(label, scale, local, remote)
        val eventStart        = evidence.size()
        val uploads           = new AtomicInteger(0)
        val uploadEntered     = new CountDownLatch(1)
        val releaseUpload     = new CountDownLatch(1)
        val firstUpload       = new AtomicBoolean(false)
        val uploadGateEnabled = new AtomicBoolean(false)
        val uploadHoldNs      = new AtomicLong(0L)
        val calls             = scala.collection.mutable.ArrayBuffer.empty[Result]
        OpenIvmStateSync.setCopyLocalFileHookForTesting((fs: FileSystem, file: File, path: Path) => {
          uploads.incrementAndGet()
          if (uploadGateEnabled.get() && firstUpload.compareAndSet(false, true)) {
            val started = System.nanoTime()
            uploadEntered.countDown()
            await(releaseUpload)
            uploadHoldNs.set(System.nanoTime() - started)
          }
          fs.copyFromLocalFile(false, true, new Path(file.getAbsolutePath), path)
        })
        (0 until scale).foreach(index => writeDatabase(spark, context, index, "seed", f"seed-$index%04d"))
        OpenIvmRocksDBRegistry.closeAll()

        val initial = awaitResults(startCohort(spark, executor, context, "initial", 1))
        calls ++= initial
        initial.head.outcome shouldBe "success"
        commitFiles(remote) should have size 1
        uploadGateEnabled.set(true)

        val beforeGate        = new Gate(8)
        val preCaptureEntered = new CountDownLatch(1)
        val releasePreCapture = new CountDownLatch(1)
        val firstPreCapture   = new AtomicBoolean(false)
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
        OpenIvmStateSync.setAfterFirstDatabaseCheckpointHookForTesting(() =>
          if (firstCapture.compareAndSet(false, true)) {
            val started = System.nanoTime()
            captureEntered.countDown()
            await(releaseCapture)
            captureHoldNs.set(System.nanoTime() - started)
          }
        )
        writeDatabase(spark, context, 0, "before-capture", "must-be-covered-by-first-mixed-publication")
        OpenIvmRocksDBRegistry.closeAll()
        drainGates.put("before", beforeGate)
        val beforeFutures = startCohort(spark, executor, context, "before", 8)
        awaitDrainGate("before", beforeGate)
        await(preCaptureEntered)
        OpenIvmStateSync.setBeforeCaptureHookForTesting(null)
        releasePreCapture.countDown()
        await(captureEntered)
        OpenIvmStateSync.setAfterFirstDatabaseCheckpointHookForTesting(null)
        val mutationStarted = new CountDownLatch(1)
        val blockedMutation = executor.submit(new Callable[Long] {
          override def call(): Long = withContext(context, "blocked-during-capture") {
            mutationStarted.countDown()
            val started = System.nanoTime()
            writeDatabase(spark, context, 0, "mutation-during-capture", "must-run-after-capture-barrier")
            System.nanoTime() - started
          }
        })
        await(mutationStarted)
        Thread.sleep(100)
        blockedMutation.isDone shouldBe false

        val duringGate = new Gate(8)
        drainGates.put("during", duringGate)
        val duringFutures = startCohort(spark, executor, context, "during", 8)
        releaseCapture.countDown()
        awaitDrainGate("during", duringGate)
        val blockedMutationElapsedNs = blockedMutation.get(120, TimeUnit.SECONDS)
        await(uploadEntered)

        val afterGate = new Gate(8)
        drainGates.put("after", afterGate)
        val afterFutures = startCohort(spark, executor, context, "after", 8)
        awaitDrainGate("after", afterGate)
        writeDatabase(spark, context, 0, "after-capture", "must-force-a-later-publication")
        OpenIvmRocksDBRegistry.closeAll()
        val changedGate = new Gate(8)
        drainGates.put("changed", changedGate)
        val changedFutures = startCohort(spark, executor, context, "changed", 8)
        awaitDrainGate("changed", changedGate)
        releaseUpload.countDown()
        val mixedResults = awaitResults(beforeFutures ++ duringFutures ++ afterFutures ++ changedFutures)
        calls ++= mixedResults
        mixedResults.foreach(_.outcome shouldBe "success")
        OpenIvmStateSync.setAfterFirstDatabaseCheckpointHookForTesting(null)

        val beforeNoChangePublications = toVector(evidence).drop(eventStart).flatMap(_.publications).size
        val noChangeFutures            = startCohort(spark, executor, context, "no_change", 16)
        val noChangeResults            = awaitResults(noChangeFutures)
        calls ++= noChangeResults
        noChangeResults.foreach(_.outcome shouldBe "success")
        val afterNoChangePublications = toVector(evidence).drop(eventStart).flatMap(_.publications).size
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
        val retryRequest = matchingRequest(toVector(evidence).drop(eventStart), retry.requestId)
        assertDurablyCovered(retryRequest, retry)
        commitFiles(remote).size should be <= 2

        val runEvidence  = toVector(evidence).drop(eventStart)
        val publications = runEvidence.flatMap(_.publications)
        publications.foreach { publication =>
          publication.counts.getOrElse("captured_databases", None) shouldBe Some(scale.toLong)
        }
        publications.count(_.published) shouldBe publications.size - 1
        val capturedDatabaseAttempts =
          publications.map(_.counts.getOrElse("captured_databases", None).getOrElse(0L)).sum
        uploads.get() shouldBe publications.size
        calls.size shouldBe 51
        val requestById = calls.map(result => result.requestId -> matchingRequest(runEvidence, result.requestId)).toMap
        val failedReceipt = requestById(failure.requestId)
        failedReceipt.published shouldBe false
        failedReceipt.acknowledgedSnapshotGenerationId shouldBe empty
        calls
          .filter(_.outcome == "success")
          .foreach(result => assertDurablyCovered(requestById(result.requestId), result))

        val stableCalls = calls.filter(_.kind == "before").toVector
        val stable      = stableCalls.map(call => requestById(call.requestId))
        stable should have size 8
        val duringCalls = calls.filter(_.kind == "during").toVector
        val during      = duringCalls.map(call => requestById(call.requestId))
        during should have size 8
        val afterCalls   = calls.filter(_.kind == "after").toVector
        val afterCapture = afterCalls.map(call => requestById(call.requestId))
        afterCapture should have size 8
        val changedCalls = calls.filter(_.kind == "changed").toVector
        val changed      = changedCalls.map(call => requestById(call.requestId))
        changed should have size 8
        val noChangeCalls = calls.filter(_.kind == "no_change").toVector
        val noChange      = noChangeCalls.map(call => requestById(call.requestId))
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
        val capturePhase           = firstStablePublication.phases.find(_.operation == "spark.backup.capture").get
        val uploadPhase            = firstStablePublication.phases.find(_.operation == "spark.backup.upload").get
        val captureEnd             = capturePhase.startMonotonicNs + capturePhase.durationNs
        val uploadEnd              = uploadPhase.startMonotonicNs + uploadPhase.durationNs
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

        val latestCommit     = commitFiles(remote).last
        val latestGeneration = generation(latestCommit)
        val markerLines      = toVector(Files.readAllLines(latestCommit.toPath, StandardCharsets.UTF_8))
        markerLines.head shouldBe "OPENIVM_STATE_SNAPSHOT_V2"
        val latestArchive = archive(remote, latestGeneration)
        latestArchive.isFile shouldBe true
        val zip = new ZipFile(latestArchive)
        val archiveDbCount =
          try {
            val entries = zip.entries()
            var count   = 0
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
        installDrainGateHook()
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
          val row     = requestRows.addObject()
          row.put("request_id", result.requestId)
          row.put("class", result.kind)
          row.put("invoked_monotonic_ns", result.invokedAtNs)
          row.put("elapsed_ms", result.elapsedNs / 1000000.0)
          row.put("outcome", result.outcome)
          result.errorClass.foreach(row.put("error_class", _))
          row.put("commits_before", result.commitsBefore.size)
          row.put("commits_after", result.commitsAfter.size)
          row.put(
            "acknowledged_archive_present_at_return",
            result.archivesAfter.contains(receipt.acknowledgedSnapshotGenerationId.getOrElse(""))
          )
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
      root.put(
        "fixture",
        "one local state root and one file destination containing 1/200/428 populated RocksDB directories"
      )
      root.put("remote_latency_measured", false)
      root.put("spark_sql_version", spark.version)
      val runs = root.putArray("runs")
      allRuns.foreach(runs.add)
      val resultFile = new File(output, s"$label.json")
      Files.write(resultFile.toPath, mapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(root))
      completed = true
    } finally {
      OpenIvmStateSync.setBeforeCaptureHookForTesting(null)
      OpenIvmStateSync.setAfterFirstDatabaseCheckpointHookForTesting(null)
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

  private def profileGateMvSnapshot(spark: SparkSession): Vector[(String, Long, Map[String, String])] =
    MvCatalog
      .list(spark)
      .map { meta =>
        val offsets = meta.changeWatermarks.map { case (source, watermark) => source -> watermark.encode }
        (meta.name.toString, meta.lastVersion, offsets)
      }
      .toVector

  private def withRequestId[A](spark: SparkSession, requestId: String)(body: => A): A = {
    val old = spark.sparkContext.getLocalProperty("openivm.request_id")
    spark.sparkContext.setLocalProperty("openivm.request_id", requestId)
    try body
    finally spark.sparkContext.setLocalProperty("openivm.request_id", old)
  }

  private def runFreshProfileRestore(
      caseRoot: File,
      remote: File,
      statePath: File,
      refreshId: String,
      operationId: String,
      profileTs: Timestamp,
      database: String,
      mvName: String,
      expectedVersion: Long,
      source: String,
      expectedOffset: Long
  ): Unit = {
    val javaHome = new File(new File(System.getProperty("java.home"), "bin"), "java").getAbsolutePath
    val args     = ManagementFactory.getRuntimeMXBean.getInputArguments
    val jvmArgs  = Vector.newBuilder[String]
    val iterator = args.iterator()
    while (iterator.hasNext) {
      val arg = iterator.next()
      if (
        arg.startsWith("--add-opens=") || arg.startsWith("--add-exports=") || arg.startsWith("-Xmx") ||
        arg == "-XX:+UseG1GC" || arg == "-Dfile.encoding=UTF-8"
      ) jvmArgs += arg
    }
    val childLog = new File(caseRoot, "fresh-restore-child.log")
    val process = new ProcessBuilder(
      (Vector(javaHome) ++ jvmArgs.result() ++ Vector(
        "-cp",
        System.getProperty("java.class.path"),
        "org.openivm.spark.common.rocksdb.StateSyncFreshProfileRestoreProbe",
        statePath.getAbsolutePath,
        remote.toURI.toString,
        refreshId,
        operationId,
        profileTs.toString,
        database,
        mvName,
        expectedVersion.toString,
        source,
        expectedOffset.toString
      )): _*
    )
      .directory(caseRoot)
      .redirectErrorStream(true)
      .redirectOutput(ProcessBuilder.Redirect.appendTo(childLog))
      .start()
    val finished = process.waitFor(180, TimeUnit.SECONDS)
    if (!finished) process.destroyForcibly()
    withClue(Option(Files.readString(childLog.toPath)).getOrElse("")) {
      finished shouldBe true
      process.exitValue() shouldBe 0
    }
  }

  test("profile-only write after capture requires its own committed generation and fresh-JVM restore") {
    val label     = s"profile-only-${UUID.randomUUID().toString.take(8)}"
    val caseRoot  = new File(output, s"profile-only-gate/$label")
    val statePath = new File(caseRoot, "warehouse")
    val localRoot = new File(statePath, "_openivm")
    val remote    = new File(caseRoot, "remote")
    Files.createDirectories(localRoot.toPath)
    Files.createDirectories(remote.toPath)

    val database      = "profile_gate_db"
    val mvName        = "profile_gate_mv"
    val source        = s"$database.source"
    val mvVersion     = 11L
    val mvOffset      = 23L
    val watermark     = ChangeWatermark.DeltaVersion(mvOffset)
    val mvIdentifier  = TableIdentifier(mvName, Some(database))
    val baseTimestamp = new Timestamp(1700000000000L)
    val operationId   = UUID.randomUUID().toString
    val refreshId     = s"profile-only-$operationId"
    val profileTs     = new Timestamp(1700000001000L)
    val targetRow = RefreshProfileRow(
      refreshId = refreshId,
      viewName = s"$database.$mvName",
      profileTimestamp = profileTs,
      stepOrder = 0,
      stepName = "profile_only_probe",
      durationMs = 17L,
      detail = s"operation_id=$operationId"
    )
    val seedRow = RefreshProfileRow(
      refreshId = s"profile-seed-$label",
      viewName = s"$database.$mvName",
      profileTimestamp = baseTimestamp,
      stepOrder = 0,
      stepName = "profile_seed",
      durationMs = 1L,
      detail = "setup=baseline"
    )
    val triggerRow = RefreshProfileRow(
      refreshId = s"capture-trigger-$label",
      viewName = s"$database.$mvName",
      profileTimestamp = new Timestamp(baseTimestamp.getTime + 1L),
      stepOrder = 0,
      stepName = "profile_capture_trigger",
      durationMs = 1L,
      detail = "setup=real-profile-catalog-write"
    )

    var spark: SparkSession       = null
    var sparkVersion              = "unknown"
    var executor: ExecutorService = null
    val evidence                  = new CopyOnWriteArrayList[OpenIvmExecutionSpan.StateSyncEvidence]()
    val captureEntered            = new CountDownLatch(1)
    val releaseCapture            = new CountDownLatch(1)
    val uploadEntered             = new CountDownLatch(1)
    val releaseUpload             = new CountDownLatch(1)
    val lateRequestQueued         = new CountDownLatch(1)
    val nextCommitEntered         = new CountDownLatch(1)
    val releaseNextCommit         = new CountDownLatch(1)
    val firstCapture              = new AtomicBoolean(false)
    val firstUpload               = new AtomicBoolean(false)
    val captureHookGeneration     = new AtomicLong(-1L)
    val drainCalls                = new AtomicInteger(0)
    val commitCalls               = new AtomicInteger(0)
    var firstFuture: Future[Unit] = null
    var lateFuture: Future[Unit]  = null

    try {
      spark = SparkSession
        .builder()
        .master("local[2]")
        .appName("openivm-profile-only-checkpoint-gate")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "localhost")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.warehouse.dir", statePath.getAbsolutePath)
        .config(FeatureGate.StatePathKey, statePath.getAbsolutePath)
        .config(FeatureGate.StateSyncUriKey, remote.toURI.toString)
        .config(FeatureGate.StateSyncRetainGenerationsKey, "2")
        .config(FeatureGate.ProfileRefreshKey, "true")
        .config("spark.openivm.rocksdb.multiProcess", "false")
        .getOrCreate()
      executor = Executors.newFixedThreadPool(3)
      sparkVersion = spark.version
      OpenIvmExecutionSpan.setStateSyncEvidenceHookForTesting(value => evidence.add(value))
      OpenIvmStateSync.setStateSyncUriHookForTesting(_ => Some(remote.toURI.toString))

      val metadata = MvMetadata(
        name = mvIdentifier,
        querySql = s"SELECT id FROM $source",
        refreshType = 0,
        refreshTypeName = "SIMPLE_PROJECTION",
        lastVersion = mvVersion,
        sourceTables = Seq(source),
        sourceSchemaFingerprint = "profile-only-check-schema",
        location = new File(statePath, mvName).toURI.toString,
        createdAt = baseTimestamp,
        properties = MvMetadata.changeWatermarkProperties(Map(source -> watermark))
      )
      MvCatalog.upsert(spark, metadata)
      RefreshProfileCatalog.ensureTables(spark)
      RefreshProfileCatalog.record(spark, Seq(seedRow))
      OpenIvmRocksDBRegistry.closeAll()

      val initialRequestId = s"$label-initial"
      withRequestId(spark, initialRequestId) { OpenIvmStateSync.backupNow(spark) }
      val initialReceipt = matchingRequest(toVector(evidence), initialRequestId)
      initialReceipt.status shouldBe "success"
      initialReceipt.published shouldBe true
      initialReceipt.acknowledgedSnapshotGenerationId shouldBe initialReceipt.snapshotGenerationId
      val initialGeneration = initialReceipt.acknowledgedSnapshotGenerationId.getOrElse(fail("missing initial ack"))
      commitFiles(remote).map(generation) should contain(initialGeneration)
      archive(remote, initialGeneration).isFile shouldBe true
      val initialCommitIds = commitFiles(remote).map(generation)

      val mvOffsetsBeforeTarget = profileGateMvSnapshot(spark)
      mvOffsetsBeforeTarget should have size 1
      mvOffsetsBeforeTarget.head._2 shouldBe mvVersion
      mvOffsetsBeforeTarget.head._3 shouldBe Map(source -> watermark.encode)
      RefreshProfileCatalog.record(spark, Seq(triggerRow))
      OpenIvmRocksDBRegistry.closeAll()

      OpenIvmStateSync.setAfterFirstDatabaseCheckpointHookForTesting(() =>
        if (firstCapture.compareAndSet(false, true)) {
          captureHookGeneration.set(OpenIvmStateBarrier.currentMutationGeneration(spark))
          captureEntered.countDown()
          await(releaseCapture)
        }
      )
      OpenIvmStateSync.setCopyLocalFileHookForTesting((fs: FileSystem, file: File, path: Path) => {
        if (firstUpload.compareAndSet(false, true)) {
          uploadEntered.countDown()
          await(releaseUpload)
        }
        fs.copyFromLocalFile(false, true, new Path(file.getAbsolutePath), path)
      })
      OpenIvmStateSync.setBeforeSynchronousDrainHookForTesting(() =>
        if (drainCalls.incrementAndGet() == 2) lateRequestQueued.countDown()
      )
      OpenIvmStateSync.setBeforeSnapshotCommitHookForTesting(() => {
        commitCalls.incrementAndGet() match {
          case 1 =>
            commitFiles(remote).map(generation) shouldBe initialCommitIds
            initialCommitIds.foreach(id => archive(remote, id).isFile shouldBe true)
          case 2 =>
            val current = commitFiles(remote).map(generation)
            (current.toSet -- initialCommitIds.toSet) should have size 1
            current should contain(initialGeneration)
            current.foreach(id => archive(remote, id).isFile shouldBe true)
            nextCommitEntered.countDown()
            await(releaseNextCommit)
          case other => fail(s"unexpected additional commit attempt $other")
        }
      })

      val firstRequestId = s"$label-capture"
      firstFuture = executor.submit(new Callable[Unit] {
        override def call(): Unit = withRequestId(spark, firstRequestId) { OpenIvmStateSync.backupNow(spark) }
      })
      await(captureEntered)
      val capturedGenerationAtGate = captureHookGeneration.get()
      capturedGenerationAtGate should be >= 0L

      val writerStarted = new CountDownLatch(1)
      val profileWriteFuture = executor.submit(new Callable[Long] {
        override def call(): Long = {
          writerStarted.countDown()
          RefreshProfileCatalog.record(spark, Seq(targetRow))
          OpenIvmStateBarrier.currentMutationGeneration(spark)
        }
      })
      await(writerStarted)
      val queueDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15)
      while (!OpenIvmStateBarrier.captureQueuedForTesting(spark) && System.nanoTime() < queueDeadline)
        Thread.sleep(5)
      OpenIvmStateBarrier.captureQueuedForTesting(spark) shouldBe true
      profileWriteFuture.isDone shouldBe false
      firstFuture.isDone shouldBe false

      releaseCapture.countDown()
      await(uploadEntered)
      val targetMutationGeneration = profileWriteFuture.get(120, TimeUnit.SECONDS)
      targetMutationGeneration should be > capturedGenerationAtGate
      profileGateMvSnapshot(spark) shouldBe mvOffsetsBeforeTarget
      commitFiles(remote).map(generation) shouldBe initialCommitIds
      initialCommitIds.foreach(id => archive(remote, id).isFile shouldBe true)

      val lateRequestId = s"$label-late-profile-row"
      lateFuture = executor.submit(new Callable[Unit] {
        override def call(): Unit = withRequestId(spark, lateRequestId) { OpenIvmStateSync.backupNow(spark) }
      })
      await(lateRequestQueued)
      lateFuture.isDone shouldBe false
      firstFuture.isDone shouldBe false
      commitFiles(remote).map(generation) shouldBe initialCommitIds
      initialCommitIds.foreach(id => archive(remote, id).isFile shouldBe true)

      releaseUpload.countDown()
      await(nextCommitEntered)
      firstFuture.get(120, TimeUnit.SECONDS)
      lateFuture.isDone shouldBe false
      val committedBeforeNext = commitFiles(remote).map(generation)
      committedBeforeNext should contain(initialGeneration)
      committedBeforeNext.foreach(id => archive(remote, id).isFile shouldBe true)
      releaseNextCommit.countDown()
      lateFuture.get(120, TimeUnit.SECONDS)

      val completedEvents = toVector(evidence)
      val firstReceipt    = matchingRequest(completedEvents, firstRequestId)
      val lateReceipt     = matchingRequest(completedEvents, lateRequestId)
      val firstCaptured   = reflectedGeneration(firstReceipt, "capturedMutationGeneration").get
      val lateRequested   = reflectedGeneration(lateReceipt, "requestedMutationGeneration").get
      val lateCaptured    = reflectedGeneration(lateReceipt, "capturedMutationGeneration").get
      val firstGeneration = firstReceipt.acknowledgedSnapshotGenerationId.getOrElse(fail("missing capture ack"))
      val lateGeneration  = lateReceipt.acknowledgedSnapshotGenerationId.getOrElse(fail("missing late ack"))
      firstReceipt.status shouldBe "success"
      firstReceipt.published shouldBe true
      firstReceipt.snapshotGenerationId shouldBe Some(firstGeneration)
      lateReceipt.status shouldBe "success"
      lateReceipt.published shouldBe true
      lateReceipt.snapshotGenerationId shouldBe Some(lateGeneration)
      firstCaptured shouldBe capturedGenerationAtGate
      firstCaptured should be < lateRequested
      lateRequested should be >= targetMutationGeneration
      lateCaptured should be >= lateRequested
      firstGeneration should not be initialGeneration
      lateGeneration should not be firstGeneration
      commitCalls.get shouldBe 2
      lateGeneration should not be initialGeneration
      profileGateMvSnapshot(spark) shouldBe mvOffsetsBeforeTarget

      val finalCommit = commitFiles(remote)
        .find(file => generation(file) == lateGeneration)
        .getOrElse(fail("missing late commit marker"))
      val markerBytes = Files.readAllBytes(finalCommit.toPath)
      val markerLines = {
        val lines = Files.readAllLines(finalCommit.toPath, StandardCharsets.UTF_8)
        val out   = Vector.newBuilder[String]
        val iter  = lines.iterator()
        while (iter.hasNext) out += iter.next()
        out.result()
      }
      markerLines should have size 4
      markerLines.head shouldBe "OPENIVM_STATE_SNAPSHOT_V2"
      markerLines(1) shouldBe lateGeneration
      val finalArchive = archive(remote, lateGeneration)
      finalArchive.isFile shouldBe true
      markerLines(2).toLong shouldBe finalArchive.length()
      val archiveSha = OpenIvmSnapshotArchive.sha256(finalArchive)
      markerLines(3) shouldBe archiveSha
      val archiveZip = new ZipFile(finalArchive)
      val hasProfileDb =
        try {
          val entries = archiveZip.entries()
          var found   = false
          while (entries.hasMoreElements) {
            if (entries.nextElement().getName == "refresh_profile/rocksdb/CURRENT") found = true
          }
          found
        } finally archiveZip.close()
      hasProfileDb shouldBe true

      OpenIvmRocksDBRegistry.closeAll()
      spark.stop()
      spark = null
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      OpenIvmStateSync.resetForTesting()
      val freshStatePath = new File(caseRoot, "fresh-warehouse")
      new File(freshStatePath, "_openivm").exists() shouldBe false
      runFreshProfileRestore(
        caseRoot,
        remote,
        freshStatePath,
        refreshId,
        operationId,
        profileTs,
        database,
        mvName,
        mvVersion,
        source,
        mvOffset
      )

      val result = new ObjectMapper().createObjectNode()
      result.put("schema_version", 1)
      result.put("source_head", sys.env.getOrElse("OPENIVM_SOURCE_HEAD", "unknown"))
      result.put("spark_sql_version", sparkVersion)
      result.put("fixture", "one state root; one actual refresh_profile RocksDB catalog and one MV metadata row")
      result.put("remote_latency_measured", false)
      result.put("operation_id", operationId)
      result.put("refresh_id", refreshId)
      result.put("profile_row_restored_fresh_jvm", true)
      result.put("mv_versions_and_consumer_offsets_unchanged", true)
      result.put("initial_generation", initialGeneration)
      result.put("first_capture_generation", firstGeneration)
      result.put("first_captured_mutation_generation", firstCaptured)
      result.put("late_requested_mutation_generation", lateRequested)
      result.put("late_captured_mutation_generation", lateCaptured)
      result.put("late_acknowledged_generation", lateGeneration)
      result.put("target_mutation_generation", targetMutationGeneration)
      result.put("capture_gate_generation", capturedGenerationAtGate)
      result.put("late_request_acknowledged_by_older_commit", lateGeneration == firstGeneration)
      result.put(
        "commit_marker_sha256",
        MessageDigest.getInstance("SHA-256").digest(markerBytes).map(b => f"${b & 0xff}%02x").mkString
      )
      result.put("archive_bytes", finalArchive.length())
      result.put("archive_sha256", archiveSha)
      result.put("commit_marker", markerLines.mkString("\n"))
      result.put("old_commits_retained_until_later_commit", true)
      val resultPath = new File(caseRoot, "profile-only-gate.json")
      Files.write(resultPath.toPath, new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsBytes(result))
    } finally {
      releaseCapture.countDown()
      releaseUpload.countDown()
      releaseNextCommit.countDown()
      OpenIvmStateSync.setAfterFirstDatabaseCheckpointHookForTesting(null)
      OpenIvmStateSync.setBeforeSynchronousDrainHookForTesting(null)
      OpenIvmStateSync.setBeforeSnapshotCommitHookForTesting(null)
      OpenIvmStateSync.setCopyLocalFileHookForTesting(null)
      OpenIvmStateSync.setStateSyncUriHookForTesting(null)
      OpenIvmExecutionSpan.setStateSyncEvidenceHookForTesting(null)
      OpenIvmStateSync.resetForTesting()
      if (executor != null) {
        executor.shutdownNow()
        executor.awaitTermination(30, TimeUnit.SECONDS)
      }
      OpenIvmRocksDBRegistry.closeAll()
      if (spark != null) spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

}
