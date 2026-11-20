package org.openivm.spark.common.rocksdb

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.FeatureGate
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.{File, IOException}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}

class StateSyncCoverageCandidateSpec extends AnyFunSuite with BeforeAndAfterAll with Matchers {
  private val output              = new File(".logs/state-sync-coverage-candidate").getCanonicalFile
  private var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    Files.createDirectories(output.toPath)
  }

  override protected def afterAll(): Unit = {
    try {
      OpenIvmStateSync.resetForTesting()
      OpenIvmRocksDBRegistry.closeAll()
      if (spark != null) spark.stop()
    } finally {
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

  private def deleteTree(file: File): Unit =
    if (file.exists()) {
      if (file.isDirectory) Option(file.listFiles()).foreach(_.foreach(deleteTree))
      Files.deleteIfExists(file.toPath)
    }

  private def prepare(label: String): (File, File) = {
    OpenIvmStateSync.resetForTesting()
    OpenIvmRocksDBRegistry.closeAll()
    if (spark != null) spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    val root = new File(output, label)
    deleteTree(root)
    val local  = new File(root, "local")
    val remote = new File(root, "remote")
    Files.createDirectories(local.toPath)
    Files.createDirectories(remote.toPath)
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName(s"openivm-state-sync-coverage-$label")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "localhost")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.warehouse.dir", new File(root, "warehouse").getAbsolutePath)
      .config(FeatureGate.StatePathKey, local.getAbsolutePath)
      .config(FeatureGate.StateSyncUriKey, remote.toURI.toString)
      .config(FeatureGate.StateSyncRetainGenerationsKey, "10")
      .config("spark.openivm.rocksdb.multiProcess", "false")
      .getOrCreate()
    writeState(local, "seed.bin", "candidate-state")
    local -> remote
  }

  private def writeState(local: File, name: String, value: String): Unit = {
    val target = new File(local, s"_openivm/candidate/$name")
    Files.createDirectories(target.getParentFile.toPath)
    Files.write(target.toPath, value.getBytes(StandardCharsets.UTF_8))
  }

  private def committed(remote: File): Vector[String] = {
    val directory = new File(remote, ".openivm-state-sync/commits")
    if (!directory.exists()) Vector.empty
    else
      Option(directory.listFiles()).toVector.flatten
        .filter(file => file.isFile && file.getName.endsWith(".commit"))
        .map(_.getName.stripSuffix(".commit"))
        .sorted
  }

  private def commitExists(remote: File, generation: String): Boolean =
    new File(remote, s".openivm-state-sync/commits/$generation.commit").isFile

  private def await(latch: CountDownLatch): Unit =
    latch.await(30, TimeUnit.SECONDS) shouldBe true

  private def awaitAck(
      queue: LinkedBlockingQueue[OpenIvmStateSync.StateSyncAcknowledgement]
  ): OpenIvmStateSync.StateSyncAcknowledgement = {
    val ack = queue.poll(60, TimeUnit.SECONDS)
    ack should not be null
    ack
  }

  private def awaitCondition(timeoutSeconds: Int = 30)(condition: => Boolean): Unit = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds.toLong)
    while (!condition && System.nanoTime() < deadline) Thread.sleep(5)
    condition shouldBe true
  }

  private def copyToRemote(fs: FileSystem, archive: File, path: Path): Unit =
    fs.copyFromLocalFile(false, true, new Path(archive.getAbsolutePath), path)

  private def ackQueue: LinkedBlockingQueue[OpenIvmStateSync.StateSyncAcknowledgement] =
    new LinkedBlockingQueue[OpenIvmStateSync.StateSyncAcknowledgement]()

  private def assertExact(ack: OpenIvmStateSync.StateSyncAcknowledgement): Unit = {
    ack.exactCoverage shouldBe true
    ack.requestedMutationGeneration should not be empty
    ack.capturedMutationGeneration should not be empty
    ack.capturedMutationGeneration.get should be >= ack.requestedMutationGeneration.get
  }

  test("sync and async acknowledgements refer to committed V2 generations and restore the archive") {
    val (local, remote) = prepare("ack-restore")
    OpenIvmStateSync.backupNow(spark)
    val first = committed(remote)
    first.size shouldBe 1

    OpenIvmStateSync.backupNow(spark)
    committed(remote) shouldBe first

    val callbacks         = ackQueue
    val callbackSawCommit = new AtomicBoolean(false)
    OpenIvmStateSync.backupAsyncWithAcknowledgement(
      spark,
      ack => {
        callbackSawCommit.set(commitExists(remote, ack.snapshotGenerationId))
        callbacks.add(ack)
        ()
      }
    )
    val ack = awaitAck(callbacks)
    assertExact(ack)
    ack.snapshotGenerationId shouldBe first.last
    callbackSawCommit.get() shouldBe true
    committed(remote) shouldBe first

    OpenIvmRocksDBRegistry.closeAll()
    deleteTree(new File(local, "_openivm"))
    OpenIvmStateSync.resetForTesting()
    OpenIvmStateSync.maybeRestore(spark)
    new String(
      Files.readAllBytes(new File(local, "_openivm/candidate/seed.bin").toPath),
      StandardCharsets.UTF_8
    ) shouldBe
      "candidate-state"
    OpenIvmStateSync.backupNow(spark)
    committed(remote).size shouldBe 2
  }

  test("requests queued before capture coalesce only after the capture covers their generations") {
    val (local, remote) = prepare("before-capture")
    val captureEntered  = new CountDownLatch(1)
    val releaseCapture  = new CountDownLatch(1)
    OpenIvmStateSync.setBeforeCaptureHookForTesting(() => {
      captureEntered.countDown()
      await(releaseCapture)
    })
    val callbacks = ackQueue
    OpenIvmStateSync.backupAsyncWithAcknowledgement(spark, callbacks.add)
    await(captureEntered)

    OpenIvmStateBarrier.withMutation(spark) {
      writeState(local, "queued-mutation.bin", "included-before-capture")
    }
    OpenIvmStateSync.backupAsyncWithAcknowledgement(spark, callbacks.add)
    releaseCapture.countDown()

    val first  = awaitAck(callbacks)
    val second = awaitAck(callbacks)
    assertExact(first)
    assertExact(second)
    first.snapshotGenerationId shouldBe second.snapshotGenerationId
    first.capturedMutationGeneration shouldBe second.capturedMutationGeneration
    awaitCondition() { OpenIvmStateSync.allBackupStatesIdleForTesting }
    committed(remote).size shouldBe 1
  }

  test("a request blocked by the real capture writer is covered if no later mutation occurs") {
    val (_, remote)    = prepare("during-capture")
    val captureEntered = new CountDownLatch(1)
    val releaseCapture = new CountDownLatch(1)
    val uploadEntered  = new CountDownLatch(1)
    val releaseUpload  = new CountDownLatch(1)
    OpenIvmStateSync.setDuringCaptureHookForTesting(() => {
      captureEntered.countDown()
      await(releaseCapture)
    })
    OpenIvmStateSync.setCopyLocalFileHookForTesting((fs, archive, path) => {
      uploadEntered.countDown()
      await(releaseUpload)
      copyToRemote(fs, archive, path)
    })

    val callbacks = ackQueue
    OpenIvmStateSync.backupAsyncWithAcknowledgement(spark, callbacks.add)
    await(captureEntered)
    val requestReturned = new CountDownLatch(1)
    val requestThread = new Thread(() => {
      OpenIvmStateSync.backupAsyncWithAcknowledgement(spark, callbacks.add)
      requestReturned.countDown()
    })
    requestThread.start()
    Thread.sleep(50)
    requestReturned.getCount shouldBe 1L
    releaseCapture.countDown()
    await(uploadEntered)
    await(requestReturned)
    releaseUpload.countDown()

    val first  = awaitAck(callbacks)
    val second = awaitAck(callbacks)
    assertExact(first)
    assertExact(second)
    first.snapshotGenerationId shouldBe second.snapshotGenerationId
    awaitCondition() { OpenIvmStateSync.allBackupStatesIdleForTesting }
    committed(remote).size shouldBe 1
    requestThread.join(5000)
  }

  test("a mutation after capture forces a later publication for the newer request") {
    val (local, remote) = prepare("after-capture")
    val uploadEntered   = new CountDownLatch(1)
    val releaseUpload   = new CountDownLatch(1)
    val uploadCount     = new AtomicInteger(0)
    OpenIvmStateSync.setCopyLocalFileHookForTesting((fs, archive, path) => {
      if (uploadCount.getAndIncrement() == 0) {
        uploadEntered.countDown()
        await(releaseUpload)
      }
      copyToRemote(fs, archive, path)
    })
    val callbacks = ackQueue
    OpenIvmStateSync.backupAsyncWithAcknowledgement(spark, callbacks.add)
    await(uploadEntered)
    OpenIvmStateBarrier.withMutation(spark) {
      writeState(local, "after-capture.bin", "requires-next-generation")
    }
    OpenIvmStateSync.backupAsyncWithAcknowledgement(spark, callbacks.add)
    releaseUpload.countDown()

    val first  = awaitAck(callbacks)
    val second = awaitAck(callbacks)
    assertExact(first)
    assertExact(second)
    first.snapshotGenerationId should not be second.snapshotGenerationId
    second.capturedMutationGeneration.get should be > first.capturedMutationGeneration.get
    awaitCondition() { OpenIvmStateSync.allBackupStatesIdleForTesting }
    committed(remote).size shouldBe 2
  }

  test("capture barriers and generations remain isolated across state roots") {
    prepare("barrier-isolation")
    val firstRoot  = new File(output, "barrier-isolation/root-one").toPath
    val secondRoot = new File(output, "barrier-isolation/root-two").toPath
    Files.createDirectories(firstRoot)
    Files.createDirectories(secondRoot)
    val mutationEntered = new CountDownLatch(1)
    val releaseMutation = new CountDownLatch(1)
    val mutationThread = new Thread(() =>
      OpenIvmStateBarrier.withMutationRootForTesting(firstRoot) {
        mutationEntered.countDown()
        await(releaseMutation)
      }
    )
    mutationThread.start()
    await(mutationEntered)

    val requestStarted      = new CountDownLatch(1)
    val requestDone         = new CountDownLatch(1)
    val requestedGeneration = new AtomicLong(-1L)
    val requestThread = new Thread(() => {
      requestStarted.countDown()
      requestedGeneration.set(OpenIvmStateBarrier.requestMutationGenerationForTesting(firstRoot))
      requestDone.countDown()
    })
    requestThread.start()
    await(requestStarted)
    await(requestDone)
    requestedGeneration.get() shouldBe 1L

    val firstCaptureAttempt = new CountDownLatch(1)
    val firstCaptureDone    = new CountDownLatch(1)
    val captureThread = new Thread(() => {
      firstCaptureAttempt.countDown()
      OpenIvmStateBarrier.withCaptureRootForTesting(firstRoot) { firstCaptureDone.countDown() }
    })
    captureThread.start()
    await(firstCaptureAttempt)
    awaitCondition() { OpenIvmStateBarrier.captureQueuedForTesting(firstRoot) }

    val secondCaptureDone = new CountDownLatch(1)
    val independentCapture =
      new Thread(() => OpenIvmStateBarrier.withCaptureRootForTesting(secondRoot) { secondCaptureDone.countDown() })
    independentCapture.start()
    await(secondCaptureDone)
    firstCaptureDone.getCount shouldBe 1L
    OpenIvmStateBarrier.mutationGenerationForTesting(firstRoot) shouldBe 0L
    OpenIvmStateBarrier.mutationGenerationForTesting(secondRoot) shouldBe 0L

    releaseMutation.countDown()
    await(firstCaptureDone)
    mutationThread.join(5000)
    requestThread.join(5000)
    captureThread.join(5000)
    independentCapture.join(5000)
    OpenIvmStateBarrier.mutationGenerationForTesting(firstRoot) shouldBe 1L
    OpenIvmStateBarrier.mutationGenerationForTesting(secondRoot) shouldBe 0L
  }

  test("an untracked async request cannot mask generation-covered requests") {
    val (_, remote)    = prepare("mixed-coverage-mode")
    val commitEntered  = new CountDownLatch(1)
    val releaseCommit  = new CountDownLatch(1)
    val commitAttempts = new AtomicInteger(0)
    OpenIvmStateSync.setBeforeSnapshotCommitHookForTesting(() =>
      if (commitAttempts.getAndIncrement() == 0) {
        commitEntered.countDown()
        await(releaseCommit)
      }
    )

    val trackedAcks     = ackQueue
    val untrackedAcks   = ackQueue
    val lateTrackedAcks = ackQueue
    try {
      OpenIvmStateSync.backupAsyncWithAcknowledgement(spark, trackedAcks.add)
      await(commitEntered)
      spark.conf.set("spark.openivm.rocksdb.multiProcess", "true")
      OpenIvmStateSync.backupAsyncWithAcknowledgement(spark, untrackedAcks.add)
      spark.conf.set("spark.openivm.rocksdb.multiProcess", "false")
      OpenIvmStateSync.backupAsyncWithAcknowledgement(spark, lateTrackedAcks.add)
      releaseCommit.countDown()

      val tracked     = awaitAck(trackedAcks)
      val lateTracked = awaitAck(lateTrackedAcks)
      assertExact(tracked)
      assertExact(lateTracked)
      tracked.snapshotGenerationId shouldBe lateTracked.snapshotGenerationId

      val untracked = awaitAck(untrackedAcks)
      untracked.exactCoverage shouldBe false
      untracked.requestedMutationGeneration shouldBe empty
      untracked.capturedMutationGeneration shouldBe empty
      untracked.snapshotGenerationId should not be tracked.snapshotGenerationId
      commitExists(remote, tracked.snapshotGenerationId) shouldBe true
      commitExists(remote, untracked.snapshotGenerationId) shouldBe true
      awaitCondition() { OpenIvmStateSync.allBackupStatesIdleForTesting }
      committed(remote).size shouldBe 2
    } finally releaseCommit.countDown()
  }

  test("failed and interrupted attempts retain requests without acknowledging before retry") {
    val (_, failedRemote) = prepare("failure-retry")
    val commitAttempts    = new AtomicInteger(0)
    OpenIvmStateSync.setBeforeSnapshotCommitHookForTesting(() =>
      if (commitAttempts.getAndIncrement() == 0) throw new IOException("candidate commit interruption")
    )
    val failedCallbacks = ackQueue
    OpenIvmStateSync.backupAsyncWithAcknowledgement(spark, failedCallbacks.add)
    awaitCondition() { OpenIvmStateSync.allBackupStatesIdleForTesting }
    failedCallbacks.poll(150, TimeUnit.MILLISECONDS) shouldBe null
    committed(failedRemote) shouldBe empty
    OpenIvmStateSync.backupAsyncWithAcknowledgement(spark, failedCallbacks.add)
    val retryA = awaitAck(failedCallbacks)
    val retryB = awaitAck(failedCallbacks)
    assertExact(retryA)
    assertExact(retryB)
    retryA.snapshotGenerationId shouldBe retryB.snapshotGenerationId
    awaitCondition() { OpenIvmStateSync.allBackupStatesIdleForTesting }
    committed(failedRemote).size shouldBe 1

    val (_, interruptedRemote) = prepare("interrupt-retry")
    val copyAttempts           = new AtomicInteger(0)
    OpenIvmStateSync.setCopyLocalFileHookForTesting((fs, archive, path) =>
      if (copyAttempts.getAndIncrement() == 0) throw new InterruptedException("candidate upload interruption")
      else copyToRemote(fs, archive, path)
    )
    val interruptedCallbacks = ackQueue
    OpenIvmStateSync.backupAsyncWithAcknowledgement(spark, interruptedCallbacks.add)
    awaitCondition() { OpenIvmStateSync.allBackupStatesIdleForTesting }
    interruptedCallbacks.poll(150, TimeUnit.MILLISECONDS) shouldBe null
    committed(interruptedRemote) shouldBe empty
    OpenIvmStateSync.backupAsyncWithAcknowledgement(spark, interruptedCallbacks.add)
    val retryAfterInterruptA = awaitAck(interruptedCallbacks)
    val retryAfterInterruptB = awaitAck(interruptedCallbacks)
    assertExact(retryAfterInterruptA)
    assertExact(retryAfterInterruptB)
    retryAfterInterruptA.snapshotGenerationId shouldBe retryAfterInterruptB.snapshotGenerationId
    awaitCondition() { OpenIvmStateSync.allBackupStatesIdleForTesting }
    committed(interruptedRemote).size shouldBe 1
  }

}
