package org.openivm.spark.common.rocksdb

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util.UUID
import java.util.concurrent.{Callable, CountDownLatch, Executors, Future, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.collection.mutable.ArrayBuffer

class StateSyncRootFanoutCandidateSpec extends AnyFunSuite with Matchers {
  private val reportDirectory = Paths.get(".logs/state-sync-coverage-candidate")

  private def await(latch: CountDownLatch): Unit =
    if (!latch.await(30, TimeUnit.SECONDS)) throw new IllegalStateException("timed out waiting for test latch")

  private def failCaptureOnce(root: Path, failures: AtomicInteger, message: String): Unit = {
    var observed = false
    try
      OpenIvmStateBarrier.withCaptureRootForTesting(root) {
        failures.incrementAndGet()
        throw new IOException(message)
      }
    catch { case _: IOException => observed = true }
    if (!observed) throw new AssertionError("injected capture failure was not observed")
  }

  private def awaitCondition(timeoutSeconds: Int = 30)(condition: => Boolean): Unit = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds.toLong)
    while (!condition && System.nanoTime() < deadline) Thread.sleep(5)
    condition shouldBe true
  }

  private def percentile(values: Seq[Long], fraction: Double): Long = {
    if (values.isEmpty) 0L
    else {
      val sorted = values.sorted
      sorted(math.max(0, math.ceil(sorted.size * fraction).toInt - 1))
    }
  }

  private def runScale(rootCount: Int, runId: String): String = {
    val scaleRoot = reportDirectory.resolve(s"root-fanout-$runId-$rootCount")
    Files.createDirectories(scaleRoot)
    val roots = Vector.tabulate(rootCount) { index =>
      val root = scaleRoot.resolve(s"root-$index")
      Files.createDirectories(root)
      root
    }
    val blockedRoot      = roots.head
    val failureRootIndex = rootCount - 1
    val mutationEntered  = new CountDownLatch(1)
    val releaseMutation  = new CountDownLatch(1)
    val captureStarted   = System.nanoTime()
    val failedCaptures   = new AtomicInteger(0)
    val retries          = new AtomicInteger(0)
    val executor         = Executors.newFixedThreadPool(math.min(32, rootCount + 1))
    val mutationFailure  = new AtomicReference[Throwable]()
    val mutationThread = new Thread(() =>
      try
        OpenIvmStateBarrier.withMutationRootForTesting(blockedRoot) {
          mutationEntered.countDown()
          await(releaseMutation)
          Files.write(blockedRoot.resolve("covered-mutation.bin"), Array[Byte](1, 2, 3))
        }
      catch { case error: Throwable => mutationFailure.set(error) }
    )
    mutationThread.setName(s"state-root-mutation-$rootCount")
    mutationThread.start()

    try {
      await(mutationEntered)
      val requestedGeneration = OpenIvmStateBarrier.requestMutationGenerationForTesting(blockedRoot)
      requestedGeneration shouldBe 1L
      val blockedCapture: Future[Long] = executor.submit(new Callable[Long] {
        override def call(): Long = {
          if (rootCount == 1) {
            failCaptureOnce(blockedRoot, failedCaptures, "injected root-local capture failure")
            retries.incrementAndGet()
          }
          var capturedGeneration = -1L
          OpenIvmStateBarrier.withCaptureRootForTesting(blockedRoot) {
            capturedGeneration = OpenIvmStateBarrier.mutationGenerationForTesting(blockedRoot)
          }
          capturedGeneration
        }
      })
      awaitCondition() { OpenIvmStateBarrier.captureQueuedForTesting(blockedRoot) }

      val peerFutures = new ArrayBuffer[Future[(Long, Long)]]()
      roots.zipWithIndex.drop(1).foreach { case (root, index) =>
        peerFutures += executor.submit(new Callable[(Long, Long)] {
          override def call(): (Long, Long) = {
            val started = System.nanoTime()
            if (index == failureRootIndex) {
              failCaptureOnce(root, failedCaptures, "injected peer capture failure")
              retries.incrementAndGet()
            }
            var generation = -1L
            OpenIvmStateBarrier.withCaptureRootForTesting(root) {
              generation = OpenIvmStateBarrier.mutationGenerationForTesting(root)
            }
            (System.nanoTime() - started, generation)
          }
        })
      }

      val peerCaptures = peerFutures.map(_.get(60, TimeUnit.SECONDS)).toVector
      peerCaptures.size shouldBe rootCount - 1
      peerCaptures.foreach(_._2 shouldBe 0L)
      blockedCapture.isDone shouldBe false
      OpenIvmStateBarrier.mutationGenerationForTesting(blockedRoot) shouldBe 0L
      val peerCompletedBeforeRelease = peerCaptures.size
      releaseMutation.countDown()
      val blockedGeneration = blockedCapture.get(60, TimeUnit.SECONDS)
      blockedGeneration should be >= requestedGeneration
      mutationThread.join(5000)
      mutationThread.isAlive shouldBe false
      Option(mutationFailure.get()) shouldBe empty

      val generations = peerCaptures.map(_._2) :+ blockedGeneration
      generations.count(_ == 0L) shouldBe rootCount - 1
      generations.count(_ == 1L) shouldBe 1
      failedCaptures.get() shouldBe 1
      retries.get() shouldBe 1
      val elapsedNanos = System.nanoTime() - captureStarted
      val peerNanos    = peerCaptures.map(_._1)
      val result =
        s"""{"root_count":$rootCount,"peer_roots":${rootCount - 1},"peer_completed_before_blocked_root_release":$peerCompletedBeforeRelease,"blocked_root_requested_generation":$requestedGeneration,"blocked_root_captured_generation":$blockedGeneration,"failed_capture_attempts":${failedCaptures
            .get()},"retries":${retries
            .get()},"successful_captures":$rootCount,"wall_clock_ms":${elapsedNanos / 1000000L},"peer_capture_p50_ms":${percentile(
            peerNanos,
            0.50
          ) / 1000000L},"peer_capture_p95_ms":${percentile(
            peerNanos,
            0.95
          ) / 1000000L},"peer_capture_max_ms":${peerNanos.foldLeft(0L)(
            _ max _
          ) / 1000000L},"archive_bytes":0,"upload_count":0,"scope":"local barrier only; no checkpoint, archive, or remote transfer"}"""
      result
    } finally {
      releaseMutation.countDown()
      mutationThread.join(5000)
      executor.shutdownNow()
      executor.awaitTermination(5, TimeUnit.SECONDS)
    }
  }

  test("1, 200, and 428 independent roots retain contention and failure isolation") {
    val runId   = UUID.randomUUID().toString
    val results = Seq(1, 200, 428).map(runScale(_, runId))
    Files.createDirectories(reportDirectory)
    val json =
      s"""{"schema_version":1,"run_id":"$runId","scope":"synthetic local OpenIvmStateBarrier contention/failure only","remote_latency_measured":false,"runs":[${results
          .mkString(",")}]}
"""
    Files.write(reportDirectory.resolve("root-scaling-results.json"), json.getBytes(StandardCharsets.UTF_8))
    results should have size 3
  }
}
