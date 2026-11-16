package org.openivm.spark.commands

import org.openivm.spark.common.{LifecycleDeadline, LifecycleLockTimeoutException}
import org.openivm.spark.telemetry.metrics.OpenIvmMetrics
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration._

class RefreshMutexDeadlineSpec extends AnyFunSpec with Matchers {
  private def awaitQueued(key: String, thread: Thread): Unit = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
    while (!RefreshMutex.hasQueuedThread(key, thread) && System.nanoTime() < deadline) Thread.`yield`()
    RefreshMutex.hasQueuedThread(key, thread) shouldBe true
  }

  describe("bounded shared lifecycle guards") {
    it("shares timed and untimed locks, rejects contention, and permits unrelated targets") {
      val entered = new CountDownLatch(1)
      val release = new CountDownLatch(1)
      val exited  = new CountDownLatch(1)
      val key     = "issue67_mutex_contended"
      val owner = new Thread(
        () => {
          try
            RefreshMutex.withLock(key) {
              entered.countDown()
              release.await()
            }
          finally exited.countDown()
        },
        "issue67-mutex-owner"
      )
      owner.setDaemon(true)
      owner.start()
      try {
        entered.await(5, TimeUnit.SECONDS) shouldBe true
        val queued = OpenIvmMetrics.RefreshQueued.get()
        intercept[LifecycleLockTimeoutException] {
          RefreshMutex.withLock(key, LifecycleDeadline.start(20.millis))(fail("Contended body must not run"))
        }
        OpenIvmMetrics.RefreshQueued.get() shouldBe queued
        RefreshMutex.withLock("issue67_mutex_unrelated", LifecycleDeadline.start(5.seconds))(7) shouldBe 7
      } finally {
        release.countDown()
        exited.await(5, TimeUnit.SECONDS) shouldBe true
      }
      RefreshMutex.withLock(key, LifecycleDeadline.start(5.seconds))(8) shouldBe 8
    }

    it("supports canonical duplicate/reentrant acquisition and unwinds every guard on failure") {
      val keys    = Seq("issue67_mutex_b", "issue67_mutex_a", "issue67_mutex_a")
      val failure = new IllegalStateException("native stop failed")
      val queued  = OpenIvmMetrics.RefreshQueued.get()
      intercept[IllegalStateException] {
        RefreshMutex.withLocks(keys, LifecycleDeadline.start(5.seconds)) {
          RefreshMutex.withLock(keys.last, LifecycleDeadline.start(5.seconds))(throw failure)
        }
      } shouldBe failure
      OpenIvmMetrics.RefreshQueued.get() shouldBe queued
      RefreshMutex.withLocks(keys, LifecycleDeadline.start(5.seconds))(true) shouldBe true
    }

    it("allows concurrent readers and gives queued writers priority over later readers") {
      val key                = "issue67_rw_fairness"
      val readersEntered     = new CountDownLatch(2)
      val releaseReaders     = new CountDownLatch(1)
      val writerEntered      = new CountDownLatch(1)
      val releaseWriter      = new CountDownLatch(1)
      val lateReaderEntered  = new CountDownLatch(1)
      val workerFailure      = new AtomicReference[Throwable]()
      val readers = (1 to 2).map { index =>
        new Thread(
          () => {
            try
              RefreshMutex.withReadLock(key) {
                readersEntered.countDown()
                releaseReaders.await()
              }
            catch { case error: Throwable => workerFailure.compareAndSet(null, error) }
          },
          s"issue67-read-$index"
        )
      }
      readers.foreach(_.start())
      val writer = new Thread(
        () => {
          try
            RefreshMutex.withLock(key) {
              writerEntered.countDown()
              releaseWriter.await()
            }
          catch { case error: Throwable => workerFailure.compareAndSet(null, error) }
        },
        "issue67-writer"
      )
      var lateReader: Thread = null
      try {
        readersEntered.await(5, TimeUnit.SECONDS) shouldBe true
        writer.start()
        awaitQueued(key, writer)
        writerEntered.getCount shouldBe 1L
        releaseReaders.countDown()
        writerEntered.await(5, TimeUnit.SECONDS) shouldBe true

        lateReader = new Thread(
          () => {
            try RefreshMutex.withReadLock(key) { lateReaderEntered.countDown() }
            catch { case error: Throwable => workerFailure.compareAndSet(null, error) }
          },
          "issue67-late-reader"
        )
        lateReader.start()
        awaitQueued(key, lateReader)
        lateReaderEntered.getCount shouldBe 1L
        releaseWriter.countDown()
        lateReaderEntered.await(5, TimeUnit.SECONDS) shouldBe true
      } finally {
        releaseReaders.countDown()
        releaseWriter.countDown()
        readers.foreach(_.join(TimeUnit.SECONDS.toMillis(5)))
        if (writer.isAlive) writer.join(TimeUnit.SECONDS.toMillis(5))
        if (lateReader != null && lateReader.isAlive) lateReader.join(TimeUnit.SECONDS.toMillis(5))
      }
      workerFailure.get() shouldBe null
    }

    it("rejects read-to-write upgrades and releases read locks after timeout or interruption") {
      val key = "issue67_rw_upgrade"
      intercept[IllegalStateException] {
        RefreshMutex.withReadLock(key) {
          RefreshMutex.withLock(key)(fail("A read-to-write upgrade must fail before waiting"))
        }
      }
      val writerEntered = new CountDownLatch(1)
      val releaseWriter = new CountDownLatch(1)
      val writer = new Thread(
        () => RefreshMutex.withLock(key) {
          writerEntered.countDown()
          releaseWriter.await()
        },
        "issue67-rw-timeout-writer"
      )
      writer.start()
      try {
        writerEntered.await(5, TimeUnit.SECONDS) shouldBe true
        intercept[LifecycleLockTimeoutException] {
          RefreshMutex.withReadLock(key, LifecycleDeadline.start(20.millis))(fail("Timed read body must not run"))
        }
      } finally {
        releaseWriter.countDown()
        writer.join(TimeUnit.SECONDS.toMillis(5))
      }

      try {
        Thread.currentThread().interrupt()
        intercept[InterruptedException] {
          RefreshMutex.withReadLock("issue67_rw_interrupt", LifecycleDeadline.start(5.seconds))(
            fail("Interrupted read body must not run")
          )
        }
        Thread.currentThread().isInterrupted shouldBe true
      } finally {
        val _ = Thread.interrupted()
      }
      RefreshMutex.withReadLock("issue67_rw_interrupt", LifecycleDeadline.start(5.seconds))(true) shouldBe true
    }

    it("normalizes mixed lock request orders and promotes duplicate reads to writes") {
      val readKey  = "issue67_rw_mixed_a"
      val writeKey = "issue67_rw_mixed_b"
      RefreshMutex.withLocks(Seq(readKey, writeKey), Seq(readKey))(true) shouldBe true

      val ready          = new CountDownLatch(2)
      val start           = new CountDownLatch(1)
      val finished        = new CountDownLatch(2)
      val failure         = new AtomicReference[Throwable]()
      val workers = Seq(
        Seq("issue67_rw_order_a", "issue67_rw_order_b"),
        Seq("issue67_rw_order_b", "issue67_rw_order_a")
      ).map { keys =>
        new Thread(
          () => {
            ready.countDown()
            try {
              start.await()
              RefreshMutex.withLocks(keys, LifecycleDeadline.start(5.seconds))(())
            } catch { case error: Throwable => failure.compareAndSet(null, error) }
            finally finished.countDown()
          },
          s"issue67-order-${keys.head}"
        )
      }
      workers.foreach(_.start())
      ready.await(5, TimeUnit.SECONDS) shouldBe true
      start.countDown()
      finished.await(5, TimeUnit.SECONDS) shouldBe true
      workers.foreach(_.join(TimeUnit.SECONDS.toMillis(5)))
      failure.get() shouldBe null
    }

    it("preserves timed-admission interruption and balances queue metrics") {
      val queued = OpenIvmMetrics.RefreshQueued.get()
      try {
        Thread.currentThread().interrupt()
        intercept[InterruptedException] {
          RefreshMutex.withLock("issue67_mutex_interrupt", LifecycleDeadline.start(5.seconds))(fail("Interrupted body"))
        }
        Thread.currentThread().isInterrupted shouldBe true
        OpenIvmMetrics.RefreshQueued.get() shouldBe queued
      } finally {
        val _ = Thread.interrupted()
      }
      RefreshMutex.withLock("issue67_mutex_interrupt", LifecycleDeadline.start(5.seconds))(true) shouldBe true
    }
  }
}
