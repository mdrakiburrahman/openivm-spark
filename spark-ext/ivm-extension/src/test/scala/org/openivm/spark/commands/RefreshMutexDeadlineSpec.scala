package org.openivm.spark.commands

import org.openivm.spark.common.{LifecycleDeadline, LifecycleLockTimeoutException}
import org.openivm.spark.telemetry.metrics.OpenIvmMetrics
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.concurrent.duration._

class RefreshMutexDeadlineSpec extends AnyFunSpec with Matchers {
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
