package org.openivm.spark.common

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import scala.concurrent.duration._

class LifecycleDeadlineSpec extends AnyFunSpec with Matchers {

  describe("shared lifecycle deadline") {
    it("spends one monotonic budget without restarting it") {
      val clock    = new AtomicLong(100L)
      val deadline = LifecycleDeadline.start(1.second, () => clock.get())
      clock.addAndGet(400.millis.toNanos)
      deadline.remainingNanos shouldBe 600.millis.toNanos
      clock.addAndGet(600.millis.toNanos)
      deadline.remainingNanos shouldBe 0L
      clock.incrementAndGet()
      deadline.remainingNanos shouldBe 0L
    }

    it("handles nanoTime wraparound without extending the deadline") {
      val clock    = new AtomicLong(Long.MaxValue - 10L)
      val deadline = LifecycleDeadline.start(20.nanos, () => clock.get())
      clock.addAndGet(15L)
      deadline.remainingNanos shouldBe 5L
      clock.addAndGet(5L)
      deadline.remainingNanos shouldBe 0L
    }

    it("rejects nonpositive budgets") {
      intercept[IllegalArgumentException](LifecycleDeadline.start(0.nanos))
      intercept[IllegalArgumentException](LifecycleDeadline.start((-1).second))
    }

    it("unwinds reentrant guards when the body fails") {
      val lock     = new ReentrantLock()
      val deadline = LifecycleDeadline.start(5.seconds)
      val failure  = new IllegalStateException("expected body failure")
      val observed = intercept[IllegalStateException] {
        deadline.withLock(lock, "issue67_deadline") {
          deadline.withLock(lock, "issue67_deadline") {
            lock.getHoldCount shouldBe 2
            throw failure
          }
        }
      }
      observed shouldBe failure
      lock.isLocked shouldBe false
    }

    it("releases earlier guards when a nested acquisition exhausts the shared budget") {
      val clock    = new AtomicLong()
      val deadline = LifecycleDeadline.start(1.second, () => clock.get())
      val first    = new ReentrantLock()
      val second   = new ReentrantLock()
      intercept[LifecycleLockTimeoutException] {
        deadline.withLock(first, "issue67_first") {
          clock.set(1.second.toNanos)
          deadline.withLock(second, "issue67_second")(fail("Expired admission must not enter its body"))
        }
      }.target shouldBe "issue67_second"
      first.isLocked shouldBe false
      second.isLocked shouldBe false
    }

    it("preserves caller interruption and acquires no lock") {
      val lock = new ReentrantLock()
      try {
        Thread.currentThread().interrupt()
        intercept[InterruptedException] {
          LifecycleDeadline.start(5.seconds).withLock(lock, "issue67_interrupt")(fail("Interrupted body"))
        }
        Thread.currentThread().isInterrupted shouldBe true
        lock.isLocked shouldBe false
      } finally {
        val _ = Thread.interrupted()
      }
    }
  }
}
