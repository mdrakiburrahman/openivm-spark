package org.openivm.spark.testkit

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import scala.concurrent.duration._

/** A pool drained with `shutdown()` alone never reclaims a worker that is
  * parked inside a production hook, so a failed concurrency assertion used to
  * leave a non-daemon thread — and the whole forked test JVM — alive long
  * after the test that owned it had finished.
  */
class TestPoolsSpec extends AnyFunSpec with Matchers {

  describe("withPool") {
    it("returns the body's value and drains the pool") {
      val ran = new AtomicBoolean(false)
      val result = TestPools.withPool(2) { ec =>
        ec.execute(() => ran.set(true))
        "done"
      }
      result shouldBe "done"
      ran.get() shouldBe true
    }

    it("drains the pool even when the body throws") {
      intercept[RuntimeException] {
        TestPools.withPool(2)(_ => throw new RuntimeException("body failed"))
      }.getMessage shouldBe "body failed"
    }
  }

  describe("drain") {
    it("interrupts a task that ignores the polite shutdown instead of waiting for it") {
      val pool        = Executors.newFixedThreadPool(1)
      val started     = new CountDownLatch(1)
      val neverEnds   = new CountDownLatch(1)
      val interrupted = new AtomicBoolean(false)

      pool.execute { () =>
        started.countDown()
        try neverEnds.await()
        catch { case _: InterruptedException => interrupted.set(true) }
      }
      started.await(30, TimeUnit.SECONDS) shouldBe true

      // A task parked with no one left to release it: the polite shutdown
      // cannot reclaim it, so `drain` must escalate rather than hold the JVM.
      TestPools.drain(pool, 200.millis) shouldBe false
      pool.awaitTermination(30, TimeUnit.SECONDS) shouldBe true
      interrupted.get() shouldBe true
    }

    it("reports a clean drain when every task finishes") {
      val pool = Executors.newFixedThreadPool(2)
      pool.execute(() => ())
      TestPools.drain(pool) shouldBe true
      pool.isTerminated shouldBe true
    }
  }
}
