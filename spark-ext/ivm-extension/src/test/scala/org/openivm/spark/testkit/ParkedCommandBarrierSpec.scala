package org.openivm.spark.testkit

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.concurrent.duration._

/** Regression coverage for the `ConcurrencySpec` full-suite failure.
  *
  * `(0) CREATE does not take a global lock` parked a CREATE inside a
  * production hook with a hard-coded `30 s` budget while allowing the observed
  * REFRESH `300 s` to finish.  Under full-suite load (32 forked JVMs x
  * `local[4]`) the REFRESH regularly took longer than `30 s`, the park expired
  * first, a matcher threw INTO the production CREATE, and the outer
  * `createFuture.isCompleted shouldBe false` reported
  * `true was not equal to false` - a locking verdict for a run in which
  * nothing was locked.
  *
  * These tests pin the properties that make that impossible: the budget
  * ordering is rejected at construction, an expired park neither throws into
  * the production command nor masquerades as a completed command, a slow
  * observation cannot move the barrier, and a failing observation always
  * releases the parked thread.
  */
class ParkedCommandBarrierSpec extends AnyFunSpec with Matchers {

  private def parkOnThread(barrier: ParkedCommandBarrier): (AtomicReference[Throwable], CountDownLatch) = {
    val thrown = new AtomicReference[Throwable](null)
    val done   = new CountDownLatch(1)
    val thread = new Thread(
      () => {
        try barrier.park()
        catch { case t: Throwable => thrown.set(t) }
        finally done.countDown()
      },
      "parked-command"
    )
    thread.setDaemon(true)
    thread.start()
    (thrown, done)
  }

  describe("budget ordering") {
    it("refuses a park failsafe that does not dominate the observation budget") {
      // Exactly the shape that failed in the full suite: park 30 s, observe
      // up to 300 s.
      val failure = intercept[IllegalArgumentException] {
        ParkedCommandBarrier.forObservation(300.seconds, parkFailsafe = 30.seconds)
      }
      failure.getMessage should include("must strictly dominate the observation budget")

      intercept[IllegalArgumentException] {
        ParkedCommandBarrier.forObservation(30.seconds, parkFailsafe = 30.seconds)
      }
      ()
    }

    it("accepts the default failsafe for every realistic observation budget") {
      val barrier = ParkedCommandBarrier.forObservation(300.seconds)
      barrier.parkFailsafe should be > barrier.observationBudget
      barrier.state shouldBe ParkedCommandBarrier.NotEntered
      barrier.isParked shouldBe false
    }
  }

  describe("parking") {
    it("holds the command inside the hook for as long as the observation takes") {
      val barrier        = ParkedCommandBarrier.forObservation(5.seconds)
      val (thrown, done) = parkOnThread(barrier)

      barrier.awaitEntered() shouldBe true
      barrier.isParked shouldBe true

      // A slow observation must not move the barrier: only `release()` can.
      Thread.sleep(750L)
      barrier.state shouldBe ParkedCommandBarrier.Parked
      barrier.parkedNow shouldBe 1

      barrier.release()
      done.await(10, TimeUnit.SECONDS) shouldBe true
      barrier.state shouldBe ParkedCommandBarrier.Released
      barrier.parkCount shouldBe 1
      barrier.parkedNow shouldBe 0
      thrown.get() shouldBe null
    }

    it("reports an expired failsafe as an abandoned park instead of throwing into the command") {
      val barrier        = ParkedCommandBarrier.forObservation(100.millis, parkFailsafe = 300.millis)
      val (thrown, done) = parkOnThread(barrier)

      barrier.awaitEntered() shouldBe true
      done.await(10, TimeUnit.SECONDS) shouldBe true

      // The production command path never sees a test matcher failure ...
      thrown.get() shouldBe null
      // ... and the void observation is self-describing rather than silently
      // folded into a locking verdict.
      barrier.state shouldBe ParkedCommandBarrier.Abandoned
      barrier.isParked shouldBe false
      barrier.describe should include("Abandoned")
    }

    it("waits for every command of a multi-command rendezvous") {
      val barrier = ParkedCommandBarrier.forObservation(5.seconds)
      val first   = parkOnThread(barrier)
      barrier.awaitParked(1) shouldBe true

      val second = parkOnThread(barrier)
      barrier.awaitParked(2) shouldBe true
      barrier.parkedNow shouldBe 2

      barrier.release()
      first._2.await(10, TimeUnit.SECONDS) shouldBe true
      second._2.await(10, TimeUnit.SECONDS) shouldBe true
      barrier.parkCount shouldBe 2
      barrier.state shouldBe ParkedCommandBarrier.Released
    }

    it("times out instead of hanging when a rendezvous partner never arrives") {
      val barrier = ParkedCommandBarrier.forObservation(200.millis)
      val parked  = parkOnThread(barrier)
      barrier.awaitParked(1) shouldBe true
      barrier.awaitParked(2) shouldBe false

      barrier.release()
      parked._2.await(10, TimeUnit.SECONDS) shouldBe true
    }
  }

  describe("release discipline") {
    it("releases the parked command even when the observation body fails") {
      val barrier        = ParkedCommandBarrier.forObservation(5.seconds)
      val (thrown, done) = parkOnThread(barrier)

      intercept[RuntimeException] {
        barrier.use {
          barrier.awaitEntered() shouldBe true
          barrier.isParked shouldBe true
          throw new RuntimeException("observation failed")
        }
      }

      done.await(10, TimeUnit.SECONDS) shouldBe true
      barrier.state shouldBe ParkedCommandBarrier.Released
      thrown.get() shouldBe null
    }

    it("fails fast when the command never reaches the hook") {
      val barrier = ParkedCommandBarrier.forObservation(200.millis)
      barrier.awaitEntered() shouldBe false
      barrier.state shouldBe ParkedCommandBarrier.NotEntered
      barrier.parkCount shouldBe 0
    }
  }
}
