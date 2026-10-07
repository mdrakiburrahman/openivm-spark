package org.openivm.spark.streaming

import org.openivm.spark.common.LifecycleDeadline
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

class StreamingTableStopLifecycleSpec extends AnyFunSpec with Matchers {
  private val observation = StreamingQueryStopObservation(
    "query",
    "run",
    60000L,
    0L,
    true,
    false,
    None,
    true,
    None,
    false
  )

  describe("writer quiescence before lifecycle mutation") {
    it("stops every required writer before any destructive work") {
      val actions = ArrayBuffer.empty[String]
      StreamingTableStopLifecycle.afterStops(
        Seq("leaf", "child", "root").map(name => () => { actions += name; () })
      ) {
        actions += "archive"
        actions += "drop"
      }
      actions.toSeq shouldBe Seq("leaf", "child", "root", "archive", "drop")
    }

    it("refuses all mutation after any root or descendant timeout in each destructive lifecycle") {
      for {
        operation <- Seq("drop", "rebuild", "restart", "mixed_cascade", "recovery")
        failed    <- 0 until 3
      } {
        val mutations = ArrayBuffer.empty[String]
        val error     = new StreamingQueryStopTimeoutException(operation, observation)
        intercept[StreamingQueryStopTimeoutException] {
          StreamingTableStopLifecycle.afterStops(
            (0 until 3).map(index =>
              () => {
                if (index == failed) throw error
              }
            )
          ) {
            mutations ++= Seq("archive", "drop", "checkpoint", "manifest", "dependencies", "journal", "create")
          }
        } shouldBe error
        mutations shouldBe empty
      }
    }

    it("uses one real coordinator budget across writers instead of multiplying the timeout") {
      val clock     = new AtomicLong()
      val deadline  = LifecycleDeadline.start(1.second, () => clock.get())
      val stopper   = new StreamingQueryStopper(4)
      val mutations = ArrayBuffer.empty[String]
      val queries = (0 until 3).map { _ =>
        val active = new AtomicBoolean(true)
        new StreamingQueryTestDouble(
          () => {
            clock.addAndGet(500.millis.toNanos)
            active.set(false)
          },
          active
        )
      }
      try {
        intercept[StreamingQueryStopTimeoutException] {
          StreamingTableStopLifecycle.afterStops(
            queries.map(query =>
              () => {
                stopper.stop(query, "issue67_cascade", deadline, () => ())
                ()
              }
            )
          ) { mutations += "drop" }
        }
        deadline.remainingNanos shouldBe 0L
        queries.last.stopCalls.get() shouldBe 0
        mutations shouldBe empty
      } finally stopper.shutdown()
    }

    it("allows already-quiescent cleanup and propagates mutation errors without success fallback") {
      StreamingTableStopLifecycle.afterStops(Seq.empty)(42) shouldBe 42
      val failure = new IllegalStateException("archive failed")
      intercept[IllegalStateException] {
        StreamingTableStopLifecycle.afterStops(Seq.empty)(throw failure)
      } shouldBe failure
    }
  }
}
