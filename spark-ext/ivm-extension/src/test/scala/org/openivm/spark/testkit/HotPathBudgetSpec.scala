package org.openivm.spark.testkit

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.sql.Timestamp
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

/** The `RefreshSqlLog` hot-path gate used to be an absolute `50 ms` wall-clock
  * budget, which is not a property of the code: it failed once during a
  * full-suite run at `55 ms` with no regression present, because the box was
  * oversubscribed.  These tests pin what the load-relative replacement does
  * and — just as importantly — what it does not: on an idle box it is exactly
  * the constant it replaced, it relaxes only in proportion to a MEASURED
  * slowdown of the same JVM, it stays far tighter than the alternative "fix"
  * of inflating the constant, and relief can never keep pace with a
  * regression that outruns the reference workload.
  */
class HotPathBudgetSpec extends AnyFunSpec with Matchers {

  private val Floor = 50.millis
  private val Ratio = 8.0

  private def measurement(subjectMs: Long, referenceMs: Long): HotPathBudget.Measurement =
    HotPathBudget.Measurement(
      label = "synthetic",
      iterations = 10000,
      repetitions = 5,
      subjectNanos = subjectMs * 1000000L,
      referenceNanos = referenceMs * 1000000L
    )

  describe("budget calibration") {
    it("is exactly the original absolute gate on a fast, idle box") {
      // Reference at 1 ms: 8x is 8 ms, far under the floor, so the floor wins
      // and this assertion is the 50 ms constant it replaced — no stricter,
      // no laxer.
      val idle = measurement(subjectMs = 40, referenceMs = 1)
      idle.budgetNanos(Ratio, Floor) shouldBe Floor.toNanos
      idle.subjectNanos should be <= idle.budgetNanos(Ratio, Floor)
      measurement(subjectMs = 51, referenceMs = 1).subjectNanos should be > idle.budgetNanos(Ratio, Floor)
    }

    it("relaxes only in proportion to a measured slowdown of the same JVM") {
      // The reference workload is 10x its idle cost, so the box itself is 10x
      // slower; a subject that is slower for that same reason is in budget ...
      val loaded = measurement(subjectMs = 60, referenceMs = 10)
      loaded.budgetNanos(Ratio, Floor) shouldBe (80L * 1000000L)
      loaded.subjectNanos should be <= loaded.budgetNanos(Ratio, Floor)
      // ... which is precisely the full-suite run that failed at 55 ms.
      loaded.subjectMillis should be > Floor.toMillis

      // No slowdown measured, no relaxation granted: a 60 ms subject on a box
      // that is demonstrably idle still fails.
      val idle = measurement(subjectMs = 60, referenceMs = 1)
      idle.subjectNanos should be > idle.budgetNanos(Ratio, Floor)
    }

    it("stays far tighter than inflating the constant would have been") {
      // The alternative fix for the same flake — bump 50 ms to, say, 500 ms —
      // buys headroom unconditionally and hides everything below it. The
      // calibrated budget only ever grants what the reference measurement
      // justifies.
      val inflatedFloor = 500.millis.toNanos
      Seq(1L, 5L, 10L, 20L).foreach { referenceMs =>
        withClue(s"reference=${referenceMs}ms: ") {
          measurement(subjectMs = 0, referenceMs = referenceMs)
            .budgetNanos(Ratio, Floor) should be < inflatedFloor
        }
      }
    }
  }

  describe("regression signal") {
    it("catches a proportional per-call regression once the box is loaded at all") {
      // A 20x-per-call regression stays caught whether the box is busy or
      // crawling: relief is granted in proportion to the reference, so it can
      // never keep pace with a regression that outruns the reference.
      Seq(5L, 20L, 200L).foreach { referenceMs =>
        val regressed = measurement(subjectMs = referenceMs * 20, referenceMs = referenceMs)
        withClue(s"reference=${referenceMs}ms: ") {
          regressed.ratio shouldBe 20.0 +- 0.001
          regressed.subjectNanos should be > regressed.budgetNanos(Ratio, Floor)
        }
      }
    }

    it("flags a hot path that starts serializing, at idle and under load alike") {
      val sink = ArrayBuffer.empty[Int]
      val heavy = HotPathBudget.measure("heavy-subject", iterations = 10000) { iterations =>
        var i = 0
        while (i < iterations) {
          // Stand-in for the class of work this gate exists to keep off the
          // hot path (serialization / IO / locking).
          var round = 0
          while (round < 8) {
            val bytes  = new ByteArrayOutputStream()
            val stream = new ObjectOutputStream(bytes)
            stream.writeObject(("rewritten_stmt", i, round, "SELECT 1"))
            stream.close()
            sink += bytes.size()
            round += 1
          }
          i += 1
        }
        sink.clear()
      }
      withClue(heavy.describe(Ratio, Floor) + ": ") {
        heavy.subjectNanos should be > heavy.budgetNanos(Ratio, Floor)
        // Relief cannot rescue it: serializing costs tens of times a bare
        // append (measured ~83x idle), so the reference workload can never
        // inflate the budget fast enough to cover it.
        heavy.ratio should be > 20.0
      }
    }

    it("passes a subject that is a bare buffered append") {
      val sink = ArrayBuffer.empty[(String, Timestamp, Int)]
      val cheap = HotPathBudget.measure("cheap-subject", iterations = 10000) { iterations =>
        var i = 0
        while (i < iterations) {
          sink += (("rewritten_stmt", new Timestamp(System.currentTimeMillis()), i))
          i += 1
        }
        sink.clear()
      }
      withClue(cheap.describe(Ratio, Floor) + ": ") {
        cheap.subjectNanos should be <= cheap.budgetNanos(Ratio, Floor)
      }
    }
  }
}
