package org.openivm.spark.testkit

import java.sql.Timestamp
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

/** Load-relative calibration for hot-path cost assertions.
  *
  * An absolute wall-clock budget ("10k calls in under 50 ms") is not a
  * property of the code — it is a property of the code AND of how contended
  * the box happens to be.  A full-suite run oversubscribes this machine (32
  * forked JVMs x `local[4]` on 32 cores), so an absolute budget calibrated on
  * an idle box eventually fails for a build that contains no regression at
  * all.  Inflating the constant would only move that point further out while
  * making the gate blinder to real regressions.
  *
  * Instead, measure the subject against a REFERENCE workload of the same shape
  * (allocate a wide row carrying a `Timestamp`, append it to an
  * `ArrayBuffer`), interleaved in the same JVM, in the same instant, under the
  * same load, and take medians.  The regression signal is preserved because
  * the property being asserted is the real one: `record()` must stay within a
  * small constant factor of a bare buffered append — no IO, no serialization,
  * no locking.  A regression that adds any of those blows the ratio no matter
  * how loaded the box is, whereas contention scales BOTH measurements and
  * cancels out.
  *
  * The absolute floor is kept as a lower bound on the budget so the gate never
  * becomes STRICTER than the original constant on a fast, idle box; it only
  * relaxes in proportion to demonstrated machine slowness.
  */
private[spark] object HotPathBudget {

  private final case class ReferenceRow(
      refreshId: String,
      viewName: String,
      profileTimestamp: Timestamp,
      stmtOrder: Int,
      attemptIdx: Int,
      mode: String,
      category: String,
      stmtKind: String,
      durationMs: Long,
      sqlText: String
  )

  /** The denominator: the irreducible work any buffering collector must do. */
  private def referenceLoop(iterations: Int): Int = {
    val sink = ArrayBuffer.empty[ReferenceRow]
    var i    = 0
    while (i < iterations) {
      sink += ReferenceRow(
        refreshId = "qlog_bench_view_1234567890",
        viewName = "qlog_bench_view",
        profileTimestamp = new Timestamp(System.currentTimeMillis()),
        stmtOrder = i,
        attemptIdx = 0,
        mode = "refresh",
        category = "rewritten_stmt",
        stmtKind = "merge",
        durationMs = 0L,
        sqlText = "SELECT 1"
      )
      i += 1
    }
    sink.length
  }

  final case class Measurement(
      label: String,
      iterations: Int,
      repetitions: Int,
      subjectNanos: Long,
      referenceNanos: Long
  ) {
    def subjectMillis: Long   = subjectNanos / 1000000L
    def referenceMillis: Long = referenceNanos / 1000000L
    def subjectNanosPerCall: Long =
      if (iterations == 0) 0L else subjectNanos / iterations
    def ratio: Double =
      if (referenceNanos == 0L) Double.PositiveInfinity else subjectNanos.toDouble / referenceNanos.toDouble

    /** Budget = the absolute floor, relaxed by however much slower than an
      * idle box this JVM is right now (never tightened below the floor).
      */
    def budgetNanos(ratioBudget: Double, floor: FiniteDuration): Long =
      math.max(floor.toNanos, (referenceNanos * ratioBudget).toLong)

    def describe(ratioBudget: Double, floor: FiniteDuration): String =
      f"$label: $iterations iterations x $repetitions reps (median) took ${subjectMillis}ms " +
        f"(${subjectNanosPerCall}ns/call); reference ${referenceMillis}ms; ratio $ratio%.2f " +
        f"(budget ${ratioBudget}x reference, floor ${floor.toMillis}ms, " +
        f"effective ${budgetNanos(ratioBudget, floor) / 1000000L}ms)"
  }

  /** Time `subject` and the reference workload interleaved, alternating which
    * one runs first so neither systematically absorbs a scheduler hiccup, and
    * report the median of each.
    */
  def measure(label: String, iterations: Int, repetitions: Int = 5)(subject: Int => Unit): Measurement = {
    require(repetitions >= 3, s"need at least 3 repetitions to take a median, got $repetitions")

    // Warm up both sides so the comparison is JIT-steady-state on both.
    subject(iterations)
    referenceLoop(iterations)

    def timed(body: => Any): Long = {
      val t0 = System.nanoTime()
      body
      System.nanoTime() - t0
    }

    val subjectSamples   = ArrayBuffer.empty[Long]
    val referenceSamples = ArrayBuffer.empty[Long]
    var rep              = 0
    while (rep < repetitions) {
      if (rep % 2 == 0) {
        subjectSamples += timed(subject(iterations))
        referenceSamples += timed(referenceLoop(iterations))
      } else {
        referenceSamples += timed(referenceLoop(iterations))
        subjectSamples += timed(subject(iterations))
      }
      rep += 1
    }

    def median(samples: ArrayBuffer[Long]): Long = samples.sorted.apply(samples.length / 2)

    Measurement(label, iterations, repetitions, median(subjectSamples), median(referenceSamples))
  }
}
