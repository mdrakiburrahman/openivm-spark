package org.openivm.spark.common

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.openivm.spark.telemetry.metrics.OpenIvmMetrics

import java.util.Collections
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicInteger

/** Application-scoped admission for the eager Delta CTAS and its synchronous
  * post-commit catalog work. It intentionally does not cover CREATE analysis,
  * compilation, watermark capture, or OpenIVM metadata publication.
  */
object CreateMaterializationAdmission {

  private final class State(width: Int) {
    private val permits  = new Semaphore(width, true)
    private val queued   = new AtomicInteger(0)
    private val inflight = new AtomicInteger(0)

    def withPermit[A](body: => A): A = {
      val waitStarted = System.nanoTime()
      OpenIvmMetrics.CreateMaterializationWidth.set(width)
      OpenIvmMetrics.CreateMaterializationQueued.set(queued.incrementAndGet())
      try permits.acquire()
      catch {
        case interrupted: InterruptedException =>
          OpenIvmMetrics.CreateMaterializationQueued.set(queued.decrementAndGet())
          Thread.currentThread().interrupt()
          throw interrupted
      }

      OpenIvmMetrics.CreateMaterializationQueued.set(queued.decrementAndGet())
      try {
        OpenIvmMetrics.updateTimer(
          "create.materialization_admission.wait",
          System.nanoTime() - waitStarted
        )
        OpenIvmMetrics.CreateMaterializationInflight.set(inflight.incrementAndGet())
        try body
        finally OpenIvmMetrics.CreateMaterializationInflight.set(inflight.decrementAndGet())
      } finally permits.release()
    }
  }

  private val states: java.util.Map[SparkContext, State] =
    Collections.synchronizedMap(new java.util.WeakHashMap[SparkContext, State]())

  def withPermit[A](spark: SparkSession)(body: => A): A = {
    val state = states.synchronized {
      val context  = spark.sparkContext
      val existing = states.get(context)
      if (existing != null) existing
      else {
        val created = new State(FeatureGate.createMaterializationMaxConcurrent(spark))
        states.put(context, created)
        created
      }
    }
    state.withPermit(body)
  }
}

private[spark] final case class CreateMaterializationTiming(
    totalMs: Long,
    dataWriteMs: Long,
    hiveCatalogPublicationMs: Long
)

private[spark] object CreateMaterializationTiming {
  def fromDeltaCommit(
      startedAtEpochMs: Long,
      totalNanos: Long,
      committedAtEpochMs: Long
  ): CreateMaterializationTiming = {
    val totalMs = math.max(0L, totalNanos / 1000000L)
    val dataWriteMs =
      math.min(totalMs, math.max(0L, committedAtEpochMs - startedAtEpochMs))
    CreateMaterializationTiming(
      totalMs = totalMs,
      dataWriteMs = dataWriteMs,
      hiveCatalogPublicationMs = totalMs - dataWriteMs
    )
  }
}
