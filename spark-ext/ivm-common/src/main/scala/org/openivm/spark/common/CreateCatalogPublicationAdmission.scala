package org.openivm.spark.common

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.openivm.spark.telemetry.OpenIvmExecutionSpan
import org.openivm.spark.telemetry.metrics.OpenIvmMetrics

import java.util.Collections
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicInteger

/** Application-scoped admission for the named Hive/external-catalog
  * registration that follows a fully concurrent path-based Delta CTAS.
  */
object CreateCatalogPublicationAdmission {

  private final class State(width: Int) {
    private val permits  = new Semaphore(width, true)
    private val queued   = new AtomicInteger(0)
    private val inflight = new AtomicInteger(0)

    def withPermit[A](body: => A): A = {
      val waitStarted = System.nanoTime()
      OpenIvmMetrics.CreateCatalogPublicationWidth.set(width)
      OpenIvmExecutionSpan.recordActiveCatalogPublicationAdmission(width, 0L)
      OpenIvmMetrics.CreateCatalogPublicationQueued.set(queued.incrementAndGet())
      try permits.acquire()
      catch {
        case interrupted: InterruptedException =>
          OpenIvmMetrics.CreateCatalogPublicationQueued.set(queued.decrementAndGet())
          Thread.currentThread().interrupt()
          throw interrupted
      }

      OpenIvmMetrics.CreateCatalogPublicationQueued.set(queued.decrementAndGet())
      try {
        val waitNanos = System.nanoTime() - waitStarted
        OpenIvmExecutionSpan.recordActiveCatalogPublicationAdmission(width, waitNanos)
        OpenIvmMetrics.updateTimer(
          "create.catalog_publication_admission.wait",
          waitNanos
        )
        OpenIvmMetrics.CreateCatalogPublicationInflight.set(inflight.incrementAndGet())
        try body
        finally OpenIvmMetrics.CreateCatalogPublicationInflight.set(inflight.decrementAndGet())
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
        val created = new State(FeatureGate.createCatalogPublicationMaxConcurrent(spark))
        states.put(context, created)
        created
      }
    }
    state.withPermit(body)
  }
}
