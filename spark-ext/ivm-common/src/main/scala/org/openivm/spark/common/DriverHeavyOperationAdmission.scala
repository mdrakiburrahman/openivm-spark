package org.openivm.spark.common

import org.apache.spark.sql.SparkSession
import org.openivm.spark.telemetry.metrics.OpenIvmMetrics

import java.util.Collections
import scala.util.control.NonFatal

/** JVM-wide AIMD admission for independent CREATE/REFRESH statements that share
  * one Spark driver but are not submitted through [[CtasBatchDispatcher]].
  */
object DriverHeavyOperationAdmission {

  private final class State(maxConfigured: Int, minHeapHeadroomBytes: Long) {
    private val monitor    = new Object
    private val controller = CtasAdmissionController.optimistic(maxConfigured)
    private var inflight   = 0
    private var queued     = 0

    private def heapHeadroomBytes: Long = {
      val runtime = Runtime.getRuntime
      val used    = runtime.totalMemory() - runtime.freeMemory()
      math.max(0L, runtime.maxMemory() - used)
    }

    private def hasHeapHeadroom: Boolean = {
      val headroom = heapHeadroomBytes
      OpenIvmMetrics.DriverAdmissionHeapHeadroomBytes.set(headroom)
      headroom >= minHeapHeadroomBytes || inflight == 0
    }

    def withPermit[A](operation: String)(body: => A): A = {
      val waitStarted = System.nanoTime()
      monitor.synchronized {
        queued += 1
        OpenIvmMetrics.DriverAdmissionQueued.set(queued)
        try {
          while (inflight >= controller.currentLimit || !hasHeapHeadroom) monitor.wait(250L)
        } catch {
          case interrupted: InterruptedException =>
            queued -= 1
            OpenIvmMetrics.DriverAdmissionQueued.set(queued)
            Thread.currentThread().interrupt()
            throw interrupted
        }
        queued -= 1
        inflight += 1
        OpenIvmMetrics.DriverAdmissionQueued.set(queued)
        OpenIvmMetrics.DriverAdmissionInflight.set(inflight)
        OpenIvmMetrics.DriverAdmissionWidth.set(controller.currentLimit)
      }
      OpenIvmMetrics.updateTimer(s"driver_admission.$operation.wait", System.nanoTime() - waitStarted)
      var capacityDrop = false
      try body
      catch {
        case error: OutOfMemoryError =>
          capacityDrop = true
          throw error
        case NonFatal(error) =>
          capacityDrop = CtasCapacitySignal.default(error)
          throw error
      } finally {
        monitor.synchronized {
          inflight -= 1
          controller.completeBatch(maxConfigured, capacityDrop)
          OpenIvmMetrics.DriverAdmissionHeapHeadroomBytes.set(heapHeadroomBytes)
          OpenIvmMetrics.DriverAdmissionInflight.set(inflight)
          OpenIvmMetrics.DriverAdmissionWidth.set(controller.currentLimit)
          if (capacityDrop) {
            OpenIvmMetrics.DriverAdmissionBackoffEvents.incrementAndGet()
            OpenIvmMetrics.increment(s"driver_admission.$operation.capacity_drop")
          }
          monitor.notifyAll()
        }
      }
    }
  }

  private val states: java.util.Map[String, State] =
    Collections.synchronizedMap(new java.util.HashMap[String, State]())

  def withPermit[A](spark: SparkSession, operation: String)(body: => A): A = {
    if (!FeatureGate.driverAdmissionEnabled(spark)) return body
    val max       = FeatureGate.driverAdmissionMaxConcurrent(spark)
    val threshold = FeatureGate.driverAdmissionMinHeapHeadroomBytes(spark)
    val appId = Option(spark.sparkContext.applicationId)
      .filter(_.nonEmpty)
      .getOrElse("local")
    val key = s"$appId:$max:$threshold"
    val state = states.synchronized {
      val existing = states.get(key)
      if (existing != null) existing
      else {
        val created = new State(max, threshold)
        states.put(key, created)
        created
      }
    }
    state.withPermit(operation)(body)
  }
}
