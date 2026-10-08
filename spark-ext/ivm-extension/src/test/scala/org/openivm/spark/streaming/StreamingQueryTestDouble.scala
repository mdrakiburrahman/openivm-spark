package org.openivm.spark.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{
  StreamingQuery,
  StreamingQueryException,
  StreamingQueryProgress,
  StreamingQueryStatus
}

import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}

private[streaming] final class StreamingQueryTestDouble(
    stopAction: () => Unit,
    val active: AtomicBoolean = new AtomicBoolean(true),
    override val id: UUID = UUID.randomUUID(),
    override val runId: UUID = UUID.randomUUID(),
    progress: StreamingQueryProgress = null,
    nativeFailure: Option[StreamingQueryException] = None,
    override val name: String = "openivm-unit-query"
) extends StreamingQuery {

  val stopCalls      = new AtomicInteger()
  val stoppingThread = new AtomicReference[Thread]()

  override def isActive: Boolean                          = active.get()
  override def lastProgress: StreamingQueryProgress       = progress
  override def exception: Option[StreamingQueryException] = nativeFailure
  override def stop(): Unit = {
    stopCalls.incrementAndGet()
    stoppingThread.set(Thread.currentThread())
    stopAction()
  }

  private def unused[A](): A = throw new UnsupportedOperationException("Unused native query API")

  override def sparkSession: SparkSession                    = unused()
  override def status: StreamingQueryStatus                  = unused()
  override def recentProgress: Array[StreamingQueryProgress] = unused()
  override def awaitTermination(): Unit                      = unused()
  override def awaitTermination(timeoutMs: Long): Boolean    = unused()
  override def processAllAvailable(): Unit                   = unused()
  override def explain(): Unit                               = unused()
  override def explain(extended: Boolean): Unit              = unused()
}
