package org.openivm.spark.common

import com.netflix.concurrency.limits.limit.AIMDLimit
import org.apache.spark.sql.SparkSession

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Callable, Executors, TimeUnit}
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

final case class CtasBatchTask[T](id: String, run: () => T)

final case class CtasAdmissionDecision(
    taskId: String,
    durationNanos: Long,
    inflight: Int,
    didDrop: Boolean,
    limitBefore: Int,
    limitAfter: Int
)

final case class CtasBatchTelemetry(
    schedulerMode: String,
    initialLimit: Int,
    learnedLimit: Int,
    maxInflight: Int,
    batchWallNanos: Long,
    decisions: Seq[CtasAdmissionDecision]
)

final case class CtasBatchResult[T](values: Seq[T], telemetry: CtasBatchTelemetry)

final class CtasBatchFailedException(
    val failures: Seq[(String, Throwable)],
    val telemetry: CtasBatchTelemetry
) extends RuntimeException(
      failures.map { case (taskId, error) => s"$taskId: ${error.getMessage}" }.mkString("CTAS batch failed: ", "; ", "")
    ) {
  failures.headOption.foreach { case (_, error) => initCause(error) }
}

/** Batch-boundary adaptive admission for independent CTAS jobs.
  *
  * A controller created with [[CtasAdmissionController.optimistic]] starts at
  * the complete cold-batch width. Explicit task failures are loss samples and
  * reduce the learned width with AIMD. Successful task duration is observed but
  * never interpreted as congestion: CTAS jobs can have very different natural
  * runtimes.
  *
  * The learned width applies to the next call to [[CtasBatchDispatcher.run]].
  * Already-running Spark jobs are not cancelled when a loss arrives.
  */
final class CtasAdmissionController private (
    val initialLimit: Int,
    private val limit: AIMDLimit
) {
  private val recorded = ArrayBuffer.empty[CtasAdmissionDecision]

  def currentLimit: Int = limit.getLimit

  def decisions: Seq[CtasAdmissionDecision] = synchronized(recorded.toVector)

  private[common] def record(
      taskId: String,
      startNanos: Long,
      durationNanos: Long,
      inflight: Int,
      didDrop: Boolean
  ): CtasAdmissionDecision = synchronized {
    val before = limit.getLimit
    limit.onSample(startNanos, durationNanos, inflight, didDrop)
    val decision = CtasAdmissionDecision(
      taskId = taskId,
      durationNanos = durationNanos,
      inflight = inflight,
      didDrop = didDrop,
      limitBefore = before,
      limitAfter = limit.getLimit
    )
    recorded += decision
    decision
  }
}

object CtasAdmissionController {
  def optimistic(batchSize: Int, backoffRatio: Double = 0.5): CtasAdmissionController = {
    require(batchSize > 0, "batchSize must be positive")
    val limit = AIMDLimit
      .newBuilder()
      .initialLimit(batchSize)
      .minLimit(1)
      .maxLimit(batchSize)
      .backoffRatio(backoffRatio)
      .timeout(Long.MaxValue, TimeUnit.NANOSECONDS)
      .build()
    new CtasAdmissionController(batchSize, limit)
  }
}

object CtasBatchDispatcher {
  private val SchedulerPoolKey  = "spark.scheduler.pool"
  private val JobDescriptionKey = "spark.job.description"

  def run[T](
      spark: SparkSession,
      tasks: Seq[CtasBatchTask[T]],
      controller: CtasAdmissionController,
      poolPrefix: String = "openivm-ctas"
  ): CtasBatchResult[T] = {
    require(tasks.nonEmpty, "tasks must not be empty")
    require(tasks.map(_.id).distinct.size == tasks.size, "task ids must be unique")

    val schedulerMode = spark.sparkContext.getConf.get("spark.scheduler.mode", "FIFO").toUpperCase
    require(schedulerMode == "FAIR", s"CTAS batch dispatch requires spark.scheduler.mode=FAIR, found $schedulerMode")

    val width       = math.min(tasks.size, controller.currentLimit)
    val executor    = Executors.newFixedThreadPool(width)
    val inflight    = new AtomicInteger(0)
    val maxInflight = new AtomicInteger(0)
    val batchStart  = System.nanoTime()

    final case class Outcome(index: Int, value: Option[T], failure: Option[Throwable])

    def recordMax(value: Int): Unit = {
      var previous = maxInflight.get()
      while (value > previous && !maxInflight.compareAndSet(previous, value)) {
        previous = maxInflight.get()
      }
    }

    val futures = tasks.zipWithIndex.map { case (task, index) =>
      executor.submit(new Callable[Outcome] {
        override def call(): Outcome = {
          val sc                  = spark.sparkContext
          val previousPool        = sc.getLocalProperty(SchedulerPoolKey)
          val previousDescription = sc.getLocalProperty(JobDescriptionKey)
          val active              = inflight.incrementAndGet()
          recordMax(active)
          val started = System.nanoTime()
          sc.setLocalProperty(SchedulerPoolKey, s"$poolPrefix-${task.id}")
          sc.setLocalProperty(JobDescriptionKey, s"OpenIVM CTAS ${task.id}")
          try {
            val value = task.run()
            controller.record(task.id, started, System.nanoTime() - started, active, didDrop = false)
            Outcome(index, Some(value), None)
          } catch {
            case NonFatal(error) =>
              controller.record(task.id, started, System.nanoTime() - started, active, didDrop = true)
              Outcome(index, None, Some(error))
          } finally {
            sc.setLocalProperty(SchedulerPoolKey, previousPool)
            sc.setLocalProperty(JobDescriptionKey, previousDescription)
            inflight.decrementAndGet()
          }
        }
      })
    }

    val outcomes =
      try futures.map(_.get()).sortBy(_.index)
      finally {
        executor.shutdown()
        executor.awaitTermination(1, TimeUnit.MINUTES)
      }
    val telemetry = CtasBatchTelemetry(
      schedulerMode = schedulerMode,
      initialLimit = controller.initialLimit,
      learnedLimit = controller.currentLimit,
      maxInflight = maxInflight.get(),
      batchWallNanos = System.nanoTime() - batchStart,
      decisions = controller.decisions
    )
    val failures = outcomes.zip(tasks).flatMap { case (outcome, task) =>
      outcome.failure.map(task.id -> _)
    }
    if (failures.nonEmpty) throw new CtasBatchFailedException(failures, telemetry)
    CtasBatchResult(outcomes.flatMap(_.value), telemetry)
  }
}
