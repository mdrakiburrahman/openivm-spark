package org.openivm.spark.common

import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Callable, ExecutionException, Executors, TimeUnit}
import scala.util.control.NonFatal

final case class CtasBatchTask[T](id: String, run: () => T)

final case class CtasTaskSpan(
    taskId: String,
    submittedEpochMs: Long,
    startedEpochMs: Long,
    endedEpochMs: Long,
    queueNanos: Long,
    durationNanos: Long,
    inflight: Int,
    threadName: String,
    outcome: String,
    capacityDrop: Boolean
)

final case class CtasBatchTelemetry(
    schedulerMode: String,
    limitBefore: Int,
    limitAfter: Int,
    maxInflight: Int,
    batchWallNanos: Long,
    spans: Seq[CtasTaskSpan]
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

/** Batch-boundary additive-increase/multiplicative-decrease admission.
  *
  * The first batch remains optimistic for unbiased cold-start benchmarks.
  * Later successful batches increase by one; only failures classified as
  * capacity pressure back off. Ordinary SQL/application failures are returned
  * to the caller without poisoning future admission.
  */
final class CtasAdmissionController private (val initialLimit: Int, backoffRatio: Double) {
  require(initialLimit > 0, "initialLimit must be positive")
  require(backoffRatio > 0.0 && backoffRatio < 1.0, "backoffRatio must be between zero and one")

  private var learnedLimit = initialLimit
  private var maximumSeen  = initialLimit

  def currentLimit: Int = synchronized(learnedLimit)

  private[common] def completeBatch(batchSize: Int, capacityDrop: Boolean): (Int, Int) = synchronized {
    maximumSeen = math.max(maximumSeen, batchSize)
    val before = learnedLimit
    learnedLimit =
      if (capacityDrop) math.max(1, math.floor(learnedLimit * backoffRatio).toInt)
      else math.min(maximumSeen, learnedLimit + 1)
    before -> learnedLimit
  }
}

object CtasAdmissionController {
  def optimistic(batchSize: Int, backoffRatio: Double = 0.5): CtasAdmissionController =
    new CtasAdmissionController(batchSize, backoffRatio)
}

object CtasCapacitySignal {
  private val pressureFragments = Seq(
    "outofmemory",
    "executor lost",
    "executor heartbeat timed out",
    "unable to acquire",
    "too many requests",
    "throttl",
    "no space left",
    "fetchfailed"
  )

  def default(error: Throwable): Boolean = {
    val chain = Iterator.iterate(error)(_.getCause).takeWhile(_ != null).toSeq
    chain.exists {
      case _: OutOfMemoryError => true
      case sparkError: SparkException =>
        pressureFragments.exists(sparkError.toString.toLowerCase(java.util.Locale.ROOT).contains)
      case other => pressureFragments.exists(other.toString.toLowerCase(java.util.Locale.ROOT).contains)
    }
  }
}

/** Utility for batch owners (for example an HTTP benchmark ingress).
  * OpenIVM's single CREATE command does not itself own a batch. */
object CtasBatchDispatcher {
  private val SchedulerPoolKey  = "spark.scheduler.pool"
  private val JobDescriptionKey = "spark.job.description"

  def run[T](
      spark: SparkSession,
      tasks: Seq[CtasBatchTask[T]],
      controller: CtasAdmissionController,
      poolPrefix: String = "openivm-ctas",
      isCapacityFailure: Throwable => Boolean = CtasCapacitySignal.default
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

    final case class Outcome(index: Int, value: Option[T], failure: Option[Throwable], span: CtasTaskSpan)

    def recordMax(value: Int): Unit = {
      var previous = maxInflight.get()
      while (value > previous && !maxInflight.compareAndSet(previous, value)) previous = maxInflight.get()
    }

    val futures = tasks.zipWithIndex.map { case (task, index) =>
      val submittedNanos   = System.nanoTime()
      val submittedEpochMs = System.currentTimeMillis()
      executor.submit(new Callable[Outcome] {
        override def call(): Outcome = {
          val sc                  = spark.sparkContext
          val previousPool        = sc.getLocalProperty(SchedulerPoolKey)
          val previousDescription = sc.getLocalProperty(JobDescriptionKey)
          val active              = inflight.incrementAndGet()
          val startedNanos        = System.nanoTime()
          val startedEpochMs      = System.currentTimeMillis()
          val threadName          = Thread.currentThread().getName
          recordMax(active)
          sc.setLocalProperty(SchedulerPoolKey, s"$poolPrefix-${task.id}")
          sc.setLocalProperty(JobDescriptionKey, s"OpenIVM CTAS ${task.id}")
          var failure: Throwable = null
          var value: Option[T]   = None
          try value = Some(task.run())
          catch {
            case error: OutOfMemoryError => failure = error
            case NonFatal(error)         => failure = error
          } finally {
            sc.setLocalProperty(SchedulerPoolKey, previousPool)
            sc.setLocalProperty(JobDescriptionKey, previousDescription)
            inflight.decrementAndGet()
          }
          val endedNanos   = System.nanoTime()
          val capacityDrop = failure != null && isCapacityFailure(failure)
          val span = CtasTaskSpan(
            taskId = task.id,
            submittedEpochMs = submittedEpochMs,
            startedEpochMs = startedEpochMs,
            endedEpochMs = System.currentTimeMillis(),
            queueNanos = startedNanos - submittedNanos,
            durationNanos = endedNanos - startedNanos,
            inflight = active,
            threadName = threadName,
            outcome = if (failure == null) "success" else "failure",
            capacityDrop = capacityDrop
          )
          Outcome(index, value, Option(failure), span)
        }
      })
    }

    val outcomes =
      try futures.map(_.get()).sortBy(_.index)
      catch {
        case interrupted: InterruptedException =>
          executor.shutdownNow()
          Thread.currentThread().interrupt()
          throw interrupted
        case execution: ExecutionException =>
          executor.shutdownNow()
          throw Option(execution.getCause).getOrElse(execution)
      } finally {
        executor.shutdown()
        if (!executor.awaitTermination(1, TimeUnit.MINUTES)) executor.shutdownNow()
      }

    val failures      = outcomes.zip(tasks).flatMap { case (outcome, task) => outcome.failure.map(task.id -> _) }
    val batchDuration = System.nanoTime() - batchStart
    val capacityDrop  = outcomes.exists(_.span.capacityDrop)
    val (limitBefore, limitAfter) = controller.completeBatch(tasks.size, capacityDrop)
    val telemetry = CtasBatchTelemetry(
      schedulerMode = schedulerMode,
      limitBefore = limitBefore,
      limitAfter = limitAfter,
      maxInflight = maxInflight.get(),
      batchWallNanos = batchDuration,
      spans = outcomes.map(_.span)
    )
    if (failures.nonEmpty) throw new CtasBatchFailedException(failures, telemetry)
    CtasBatchResult(outcomes.flatMap(_.value), telemetry)
  }
}
