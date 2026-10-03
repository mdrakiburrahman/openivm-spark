package org.openivm.spark.insights

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{
  AccumulableInfo,
  SparkListener,
  SparkListenerApplicationEnd,
  SparkListenerJobEnd,
  SparkListenerJobStart,
  SparkListenerEvent,
  SparkListenerStageCompleted,
  SparkListenerTaskEnd
}

import java.util.Properties
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import scala.util.control.NonFatal

/**
 * One application-scoped Spark-listener collector for request metrics.
 *
 * Job-start properties are the sole correlation boundary. Task and stage
 * events are joined back through the stage IDs announced by the correlated
 * job. All mutable state is guarded by [[lock]], while the no-request hot path
 * is a single volatile read.
 */
private[insights] final class OpenIvmRequestMetricsCollector extends SparkListener {
  import OpenIvmRequestMetricsCollector._

  private val lock          = new AnyRef
  private val requests      = mutable.HashMap.empty[String, RequestState]
  private val jobOwners     = mutable.HashMap.empty[Int, String]
  private val stageOwners   = mutable.HashMap.empty[Int, mutable.Set[String]]
  private val activeCounter = new AtomicInteger(0)

  private[insights] def begin(requestId: String): Unit =
    lock.synchronized {
      requests.put(requestId, new RequestState)
      activeCounter.set(requests.size)
    }

  private[insights] def finish(requestId: String, requestEndMs: Long): RequestMetrics =
    lock.synchronized {
      requests.remove(requestId) match {
        case Some(state) =>
          val result = state.snapshot(requestEndMs)
          cleanupOwners(requestId, state)
          activeCounter.set(requests.size)
          result
        case None => RequestMetrics.empty
      }
    }

  private[insights] def discard(requestId: String): Unit =
    lock.synchronized {
      requests.remove(requestId).foreach(cleanupOwners(requestId, _))
      activeCounter.set(requests.size)
    }

  private[insights] def activeRequestCount: Int = activeCounter.get()

  override def onJobStart(event: SparkListenerJobStart): Unit =
    if (activeCounter.get() != 0) {
      recordJobStart(
        event.jobId,
        event.time,
        event.stageInfos.iterator.map(_.stageId).toVector,
        event.properties
      )
    }

  override def onJobEnd(event: SparkListenerJobEnd): Unit =
    if (activeCounter.get() != 0) recordJobEnd(event.jobId, event.time)

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit =
    if (activeCounter.get() != 0 && event.taskInfo != null) {
      val info = event.taskInfo
      recordTaskEnd(
        stageId = event.stageId,
        stageAttemptId = event.stageAttemptId,
        taskIndex = info.index,
        taskId = info.taskId,
        taskAttempt = info.attemptNumber,
        finishTimeMs = info.finishTime,
        successful = info.successful,
        metrics = TaskMetricValues.fromSpark(event.taskMetrics)
      )
    }

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit =
    if (activeCounter.get() != 0 && event.stageInfo != null) {
      val info = event.stageInfo
      recordStageCompleted(
        stageId = info.stageId,
        stageAttemptId = info.attemptNumber(),
        successful = info.failureReason.isEmpty,
        accumulators = boundedAccumulators(info.accumulables.valuesIterator)
      )
    }

  override def onApplicationEnd(event: SparkListenerApplicationEnd): Unit =
    lock.synchronized {
      requests.clear()
      jobOwners.clear()
      stageOwners.clear()
      activeCounter.set(0)
    }

  override def onOtherEvent(event: SparkListenerEvent): Unit =
    event match {
      case barrier: ListenerBarrier => barrier.latch.countDown()
      case _                        =>
    }

  private[insights] def recordJobStart(
      jobId: Int,
      startTimeMs: Long,
      stageIds: Seq[Int],
      properties: Properties
  ): Unit = {
    if (activeCounter.get() == 0) return
    val requestId =
      Option(properties).flatMap(value => Option(value.getProperty(OpenIvmInsightsContract.RequestIdProperty)))
    requestId.foreach { id =>
      lock.synchronized {
        requests.get(id).foreach { state =>
          state.jobs.get(jobId) match {
            case Some(existing) => existing.startTimeMs = math.min(existing.startTimeMs, startTimeMs)
            case None           => state.jobs.put(jobId, JobWindow(startTimeMs, None))
          }
          jobOwners.put(jobId, id)
          stageIds.distinct.foreach { stageId =>
            state.stages.getOrElseUpdate(stageId, new StageState)
            stageOwners.getOrElseUpdate(stageId, mutable.HashSet.empty) += id
          }
        }
      }
    }
  }

  private[insights] def recordJobEnd(jobId: Int, endTimeMs: Long): Unit = {
    if (activeCounter.get() == 0) return
    lock.synchronized {
      jobOwners.get(jobId).flatMap(requests.get).foreach { state =>
        state.jobs.get(jobId).foreach { job =>
          job.endTimeMs = Some(job.endTimeMs.fold(endTimeMs)(math.max(_, endTimeMs)))
        }
      }
    }
  }

  private[insights] def recordTaskEnd(
      stageId: Int,
      stageAttemptId: Int,
      taskIndex: Int,
      taskId: Long,
      taskAttempt: Int,
      finishTimeMs: Long,
      successful: Boolean,
      metrics: TaskMetricValues
  ): Unit = {
    if (activeCounter.get() == 0) return
    lock.synchronized {
      stageOwners.get(stageId).foreach { owners =>
        owners.foreach { requestId =>
          requests.get(requestId).foreach { state =>
            val stage   = state.stages.getOrElseUpdate(stageId, new StageState)
            val attempt = stage.attempts.getOrElseUpdate(stageAttemptId, new StageAttemptState)
            val observed =
              TaskObservation(taskId, taskAttempt, finishTimeMs, successful, metrics)
            attempt.tasks.get(taskIndex) match {
              case Some(existing) if preferred(existing, observed) == existing =>
              case _                                                           => attempt.tasks.put(taskIndex, observed)
            }
          }
        }
      }
    }
  }

  private[insights] def recordStageCompleted(
      stageId: Int,
      stageAttemptId: Int,
      successful: Boolean,
      accumulators: Seq[AccumulatorValue]
  ): Unit = {
    if (activeCounter.get() == 0) return
    lock.synchronized {
      stageOwners.get(stageId).foreach { owners =>
        owners.foreach { requestId =>
          requests.get(requestId).foreach { state =>
            val stage   = state.stages.getOrElseUpdate(stageId, new StageState)
            val attempt = stage.attempts.getOrElseUpdate(stageAttemptId, new StageAttemptState)
            attempt.completed = true
            attempt.successful = successful
            attempt.accumulators = accumulators.take(MaxAccumulatorsPerStage)
          }
        }
      }
    }
  }

  private def cleanupOwners(requestId: String, state: RequestState): Unit = {
    state.jobs.keys.foreach { jobId =>
      if (jobOwners.get(jobId).contains(requestId)) jobOwners.remove(jobId)
    }
    state.stages.keys.foreach { stageId =>
      stageOwners.get(stageId).foreach { owners =>
        owners -= requestId
        if (owners.isEmpty) stageOwners.remove(stageId)
      }
    }
  }
}

private[insights] object OpenIvmRequestMetricsCollector {

  private val MaxAccumulatorsPerStage = 4096
  private val MaxAccumulatorNameChars = 256

  private final class ListenerBarrier(val latch: CountDownLatch) extends SparkListenerEvent {
    override def logEvent: Boolean = false
  }

  final case class RequestMetrics(
      wallDurationMs: Option[Long],
      jobCount: Option[Long],
      stageCount: Option[Long],
      taskCount: Option[Long],
      inputRecords: Option[Long],
      inputBytes: Option[Long],
      outputRecords: Option[Long],
      outputBytes: Option[Long],
      shuffleReadRecords: Option[Long],
      shuffleReadBytes: Option[Long],
      shuffleWriteRecords: Option[Long],
      shuffleWriteBytes: Option[Long],
      memorySpillBytes: Option[Long],
      diskSpillBytes: Option[Long],
      filesRead: Option[Long],
      filesWritten: Option[Long]
  ) {
    def nonEmpty: Boolean = productIterator.exists {
      case value: Option[_] => value.nonEmpty
      case _                => false
    }

    def details: Map[String, Any] =
      Seq(
        OpenIvmInsightsContract.MetricField.WallDurationMs      -> wallDurationMs,
        OpenIvmInsightsContract.MetricField.JobCount            -> jobCount,
        OpenIvmInsightsContract.MetricField.StageCount          -> stageCount,
        OpenIvmInsightsContract.MetricField.TaskCount           -> taskCount,
        OpenIvmInsightsContract.MetricField.InputRecords        -> inputRecords,
        OpenIvmInsightsContract.MetricField.InputBytes          -> inputBytes,
        OpenIvmInsightsContract.MetricField.OutputRecords       -> outputRecords,
        OpenIvmInsightsContract.MetricField.OutputBytes         -> outputBytes,
        OpenIvmInsightsContract.MetricField.ShuffleReadRecords  -> shuffleReadRecords,
        OpenIvmInsightsContract.MetricField.ShuffleReadBytes    -> shuffleReadBytes,
        OpenIvmInsightsContract.MetricField.ShuffleWriteRecords -> shuffleWriteRecords,
        OpenIvmInsightsContract.MetricField.ShuffleWriteBytes   -> shuffleWriteBytes,
        OpenIvmInsightsContract.MetricField.MemorySpillBytes    -> memorySpillBytes,
        OpenIvmInsightsContract.MetricField.DiskSpillBytes      -> diskSpillBytes,
        OpenIvmInsightsContract.MetricField.FilesRead           -> filesRead,
        OpenIvmInsightsContract.MetricField.FilesWritten        -> filesWritten
      ).collect { case (name, Some(value)) => name -> value }.toMap
  }

  object RequestMetrics {
    val empty: RequestMetrics =
      RequestMetrics(
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None
      )
  }

  final case class TaskMetricValues(
      inputRecords: Option[Long] = None,
      inputBytes: Option[Long] = None,
      outputRecords: Option[Long] = None,
      outputBytes: Option[Long] = None,
      shuffleReadRecords: Option[Long] = None,
      shuffleReadBytes: Option[Long] = None,
      shuffleWriteRecords: Option[Long] = None,
      shuffleWriteBytes: Option[Long] = None,
      memorySpillBytes: Option[Long] = None,
      diskSpillBytes: Option[Long] = None
  )

  object TaskMetricValues {
    def fromSpark(metrics: org.apache.spark.executor.TaskMetrics): TaskMetricValues = {
      if (metrics == null) return TaskMetricValues()

      val input = Option(metrics.inputMetrics).filter(value => value.recordsRead != 0L || value.bytesRead != 0L)
      val output =
        Option(metrics.outputMetrics).filter(value => value.recordsWritten != 0L || value.bytesWritten != 0L)
      val shuffleRead = Option(metrics.shuffleReadMetrics)
        .filter(value => value.recordsRead != 0L || value.totalBytesRead != 0L)
      val shuffleWrite = Option(metrics.shuffleWriteMetrics)
        .filter(value => value.recordsWritten != 0L || value.bytesWritten != 0L)

      TaskMetricValues(
        inputRecords = input.map(_.recordsRead),
        inputBytes = input.map(_.bytesRead),
        outputRecords = output.map(_.recordsWritten),
        outputBytes = output.map(_.bytesWritten),
        shuffleReadRecords = shuffleRead.map(_.recordsRead),
        shuffleReadBytes = shuffleRead.map(_.totalBytesRead),
        shuffleWriteRecords = shuffleWrite.map(_.recordsWritten),
        shuffleWriteBytes = shuffleWrite.map(_.bytesWritten),
        memorySpillBytes = positive(metrics.memoryBytesSpilled),
        diskSpillBytes = positive(metrics.diskBytesSpilled)
      )
    }
  }

  final case class AccumulatorValue(id: Long, name: String, value: Long)

  private final case class JobWindow(var startTimeMs: Long, var endTimeMs: Option[Long])

  private final case class TaskObservation(
      taskId: Long,
      attemptNumber: Int,
      finishTimeMs: Long,
      successful: Boolean,
      metrics: TaskMetricValues
  )

  private final class StageAttemptState {
    val tasks: mutable.Map[Int, TaskObservation] = mutable.HashMap.empty
    var completed: Boolean                       = false
    var successful: Boolean                      = false
    var accumulators: Seq[AccumulatorValue]      = Seq.empty
  }

  private final class StageState {
    val attempts: mutable.Map[Int, StageAttemptState] = mutable.HashMap.empty

    def selectedAttempt: Option[StageAttemptState] = {
      val completedSuccesses = attempts.iterator.collect {
        case (id, value) if value.completed && value.successful =>
          id -> value
      }.toVector
      if (completedSuccesses.nonEmpty) Some(completedSuccesses.maxBy(_._1)._2)
      else {
        val completed = attempts.iterator.collect { case (id, value) if value.completed => id -> value }.toVector
        if (completed.nonEmpty) Some(completed.maxBy(_._1)._2)
        else attempts.toVector.sortBy(_._1).lastOption.map(_._2)
      }
    }
  }

  private final class RequestState {
    val jobs: mutable.Map[Int, JobWindow]    = mutable.HashMap.empty
    val stages: mutable.Map[Int, StageState] = mutable.HashMap.empty

    def snapshot(requestEndMs: Long): RequestMetrics = {
      val selectedAttempts = stages.valuesIterator.flatMap(_.selectedAttempt).toVector
      val tasks            = selectedAttempts.iterator.flatMap(_.tasks.valuesIterator).toVector
      val accumulators     = selectedAttempts.iterator.flatMap(_.accumulators.iterator).toVector

      val earliestStart = if (jobs.isEmpty) None else Some(jobs.valuesIterator.map(_.startTimeMs).min)
      val latestEnd =
        if (jobs.isEmpty) None
        else Some(jobs.valuesIterator.map(_.endTimeMs.getOrElse(requestEndMs)).max)
      val wallDuration = for {
        start <- earliestStart
        end   <- latestEnd
      } yield math.max(0L, end - start)

      val deduplicatedAccumulators = accumulators
        .groupBy(_.id)
        .valuesIterator
        .map(_.maxBy(_.value))
        .toVector
      val filesRead = sumPresent(
        deduplicatedAccumulators.collect {
          case value if fileMetricKind(value.name).contains(FileMetricKind.Read) => Some(value.value)
        }
      )
      val filesWritten = sumPresent(
        deduplicatedAccumulators.collect {
          case value if fileMetricKind(value.name).contains(FileMetricKind.Written) => Some(value.value)
        }
      )

      RequestMetrics(
        wallDurationMs = wallDuration,
        jobCount = nonEmptyCount(jobs.size),
        stageCount = nonEmptyCount(stages.size),
        taskCount = nonEmptyCount(tasks.size),
        inputRecords = sumPresent(tasks.map(_.metrics.inputRecords)),
        inputBytes = sumPresent(tasks.map(_.metrics.inputBytes)),
        outputRecords = sumPresent(tasks.map(_.metrics.outputRecords)),
        outputBytes = sumPresent(tasks.map(_.metrics.outputBytes)),
        shuffleReadRecords = sumPresent(tasks.map(_.metrics.shuffleReadRecords)),
        shuffleReadBytes = sumPresent(tasks.map(_.metrics.shuffleReadBytes)),
        shuffleWriteRecords = sumPresent(tasks.map(_.metrics.shuffleWriteRecords)),
        shuffleWriteBytes = sumPresent(tasks.map(_.metrics.shuffleWriteBytes)),
        memorySpillBytes = sumPresent(tasks.map(_.metrics.memorySpillBytes)),
        diskSpillBytes = sumPresent(tasks.map(_.metrics.diskSpillBytes)),
        filesRead = filesRead,
        filesWritten = filesWritten
      )
    }
  }

  private object FileMetricKind extends Enumeration {
    val Read, Written = Value
  }

  private def preferred(left: TaskObservation, right: TaskObservation): TaskObservation = {
    if (left.successful != right.successful) {
      if (left.successful) left else right
    } else {
      val leftKey  = (left.attemptNumber, left.finishTimeMs, left.taskId)
      val rightKey = (right.attemptNumber, right.finishTimeMs, right.taskId)
      val ordering = implicitly[Ordering[(Int, Long, Long)]]
      if (left.successful) {
        if (ordering.lteq(leftKey, rightKey)) left else right
      } else if (ordering.gteq(leftKey, rightKey)) left
      else right
    }
  }

  private def boundedAccumulators(
      values: Iterator[AccumulableInfo]
  ): Seq[AccumulatorValue] =
    values
      .take(MaxAccumulatorsPerStage)
      .flatMap { value =>
        for {
          name <- value.name
          if name.length <= MaxAccumulatorNameChars
          number <- value.value.flatMap(nonNegativeLong)
          if fileMetricKind(name).nonEmpty
        } yield AccumulatorValue(value.id, name, number)
      }
      .toVector

  private def nonNegativeLong(value: Any): Option[Long] =
    value match {
      case number: java.lang.Byte                              => Some(number.longValue())
      case number: java.lang.Short                             => Some(number.longValue())
      case number: java.lang.Integer                           => Some(number.longValue())
      case number: java.lang.Long                              => Some(number.longValue())
      case number: BigInt if number.isValidLong && number >= 0 => Some(number.longValue)
      case text: String if text.length <= 32 =>
        try {
          val number = text.toLong
          if (number >= 0L) Some(number) else None
        } catch {
          case _: NumberFormatException => None
        }
      case _ => None
    }

  private def fileMetricKind(name: String): Option[FileMetricKind.Value] = {
    val normalized = Option(name)
      .getOrElse("")
      .toLowerCase(java.util.Locale.ROOT)
      .replaceAll("[^a-z0-9]+", " ")
      .trim
    if (!normalized.contains("file")) None
    else if (Seq("read", "scan").exists(normalized.contains)) Some(FileMetricKind.Read)
    else if (Seq("written", "write", "output", "created", "added").exists(normalized.contains))
      Some(FileMetricKind.Written)
    else None
  }

  private def positive(value: Long): Option[Long] =
    if (value > 0L) Some(value) else None

  private def nonEmptyCount(value: Int): Option[Long] =
    if (value > 0) Some(value.toLong) else None

  private def sumPresent(values: Iterable[Option[Long]]): Option[Long] = {
    val present = values.iterator.flatten.toVector
    if (present.isEmpty) None
    else Some(present.foldLeft(0L)(saturatingAdd))
  }

  private def saturatingAdd(left: Long, right: Long): Long =
    if (right > 0L && left > Long.MaxValue - right) Long.MaxValue else left + right

  /**
   * END posts a collector-only listener barrier so its snapshot includes all
   * previously posted job/task events. This schedules no Spark work and does
   * not wait for unrelated events posted after the barrier.
   */
  def awaitListenerDrain(context: SparkContext, timeoutMs: Long): Unit =
    try {
      val barrier     = new ListenerBarrier(new CountDownLatch(1))
      val listenerBus = context.getClass.getMethod("listenerBus").invoke(context)
      listenerBus.getClass
        .getMethod("post", classOf[SparkListenerEvent])
        .invoke(listenerBus, barrier)
      barrier.latch.await(timeoutMs, TimeUnit.MILLISECONDS)
      ()
    } catch {
      case _: InterruptedException =>
        Thread.currentThread().interrupt()
      case NonFatal(_) => ()
    }
}
