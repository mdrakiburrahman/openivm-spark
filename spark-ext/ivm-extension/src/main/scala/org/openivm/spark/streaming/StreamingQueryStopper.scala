package org.openivm.spark.streaming

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{
  Callable,
  ExecutionException,
  Future,
  FutureTask,
  RejectedExecutionException,
  SynchronousQueue,
  ThreadFactory,
  ThreadPoolExecutor,
  TimeUnit,
  TimeoutException
}
import scala.collection.mutable
import scala.util.control.NonFatal
import org.apache.spark.sql.streaming.StreamingQuery
import org.openivm.spark.common.LifecycleDeadline

final case class StreamingQueryStopObservation(
    queryId: String,
    runId: String,
    timeoutMs: Long,
    remainingBudgetMs: Long,
    active: Boolean,
    progressObserved: Boolean,
    lastBatchId: Option[Long],
    cancellationAttempted: Boolean,
    cancellationErrorClass: Option[String],
    terminationConfirmed: Boolean
)

sealed abstract class StreamingQueryStopException private[streaming] (
    val target: String,
    val observation: StreamingQueryStopObservation,
    reason: String,
    cause: Throwable
) extends RuntimeException(StreamingQueryStopMessages.message(target, observation, reason), cause)

final class StreamingQueryStopTimeoutException(
    target: String,
    observation: StreamingQueryStopObservation,
    cause: Throwable = null
) extends StreamingQueryStopException(
      target,
      observation,
      "exceeded the shared lifecycle deadline while waiting for query.stop()",
      cause
    )

final class StreamingQueryStopFailedException(
    target: String,
    observation: StreamingQueryStopObservation,
    cause: Throwable = null
) extends StreamingQueryStopException(
      target,
      observation,
      if (cause == null) "returned from query.stop() while the query was still active"
      else "failed in the native stop invocation (inspect the preserved cause)",
      cause
    )

final class StreamingQueryStopRejectedException(
    target: String,
    observation: StreamingQueryStopObservation,
    cause: Throwable = null
) extends StreamingQueryStopException(
      target,
      observation,
      "could not be admitted because the bounded native-stop worker pool is saturated or shut down",
      cause
    )

final class StreamingQueryStopInterruptedException(
    target: String,
    observation: StreamingQueryStopObservation,
    cause: Throwable = null
) extends StreamingQueryStopException(
      target,
      observation,
      "was interrupted while waiting for native stop; the caller interrupt flag is preserved",
      cause
    )

private[streaming] object StreamingQueryStopMessages {

  private def bounded(value: String): String =
    Option(value).getOrElse("<unknown>").take(160).map {
      case character if Character.isISOControl(character) => '?'
      case character                                      => character
    }

  def message(target: String, observation: StreamingQueryStopObservation, reason: String): String =
    s"OpenIVM native stop for ${bounded(target)} " +
      s"(queryId=${bounded(observation.queryId)}, runId=${bounded(observation.runId)}, " +
      s"finite timeout=${observation.timeoutMs} ms) $reason. " +
      "Cleanup is unsafe; retain ownership and retry only after termination is confirmed. " +
      "The orchestrator may recycle the Spark session."
}

private[streaming] object StreamingQueryStopper {

  private type QueryIdentity = (String, String)

  private final case class Evidence(
      active: Boolean = true,
      progressObserved: Boolean = false,
      lastBatchId: Option[Long] = None,
      cancellationAttempted: Boolean = false,
      cancellationErrorClass: Option[String] = None
  )
}

private[streaming] final class StreamingQueryStopper(maxWorkers: Int = 32) {

  import StreamingQueryStopper.{Evidence, QueryIdentity}

  require(maxWorkers > 0, "Native-stop maxWorkers must be positive")

  private val guard     = new AnyRef
  private val inFlight  = mutable.HashMap.empty[QueryIdentity, Invocation]
  private val threadIds = new AtomicInteger()
  private var closed    = false

  private val workers = new ThreadPoolExecutor(
    0,
    maxWorkers,
    60L,
    TimeUnit.SECONDS,
    new SynchronousQueue[Runnable](),
    new ThreadFactory {
      override def newThread(task: Runnable): Thread = {
        // Pooled stop workers must not inherit request-local insight capture state.
        val thread = new Thread(null, task, s"openivm-native-stop-${threadIds.incrementAndGet()}", 0L, false)
        thread.setDaemon(true)
        thread
      }
    },
    new ThreadPoolExecutor.AbortPolicy()
  )

  def stop(
      query: StreamingQuery,
      target: String,
      deadline: LifecycleDeadline,
      cancel: () => Unit
  ): StreamingQueryStopObservation = {
    val identity  = query.id.toString -> query.runId.toString
    val candidate = new Invocation(identity, query, cancel, collectEvidence(query, Evidence()))
    val invocation = guard.synchronized {
      val current = inFlight.getOrElse(identity, candidate)
      if (Thread.currentThread().isInterrupted)
        interrupted(current, target, deadline, new InterruptedException("Native-stop caller already interrupted"))
      if (deadline.remainingNanos <= 0L) timedOut(current, target, deadline)
      if (closed)
        throw new StreamingQueryStopRejectedException(
          target,
          current.observation(deadline),
          new RejectedExecutionException("OpenIVM native-stop coordinator is shut down")
        )

      inFlight.get(identity) match {
        case Some(existing) => existing
        case None =>
          inFlight.put(identity, candidate)
          try workers.execute(candidate)
          catch {
            case rejected: RejectedExecutionException =>
              inFlight.remove(identity)
              throw new StreamingQueryStopRejectedException(target, candidate.observation(deadline), rejected)
          }
          candidate
      }
    }

    if (Thread.currentThread().isInterrupted)
      interrupted(invocation, target, deadline, new InterruptedException("Native-stop caller interrupted"))
    val remaining = deadline.remainingNanos
    if (remaining <= 0L) timedOut(invocation, target, deadline)

    val terminated =
      try invocation.result.get(remaining, TimeUnit.NANOSECONDS)
      catch {
        case failure: ExecutionException =>
          throw new StreamingQueryStopFailedException(
            target,
            invocation.observation(deadline),
            Option(failure.getCause).getOrElse(failure)
          )
        case timeout: TimeoutException =>
          timedOut(invocation, target, deadline, timeout)
        case interruption: InterruptedException =>
          interrupted(invocation, target, deadline, interruption)
      }

    if (Thread.currentThread().isInterrupted)
      interrupted(invocation, target, deadline, new InterruptedException("Native-stop caller interrupted"))
    val observation = invocation.observation(deadline, confirmed = terminated)
    if (!observation.terminationConfirmed)
      throw new StreamingQueryStopFailedException(target, observation)
    observation
  }

  def isStopping(queryId: String, runId: String): Boolean =
    guard.synchronized(inFlight.contains(queryId -> runId))

  def shutdown(): Unit = guard.synchronized {
    closed = true
    val _ = workers.shutdownNow()
  }

  private def collectEvidence(query: StreamingQuery, previous: Evidence): Evidence = {
    val active = query.isActive
    val batch  = Option(query.lastProgress).map(_.batchId)
    previous.copy(
      active = active,
      progressObserved = previous.progressObserved || batch.isDefined,
      lastBatchId = batch.orElse(previous.lastBatchId)
    )
  }

  private def timedOut(
      invocation: Invocation,
      target: String,
      deadline: LifecycleDeadline,
      cause: Throwable = null
  ): Nothing = {
    val observation = invocation.observation(deadline)
    invocation.interruptWorker()
    throw new StreamingQueryStopTimeoutException(target, observation, cause)
  }

  private def interrupted(
      invocation: Invocation,
      target: String,
      deadline: LifecycleDeadline,
      cause: InterruptedException
  ): Nothing = {
    Thread.currentThread().interrupt()
    val observation = invocation.observation(deadline)
    invocation.interruptWorker()
    throw new StreamingQueryStopInterruptedException(target, observation, cause)
  }

  private final class StopResult
      extends FutureTask[Boolean](new Callable[Boolean] {
        override def call(): Boolean =
          throw new IllegalStateException("Native-stop results are completed by their invocation")
      }) {

    def complete(value: Boolean): Unit = set(value)

    def completeExceptionally(cause: Throwable): Unit = setException(cause)
  }

  private final class Invocation(
      identity: QueryIdentity,
      query: StreamingQuery,
      cancel: () => Unit,
      initial: Evidence
  ) extends Runnable {

    private val completion      = new StopResult
    val result: Future[Boolean] = completion

    @volatile private var evidence = initial
    private val threadGuard        = new AnyRef
    private var worker: Thread     = null
    private var interruptRequested = false

    def observation(deadline: LifecycleDeadline, confirmed: Boolean = false): StreamingQueryStopObservation = {
      val snapshot = evidence
      StreamingQueryStopObservation(
        queryId = identity._1,
        runId = identity._2,
        timeoutMs = deadline.timeout.toMillis,
        remainingBudgetMs = TimeUnit.NANOSECONDS.toMillis(deadline.remainingNanos),
        active = snapshot.active,
        progressObserved = snapshot.progressObserved,
        lastBatchId = snapshot.lastBatchId,
        cancellationAttempted = snapshot.cancellationAttempted,
        cancellationErrorClass = snapshot.cancellationErrorClass,
        terminationConfirmed = confirmed && !snapshot.active
      )
    }

    def interruptWorker(): Unit = threadGuard.synchronized {
      // Interruption is a signal, not evidence that the native invocation has exited.
      interruptRequested = true
      if (worker != null) worker.interrupt()
    }

    override def run(): Unit = {
      threadGuard.synchronized {
        worker = Thread.currentThread()
        if (interruptRequested) worker.interrupt()
      }

      var terminated         = false
      var failure: Throwable = null
      try {
        evidence = evidence.copy(cancellationAttempted = true)
        try cancel()
        catch {
          case cancellation: InterruptedException =>
            evidence = evidence.copy(cancellationErrorClass = Some(cancellation.getClass.getName.take(160)))
            Thread.currentThread().interrupt()
          case NonFatal(cancellation) =>
            evidence = evidence.copy(cancellationErrorClass = Some(cancellation.getClass.getName.take(160)))
        }

        try query.stop()
        catch { case nativeFailure: Throwable => failure = nativeFailure }
        try {
          evidence = collectEvidence(query, evidence)
          terminated = failure == null && !evidence.active
        } catch {
          case observationFailure: Throwable =>
            if (failure == null) failure = observationFailure
            else if (failure ne observationFailure) failure.addSuppressed(observationFailure)
        }
      } catch {
        case nativeFailure: Throwable => failure = nativeFailure
      } finally {
        threadGuard.synchronized {
          worker = null
        }
        guard.synchronized {
          if (inFlight.get(identity).contains(this)) inFlight.remove(identity)
        }
        if (failure == null) completion.complete(terminated)
        else completion.completeExceptionally(failure)
      }
    }
  }
}
