package org.openivm.spark.common

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.{Lock, ReentrantLock, ReentrantReadWriteLock}
import scala.concurrent.duration.{FiniteDuration, NANOSECONDS}

private[spark] final case class LifecycleDeadlineContext(
    phase: String,
    operationId: Option[String] = None,
    command: Option[String] = None,
    targetRelation: Option[String] = None
)

private[spark] object LifecycleDeadlineContext {
  val Unspecified: LifecycleDeadlineContext = LifecycleDeadlineContext("lifecycle")
}

final class LifecycleLockTimeoutException private (
    val target: String,
    val timeout: FiniteDuration,
    val phase: String,
    val waiterOperationId: Option[String],
    val waiterCommand: Option[String],
    val waiterTarget: Option[String],
    val waitDuration: FiniteDuration,
    val deadlineElapsed: FiniteDuration,
    val ownerThread: Option[String]
) extends RuntimeException(
      LifecycleLockTimeoutException.message(
        target,
        timeout,
        phase,
        waiterOperationId,
        waiterCommand,
        waiterTarget,
        waitDuration,
        deadlineElapsed,
        ownerThread
      )
    ) {

  def this(target: String, timeout: FiniteDuration) =
    this(
      target,
      timeout,
      LifecycleDeadlineContext.Unspecified.phase,
      None,
      None,
      None,
      FiniteDuration(0L, NANOSECONDS),
      FiniteDuration(0L, NANOSECONDS),
      None
    )
}

private[spark] object LifecycleLockTimeoutException {

  def apply(
      target: String,
      timeout: FiniteDuration,
      context: LifecycleDeadlineContext,
      waitNanos: Long,
      deadlineElapsedNanos: Long,
      ownerThread: Option[String]
  ): LifecycleLockTimeoutException =
    new LifecycleLockTimeoutException(
      target,
      timeout,
      context.phase,
      context.operationId,
      context.command,
      context.targetRelation,
      FiniteDuration(waitNanos, NANOSECONDS),
      FiniteDuration(deadlineElapsedNanos, NANOSECONDS),
      ownerThread
    )

  private def message(
      target: String,
      timeout: FiniteDuration,
      phase: String,
      waiterOperationId: Option[String],
      waiterCommand: Option[String],
      waiterTarget: Option[String],
      waitDuration: FiniteDuration,
      deadlineElapsed: FiniteDuration,
      ownerThread: Option[String]
  ): String = {
    val waiter = Seq(
      waiterOperationId.map(value => s"operation=$value"),
      waiterCommand.map(value => s"command=$value"),
      waiterTarget.map(value => s"target=$value")
    ).flatten.mkString(", ")
    val waiterDetails = if (waiter.nonEmpty) s"; waiter {$waiter}" else ""
    val ownerDetails  = ownerThread.map(value => s"; owner_thread=$value").getOrElse("")
    s"OpenIVM could not acquire the lifecycle guard for $target during $phase within $timeout" +
      s"$waiterDetails$ownerDetails; waiter_duration=$waitDuration; deadline_elapsed=$deadlineElapsed; " +
      "no cleanup was authorized. Retry after the owning operation finishes or recycle the Spark session."
  }
}

private[spark] final class LifecycleDeadline private (
    val timeout: FiniteDuration,
    startedAt: Long,
    nanoTime: () => Long,
    context: LifecycleDeadlineContext
) {

  private def elapsedSince(start: Long, end: Long): Long =
    math.max(0L, end - start)

  def remainingNanos: Long = {
    val elapsed = elapsedSince(startedAt, nanoTime())
    if (elapsed >= timeout.toNanos) 0L else timeout.toNanos - elapsed
  }

  def withLock[A](lock: ReentrantLock, target: String)(body: => A): A = {
    val waitStarted = nanoTime()
    val remaining   = remainingNanos
    val acquired =
      if (lock.isHeldByCurrentThread) {
        lock.lock()
        true
      } else
        try remaining > 0L && lock.tryLock(remaining, TimeUnit.NANOSECONDS)
        catch {
          case interrupted: InterruptedException =>
            Thread.currentThread().interrupt()
            throw interrupted
        }
    if (!acquired) {
      val failedAt = nanoTime()
      throw LifecycleLockTimeoutException(
        target,
        timeout,
        context,
        elapsedSince(waitStarted, failedAt),
        elapsedSince(startedAt, failedAt),
        LifecycleDeadline.ownerThread(lock)
      )
    }
    try body
    finally lock.unlock()
  }

  def withReadLock[A](lock: ReentrantReadWriteLock, target: String)(body: => A): A =
    withTimedLock(
      lock.readLock(),
      target,
      reentrant = lock.getReadHoldCount > 0 || lock.isWriteLockedByCurrentThread,
      ownerThread = None
    )(body)

  def withWriteLock[A](lock: ReentrantReadWriteLock, target: String)(body: => A): A = {
    if (lock.getReadHoldCount > 0 && !lock.isWriteLockedByCurrentThread)
      throw new IllegalStateException(s"Cannot upgrade a held lifecycle read lock to write for $target")
    withTimedLock(
      lock.writeLock(),
      target,
      reentrant = lock.isWriteLockedByCurrentThread,
      ownerThread = None
    )(body)
  }

  private def withTimedLock[A](
      lock: Lock,
      target: String,
      reentrant: Boolean,
      ownerThread: Option[String]
  )(body: => A): A = {
    val waitStarted = nanoTime()
    val remaining   = remainingNanos
    val acquired =
      if (reentrant) {
        lock.lock()
        true
      } else
        try remaining > 0L && lock.tryLock(remaining, TimeUnit.NANOSECONDS)
        catch {
          case interrupted: InterruptedException =>
            Thread.currentThread().interrupt()
            throw interrupted
        }
    if (!acquired) {
      val failedAt = nanoTime()
      throw LifecycleLockTimeoutException(
        target,
        timeout,
        context,
        elapsedSince(waitStarted, failedAt),
        elapsedSince(startedAt, failedAt),
        ownerThread
      )
    }
    try body
    finally lock.unlock()
  }
}

private[spark] object LifecycleDeadline {

  def start(
      timeout: FiniteDuration,
      nanoTime: () => Long = () => System.nanoTime(),
      context: LifecycleDeadlineContext = LifecycleDeadlineContext.Unspecified
  ): LifecycleDeadline = {
    require(timeout.toNanos > 0L, "Lifecycle timeout must be positive and representable in nanoseconds")
    new LifecycleDeadline(timeout, nanoTime(), nanoTime, context)
  }

  private def ownerThread(lock: ReentrantLock): Option[String] = {
    val text   = lock.toString
    val prefix = "[Locked by thread "
    val start  = text.indexOf(prefix)
    if (start < 0) None
    else {
      val valueStart = start + prefix.length
      val end        = text.indexOf(']', valueStart)
      if (end < 0) None else Option(text.substring(valueStart, end)).filter(_.nonEmpty)
    }
  }
}
