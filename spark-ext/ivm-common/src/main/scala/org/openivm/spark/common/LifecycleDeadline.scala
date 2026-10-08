package org.openivm.spark.common

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import scala.concurrent.duration.FiniteDuration

final class LifecycleLockTimeoutException(
    val target: String,
    val timeout: FiniteDuration
) extends RuntimeException(
      s"OpenIVM could not acquire the lifecycle guard for $target within $timeout; " +
        "no cleanup was authorized. Retry after the owning operation finishes or recycle the Spark session."
    )

private[spark] final class LifecycleDeadline private (
    val timeout: FiniteDuration,
    startedAt: Long,
    nanoTime: () => Long
) {

  def remainingNanos: Long = {
    val elapsed = math.max(0L, nanoTime() - startedAt)
    if (elapsed >= timeout.toNanos) 0L else timeout.toNanos - elapsed
  }

  def withLock[A](lock: ReentrantLock, target: String)(body: => A): A = {
    val remaining = remainingNanos
    val acquired =
      try remaining > 0L && lock.tryLock(remaining, TimeUnit.NANOSECONDS)
      catch {
        case interrupted: InterruptedException =>
          Thread.currentThread().interrupt()
          throw interrupted
      }
    if (!acquired) throw new LifecycleLockTimeoutException(target, timeout)
    try body
    finally lock.unlock()
  }
}

private[spark] object LifecycleDeadline {

  def start(timeout: FiniteDuration, nanoTime: () => Long = () => System.nanoTime()): LifecycleDeadline = {
    require(timeout.toNanos > 0L, "Lifecycle timeout must be positive and representable in nanoseconds")
    new LifecycleDeadline(timeout, nanoTime(), nanoTime)
  }
}
