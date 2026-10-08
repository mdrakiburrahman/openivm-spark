package org.openivm.spark.streaming

import org.openivm.spark.common.LifecycleDeadline

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.concurrent.{
  Delayed,
  RejectedExecutionException,
  ScheduledExecutorService,
  ScheduledFuture,
  ScheduledThreadPoolExecutor,
  ThreadFactory,
  TimeUnit
}
import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

/** Work evidence is run-scoped: Some(false) means confirmed zero work, not an unavailable snapshot. */
private[streaming] final case class StreamingStartupObservation(
    active: Boolean,
    progressObserved: Boolean,
    workActive: Option[Boolean]
)

/** Diagnoses startup without stopping a query. This watchdog owns its supplied scheduler.
  *
  * Confirmed zero work must remain continuous for the configured grace after the startup deadline.
  * Non-fatal observation/delivery failures are reported and retried; delivery must be idempotent.
  * Fatal failures or a throwing reporter fail the returned future, never count as a successful diagnosis.
  * Cancellation never interrupts an in-flight observation/delivery. The default daemon starts on registration.
  */
private[streaming] final class StreamingFirstProgressWatchdog(
    reportError: Throwable => Unit,
    scheduler: ScheduledExecutorService = StreamingFirstProgressWatchdog.newScheduler(),
    zeroWorkGrace: FiniteDuration = FiniteDuration(30L, TimeUnit.SECONDS),
    nanoTime: () => Long = () => System.nanoTime()
) {

  require(zeroWorkGrace.toNanos > 0L, "Streaming zero-work grace must be positive and representable in nanoseconds")

  private val lock             = new AnyRef
  private val registrations    = mutable.Set.empty[Registration]
  @volatile private var closed = false

  def watch(
      deadline: LifecycleDeadline,
      observe: () => StreamingStartupObservation,
      onUnhealthy: () => Unit
  ): ScheduledFuture[_] = {
    val registration = new Registration(deadline, observe, onUnhealthy)
    lock.synchronized {
      if (closed) throw new RejectedExecutionException("Streaming first-progress watchdog is shut down")
      registrations += registration
    }

    try {
      val task = scheduler.scheduleWithFixedDelay(
        registration,
        deadline.remainingNanos,
        StreamingFirstProgressWatchdog.PollNanos,
        TimeUnit.NANOSECONDS
      )
      registration.attach(task)
      registration
    } catch {
      case error: Throwable =>
        registration.cancel(false)
        throw error
    }
  }

  def shutdown(): Unit = {
    val pending = lock.synchronized {
      if (closed) None
      else {
        closed = true
        val result = registrations.toVector
        registrations.clear()
        Some(result)
      }
    }
    pending.foreach { entries =>
      entries.foreach(_.cancel(false))
      scheduler.shutdown()
    }
  }

  private def forget(registration: Registration): Unit =
    lock.synchronized {
      registrations -= registration
      ()
    }

  private final class Registration(
      deadline: LifecycleDeadline,
      observe: () => StreamingStartupObservation,
      onUnhealthy: () => Unit
  ) extends Runnable
      with ScheduledFuture[Unit] {

    private val finished              = new AtomicBoolean(false)
    private val cancelled             = new AtomicBoolean(false)
    private val scheduled             = new AtomicReference[ScheduledFuture[_]]()
    private var zeroWorkGraceDeadline = Option.empty[LifecycleDeadline]

    def attach(task: ScheduledFuture[_]): Unit = {
      scheduled.set(task)
      // A zero-delay first execution can finish before the scheduler returns its handle.
      if (cancelled.get()) task.cancel(false)
    }

    override def run(): Unit =
      if (!finished.get() && !closed) {
        try {
          val observation =
            try observe()
            catch {
              case error: Throwable =>
                zeroWorkGraceDeadline = None
                throw error
            }
          if (!finished.get() && !closed) {
            if (observation.progressObserved || !observation.active) cancel(false)
            else if (deadline.remainingNanos > 0L) zeroWorkGraceDeadline = None
            else
              observation.workActive match {
                case Some(false) =>
                  zeroWorkGraceDeadline match {
                    case Some(graceDeadline) if graceDeadline.remainingNanos == 0L =>
                      onUnhealthy()
                      cancel(false)
                    case Some(_) => ()
                    case None =>
                      zeroWorkGraceDeadline = Some(LifecycleDeadline.start(zeroWorkGrace, nanoTime))
                  }
                case _ => zeroWorkGraceDeadline = None
              }
          }
        } catch {
          case error: Throwable => reportFailure(error)
        }
      }

    private def fail(): Unit = {
      finished.set(true)
      forget(this)
    }

    private def reportFailure(error: Throwable): Unit = {
      val retry = NonFatal(error)
      if (!retry) {
        fail()
        if (error.isInstanceOf[InterruptedException]) Thread.currentThread().interrupt()
      }
      try reportError(error)
      catch {
        case reportingFailure: Throwable =>
          fail()
          if (reportingFailure ne error) reportingFailure.addSuppressed(error)
          throw reportingFailure
      }
      if (!retry) throw error
    }

    override def cancel(mayInterruptIfRunning: Boolean): Boolean =
      if (finished.compareAndSet(false, true)) {
        cancelled.set(true)
        forget(this)
        val task = scheduled.get()
        if (task == null) true else task.cancel(false)
      } else false

    override def isCancelled: Boolean = scheduled.get().isCancelled
    override def isDone: Boolean      = scheduled.get().isDone

    override def getDelay(unit: TimeUnit): Long = scheduled.get().getDelay(unit)
    override def compareTo(other: Delayed): Int = scheduled.get().compareTo(other)

    override def get(): Unit = {
      scheduled.get().get()
      ()
    }

    override def get(timeout: Long, unit: TimeUnit): Unit = {
      scheduled.get().get(timeout, unit)
      ()
    }
  }
}

private[streaming] object StreamingFirstProgressWatchdog {

  private val PollNanos = TimeUnit.SECONDS.toNanos(1L)

  private def newScheduler(): ScheduledExecutorService = {
    val scheduler = new ScheduledThreadPoolExecutor(
      1,
      new ThreadFactory {
        override def newThread(runnable: Runnable): Thread = {
          val thread = new Thread(null, runnable, "openivm-streaming-first-progress-watchdog", 0L, false)
          thread.setDaemon(true)
          thread
        }
      }
    )
    scheduler.setRemoveOnCancelPolicy(true)
    scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false)
    scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false)
    scheduler
  }
}
