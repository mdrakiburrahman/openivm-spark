package org.openivm.spark.streaming

import org.openivm.spark.common.LifecycleDeadline
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.{
  AbstractExecutorService,
  Callable,
  CancellationException,
  Delayed,
  ExecutionException,
  RejectedExecutionException,
  ScheduledExecutorService,
  ScheduledFuture,
  TimeUnit,
  TimeoutException
}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

class StreamingFirstProgressWatchdogSpec extends AnyFunSpec with Matchers {

  describe("first-progress diagnostic watchdog") {
    it("uses the remaining startup budget and diagnoses exactly at, never before, the deadline") {
      val fixture = new Fixture
      fixture.clock.advance(2.seconds)
      val future = fixture.watch()
      future.getDelay(TimeUnit.NANOSECONDS) shouldBe 3.seconds.toNanos

      fixture.clock.advance(3.seconds - 1.nanosecond)
      fixture.scheduler.runDue()
      fixture.observations shouldBe 0
      fixture.unhealthyDeliveries shouldBe 0

      // Force an early wake-up to exercise the monotonic deadline check as well as the initial delay.
      fixture.scheduler.runNextNow()
      fixture.unhealthyDeliveries shouldBe 0
      fixture.scheduler.pendingCount shouldBe 1
      fixture.clock.advance(1.nanosecond)
      fixture.scheduler.runNextNow()

      fixture.unhealthyAttempts shouldBe 1
      fixture.unhealthyDeliveries shouldBe 1
      future.isCancelled shouldBe true
      fixture.scheduler.pendingCount shouldBe 0
      fixture.tick()
      fixture.observations shouldBe 2
      fixture.unhealthyAttempts shouldBe 1
    }

    it("cancels when a completed empty batch or idle trigger supplies first progress") {
      val fixture = new Fixture
      val future  = fixture.watch()
      fixture.observation =
        StreamingStartupObservation(active = true, progressObserved = true, workActive = Some(false))
      fixture.atDeadline()

      future.isCancelled shouldBe true
      fixture.scheduler.pendingCount shouldBe 0
      fixture.unhealthyAttempts shouldBe 0
      fixture.tick()
      fixture.observations shouldBe 1
    }

    it("cancels for an inactive run even if it never reported progress") {
      val fixture = new Fixture
      val future  = fixture.watch()
      fixture.observation =
        StreamingStartupObservation(active = false, progressObserved = false, workActive = Some(false))
      fixture.atDeadline()

      future.isCancelled shouldBe true
      fixture.scheduler.pendingCount shouldBe 0
      fixture.unhealthyAttempts shouldBe 0
      fixture.tick()
      fixture.observations shouldBe 1
    }

    it("keeps polling active and unknown work after the deadline until confirmed work disappears") {
      val fixture = new Fixture
      fixture.observation =
        StreamingStartupObservation(active = true, progressObserved = false, workActive = Some(true))
      val future = fixture.watch()
      fixture.atDeadline()

      future.isDone shouldBe false
      fixture.unhealthyAttempts shouldBe 0
      fixture.scheduler.pendingCount shouldBe 1
      future.getDelay(TimeUnit.NANOSECONDS) shouldBe 1.second.toNanos

      fixture.observation = fixture.observation.copy(workActive = None)
      fixture.tick()
      fixture.tick()
      fixture.observations shouldBe 3
      future.isDone shouldBe false
      fixture.unhealthyAttempts shouldBe 0
      fixture.scheduler.pendingCount shouldBe 1

      fixture.observation = fixture.observation.copy(workActive = Some(false))
      fixture.tick()
      fixture.unhealthyDeliveries shouldBe 1
      future.isCancelled shouldBe true
      fixture.scheduler.pendingCount shouldBe 0
      fixture.tick()
      fixture.observations shouldBe 4
      fixture.unhealthyAttempts shouldBe 1
    }

    it("ends continued unknown-work polling on subsequent progress or termination without a diagnosis") {
      Seq(
        StreamingStartupObservation(active = true, progressObserved = true, workActive = Some(false)),
        StreamingStartupObservation(active = false, progressObserved = false, workActive = None)
      ).foreach { terminal =>
        val fixture = new Fixture
        fixture.observation = StreamingStartupObservation(active = true, progressObserved = false, workActive = None)
        val future = fixture.watch()
        fixture.atDeadline()
        future.isDone shouldBe false
        fixture.unhealthyAttempts shouldBe 0

        fixture.observation = terminal
        fixture.tick()
        future.isCancelled shouldBe true
        fixture.scheduler.pendingCount shouldBe 0
        fixture.unhealthyAttempts shouldBe 0
        fixture.tick()
        fixture.observations shouldBe 2
      }
    }

    it("reports observer errors and retains coverage without treating the failure as zero work") {
      val fixture = new Fixture
      val failure = new IllegalStateException("observation unavailable")
      fixture.observationFailure = Some(failure)
      val future = fixture.watch()
      fixture.atDeadline()

      fixture.reported.toVector shouldBe Vector(failure)
      future.isDone shouldBe false
      fixture.scheduler.pendingCount shouldBe 1
      fixture.unhealthyAttempts shouldBe 0

      fixture.observationFailure = None
      fixture.observation = fixture.observation.copy(workActive = None)
      fixture.tick()
      fixture.unhealthyAttempts shouldBe 0
      future.isDone shouldBe false

      fixture.observation = fixture.observation.copy(workActive = Some(false))
      fixture.tick()
      fixture.unhealthyDeliveries shouldBe 1
      future.isCancelled shouldBe true
      fixture.reported.toVector shouldBe Vector(failure)
    }

    it("reports failed unhealthy delivery and retries until one successful delivery cancels checking") {
      val fixture = new Fixture
      val failure = new IllegalStateException("delivery unavailable")
      fixture.deliveryFailure = Some(failure)
      val future = fixture.watch()
      fixture.atDeadline()

      fixture.reported.toVector shouldBe Vector(failure)
      fixture.unhealthyAttempts shouldBe 1
      fixture.unhealthyDeliveries shouldBe 0
      future.isDone shouldBe false
      fixture.scheduler.pendingCount shouldBe 1

      fixture.deliveryFailure = None
      fixture.tick()
      fixture.unhealthyAttempts shouldBe 2
      fixture.unhealthyDeliveries shouldBe 1
      future.isCancelled shouldBe true
      fixture.scheduler.pendingCount shouldBe 0
      fixture.tick()
      fixture.unhealthyAttempts shouldBe 2
      fixture.reported.toVector shouldBe Vector(failure)
    }

    it("exposes a throwing reporter through the failed future with the original error retained") {
      val fixture            = new Fixture
      val observationFailure = new IllegalStateException("observation unavailable")
      val reportingFailure   = new IllegalArgumentException("reporting unavailable")
      fixture.observationFailure = Some(observationFailure)
      fixture.reporterFailure = Some(reportingFailure)
      fixture.clock.advance(5.seconds)
      fixture.scheduler.executeDuringRegistration = true
      val future = fixture.watch()

      future.isDone shouldBe true
      future.isCancelled shouldBe false
      fixture.scheduler.pendingCount shouldBe 0
      fixture.unhealthyAttempts shouldBe 0
      fixture.reported.toVector shouldBe Vector(observationFailure)
      val failure = intercept[ExecutionException] {
        future.get()
      }
      failure.getCause shouldBe reportingFailure
      reportingFailure.getSuppressed.toVector should contain(observationFailure)
      fixture.tick()
      fixture.observations shouldBe 1
    }

    it("cancels removed handles and all pending tasks on nonblocking, non-interrupting shutdown") {
      val fixture = new Fixture
      val first   = fixture.watch()
      val second  = fixture.watch()
      first.cancel(true) shouldBe true
      first.cancel(false) shouldBe false
      fixture.scheduler.pendingCount shouldBe 1

      fixture.watchdog.shutdown()
      fixture.watchdog.shutdown()
      first.isCancelled shouldBe true
      second.isCancelled shouldBe true
      fixture.scheduler.pendingCount shouldBe 0
      fixture.scheduler.isShutdown shouldBe true
      fixture.scheduler.shutdownCalls shouldBe 1
      fixture.scheduler.shutdownNowCalls shouldBe 0
      fixture.scheduler.interruptRequests.toVector shouldBe Vector(false, false)

      fixture.clock.advance(10.seconds)
      fixture.scheduler.runDue()
      fixture.observations shouldBe 0
      fixture.unhealthyAttempts shouldBe 0
      intercept[RejectedExecutionException] {
        fixture.watch()
      }
    }

    it("does not leak periodic tasks when first-execution completion or shutdown precedes handle attachment") {
      Seq(
        StreamingStartupObservation(active = true, progressObserved = true, workActive = Some(false)),
        StreamingStartupObservation(active = false, progressObserved = false, workActive = Some(false)),
        StreamingStartupObservation(active = true, progressObserved = false, workActive = Some(false))
      ).foreach { observation =>
        val fixture = new Fixture
        fixture.observation = observation
        fixture.clock.advance(5.seconds)
        fixture.scheduler.executeDuringRegistration = true
        val future = fixture.watch()

        future.isCancelled shouldBe true
        fixture.scheduler.pendingCount shouldBe 0
        val deliveries = if (observation.active && !observation.progressObserved) 1 else 0
        fixture.unhealthyDeliveries shouldBe deliveries
        fixture.tick()
        fixture.observations shouldBe 1
        fixture.unhealthyDeliveries shouldBe deliveries
      }

      Seq(true, false).foreach { duringObservation =>
        val fixture = new Fixture
        if (duringObservation) fixture.afterObserve = () => fixture.watchdog.shutdown()
        else fixture.afterDelivery = () => fixture.watchdog.shutdown()
        fixture.clock.advance(5.seconds)
        fixture.scheduler.executeDuringRegistration = true
        val future = fixture.watch()

        future.isCancelled shouldBe true
        fixture.scheduler.isShutdown shouldBe true
        fixture.scheduler.pendingCount shouldBe 0
        fixture.unhealthyDeliveries shouldBe (if (duringObservation) 0 else 1)
        fixture.tick()
        fixture.observations shouldBe 1
      }
    }
  }

  private final class Fixture {
    val clock     = new ManualClock
    val deadline  = LifecycleDeadline.start(5.seconds, () => clock.nanoTime())
    val scheduler = new ManualScheduler(() => clock.nanoTime())
    val reported  = ArrayBuffer.empty[Throwable]

    var observation =
      StreamingStartupObservation(active = true, progressObserved = false, workActive = Some(false))
    var observationFailure        = Option.empty[Throwable]
    var deliveryFailure           = Option.empty[Throwable]
    var reporterFailure           = Option.empty[Throwable]
    var afterObserve: () => Unit  = () => ()
    var afterDelivery: () => Unit = () => ()
    var observations              = 0
    var unhealthyAttempts         = 0
    var unhealthyDeliveries       = 0

    val watchdog = new StreamingFirstProgressWatchdog(
      reportError = error => {
        reported += error
        reporterFailure.foreach(failure => throw failure)
      },
      scheduler = scheduler
    )

    def watch(): ScheduledFuture[_] =
      watchdog.watch(
        deadline,
        () => {
          observations += 1
          observationFailure.foreach(failure => throw failure)
          afterObserve()
          observation
        },
        () => {
          unhealthyAttempts += 1
          deliveryFailure.foreach(failure => throw failure)
          afterDelivery()
          unhealthyDeliveries += 1
        }
      )

    def atDeadline(): Unit = {
      clock.advance(5.seconds)
      scheduler.runDue()
    }

    def tick(): Unit = {
      clock.advance(1.second)
      scheduler.runDue()
    }
  }

  private final class ManualClock {
    private var now = 0L

    def nanoTime(): Long = now

    def advance(duration: FiniteDuration): Unit = {
      now += duration.toNanos
    }
  }

  private final class ManualScheduler(clock: () => Long) extends AbstractExecutorService with ScheduledExecutorService {

    private val tasks             = ArrayBuffer.empty[ManualFuture]
    private var stopped           = false
    var executeDuringRegistration = false
    var shutdownCalls             = 0
    var shutdownNowCalls          = 0
    val interruptRequests         = ArrayBuffer.empty[Boolean]

    def pendingCount: Int = tasks.count(task => !task.isDone)

    def runDue(): Unit =
      tasks.toVector
        .filter(task => !task.isDone && task.getDelay(TimeUnit.NANOSECONDS) <= 0L)
        .foreach(_.runOnce())

    def runNextNow(): Unit = tasks.find(task => !task.isDone).foreach(_.runOnce())

    override def scheduleWithFixedDelay(
        command: Runnable,
        initialDelay: Long,
        delay: Long,
        unit: TimeUnit
    ): ScheduledFuture[_] = {
      if (stopped) throw new RejectedExecutionException("Manual scheduler is shut down")
      require(delay > 0L)
      val task = new ManualFuture(
        command,
        clock() + math.max(0L, unit.toNanos(initialDelay)),
        unit.toNanos(delay)
      )
      tasks += task
      if (executeDuringRegistration) task.runOnce()
      task
    }

    override def shutdown(): Unit = {
      shutdownCalls += 1
      stopped = true
    }

    override def shutdownNow(): java.util.List[Runnable] = {
      shutdownNowCalls += 1
      shutdown()
      tasks.foreach(_.cancel(false))
      java.util.Collections.emptyList[Runnable]()
    }

    override def isShutdown: Boolean   = stopped
    override def isTerminated: Boolean = stopped && pendingCount == 0

    override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean =
      throw new UnsupportedOperationException("Watchdog must not await termination")

    override def execute(command: Runnable): Unit =
      throw new UnsupportedOperationException("Only fixed-delay scheduling is needed")

    override def schedule(command: Runnable, delay: Long, unit: TimeUnit): ScheduledFuture[_] =
      throw new UnsupportedOperationException("Only fixed-delay scheduling is needed")

    override def schedule[V](command: Callable[V], delay: Long, unit: TimeUnit): ScheduledFuture[V] =
      throw new UnsupportedOperationException("Only fixed-delay scheduling is needed")

    override def scheduleAtFixedRate(
        command: Runnable,
        initialDelay: Long,
        period: Long,
        unit: TimeUnit
    ): ScheduledFuture[_] =
      throw new UnsupportedOperationException("Only fixed-delay scheduling is needed")

    private final class ManualFuture(command: Runnable, initialRunNanos: Long, intervalNanos: Long)
        extends ScheduledFuture[Unit] {

      private var nextRunNanos = initialRunNanos
      private var cancelled    = false
      private var failure      = Option.empty[Throwable]

      def runOnce(): Unit =
        if (!isDone) {
          try command.run()
          catch {
            case error: Throwable => failure = Some(error)
          }
          if (!isDone) nextRunNanos = clock() + intervalNanos
        }

      override def cancel(mayInterruptIfRunning: Boolean): Boolean = {
        interruptRequests += mayInterruptIfRunning
        if (isDone) false
        else {
          cancelled = true
          true
        }
      }

      override def isCancelled: Boolean = cancelled
      override def isDone: Boolean      = cancelled || failure.nonEmpty

      override def getDelay(unit: TimeUnit): Long = unit.convert(nextRunNanos - clock(), TimeUnit.NANOSECONDS)

      override def compareTo(other: Delayed): Int =
        java.lang.Long.compare(getDelay(TimeUnit.NANOSECONDS), other.getDelay(TimeUnit.NANOSECONDS))

      override def get(): Unit = {
        if (cancelled) throw new CancellationException()
        failure match {
          case Some(error) => throw new ExecutionException(error)
          case None        => throw new IllegalStateException("Pending manual future cannot block")
        }
      }

      override def get(timeout: Long, unit: TimeUnit): Unit = {
        if (!isDone) throw new TimeoutException()
        get()
      }
    }
  }
}
