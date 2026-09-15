package org.openivm.spark.testkit

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.concurrent.duration._

/** Deterministic park / observe / release handshake for the
  * `CommandConcurrencyInjection` hooks.
  *
  * == Why this exists ==
  *
  * The natural way to write "command A must not block command B" is to park A
  * inside a production hook and, while it is parked, run B to completion:
  *
  * {{{
  * CommandConcurrencyInjection.withBeforeCreateBody({
  *   entered.countDown()
  *   release.await(30, SECONDS) shouldBe true      // <-- park budget
  * }) {
  *   ...
  *   Await.result(bFuture, 300.seconds)            // <-- observation budget
  *   aFuture.isCompleted shouldBe false
  *   release.countDown()
  * }
  * }}}
  *
  * That shape has a fatal ordering bug: the parked command is released only
  * AFTER B has been observed, so the park budget must dominate the observation
  * budget.  With `30 s` inside and `300 s` outside it does not.  Whenever B
  * legitimately takes longer than the park budget (a full-suite run puts 32
  * forked JVMs x `local[4]` Spark on the box, inflating a 20 s refresh past a
  * minute) the park expires first, the matcher inside the hook throws INTO the
  * production command, A completes exceptionally, and the outer
  * `isCompleted shouldBe false` reports `true was not equal to false` - a
  * phantom "global lock" verdict for a run in which nothing was ever locked.
  * A genuine global lock produces that exact same symptom, so the assertion
  * could be trusted neither when it failed nor when it passed.
  *
  * == The contract implemented here ==
  *
  *   - The park budget is a *failsafe*, not a synchronisation point: it is
  *     required at construction to strictly dominate the observation budget
  *     ([[ParkedCommandBarrier.forObservation]] refuses any other ordering),
  *     so the observer always reaches its verdict first.
  *   - [[park]] never throws into the production command path.  If the
  *     failsafe does expire it records [[ParkedCommandBarrier.Abandoned]], and
  *     the observer turns that into an explicit, self-describing failure
  *     instead of a mis-attributed locking verdict.
  *   - Observations are made against barrier state ([[state]] /
  *     [[parkedNow]] / [[parkCount]]), which only the test can move, rather
  *     than against a wall-clock race.
  *   - [[use]] releases every parked command in a `finally`, so a failing
  *     observation can never wedge a production thread or the fork.
  */
private[spark] final class ParkedCommandBarrier private (
    val observationBudget: FiniteDuration,
    val parkFailsafe: FiniteDuration
) {

  import ParkedCommandBarrier._

  private val releaseLatch = new CountDownLatch(1)
  private val monitor      = new Object
  private val parks        = new AtomicInteger(0)
  private val active       = new AtomicInteger(0)
  private val abandoned    = new AtomicBoolean(false)

  /** Body to install into a `CommandConcurrencyInjection` hook.  Publishes
    * "this command reached the hook", then parks until [[release]].  Never
    * throws: an expired failsafe is reported as [[Abandoned]] state.
    */
  def park(): Unit = {
    monitor.synchronized {
      parks.incrementAndGet()
      active.incrementAndGet()
      monitor.notifyAll()
    }
    val released =
      try releaseLatch.await(parkFailsafe.toMillis, TimeUnit.MILLISECONDS)
      catch {
        case _: InterruptedException =>
          Thread.currentThread().interrupt()
          false
      }
    if (!released) abandoned.set(true)
    monitor.synchronized {
      active.decrementAndGet()
      monitor.notifyAll()
    }
  }

  /** Wait until `count` commands are parked inside the hook simultaneously.
    * Bounded by the observation budget so a command that dies before the hook
    * fails fast instead of hanging the suite.
    */
  def awaitParked(count: Int): Boolean = {
    require(count >= 1, s"count must be >= 1, got $count")
    val deadlineNanos = System.nanoTime() + observationBudget.toNanos
    monitor.synchronized {
      var remainingMillis = (deadlineNanos - System.nanoTime()) / 1000000L
      while (active.get() < count && remainingMillis > 0L) {
        monitor.wait(remainingMillis)
        remainingMillis = (deadlineNanos - System.nanoTime()) / 1000000L
      }
      active.get() >= count
    }
  }

  /** Wait until the (single) observed command is parked inside the hook. */
  def awaitEntered(): Boolean = awaitParked(1)

  def state: State =
    if (abandoned.get()) Abandoned
    else if (parks.get() == 0) NotEntered
    else if (active.get() > 0) Parked
    else Released

  /** Total number of commands that have entered the hook. */
  def parkCount: Int = parks.get()

  /** Number of commands currently parked inside the hook. */
  def parkedNow: Int = active.get()

  def isParked: Boolean = state == Parked

  def release(): Unit = releaseLatch.countDown()

  /** Run `body` with the guarantee that every parked command is released
    * afterwards - including when `body` throws.
    */
  def use[A](body: => A): A =
    try body
    finally release()

  def describe: String =
    s"barrier(state=$state, parkedNow=$parkedNow, parks=$parkCount, " +
      s"observationBudget=$observationBudget, parkFailsafe=$parkFailsafe)"
}

private[spark] object ParkedCommandBarrier {

  sealed trait State

  /** No command has reached the hook yet. */
  case object NotEntered extends State

  /** At least one command is parked inside the hook, waiting for `release`. */
  case object Parked extends State

  /** Every command that parked has left the hook after a `release`. */
  case object Released extends State

  /** A park failsafe expired before the test released the command - the
    * observation is void and must be reported as such rather than folded into
    * a locking verdict.  Sticky.
    */
  case object Abandoned extends State

  /** Failsafe ceiling for a parked production thread.  It exists only so a
    * catastrophically broken test cannot wedge a fork forever; no path on
    * which the test itself completes can reach it.
    */
  val DefaultParkFailsafe: FiniteDuration = 30.minutes

  /** Build a barrier for an observation that may legitimately take up to
    * `observationBudget`.
    *
    * @throws IllegalArgumentException
    *   if the park failsafe does not strictly dominate the observation
    *   budget.  That ordering violation is exactly the defect that made
    *   `ConcurrencySpec` fail under full-suite load.
    */
  def forObservation(
      observationBudget: FiniteDuration,
      parkFailsafe: FiniteDuration = DefaultParkFailsafe
  ): ParkedCommandBarrier = {
    require(
      observationBudget > Duration.Zero,
      s"observationBudget must be positive, got $observationBudget"
    )
    require(
      parkFailsafe > observationBudget,
      s"park failsafe ($parkFailsafe) must strictly dominate the observation budget " +
        s"($observationBudget): a parked command is released only after the observation " +
        "completes, so a shorter park budget turns a slow observation into a false verdict"
    )
    new ParkedCommandBarrier(observationBudget, parkFailsafe)
  }
}
