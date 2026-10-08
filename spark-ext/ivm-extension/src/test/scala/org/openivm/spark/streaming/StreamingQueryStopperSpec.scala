package org.apache.spark.sql.streaming {

  import java.util.{HashMap, UUID}
  import org.apache.spark.sql.Row

  object OpenIvmStopProgressFixture {
    def progress(id: UUID, runId: UUID, batchId: Long): StreamingQueryProgress =
      new StreamingQueryProgress(
        id = id,
        runId = runId,
        name = "stopper_spec_progress",
        timestamp = "2026-01-01T00:00:00.000Z",
        batchId = batchId,
        batchDuration = 0L,
        durationMs = new HashMap[String, java.lang.Long](),
        eventTime = new HashMap[String, String](),
        stateOperators = Array.empty[StateOperatorProgress],
        sources = Array.empty[SourceProgress],
        sink = new SinkProgress("stopper_spec_sink", 0L),
        observedMetrics = new HashMap[String, Row]()
      ) {
        override def json: String       = throw new UnsupportedOperationException("Raw progress must not be read")
        override def prettyJson: String = throw new UnsupportedOperationException("Raw progress must not be read")
        override def toString: String   = throw new UnsupportedOperationException("Raw progress must not be read")
      }
  }
}

package org.openivm.spark.streaming {

  import java.util.UUID
  import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong, AtomicReference}
  import java.util.concurrent.{
    CancellationException,
    CompletionException,
    ConcurrentLinkedQueue,
    CountDownLatch,
    RejectedExecutionException,
    TimeUnit,
    TimeoutException
  }
  import scala.concurrent.duration._
  import org.apache.spark.sql.streaming.OpenIvmStopProgressFixture
  import org.openivm.spark.common.LifecycleDeadline
  import org.scalatest.funspec.AnyFunSpec
  import org.scalatest.matchers.should.Matchers

  class StreamingQueryStopperSpec extends AnyFunSpec with Matchers {

    private val target     = "stopper_spec_target"
    private val waitBudget = 5.seconds

    private def await(latch: CountDownLatch): Unit =
      assert(latch.await(waitBudget.toNanos, TimeUnit.NANOSECONDS), "Test latch did not complete within its guard")

    private final class HeldStop {
      val entered     = new CountDownLatch(1)
      val release     = new CountDownLatch(1)
      val interrupted = new CountDownLatch(1)
      val exited      = new CountDownLatch(1)

      def run(): Unit = {
        entered.countDown()
        try {
          var released = false
          while (!released) {
            try {
              release.await()
              released = true
            } catch {
              case _: InterruptedException => interrupted.countDown()
            }
          }
        } finally exited.countDown()
      }
    }

    private final class Caller[A](body: () => A) {
      private val outcome = new AtomicReference[Either[Throwable, A]]()
      val done            = new CountDownLatch(1)
      val interruptFlag   = new AtomicBoolean()
      val thread = new Thread(
        new Runnable {
          override def run(): Unit =
            try outcome.set(Right(body()))
            catch {
              case error: Throwable => outcome.set(Left(error))
            } finally {
              interruptFlag.set(Thread.currentThread().isInterrupted)
              done.countDown()
            }
        },
        "openivm-stop-test-caller"
      )
      thread.setDaemon(true)
      thread.start()

      def awaitFinished(): Unit = await(done)

      def success: A = {
        awaitFinished()
        outcome.get() match {
          case Right(value) => value
          case Left(error)  => throw error
        }
      }

      def failure: Throwable = {
        awaitFinished()
        outcome.get() match {
          case Left(error) => error
          case Right(_)    => fail("Expected the test caller to fail")
        }
      }
    }

    private type FakeQuery = StreamingQueryTestDouble

    describe("bounded native streaming-query stop") {
      it("confirms native termination, retains only batch evidence, and cancels first on an isolated daemon") {
        val stopper      = new StreamingQueryStopper(1)
        val active       = new AtomicBoolean(true)
        val id           = UUID.randomUUID()
        val runId        = UUID.randomUUID()
        val order        = new ConcurrentLinkedQueue[String]()
        val cancelThread = new AtomicReference[Thread]()
        val inherited    = new InheritableThreadLocal[String]()
        val workerValue  = new AtomicReference[String]()
        val query = new FakeQuery(
          () => {
            order.add("stop")
            active.set(false)
          },
          active,
          id,
          runId,
          OpenIvmStopProgressFixture.progress(id, runId, 7L)
        )
        inherited.set("request-local-insight-capture")

        try {
          val observation = stopper.stop(
            query,
            target,
            LifecycleDeadline.start(5.seconds),
            () => {
              order.add("cancel")
              cancelThread.set(Thread.currentThread())
              workerValue.set(inherited.get())
            }
          )

          observation.queryId shouldBe id.toString
          observation.runId shouldBe runId.toString
          observation.active shouldBe false
          observation.progressObserved shouldBe true
          observation.lastBatchId shouldBe Some(7L)
          observation.cancellationAttempted shouldBe true
          observation.cancellationErrorClass shouldBe None
          observation.terminationConfirmed shouldBe true
          query.stopCalls.get() shouldBe 1
          stopper.isStopping(id.toString, runId.toString) shouldBe false
          order.poll() shouldBe "cancel"
          order.poll() shouldBe "stop"
          order.isEmpty shouldBe true
          cancelThread.get() shouldBe query.stoppingThread.get()
          cancelThread.get() should not be Thread.currentThread()
          cancelThread.get().isDaemon shouldBe true
          workerValue.get() shouldBe null
        } finally {
          inherited.remove()
          stopper.shutdown()
        }
      }

      it("reports a distinct native failure when stop returns but the query remains active") {
        val stopper = new StreamingQueryStopper(1)
        val query   = new FakeQuery(() => ())
        try {
          val error = intercept[StreamingQueryStopFailedException] {
            stopper.stop(query, target, LifecycleDeadline.start(5.seconds), () => ())
          }

          error.getCause shouldBe null
          error.observation.active shouldBe true
          error.observation.terminationConfirmed shouldBe false
          error.getMessage should include("still active")
          query.stopCalls.get() shouldBe 1
          stopper.isStopping(query.id.toString, query.runId.toString) shouldBe false
        } finally stopper.shutdown()
      }

      it("releases timed-out callers without retiring an interrupt-ignoring invocation and deduplicates by run") {
        val stopper     = new StreamingQueryStopper(2)
        val held        = new HeldStop
        val active      = new AtomicBoolean(true)
        val cancelCalls = new AtomicInteger()
        val query = new FakeQuery(
          () => {
            held.run()
            active.set(false)
          },
          active
        )
        val duplicateCancel = new AtomicInteger()
        val duplicate = new FakeQuery(
          () => throw new AssertionError("A duplicate native stop must not run"),
          active,
          query.id,
          query.runId
        )
        try {
          val first = intercept[StreamingQueryStopTimeoutException] {
            stopper.stop(
              query,
              target,
              LifecycleDeadline.start(50.millis),
              () => {
                cancelCalls.incrementAndGet()
                ()
              }
            )
          }
          await(held.entered)
          await(held.interrupted)
          first.observation.timeoutMs shouldBe 50L
          first.observation.terminationConfirmed shouldBe false
          stopper.isStopping(query.id.toString, query.runId.toString) shouldBe true
          query.stoppingThread.get().isDaemon shouldBe true

          val second = intercept[StreamingQueryStopTimeoutException] {
            stopper.stop(
              duplicate,
              target,
              LifecycleDeadline.start(50.millis),
              () => {
                duplicateCancel.incrementAndGet()
                ()
              }
            )
          }
          second.observation.cancellationAttempted shouldBe true
          second.observation.active shouldBe true
          second.observation.terminationConfirmed shouldBe false
          query.stopCalls.get() shouldBe 1
          duplicate.stopCalls.get() shouldBe 0
          cancelCalls.get() shouldBe 1
          duplicateCancel.get() shouldBe 0

          val restartedActive = new AtomicBoolean(true)
          val restarted = new FakeQuery(
            () => restartedActive.set(false),
            restartedActive,
            query.id,
            UUID.randomUUID()
          )
          stopper
            .stop(restarted, target, LifecycleDeadline.start(5.seconds), () => ())
            .terminationConfirmed shouldBe true
          restarted.stopCalls.get() shouldBe 1
          stopper.isStopping(query.id.toString, query.runId.toString) shouldBe true
        } finally {
          held.release.countDown()
          await(held.exited)
          stopper.shutdown()
        }
      }

      it("propagates the exact shared lifecycle budget and starts no work after it is exhausted") {
        val stopper  = new StreamingQueryStopper(1)
        val clock    = new AtomicLong(123L)
        val started  = clock.get()
        val deadline = LifecycleDeadline.start(2.seconds, () => clock.get())
        clock.addAndGet(750.millis.toNanos)
        val active = new AtomicBoolean(true)
        val query = new FakeQuery(
          () => {
            clock.addAndGet(250.millis.toNanos)
            active.set(false)
          },
          active
        )
        try {
          val observation = stopper.stop(
            query,
            target,
            deadline,
            () => {
              clock.addAndGet(250.millis.toNanos)
              ()
            }
          )
          observation.timeoutMs shouldBe 2000L
          observation.remainingBudgetMs shouldBe 750L
          deadline.remainingNanos shouldBe 750.millis.toNanos

          clock.set(started + deadline.timeout.toNanos)
          val expiredCancel = new AtomicInteger()
          val expired       = new FakeQuery(() => throw new AssertionError("Exhausted stop must not run"))
          val error = intercept[StreamingQueryStopTimeoutException] {
            stopper.stop(
              expired,
              target,
              deadline,
              () => {
                expiredCancel.incrementAndGet()
                ()
              }
            )
          }
          error.observation.timeoutMs shouldBe 2000L
          error.observation.remainingBudgetMs shouldBe 0L
          error.observation.cancellationAttempted shouldBe false
          expired.stopCalls.get() shouldBe 0
          expiredCancel.get() shouldBe 0
          stopper.isStopping(expired.id.toString, expired.runId.toString) shouldBe false
        } finally stopper.shutdown()
      }

      it("waits only for the remaining budget rather than restarting the configured timeout") {
        val stopper  = new StreamingQueryStopper(1)
        val held     = new HeldStop
        val active   = new AtomicBoolean(true)
        val clock    = new AtomicLong()
        val deadline = LifecycleDeadline.start(30.seconds, () => clock.get())
        clock.set(30.seconds.toNanos - 25.millis.toNanos)
        val query = new FakeQuery(
          () => {
            held.run()
            active.set(false)
          },
          active
        )
        val caller = new Caller(() => stopper.stop(query, target, deadline, () => ()))
        try {
          await(held.entered)
          val error = caller.failure match {
            case timeout: StreamingQueryStopTimeoutException => timeout
            case other => fail(s"Unexpected stop failure class ${other.getClass.getName}")
          }
          error.observation.timeoutMs shouldBe 30000L
          error.observation.remainingBudgetMs shouldBe 25L
          error.observation.terminationConfirmed shouldBe false
          stopper.isStopping(query.id.toString, query.runId.toString) shouldBe true
        } finally {
          held.release.countDown()
          await(held.exited)
          stopper.shutdown()
          caller.awaitFinished()
        }
      }

      it("preserves immediate native causes, including native TimeoutException, without exposing their SQL") {
        val nativeFailures = Seq(
          new IllegalStateException("SELECT confidential_native_plan " * 100),
          new TimeoutException("SELECT confidential_native_plan " * 100),
          new InterruptedException("native worker interruption"),
          new CancellationException("native cancellation"),
          new CompletionException(new IllegalStateException("native completion failure"))
        )
        nativeFailures.foreach { cause =>
          val stopper    = new StreamingQueryStopper(1)
          val query      = new FakeQuery(() => throw cause)
          val longTarget = "table\n" + ("x" * 2000)
          try {
            val error = intercept[StreamingQueryStopFailedException] {
              stopper.stop(query, longTarget, LifecycleDeadline.start(5.seconds), () => ())
            }

            assert(error.getCause eq cause)
            error.target shouldBe longTarget
            error.observation.terminationConfirmed shouldBe false
            error.observation.cancellationAttempted shouldBe true
            error.getMessage.length should be < 1024
            error.getMessage should include(query.id.toString)
            error.getMessage should include(query.runId.toString)
            error.getMessage should include("finite timeout=5000 ms")
            error.getMessage should include("Cleanup is unsafe")
            error.getMessage should include("orchestrator may recycle")
            error.getMessage should not include "confidential_native_plan"
            error.getMessage should not include "\n"
            query.stopCalls.get() shouldBe 1
          } finally stopper.shutdown()
        }
      }

      it("records cancellation NonFatal errors without suppressing native success or the native failure cause") {
        val nativeFailure = new IllegalStateException("native failure")
        Seq(None, Some(nativeFailure)).foreach { failure =>
          val stopper = new StreamingQueryStopper(1)
          val active  = new AtomicBoolean(true)
          val query = new FakeQuery(
            () =>
              failure match {
                case Some(error) => throw error
                case None        => active.set(false)
              },
            active
          )
          val cancel = () => throw new IllegalArgumentException("cancellation SQL must not enter observations")
          try {
            val observation = failure match {
              case None => stopper.stop(query, target, LifecycleDeadline.start(5.seconds), cancel)
              case Some(cause) =>
                val error = intercept[StreamingQueryStopFailedException] {
                  stopper.stop(query, target, LifecycleDeadline.start(5.seconds), cancel)
                }
                assert(error.getCause eq cause)
                error.observation
            }

            observation.cancellationAttempted shouldBe true
            observation.cancellationErrorClass shouldBe Some(classOf[IllegalArgumentException].getName)
            observation.terminationConfirmed shouldBe failure.isEmpty
            query.stopCalls.get() shouldBe 1
          } finally stopper.shutdown()
        }
      }

      it("preserves caller interruption distinctly, including an already-interrupted caller") {
        val stopper = new StreamingQueryStopper(1)
        val held    = new HeldStop
        val active  = new AtomicBoolean(true)
        val query = new FakeQuery(
          () => {
            held.run()
            active.set(false)
          },
          active
        )
        val caller = new Caller(() => stopper.stop(query, target, LifecycleDeadline.start(10.seconds), () => ()))
        try {
          await(held.entered)
          caller.thread.interrupt()
          val error = caller.failure match {
            case interruption: StreamingQueryStopInterruptedException => interruption
            case other => fail(s"Unexpected stop failure class ${other.getClass.getName}")
          }
          caller.interruptFlag.get() shouldBe true
          error.getCause shouldBe a[InterruptedException]
          error.observation.terminationConfirmed shouldBe false
          await(held.interrupted)
          stopper.isStopping(query.id.toString, query.runId.toString) shouldBe true
        } finally {
          held.release.countDown()
          await(held.exited)
          stopper.shutdown()
          caller.awaitFinished()
        }

        val preInterruptedStopper = new StreamingQueryStopper(1)
        val unusedQuery = new FakeQuery(() => throw new AssertionError("Interrupted caller must not start work"))
        val cancelCalls = new AtomicInteger()
        val preInterrupted = new Caller(() => {
          Thread.currentThread().interrupt()
          preInterruptedStopper.stop(
            unusedQuery,
            target,
            LifecycleDeadline.start(5.seconds),
            () => {
              cancelCalls.incrementAndGet()
              ()
            }
          )
        })
        try {
          preInterrupted.failure shouldBe a[StreamingQueryStopInterruptedException]
          preInterrupted.interruptFlag.get() shouldBe true
          unusedQuery.stopCalls.get() shouldBe 0
          cancelCalls.get() shouldBe 0
        } finally {
          preInterruptedStopper.shutdown()
          preInterrupted.awaitFinished()
        }
      }

      it("rejects saturation and shutdown explicitly while shutdown never waits for blocked native workers") {
        val stopper = new StreamingQueryStopper(1)
        val held    = new HeldStop
        val active  = new AtomicBoolean(true)
        val query = new FakeQuery(
          () => {
            held.run()
            active.set(false)
          },
          active
        )
        val caller        = new Caller(() => stopper.stop(query, target, LifecycleDeadline.start(30.seconds), () => ()))
        val rejectedQuery = new FakeQuery(() => throw new AssertionError("Rejected native stop must not run"))
        val rejectedCancel = new AtomicInteger()
        try {
          await(held.entered)
          val saturated = intercept[StreamingQueryStopRejectedException] {
            stopper.stop(
              rejectedQuery,
              target,
              LifecycleDeadline.start(5.seconds),
              () => {
                rejectedCancel.incrementAndGet()
                ()
              }
            )
          }
          saturated.getCause shouldBe a[RejectedExecutionException]
          saturated.observation.cancellationAttempted shouldBe false
          saturated.observation.terminationConfirmed shouldBe false
          rejectedQuery.stopCalls.get() shouldBe 0
          rejectedCancel.get() shouldBe 0
          stopper.isStopping(rejectedQuery.id.toString, rejectedQuery.runId.toString) shouldBe false

          val shutdownCaller = new Caller(() => stopper.shutdown())
          shutdownCaller.success
          await(held.interrupted)
          held.release.getCount shouldBe 1L
          held.exited.getCount shouldBe 1L
          caller.done.getCount shouldBe 1L
          stopper.isStopping(query.id.toString, query.runId.toString) shouldBe true
          intercept[StreamingQueryStopRejectedException] {
            stopper.stop(query, target, LifecycleDeadline.start(5.seconds), () => ())
          }.observation.terminationConfirmed shouldBe false
        } finally {
          held.release.countDown()
          await(held.exited)
          stopper.shutdown()
          caller.awaitFinished()
        }
        caller.success.terminationConfirmed shouldBe true
      }

      it("includes a blocking scoped cancellation in the deadline and still attempts stop after it exits") {
        val stopper = new StreamingQueryStopper(1)
        val held    = new HeldStop
        val active  = new AtomicBoolean(true)
        val stopped = new CountDownLatch(1)
        val query = new FakeQuery(
          () => {
            active.set(false)
            stopped.countDown()
          },
          active
        )
        val caller = new Caller(() => stopper.stop(query, target, LifecycleDeadline.start(50.millis), () => held.run()))
        try {
          await(held.entered)
          val error = caller.failure match {
            case timeout: StreamingQueryStopTimeoutException => timeout
            case other => fail(s"Unexpected stop failure class ${other.getClass.getName}")
          }
          await(held.interrupted)
          error.observation.terminationConfirmed shouldBe false
          val duplicate = intercept[StreamingQueryStopTimeoutException] {
            stopper.stop(
              query,
              target,
              LifecycleDeadline.start(25.millis),
              () => throw new AssertionError("A duplicate cancellation must not run")
            )
          }
          duplicate.observation.cancellationAttempted shouldBe true
          duplicate.observation.cancellationErrorClass shouldBe None
          duplicate.observation.terminationConfirmed shouldBe false
          query.stopCalls.get() shouldBe 0
          stopper.isStopping(query.id.toString, query.runId.toString) shouldBe true
        } finally {
          held.release.countDown()
          await(held.exited)
          await(stopped)
          stopper.shutdown()
          caller.awaitFinished()
        }
        query.stopCalls.get() shouldBe 1
      }
    }
  }
}
