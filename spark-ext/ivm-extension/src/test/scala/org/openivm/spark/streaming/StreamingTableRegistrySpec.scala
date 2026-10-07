package org.apache.spark.sql.streaming {
  import java.util.UUID

  object OpenIvmRegistryTerminationFixture {
    def event(id: UUID, run: UUID): StreamingQueryListener.QueryTerminatedEvent =
      new StreamingQueryListener.QueryTerminatedEvent(id, run, None, None)
  }
}

package org.openivm.spark.streaming {
  import org.apache.spark.sql.AnalysisException
  import org.apache.spark.sql.streaming.OpenIvmRegistryTerminationFixture
  import org.openivm.spark.common.LifecycleDeadline
  import org.scalatest.funspec.AnyFunSpec
  import org.scalatest.matchers.should.Matchers

  import java.util.concurrent.{CountDownLatch, TimeUnit}
  import java.util.concurrent.atomic.AtomicBoolean
  import scala.concurrent.duration._

  class StreamingTableRegistrySpec extends AnyFunSpec with Matchers {
    private val target = StreamingTableTarget(
      Seq("issue67_registry"),
      "issue67_registry",
      "`issue67_registry`",
      "file:/issue67_registry",
      "delta",
      "owner"
    )

    private def entry(query: StreamingQueryTestDouble): StreamingTableRegistry.Entry = {
      val result = new StreamingTableRegistry.Entry(target)
      result.bind(query, "definition")
      result
    }

    private def context(current: StreamingTableRegistry.Entry): StreamingTableRegistry.ContextRegistry = {
      val registry = new StreamingTableRegistry.ContextRegistry(_ => Some(false))
      val key      = StreamingTableRegistry.targetKey(target)
      registry.entries.put(key, current)
      registry.queryToTarget.put(current.queryId, key)
      registry
    }

    describe("termination-aware streaming registry") {
      it("confirms an inactive run through bounded stop before admitting a resume") {
        val query   = new StreamingQueryTestDouble(() => (), new AtomicBoolean(false))
        val current = entry(query)
        val stopper = new StreamingQueryStopper(1)
        try {
          current.snapshot.terminationUnconfirmed shouldBe true
          StreamingTableRegistry
            .stopEntry(current, query, stopper, LifecycleDeadline.start(5.seconds), () => ())
            .terminationConfirmed shouldBe true
          query.stopCalls.get() shouldBe 1
          val resumed = new StreamingQueryTestDouble(() => (), id = query.id)
          current.bind(resumed, "definition").isDefined shouldBe true
          current.currentQuery shouldBe Some(resumed)
          current.snapshot.runId shouldBe Some(resumed.runId.toString)
        } finally stopper.shutdown()
      }

      it("retains an inactive timed-out writer until a matching termination listener event") {
        val active  = new AtomicBoolean(true)
        val entered = new CountDownLatch(1)
        val release = new CountDownLatch(1)
        val exited  = new CountDownLatch(1)
        val query = new StreamingQueryTestDouble(
          () => {
            active.set(false)
            entered.countDown()
            try {
              var done = false
              while (!done)
                try {
                  release.await()
                  done = true
                } catch { case _: InterruptedException => () }
            } finally exited.countDown()
          },
          active
        )
        val current  = entry(query)
        val registry = context(current)
        val stopper  = new StreamingQueryStopper(1)
        try {
          intercept[StreamingQueryStopTimeoutException] {
            StreamingTableRegistry.stopEntry(current, query, stopper, LifecycleDeadline.start(50.millis), () => ())
          }
          entered.await(5, TimeUnit.SECONDS) shouldBe true
          query.isActive shouldBe false
          current.currentQuery shouldBe Some(query)
          current.snapshot.diagnosticStatus shouldBe Some("stop_timed_out")
          current.snapshot.terminationUnconfirmed shouldBe true
          current.snapshot.queryId shouldBe Some(query.id.toString)
          current.snapshot.runId shouldBe Some(query.runId.toString)
          new StreamingTableRegistry.RegistryListener(registry)
            .onQueryTerminated(OpenIvmRegistryTerminationFixture.event(query.id, query.runId))
          current.currentQuery shouldBe None
          current.snapshot.terminationUnconfirmed shouldBe false
          current.snapshot.diagnosticStatus shouldBe None
          current.snapshot.lastFailure shouldBe None
        } finally {
          release.countDown()
          exited.await(5, TimeUnit.SECONDS) shouldBe true
          stopper.shutdown()
          registry.close()
        }
      }

      it("ignores an old run's termination and progress after a replacement binds") {
        val old     = new StreamingQueryTestDouble(() => ())
        val current = entry(old)
        current.finish(old.runId.toString, None)
        val replacement = new StreamingQueryTestDouble(() => (), id = old.id)
        current.bind(replacement, "replacement")
        val registry = context(current)
        try {
          current.progress(old.runId.toString, Some("old progress"))
          new StreamingTableRegistry.RegistryListener(registry)
            .onQueryTerminated(OpenIvmRegistryTerminationFixture.event(old.id, old.runId))
          current.currentQuery shouldBe Some(replacement)
          current.snapshot.runId shouldBe Some(replacement.runId.toString)
          current.snapshot.lastProgress shouldBe None
          current.snapshot.progressObserved shouldBe false
          current.snapshot.terminationUnconfirmed shouldBe true
        } finally registry.close()
      }

      it("preserves early termination before the native query object is registered") {
        val query   = new StreamingQueryTestDouble(() => ())
        val current = new StreamingTableRegistry.Entry(target)
        current.started(query.id.toString, query.runId.toString)
        val registry = context(current)
        try {
          new StreamingTableRegistry.RegistryListener(registry)
            .onQueryTerminated(OpenIvmRegistryTerminationFixture.event(query.id, query.runId))
          current.bind(query, "definition") shouldBe None
          current.currentQuery shouldBe None
          current.snapshot.terminationUnconfirmed shouldBe false
        } finally registry.close()
      }

      it("preserves progress on same-run rediscovery but resets it for a new run") {
        val query   = new StreamingQueryTestDouble(() => ())
        val current = entry(query)
        val json    = """{"batchId":0,"numInputRows":0}"""
        current.progress(query.runId.toString, Some(json))
        current.bind(query, "definition") shouldBe None
        current.snapshot.lastProgress shouldBe Some(json)
        current.snapshot.progressObserved shouldBe true
        val failure = ("native stack frame " * 512) + "expected streaming failure"
        current.finish(query.runId.toString, Some(failure))
        current.snapshot.lastFailure.get should endWith("expected streaming failure")
        val replacement = new StreamingQueryTestDouble(() => (), id = query.id)
        current.bind(replacement, "definition").isDefined shouldBe true
        current.snapshot.lastProgress shouldBe None
        current.snapshot.progressObserved shouldBe false
      }

      it("refuses a second run while the previous run has not confirmed termination") {
        val query   = new StreamingQueryTestDouble(() => ())
        val current = entry(query)
        current.beginStop(query.runId.toString)
        intercept[AnalysisException] {
          current.bind(new StreamingQueryTestDouble(() => (), id = query.id), "replacement")
        }
        current.currentQuery shouldBe Some(query)
      }

      it("distinguishes immediate native failure without leaking its SQL into SHOW diagnostics") {
        val failure = new IllegalStateException("SELECT secret_native_plan")
        val query   = new StreamingQueryTestDouble(() => throw failure)
        val current = entry(query)
        val stopper = new StreamingQueryStopper(1)
        try {
          intercept[StreamingQueryStopFailedException] {
            StreamingTableRegistry.stopEntry(current, query, stopper, LifecycleDeadline.start(5.seconds), () => ())
          }.getCause shouldBe failure
          current.currentQuery shouldBe Some(query)
          current.snapshot.diagnosticStatus shouldBe Some("stop_failed")
          current.snapshot.lastFailure.get should not include "secret_native_plan"
          current.snapshot.terminationUnconfirmed shouldBe true
        } finally stopper.shutdown()
      }

      it("clears stop markers only after successful native completion") {
        val active  = new AtomicBoolean(true)
        val query   = new StreamingQueryTestDouble(() => active.set(false), active)
        val current = entry(query)
        val stopper = new StreamingQueryStopper(1)
        try {
          StreamingTableRegistry
            .stopEntry(
              current,
              query,
              stopper,
              LifecycleDeadline.start(5.seconds),
              () => ()
            )
            .terminationConfirmed shouldBe true
          current.currentQuery shouldBe None
          current.snapshot.terminationUnconfirmed shouldBe false
          current.snapshot.diagnosticStatus shouldBe None
        } finally stopper.shutdown()
      }

      it("diagnoses stalled startup and clears startup-only failure on empty progress or idle evidence") {
        Seq(None, Some("""{"batchId":0,"numInputRows":0}""")).foreach { progress =>
          val query   = new StreamingQueryTestDouble(() => ())
          val current = entry(query)
          current.unhealthy(query.runId.toString, 5.minutes)
          current.snapshot.diagnosticStatus shouldBe Some("unhealthy")
          current.currentQuery shouldBe Some(query)
          current.progress(query.runId.toString, progress)
          current.snapshot.diagnosticStatus shouldBe None
          current.snapshot.lastFailure shouldBe None
          current.snapshot.progressObserved shouldBe true
        }
      }

      it("shares target guards and releases nested acquisitions after a stop failure") {
        val query    = new StreamingQueryTestDouble(() => ())
        val current  = entry(query)
        val registry = context(current)
        val key      = StreamingTableRegistry.targetKey(target)
        val lock     = StreamingTableRegistry.targetLock(registry, key)
        StreamingTableRegistry.targetLock(registry, key) shouldBe lock
        val failure  = new IllegalStateException("stop failed")
        val deadline = LifecycleDeadline.start(5.seconds)
        try {
          intercept[IllegalStateException] {
            deadline.withLock(lock, key) {
              deadline.withLock(lock, key)(throw failure)
            }
          } shouldBe failure
          lock.isLocked shouldBe false
          deadline.withLock(lock, key)(true) shouldBe true
        } finally registry.close()
      }
    }
  }
}
