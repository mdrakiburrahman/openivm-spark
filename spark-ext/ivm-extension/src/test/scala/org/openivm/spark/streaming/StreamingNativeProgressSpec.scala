package org.openivm.spark.streaming

import java.util.concurrent.{CountDownLatch, TimeUnit}
import org.openivm.spark.common.LifecycleDeadline
import org.scalatest.funspec.AnyFunSpec
import scala.concurrent.duration._

class StreamingNativeProgressSpec extends AnyFunSpec with StreamingTableTestFixture {

  describe("native progress reconciliation") {
    it("clears startup diagnostics in the same SHOW call without a progress callback") {
      val source = "native_progress_source"
      val name   = "native_progress_target"
      createSource(source)
      appendRows(source, "(1, 'one', 'p')")
      val status   = createStreaming(name, readStream(source), s"SELECT id, value, part FROM STREAM $source")
      val native   = process(status)
      val progress = native.lastProgress
      StreamingTableManager.stop(spark, Seq(name))
      val target = StreamingTableMetadata.resolveDeltaTarget(spark, Seq(name), requireTableIdMarker = true)
      val key    = StreamingTableRegistry.targetKey(target)
      StreamingTableRegistry.forget(spark, key)
      val entry = StreamingTableRegistry.entry(spark, key, target)
      val query = new StreamingQueryTestDouble(() => (), progress = progress)
      progress should not be null
      entry.bind(query, status.definitionHash.get)
      entry.unhealthy(query.runId.toString, 1.second)
      entry.snapshot.diagnosticStatus shouldBe Some("unhealthy")
      try {
        val row = StreamingTableManager.show(spark, None, Some(name)).head
        row.status shouldBe "active"
        row.lastFailure shouldBe None
        row.lastProgress should not be empty
        entry.snapshot.progressObserved shouldBe true

        entry.diagnosticError(query.runId.toString, new IllegalStateException("observation unavailable"))
        entry.snapshot.diagnosticStatus shouldBe Some("diagnostic_error")
        val recovered = StreamingTableManager.show(spark, None, Some(name)).head
        recovered.status shouldBe "active"
        recovered.lastFailure shouldBe None
        entry.lastFailure = "real native failure"
        val failed = StreamingTableManager.show(spark, None, Some(name)).head
        failed.lastFailure shouldBe Some("real native failure")
        entry.beginStop(query.runId.toString)
        entry.failedStop(query.runId.toString, new IllegalStateException("stop failed"))
        val stopping = StreamingTableManager.show(spark, None, Some(name)).head
        stopping.status shouldBe "stop_failed"
        stopping.lastFailure.get should include("termination is unconfirmed")
      } finally {
        entry.finish(query.runId.toString, None)
        StreamingTableRegistry.forget(spark, key)
      }
    }

    it("lets the watchdog observe native progress without any registered progress listener") {
      val source = "native_watchdog_source"
      val name   = "native_watchdog_target"
      createSource(source)
      appendRows(source, "(1, 'one', 'p')")
      val status   = createStreaming(name, readStream(source), s"SELECT id, value, part FROM STREAM $source")
      val progress = process(status).lastProgress
      progress should not be null
      StreamingTableManager.stop(spark, Seq(name))
      val target = StreamingTableMetadata.resolveDeltaTarget(spark, Seq(name), requireTableIdMarker = true)
      val query  = new StreamingQueryTestDouble(() => (), progress = progress)
      val entry  = new StreamingTableRegistry.Entry(target)
      entry.bind(query, status.definitionHash.get)
      entry.unhealthy(query.runId.toString, 1.second)
      val observed = new CountDownLatch(1)
      val registry = new StreamingTableRegistry.ContextRegistry(_ => {
        observed.countDown()
        Some(false)
      })
      registry.entries.put(StreamingTableRegistry.targetKey(target), entry)
      try {
        registry.watch(entry, query.runId.toString, LifecycleDeadline.start(1.nanosecond))
        observed.await(5, TimeUnit.SECONDS) shouldBe true
        entry.snapshot.progressObserved shouldBe true
        entry.snapshot.lastFailure shouldBe None
        entry.snapshot.diagnosticStatus shouldBe None
      } finally registry.close()
    }
  }
}
