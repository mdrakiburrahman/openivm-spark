package org.openivm.spark.streaming

import org.openivm.spark.commands.RefreshMutex
import org.openivm.spark.common.{FeatureGate, LifecycleLockTimeoutException}
import org.scalatest.funspec.AnyFunSpec

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import scala.concurrent.duration._

class StreamingRebuildAdmissionSpec extends AnyFunSpec with StreamingTableTestFixture {

  private def withAdmissionTimeout[A](value: String)(body: => A): A = {
    val previous = spark.conf.getOption(FeatureGate.StreamingLifecycleAdmissionTimeoutKey)
    spark.conf.set(FeatureGate.StreamingLifecycleAdmissionTimeoutKey, value)
    try body
    finally {
      previous match {
        case Some(configured) => spark.conf.set(FeatureGate.StreamingLifecycleAdmissionTimeoutKey, configured)
        case None             => spark.conf.unset(FeatureGate.StreamingLifecycleAdmissionTimeoutKey)
      }
      StreamingTableManager.setAdmissionNanoTimeForTesting(() => System.nanoTime())
      StreamingTableManager.setAfterRebuildPlanHookForTesting(() => ())
    }
  }

  private def rebuild(root: String, source: String): StreamingTableStatus =
    createStreaming(
      root,
      readStream(source).where("id >= 2"),
      s"SELECT id, value, part FROM STREAM $source WHERE id >= 2",
      options = Map("onQueryChange" -> "rebuild")
    )

  describe("cascade rebuild lifecycle admission") {
    it("admits an uncontended cascade after metadata preparation outlives the admission budget") {
      val source = "rebuild_admission_slow_source"
      val root   = "rebuild_admission_slow_root"
      val child  = "rebuild_admission_slow_child"
      createSource(source)
      appendRows(source, "(1, 'one', 'p'), (2, 'two', 'p')")
      process(createStreaming(root, readStream(source), s"SELECT id, value, part FROM STREAM $source"))
      process(createStreaming(child, readStream(root), s"SELECT id, value, part FROM STREAM $root"))
      val original = StreamingTableMetadata.resolveDeltaTarget(spark, Seq(root), requireTableIdMarker = true)
      val clock    = new AtomicLong()
      val plans    = new AtomicLong()

      val rebuilt = withAdmissionTimeout("60s") {
        StreamingTableManager.setAdmissionNanoTimeForTesting(() => clock.get())
        StreamingTableManager.setAfterRebuildPlanHookForTesting { () =>
          plans.incrementAndGet()
          clock.addAndGet(61.seconds.toNanos)
          ()
        }
        rebuild(root, source)
      }
      plans.get() shouldBe 1L
      process(rebuilt)
      spark.catalog.tableExists(child) shouldBe false
      archivedCheckpoints(original) should have size 1
      assertBagEqual(root, s"SELECT * FROM $source WHERE id >= 2")
      process(createStreaming(child, readStream(root), s"SELECT id, value, part FROM STREAM $root"))
      assertBagEqual(child, s"SELECT * FROM $source WHERE id >= 2")
    }

    it("times out a held descendant guard before stopping writers or authorizing cleanup") {
      val source = "rebuild_admission_held_source"
      val root   = "rebuild_admission_held_root"
      val child  = "rebuild_admission_held_child"
      createSource(source)
      appendRows(source, "(1, 'one', 'p')")
      val rootStatus  = createStreaming(root, readStream(source), s"SELECT id, value, part FROM STREAM $source")
      val rootQuery   = process(rootStatus)
      val childStatus = createStreaming(child, readStream(root), s"SELECT id, value, part FROM STREAM $root")
      val childQuery  = process(childStatus)
      val original    = StreamingTableMetadata.resolveDeltaTarget(spark, Seq(root), requireTableIdMarker = true)
      val childInfo   = StreamingTableMetadata.resolveDeltaTarget(spark, Seq(child), requireTableIdMarker = true)
      val entered     = new CountDownLatch(1)
      val release     = new CountDownLatch(1)
      val ownerError  = new AtomicReference[Throwable]()
      val owner = new Thread(
        () =>
          try
            RefreshMutex.withLock(childInfo.identity) {
              entered.countDown()
              release.await()
            }
          catch {
            case error: Throwable => ownerError.set(error)
          },
        "rebuild-admission-descendant-owner"
      )
      owner.setDaemon(true)
      owner.start()
      try {
        entered.await(5, TimeUnit.SECONDS) shouldBe true
        val error = withAdmissionTimeout("50ms") {
          intercept[LifecycleLockTimeoutException](rebuild(root, source))
        }
        error.ownerThread shouldBe Some("rebuild-admission-descendant-owner")
        error.phase shouldBe "lifecycle_admission"
        error.waiterTarget shouldBe Some(root)
        rootQuery.isActive shouldBe true
        childQuery.isActive shouldBe true
        StreamingTableMetadata.pendingResetIntent(spark, original.identity) shouldBe None
        archivedCheckpoints(original) shouldBe empty
        archivedCheckpoints(childInfo) shouldBe empty
        pathExists(original.checkpointLocation) shouldBe true
        pathExists(childInfo.checkpointLocation) shouldBe true
        assertBagEqual(root, s"SELECT * FROM $source")
        assertBagEqual(child, s"SELECT * FROM $source")
      } finally {
        release.countDown()
        owner.join(5000L)
      }
      owner.isAlive shouldBe false
      ownerError.get() shouldBe null
    }

    it("rejects a changed dependency graph before stopping any writer") {
      val source = "rebuild_admission_graph_source"
      val root   = "rebuild_admission_graph_root"
      val child  = "rebuild_admission_graph_child"
      createSource(source)
      appendRows(source, "(1, 'one', 'p')")
      val original  = createStreaming(root, readStream(source), s"SELECT id, value, part FROM STREAM $source")
      val rootQuery = process(original)
      var childQuery: Option[org.apache.spark.sql.streaming.StreamingQuery] = None

      val error = withAdmissionTimeout("60s") {
        StreamingTableManager.setAfterRebuildPlanHookForTesting { () =>
          childQuery = Some(
            process(createStreaming(child, readStream(root), s"SELECT id, value, part FROM STREAM $root"))
          )
        }
        intercept[org.apache.spark.sql.AnalysisException](rebuild(root, source))
      }
      error.getMessage should include("changed during rebuild admission")
      rootQuery.isActive shouldBe true
      childQuery.exists(_.isActive) shouldBe true
      val target = StreamingTableMetadata.resolveDeltaTarget(spark, Seq(root), requireTableIdMarker = true)
      StreamingTableMetadata.pendingResetIntent(spark, target.identity) shouldBe None
      archivedCheckpoints(target) shouldBe empty
      assertBagEqual(root, s"SELECT * FROM $source")
      assertBagEqual(child, s"SELECT * FROM $source")
    }
  }
}
