package org.openivm.spark.common

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.CyclicBarrier

class CtasBatchDispatcherSpec extends AnyFunSpec with BeforeAndAfterAll with Matchers {
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("openivm-spark-CtasBatchDispatcherSpec")
      .config("spark.scheduler.mode", "FAIR")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  describe("CtasAdmissionController") {
    it("starts at the complete cold-batch width and ignores successful task duration") {
      val controller = CtasAdmissionController.optimistic(batchSize = 10)

      controller.currentLimit shouldBe 10
      controller.record("slow", 0L, Long.MaxValue, inflight = 10, didDrop = false)
      controller.currentLimit shouldBe 10
    }

    it("backs off on explicit pressure") {
      val controller = CtasAdmissionController.optimistic(batchSize = 10)

      val decision = controller.record("failed", 0L, 1L, inflight = 10, didDrop = true)

      decision.limitBefore shouldBe 10
      decision.limitAfter shouldBe 5
      controller.currentLimit shouldBe 5
    }
  }

  describe("CtasBatchDispatcher") {
    it("fans out the cold batch into distinct FAIR pools") {
      val taskCount = 4
      val barrier   = new CyclicBarrier(taskCount)
      val tasks = (1 to taskCount).map { index =>
        CtasBatchTask(
          id = s"mv-$index",
          run = () => {
            barrier.await()
            spark.sparkContext.getLocalProperty("spark.scheduler.pool")
          }
        )
      }
      val controller = CtasAdmissionController.optimistic(taskCount)

      val result = CtasBatchDispatcher.run(spark, tasks, controller)

      result.values.toSet shouldBe (1 to taskCount).map(index => s"openivm-ctas-mv-$index").toSet
      result.telemetry.schedulerMode shouldBe "FAIR"
      result.telemetry.initialLimit shouldBe taskCount
      result.telemetry.learnedLimit shouldBe taskCount
      result.telemetry.maxInflight shouldBe taskCount
      result.telemetry.decisions should have size taskCount
    }

    it("applies a learned limit at the next batch boundary") {
      val controller = CtasAdmissionController.optimistic(batchSize = 4)
      controller.record("pressure", 0L, 1L, inflight = 4, didDrop = true)
      val tasks = (1 to 4).map { index =>
        CtasBatchTask(id = s"mv-$index", run = () => index)
      }

      val result = CtasBatchDispatcher.run(spark, tasks, controller)

      result.telemetry.initialLimit shouldBe 4
      result.telemetry.maxInflight should be <= 2
      result.values shouldBe (1 to 4)
    }
  }
}
