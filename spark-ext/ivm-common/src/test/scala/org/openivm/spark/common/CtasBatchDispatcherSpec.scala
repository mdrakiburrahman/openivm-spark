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
      controller.completeBatch(batchSize = 10, capacityDrop = false)
      controller.currentLimit shouldBe 10
    }

    it("backs off on explicit pressure") {
      val controller = CtasAdmissionController.optimistic(batchSize = 10)

      val (before, after) = controller.completeBatch(batchSize = 10, capacityDrop = true)

      before shouldBe 10
      after shouldBe 5
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
      result.telemetry.limitBefore shouldBe taskCount
      result.telemetry.limitAfter shouldBe taskCount
      result.telemetry.maxInflight shouldBe taskCount
      result.telemetry.spans should have size taskCount
      all(result.telemetry.spans.map(_.startedEpochMs)) should be >= result.telemetry.spans.map(_.submittedEpochMs).min
    }

    it("applies a learned limit at the next batch boundary") {
      val controller = CtasAdmissionController.optimistic(batchSize = 4)
      controller.completeBatch(batchSize = 4, capacityDrop = true)
      val tasks = (1 to 4).map { index =>
        CtasBatchTask(id = s"mv-$index", run = () => index)
      }

      val result = CtasBatchDispatcher.run(spark, tasks, controller)

      result.telemetry.limitBefore shouldBe 2
      result.telemetry.maxInflight should be <= 2
      result.values shouldBe (1 to 4)
    }

    it("retains a failed batch's AIMD backoff for the next batch") {
      val controller = CtasAdmissionController.optimistic(batchSize = 10)
      val failedTasks = (1 to 10).map { index =>
        CtasBatchTask(
          id = s"mv-$index",
          run = () => {
            if (index == 1) throw new IllegalStateException("pressure")
            index
          }
        )
      }

      val failed = intercept[CtasBatchFailedException] {
        CtasBatchDispatcher.run(spark, failedTasks, controller, isCapacityFailure = _ => true)
      }

      failed.telemetry.limitBefore shouldBe 10
      failed.telemetry.limitAfter shouldBe 5
      failed.failures.map(_._1) shouldBe Seq("mv-1")

      val recoveryTasks = (1 to 10).map { index =>
        CtasBatchTask(id = s"recovery-$index", run = () => index)
      }
      val recovered = CtasBatchDispatcher.run(spark, recoveryTasks, controller)
      recovered.telemetry.maxInflight should be <= 5
      recovered.telemetry.limitAfter shouldBe 6
      recovered.telemetry.spans should have size 10
    }

    it("does not interpret ordinary SQL failures as capacity pressure") {
      val controller = CtasAdmissionController.optimistic(batchSize = 4)
      val failed = intercept[CtasBatchFailedException] {
        CtasBatchDispatcher.run(
          spark,
          Seq(CtasBatchTask("bad-sql", () => throw new IllegalArgumentException("missing table"))),
          controller
        )
      }

      failed.telemetry.spans.head.capacityDrop shouldBe false
      controller.currentLimit shouldBe 4
    }
  }
}
