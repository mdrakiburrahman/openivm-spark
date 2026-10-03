package org.openivm.spark.insights

import org.apache.spark.sql.SparkSession
import org.openivm.spark.insights.OpenIvmInsightsContract.{CaptureStatus, FailureCode, RequestStatus}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

class OpenIvmInsightsBrokerTerminalBoundsSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private val warehouse =
    new File(s"target/test-warehouse-insights-terminal-bounds-${UUID.randomUUID().toString.take(8)}")

  override def beforeAll(): Unit = {
    super.beforeAll()
    warehouse.mkdirs()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("OpenIvmInsightsBrokerTerminalBoundsSpec")
      .config("spark.sql.warehouse.dir", warehouse.getAbsolutePath)
      .config("spark.ui.enabled", "false")
      // request_started fits exactly; request_succeeded does not.
      .config(OpenIvmInsightsContract.MaxFieldBytesKey, "15")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try {
      if (spark != null) spark.stop()
      delete(warehouse)
    } finally super.afterAll()
  }

  describe("built-in terminal event bounds") {
    it("degrades the capture without failing END and still restores context") {
      val context = spark.sparkContext
      context.setLocalProperty(OpenIvmInsightsContract.RequestIdProperty, "outer-request")
      context.setLocalProperty(OpenIvmInsightsContract.RunIdProperty, "outer-run")
      context.setLocalProperty(OpenIvmInsightsContract.DbtNodeIdProperty, "outer-node")

      OpenIvmInsightsBroker.begin(spark, "bounded", "run-bounded", "node-bounded")
      noException should be thrownBy {
        OpenIvmInsightsBroker.end(spark, "bounded", succeeded = true)
      }

      context.getLocalProperty(OpenIvmInsightsContract.RequestIdProperty) shouldBe "outer-request"
      context.getLocalProperty(OpenIvmInsightsContract.RunIdProperty) shouldBe "outer-run"
      context.getLocalProperty(OpenIvmInsightsContract.DbtNodeIdProperty) shouldBe "outer-node"

      val page = OpenIvmInsightsBroker.page(spark, "bounded", 0L, 100)
      page.status.captureStatus shouldBe CaptureStatus.Degraded
      page.status.requestStatus shouldBe Some(RequestStatus.Succeeded)
      page.status.code shouldBe Some(FailureCode.EmitFailed)
      page.status.terminal shouldBe true

      OpenIvmInsightsBroker.release(spark, "bounded")
    }
  }

  private def delete(file: File): Unit = {
    if (file.isDirectory) Option(file.listFiles()).foreach(_.foreach(delete))
    file.delete()
    ()
  }
}
