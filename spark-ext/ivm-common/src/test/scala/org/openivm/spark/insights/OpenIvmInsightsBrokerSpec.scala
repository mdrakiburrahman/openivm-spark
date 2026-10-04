package org.openivm.spark.insights

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.SparkSession
import org.openivm.spark.insights.OpenIvmInsightsContract.{
  BranchCode,
  CaptureStatus,
  DetailField,
  EventDraft,
  EventType,
  ExecutionMode,
  FailureCode,
  FailureIdentity,
  Level,
  Materialization,
  ModelContext,
  MvQueryHashDecision,
  Operation
}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID
import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit}
import scala.collection.mutable

class OpenIvmInsightsBrokerSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  private var spark: SparkSession = _
  private val retained            = mutable.ArrayBuffer.empty[String]
  private val mapper              = new ObjectMapper()
  private val warehouse =
    new File(s"target/test-warehouse-insights-broker-${UUID.randomUUID().toString.take(8)}")

  override def beforeAll(): Unit = {
    super.beforeAll()
    warehouse.mkdirs()
    spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("OpenIvmInsightsBrokerSpec")
      .config("spark.sql.warehouse.dir", warehouse.getAbsolutePath)
      .config("spark.ui.enabled", "false")
      .config(OpenIvmInsightsContract.ConfigPrefix + "maxCaptures", "4")
      .config(OpenIvmInsightsContract.ConfigPrefix + "maxEventsPerCapture", "64")
      .config(OpenIvmInsightsContract.ConfigPrefix + "maxBytesPerCapture", "32768")
      .config(OpenIvmInsightsContract.ConfigPrefix + "maxTotalEvents", "256")
      .config(OpenIvmInsightsContract.ConfigPrefix + "maxTotalBytes", "131072")
      .config(OpenIvmInsightsContract.ConfigPrefix + "maxDetailsBytes", "1024")
      .config(OpenIvmInsightsContract.ConfigPrefix + "maxMessageBytes", "128")
      .config(OpenIvmInsightsContract.ConfigPrefix + "maxFieldBytes", "256")
      .getOrCreate()
  }

  override def afterEach(): Unit = {
    try {
      retained.reverseIterator.foreach { requestId =>
        val page = OpenIvmInsightsBroker.page(spark, requestId, 0L, 1000)
        if (page.status.captureStatus != CaptureStatus.Missing) {
          if (!page.status.terminal) OpenIvmInsightsBroker.end(spark, requestId, succeeded = false)
          OpenIvmInsightsBroker.release(spark, requestId)
        }
      }
      retained.clear()
      Seq(
        OpenIvmInsightsContract.RequestIdProperty,
        OpenIvmInsightsContract.RunIdProperty,
        OpenIvmInsightsContract.DbtNodeIdProperty
      ).foreach(spark.sparkContext.setLocalProperty(_, null))
    } finally super.afterEach()
  }

  override def afterAll(): Unit = {
    try {
      if (spark != null) spark.stop()
      delete(warehouse)
    } finally super.afterAll()
  }

  describe("bounded request-scoped insights") {
    it("sets and restores correlation properties and returns a status row model for missing requests") {
      val context = spark.sparkContext
      context.setLocalProperty(OpenIvmInsightsContract.RequestIdProperty, "outer-request")
      context.setLocalProperty(OpenIvmInsightsContract.RunIdProperty, "outer-run")
      context.setLocalProperty(OpenIvmInsightsContract.DbtNodeIdProperty, "outer-node")

      begin("properties")
      context.getLocalProperty(OpenIvmInsightsContract.RequestIdProperty) shouldBe "properties"
      context.getLocalProperty(OpenIvmInsightsContract.RunIdProperty) shouldBe "run-properties"
      context.getLocalProperty(OpenIvmInsightsContract.DbtNodeIdProperty) shouldBe "node-properties"

      OpenIvmInsightsBroker.end(spark, "properties", succeeded = true)
      context.getLocalProperty(OpenIvmInsightsContract.RequestIdProperty) shouldBe "outer-request"
      context.getLocalProperty(OpenIvmInsightsContract.RunIdProperty) shouldBe "outer-run"
      context.getLocalProperty(OpenIvmInsightsContract.DbtNodeIdProperty) shouldBe "outer-node"

      val missing = OpenIvmInsightsBroker.page(spark, "never-reserved", 7L, 10)
      missing.status.captureStatus shouldBe CaptureStatus.Missing
      missing.nextSequence shouldBe 7L
      missing.hasMore shouldBe false
      missing.events shouldBe empty
      OpenIvmInsightsBroker.release(spark, "never-reserved")
    }

    it("stores model context and emits it with bounded failed-request identity") {
      val requestId = "model-context"
      OpenIvmInsightsBroker.begin(
        spark,
        requestId,
        "run-model-context",
        "model.analytics.sales_summary",
        Some(Materialization.MaterializedView),
        Some("analytics.sales_summary")
      )
      retained += requestId

      OpenIvmInsightsBroker.currentModelContext(spark) shouldBe Some(
        ModelContext(
          ExecutionMode.MaterializedView,
          Materialization.MaterializedView,
          "analytics.sales_summary"
        )
      )
      val started = OpenIvmInsightsBroker
        .page(spark, requestId, 0L, 100)
        .events
        .find(_.eventType == EventType.RequestStarted)
        .getOrElse(fail("Missing request-start event"))
      val startedDetails = mapper.readTree(started.detailsJson.get)
      startedDetails.path(DetailField.ExecutionMode).asText() shouldBe ExecutionMode.MaterializedView
      startedDetails.path(DetailField.Materialization).asText() shouldBe Materialization.MaterializedView
      startedDetails.path(DetailField.TargetRelation).asText() shouldBe "analytics.sales_summary"

      OpenIvmInsightsBroker.end(
        spark,
        requestId,
        succeeded = false,
        Some(FailureIdentity("org.apache.spark.SparkException", "SPARK_JOB_CANCELLED"))
      )
      val failed = OpenIvmInsightsBroker
        .page(spark, requestId, 0L, 100)
        .events
        .find(_.eventType == EventType.RequestFailed)
        .getOrElse(fail("Missing request-failed event"))
      val failedDetails = mapper.readTree(failed.detailsJson.get)
      failedDetails.path(DetailField.ExecutionMode).asText() shouldBe ExecutionMode.MaterializedView
      failedDetails.path(DetailField.Materialization).asText() shouldBe Materialization.MaterializedView
      failedDetails.path(DetailField.TargetRelation).asText() shouldBe "analytics.sales_summary"
      failedDetails.path(DetailField.ErrorClass).asText() shouldBe "org.apache.spark.SparkException"
      failedDetails.path(DetailField.ErrorCode).asText() shouldBe "SPARK_JOB_CANCELLED"
    }

    it("does not restore an ended request over a later active request on retry or conflict") {
      val context = spark.sparkContext
      context.setLocalProperty(OpenIvmInsightsContract.RequestIdProperty, "outer-request")
      context.setLocalProperty(OpenIvmInsightsContract.RunIdProperty, "outer-run")
      context.setLocalProperty(OpenIvmInsightsContract.DbtNodeIdProperty, "outer-node")

      begin("request-a")
      OpenIvmInsightsBroker.end(spark, "request-a", succeeded = true)
      context.getLocalProperty(OpenIvmInsightsContract.RequestIdProperty) shouldBe "outer-request"

      begin("request-b")
      context.getLocalProperty(OpenIvmInsightsContract.RequestIdProperty) shouldBe "request-b"
      OpenIvmInsightsBroker.end(spark, "request-a", succeeded = true)
      context.getLocalProperty(OpenIvmInsightsContract.RequestIdProperty) shouldBe "request-b"
      context.getLocalProperty(OpenIvmInsightsContract.RunIdProperty) shouldBe "run-request-b"
      context.getLocalProperty(OpenIvmInsightsContract.DbtNodeIdProperty) shouldBe "node-request-b"

      intercept[IllegalArgumentException] {
        OpenIvmInsightsBroker.end(spark, "request-a", succeeded = false)
      }.getMessage should include(FailureCode.EndStatusConflict)
      context.getLocalProperty(OpenIvmInsightsContract.RequestIdProperty) shouldBe "request-b"
      context.getLocalProperty(OpenIvmInsightsContract.RunIdProperty) shouldBe "run-request-b"
      context.getLocalProperty(OpenIvmInsightsContract.DbtNodeIdProperty) shouldBe "node-request-b"

      OpenIvmInsightsBroker.end(spark, "request-b", succeeded = true)
      context.getLocalProperty(OpenIvmInsightsContract.RequestIdProperty) shouldBe "outer-request"
      context.getLocalProperty(OpenIvmInsightsContract.RunIdProperty) shouldBe "outer-run"
      context.getLocalProperty(OpenIvmInsightsContract.DbtNodeIdProperty) shouldBe "outer-node"
    }

    it("does not restore an out-of-order END over another active request") {
      val context = spark.sparkContext
      begin("out-of-order-a")
      begin("out-of-order-b")
      OpenIvmInsightsBroker.activeMetricRequestCount(spark) shouldBe 2

      OpenIvmInsightsBroker.end(spark, "out-of-order-a", succeeded = true)
      OpenIvmInsightsBroker.activeMetricRequestCount(spark) shouldBe 1
      context.getLocalProperty(OpenIvmInsightsContract.RequestIdProperty) shouldBe "out-of-order-b"
      context.getLocalProperty(OpenIvmInsightsContract.RunIdProperty) shouldBe "run-out-of-order-b"
      context.getLocalProperty(OpenIvmInsightsContract.DbtNodeIdProperty) shouldBe "node-out-of-order-b"

      OpenIvmInsightsBroker.end(spark, "out-of-order-b", succeeded = true)
      OpenIvmInsightsBroker.activeMetricRequestCount(spark) shouldBe 0
    }

    it("pages events by a stable exclusive cursor with a monotonic request sequence") {
      begin("cursor")
      emit("first")
      emit("second")

      val firstPage = OpenIvmInsightsBroker.page(spark, "cursor", 0L, 2)
      firstPage.events.map(_.sequence) shouldBe Vector(1L, 2L)
      firstPage.events.map(_.code) shouldBe Vector(
        OpenIvmInsightsContract.Code.RequestStarted,
        "first"
      )
      firstPage.nextSequence shouldBe 2L
      firstPage.hasMore shouldBe true

      val secondPage = OpenIvmInsightsBroker.page(spark, "cursor", firstPage.nextSequence, 2)
      secondPage.events.map(_.sequence) shouldBe Vector(3L)
      secondPage.events.map(_.code) shouldBe Vector("second")
      secondPage.nextSequence shouldBe 3L
      secondPage.hasMore shouldBe false
    }

    it("serializes concurrent emitters without collisions or cross-request eviction") {
      begin("concurrent")
      val executor = Executors.newFixedThreadPool(4)
      try {
        val tasks = (0 until 4).map { worker =>
          executor.submit(new Callable[Unit] {
            override def call(): Unit = {
              val context  = spark.sparkContext
              val previous = context.getLocalProperty(OpenIvmInsightsContract.RequestIdProperty)
              context.setLocalProperty(OpenIvmInsightsContract.RequestIdProperty, "concurrent")
              try {
                (0 until 5).foreach(index => emit(s"worker_${worker}_$index"))
              } finally context.setLocalProperty(OpenIvmInsightsContract.RequestIdProperty, previous)
            }
          })
        }
        tasks.foreach(_.get(20, TimeUnit.SECONDS))
      } finally executor.shutdownNow()

      val page = OpenIvmInsightsBroker.page(spark, "concurrent", 0L, 100)
      page.events.map(_.sequence) shouldBe (1L to 21L).toVector
      page.events.map(_.code).toSet should contain allElementsOf
        (0 until 4).flatMap(worker => (0 until 5).map(index => s"worker_${worker}_$index"))
      page.status.captureStatus shouldBe CaptureStatus.Running
    }

    it("isolates concurrent request contexts, events, and terminal branch ownership") {
      val requestIds = Vector("isolated-request-a", "isolated-request-b")
      retained ++= requestIds
      val ready    = new CountDownLatch(requestIds.size)
      val start    = new CountDownLatch(1)
      val executor = Executors.newFixedThreadPool(requestIds.size)
      try {
        val tasks = requestIds.zipWithIndex.map { case (requestId, index) =>
          executor.submit(new Callable[Vector[String]] {
            override def call(): Vector[String] = {
              val context = spark.sparkContext
              OpenIvmInsightsBroker.begin(
                spark,
                requestId,
                s"run-isolated-$index",
                s"model.analytics.isolated_$index",
                Some("table"),
                Some(s"default.isolated_$index")
              )
              ready.countDown()
              if (!start.await(20, TimeUnit.SECONDS))
                throw new IllegalStateException("Timed out starting concurrent insight requests")
              OpenIvmInsightsBroker.emit(spark) {
                event(s"${requestId}_only").copy(
                  operation = Operation.RegularSpark,
                  materializedView = None
                )
              }
              OpenIvmInsightsBroker.end(spark, requestId, succeeded = true)
              Vector(
                context.getLocalProperty(OpenIvmInsightsContract.RequestIdProperty),
                context.getLocalProperty(OpenIvmInsightsContract.RunIdProperty),
                context.getLocalProperty(OpenIvmInsightsContract.DbtNodeIdProperty)
              )
            }
          })
        }
        ready.await(20, TimeUnit.SECONDS) shouldBe true
        start.countDown()
        tasks.foreach(_.get(20, TimeUnit.SECONDS) shouldBe Vector(null, null, null))
      } finally executor.shutdownNow()

      requestIds.zipWithIndex.foreach { case (requestId, index) =>
        val page  = OpenIvmInsightsBroker.page(spark, requestId, 0L, 100)
        val codes = page.events.map(_.code)
        page.status.captureStatus shouldBe CaptureStatus.Complete
        codes should contain(s"${requestId}_only")
        codes should not contain s"${requestIds(1 - index)}_only"
        codes should contain(OpenIvmInsightsContract.Code.RegularSparkCompleted)
        val started = page.events
          .find(_.eventType == EventType.RequestStarted)
          .getOrElse(fail(s"Missing request start for $requestId"))
        val startedDetails = mapper.readTree(started.detailsJson.get)
        startedDetails.path("request_id").asText() shouldBe requestId
        startedDetails.path("run_id").asText() shouldBe s"run-isolated-$index"
        startedDetails.path("dbt_node_id").asText() shouldBe s"model.analytics.isolated_$index"
      }
      OpenIvmInsightsBroker.activeMetricRequestCount(spark) shouldBe 0
    }

    it("rejects duplicate or exhausted reservations without evicting retained captures") {
      (1 to 4).foreach(index => begin(s"slot-$index"))
      intercept[IllegalArgumentException] {
        OpenIvmInsightsBroker.begin(spark, "slot-1", "duplicate-run", "duplicate-node")
      }.getMessage should include(FailureCode.CaptureExists)
      intercept[IllegalArgumentException] {
        OpenIvmInsightsBroker.begin(spark, "overflow", "overflow-run", "overflow-node")
      }.getMessage should include(FailureCode.CaptureLimitExceeded)
      (1 to 4).foreach { index =>
        OpenIvmInsightsBroker.page(spark, s"slot-$index", 0L, 10).status.captureStatus shouldBe
          CaptureStatus.Running
      }
    }

    it("requires a terminal capture for release and keeps missing release idempotent") {
      begin("release")
      intercept[IllegalArgumentException] {
        OpenIvmInsightsBroker.release(spark, "release")
      }.getMessage should include(FailureCode.CaptureNotTerminal)
      OpenIvmInsightsBroker.end(spark, "release", succeeded = true)
      OpenIvmInsightsBroker.release(spark, "release")
      OpenIvmInsightsBroker.release(spark, "release")
      OpenIvmInsightsBroker.page(spark, "release", 0L, 10).status.captureStatus shouldBe
        CaptureStatus.Missing
    }

    it("degrades rather than throwing when event-count or UTF-8 byte bounds overflow") {
      begin("event-overflow")
      noException should be thrownBy {
        (1 to 70).foreach(index => emit(s"event_$index"))
      }
      val eventOverflow = OpenIvmInsightsBroker.page(spark, "event-overflow", 0L, 100)
      eventOverflow.status.captureStatus shouldBe CaptureStatus.Degraded
      eventOverflow.status.code shouldBe Some(FailureCode.EventLimitExceeded)
      OpenIvmInsightsBroker.end(spark, "event-overflow", succeeded = true)

      begin("byte-overflow")
      noException should be thrownBy {
        (1 to 60).foreach { index =>
          OpenIvmInsightsBroker.emit(spark) {
            event(
              s"bytes_$index",
              details = Some(OpenIvmInsightJson.obj("payload" -> ("é" * 350)))
            )
          }
        }
      }
      val byteOverflow = OpenIvmInsightsBroker.page(spark, "byte-overflow", 0L, 100)
      byteOverflow.status.captureStatus shouldBe CaptureStatus.Degraded
      byteOverflow.status.code shouldBe Some(FailureCode.ByteLimitExceeded)
    }

    it("degrades malformed or raw-SQL-shaped details without leaking their value") {
      begin("bad-details")
      val secret = "SELECT secret_insight_value FROM private_table"
      noException should be thrownBy {
        OpenIvmInsightsBroker.emit(spark) {
          event("bad_details", details = Some(OpenIvmInsightJson.obj("sql_text" -> secret)))
        }
      }
      val page = OpenIvmInsightsBroker.page(spark, "bad-details", 0L, 100)
      page.status.captureStatus shouldBe CaptureStatus.Degraded
      page.status.code shouldBe Some(FailureCode.EmitFailed)
      page.events.flatMap(_.detailsJson).mkString should not include secret
    }

    it("requires an active matching materialized-view context for typed annotations") {
      val annotation = MvQueryHashDecision(
        "analytics.annotation_mv",
        "0123456789abcdef0123456789abcdef",
        "fedcba9876543210fedcba9876543210",
        MvQueryHashDecision.RebuildPolicy,
        MvQueryHashDecision.RebuildDecision,
        "query_hash_changed"
      )
      intercept[IllegalArgumentException] {
        OpenIvmInsightsBroker.annotateMvQueryHashDecision(spark, annotation)
      }.getMessage should include(FailureCode.ActiveCaptureRequired)

      val requestId = "annotation-context"
      OpenIvmInsightsBroker.begin(
        spark,
        requestId,
        "run-annotation-context",
        "model.analytics.annotation",
        Some("table"),
        Some("analytics.annotation_mv")
      )
      retained += requestId
      intercept[IllegalArgumentException] {
        OpenIvmInsightsBroker.annotateMvQueryHashDecision(spark, annotation)
      }.getMessage should include(FailureCode.AnnotationContextMismatch)

      OpenIvmInsightsBroker
        .page(spark, requestId, 0L, 100)
        .events
        .map(_.code) should not contain OpenIvmInsightsContract.Code.MvQueryHashDecisionAnnotated
    }

    it("accepts new operation values and rejects raw-text typed annotation fields") {
      val requestId = "extended-operations"
      OpenIvmInsightsBroker.begin(
        spark,
        requestId,
        "run-extended-operations",
        "model.analytics.extended",
        Some(Materialization.MaterializedView),
        Some("analytics.extended_mv")
      )
      retained += requestId

      Seq(Operation.Streaming, Operation.RegularSpark).foreach { operation =>
        OpenIvmInsightsBroker.emit(spark) {
          event(s"${operation}_event").copy(operation = operation)
        }
      }
      val secret = "SELECT secret_value FROM private_table"
      intercept[IllegalArgumentException] {
        OpenIvmInsightsBroker.annotateMvQueryHashDecision(
          spark,
          MvQueryHashDecision(
            "analytics.extended_mv",
            "0123456789abcdef0123456789abcdef",
            "fedcba9876543210fedcba9876543210",
            MvQueryHashDecision.RebuildPolicy,
            MvQueryHashDecision.RebuildDecision,
            secret
          )
        )
      }.getMessage should include(FailureCode.ConfigInvalid)
      intercept[IllegalArgumentException] {
        OpenIvmInsightsBroker.end(
          spark,
          requestId,
          succeeded = false,
          Some(FailureIdentity("org.example.Failure", secret))
        )
      }.getMessage should include(FailureCode.ConfigInvalid)

      val page = OpenIvmInsightsBroker.page(spark, requestId, 0L, 100)
      page.status.captureStatus shouldBe CaptureStatus.Running
      page.events.map(_.operation) should contain allOf (Operation.Streaming, Operation.RegularSpark)
      page.events.flatMap(_.detailsJson).mkString should not include secret
    }

    it("emits R1 with correlated Spark metrics for a successful table materialization") {
      val requestId = "regular-spark-success"
      val tableName = "regular_spark_success_table"
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
      OpenIvmInsightsBroker.begin(
        spark,
        requestId,
        "run-regular-spark-success",
        "model.analytics.regular_success",
        Some("table"),
        Some(s"default.$tableName")
      )
      retained += requestId

      spark
        .sql(s"CREATE TABLE $tableName USING parquet AS SELECT id FROM range(0, 128, 1, 4)")
        .collect()
      OpenIvmInsightsBroker.end(spark, requestId, succeeded = true)

      val page = OpenIvmInsightsBroker.page(spark, requestId, 0L, 100)
      val event = page.events
        .find(_.code == OpenIvmInsightsContract.Code.RegularSparkCompleted)
        .getOrElse(fail("Missing R1 regular Spark event"))
      event.operation shouldBe Operation.RegularSpark
      event.materializedView shouldBe None
      event.durationMs.get should be >= 0L
      val details = mapper.readTree(event.detailsJson.get)
      details.path(DetailField.BranchCode).asText() shouldBe OpenIvmInsightsContract.BranchCode.R1
      details.path(DetailField.ExecutionMode).asText() shouldBe ExecutionMode.RegularSpark
      details.path(DetailField.Materialization).asText() shouldBe "table"
      details.path(DetailField.TargetRelation).asText() shouldBe s"default.$tableName"
      details.path(DetailField.Metrics).path(OpenIvmInsightsContract.MetricField.JobCount).asLong() should be > 0L
      details.path(DetailField.Metrics).path(OpenIvmInsightsContract.MetricField.StageCount).asLong() should be > 0L
      details.path(DetailField.Metrics).path(OpenIvmInsightsContract.MetricField.TaskCount).asLong() should be > 0L
      OpenIvmInsightsBroker.activeMetricRequestCount(spark) shouldBe 0

      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }

    it("emits R3 with safe failure identity and no exception-message leakage") {
      val requestId = "regular-spark-failure"
      val secret    = "secret-r3-message"
      OpenIvmInsightsBroker.begin(
        spark,
        requestId,
        "run-regular-spark-failure",
        "model.analytics.regular_failure",
        Some("table"),
        Some("default.regular_spark_failure")
      )
      retained += requestId

      an[Exception] should be thrownBy {
        spark.sql(s"SELECT id, raise_error('$secret') FROM range(0, 4, 1, 2)").collect()
      }
      OpenIvmInsightsBroker.end(
        spark,
        requestId,
        succeeded = false,
        Some(FailureIdentity("org.apache.spark.SparkException", "REGULAR_MODEL_FAILED"))
      )

      val event = OpenIvmInsightsBroker
        .page(spark, requestId, 0L, 100)
        .events
        .find(_.code == OpenIvmInsightsContract.Code.RegularSparkFailed)
        .getOrElse(fail("Missing R3 regular Spark failure event"))
      event.operation shouldBe Operation.RegularSpark
      event.status shouldBe Some(OpenIvmInsightsContract.RequestStatus.Failed)
      val details = mapper.readTree(event.detailsJson.get)
      details.path(DetailField.BranchCode).asText() shouldBe OpenIvmInsightsContract.BranchCode.R3
      details.path(DetailField.ExecutionMode).asText() shouldBe ExecutionMode.RegularSpark
      details.path(DetailField.Materialization).asText() shouldBe "table"
      details.path(DetailField.TargetRelation).asText() shouldBe "default.regular_spark_failure"
      details.path(DetailField.ErrorClass).asText() shouldBe "org.apache.spark.SparkException"
      details.path(DetailField.ErrorCode).asText() shouldBe "REGULAR_MODEL_FAILED"
      details.path(DetailField.Metrics).path(OpenIvmInsightsContract.MetricField.JobCount).asLong() should be > 0L
      val requestFailed = OpenIvmInsightsBroker
        .page(spark, requestId, 0L, 100)
        .events
        .find(_.eventType == EventType.RequestFailed)
        .getOrElse(fail("Missing request failure event"))
      val requestDetails = mapper.readTree(requestFailed.detailsJson.get)
      requestDetails.path(DetailField.ErrorClass).asText() shouldBe "org.apache.spark.SparkException"
      requestDetails.path(DetailField.ErrorCode).asText() shouldBe "REGULAR_MODEL_FAILED"
      event.detailsJson.get should not include secret
      pageText(requestId) should not include secret
      OpenIvmInsightsBroker.activeMetricRequestCount(spark) shouldBe 0
    }

    it("does not classify a request owned by a streaming or MV operation as regular Spark") {
      Seq(Operation.Streaming, Operation.Create).zipWithIndex.foreach { case (operation, index) =>
        val requestId = s"managed-owner-$index"
        begin(requestId)
        OpenIvmInsightsBroker.hasActiveRegularSparkCapture(spark) shouldBe true
        spark.range(0L, 32L, 1L, 2).count() shouldBe 32L
        OpenIvmInsightsBroker.emit(spark) {
          event(s"managed_owner_$index").copy(operation = operation)
        }
        OpenIvmInsightsBroker.hasActiveRegularSparkCapture(spark) shouldBe false
        OpenIvmInsightsBroker.end(spark, requestId, succeeded = true)

        val events = OpenIvmInsightsBroker.page(spark, requestId, 0L, 100).events
        events.map(_.code) should contain noneOf (
          OpenIvmInsightsContract.Code.RegularSparkCompleted,
          OpenIvmInsightsContract.Code.RegularSparkFailed
        )
        val requestCompleted = events
          .find(_.eventType == EventType.RequestCompleted)
          .getOrElse(fail(s"Missing request completion for $requestId"))
        mapper
          .readTree(requestCompleted.detailsJson.get)
          .path(DetailField.Metrics)
          .isMissingNode shouldBe true
        OpenIvmInsightsBroker.activeMetricRequestCount(spark) shouldBe 0
      }
    }

    it("normalizes typed query-hash decisions to M7/M8 and rejects malformed hashes or pairs") {
      val requestId = "typed-query-hash"
      OpenIvmInsightsBroker.begin(
        spark,
        requestId,
        "run-typed-query-hash",
        "model.analytics.typed_query_hash",
        Some(Materialization.MaterializedView),
        Some("analytics.typed_query_hash_mv")
      )
      retained += requestId

      OpenIvmInsightsBroker.annotateMvQueryHashDecision(
        spark,
        MvQueryHashDecision(
          "analytics.typed_query_hash_mv",
          "0123456789abcdef0123456789abcdef",
          "fedcba9876543210fedcba9876543210",
          MvQueryHashDecision.RebuildPolicy,
          MvQueryHashDecision.RebuildDecision,
          "query_hash_changed"
        )
      )
      val rebuild = OpenIvmInsightsBroker
        .page(spark, requestId, 0L, 100)
        .events
        .find(_.code == OpenIvmInsightsContract.Code.MvQueryHashDecisionAnnotated)
        .getOrElse(fail("Missing rebuild annotation"))
      mapper.readTree(rebuild.detailsJson.get).path(DetailField.BranchCode).asText() shouldBe BranchCode.M7

      Seq(
        MvQueryHashDecision(
          "analytics.typed_query_hash_mv",
          "not-a-hash",
          "fedcba9876543210fedcba9876543210",
          MvQueryHashDecision.RebuildPolicy,
          MvQueryHashDecision.RebuildDecision,
          "query_hash_changed"
        ),
        MvQueryHashDecision(
          "analytics.typed_query_hash_mv",
          "0123456789abcdef0123456789abcdef",
          "fedcba9876543210fedcba9876543210",
          MvQueryHashDecision.FailPolicy,
          MvQueryHashDecision.RebuildDecision,
          "query_hash_changed"
        )
      ).foreach { invalid =>
        intercept[IllegalArgumentException] {
          OpenIvmInsightsBroker.annotateMvQueryHashDecision(spark, invalid)
        }.getMessage should include(FailureCode.ConfigInvalid)
      }

      OpenIvmInsightsBroker.annotateMvQueryHashDecision(
        spark,
        MvQueryHashDecision(
          "analytics.typed_query_hash_mv",
          MvQueryHashDecision.MissingHash,
          "abcdef0123456789abcdef0123456789",
          MvQueryHashDecision.FailPolicy,
          MvQueryHashDecision.RejectedDecision,
          "query_hash_missing"
        )
      )
      val rejected = OpenIvmInsightsBroker
        .page(spark, requestId, rebuild.sequence, 100)
        .events
        .find(_.code == OpenIvmInsightsContract.Code.MvQueryHashDecisionAnnotated)
        .getOrElse(fail("Missing rejected annotation"))
      mapper.readTree(rejected.detailsJson.get).path(DetailField.BranchCode).asText() shouldBe BranchCode.M8
      OpenIvmInsightsBroker.currentMvQueryHashDecision(spark).map(_.decision) shouldBe
        Some(MvQueryHashDecision.RejectedDecision)
    }
  }

  private def begin(requestId: String): Unit = {
    OpenIvmInsightsBroker.begin(spark, requestId, s"run-$requestId", s"node-$requestId")
    retained += requestId
  }

  private def emit(code: String): Unit = {
    OpenIvmInsightsBroker.emit(spark) {
      event(code)
    }
    ()
  }

  private def event(code: String, details: Option[String] = None): EventDraft =
    EventDraft(
      operation = Operation.Refresh,
      stage = "test",
      eventType = EventType.ActionCompleted,
      level = Level.Info,
      code = code,
      message = s"event $code",
      operationId = Some("test-operation"),
      materializedView = Some("default.test_mv"),
      status = Some("completed"),
      detailsJson = details,
      terminal = Some(false)
    )

  private def pageText(requestId: String): String =
    OpenIvmInsightsBroker
      .page(spark, requestId, 0L, 1000)
      .events
      .flatMap(event => Seq(Some(event.message), event.detailsJson).flatten)
      .mkString("\n")

  private def delete(file: File): Unit = {
    if (file.isDirectory) Option(file.listFiles()).foreach(_.foreach(delete))
    file.delete()
    ()
  }
}
