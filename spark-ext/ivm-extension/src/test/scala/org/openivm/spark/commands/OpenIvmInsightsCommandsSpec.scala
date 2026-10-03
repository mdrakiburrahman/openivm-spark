package org.openivm.spark.commands

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.SparkSession
import org.openivm.spark.insights.{OpenIvmInsightsBroker, OpenIvmInsightsContract}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

class OpenIvmInsightsCommandsSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private val mapper              = new ObjectMapper()
  private val warehouse =
    new File(s"target/test-warehouse-insights-command-${UUID.randomUUID().toString.take(8)}")

  override def beforeAll(): Unit = {
    super.beforeAll()
    warehouse.mkdirs()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("OpenIvmInsightsCommandsSpec")
      .config("spark.sql.extensions", "org.openivm.spark.OpenIvmSparkExtensions")
      .config("spark.openivm.enabled", "true")
      .config("spark.sql.warehouse.dir", warehouse.getAbsolutePath)
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try {
      if (spark != null) spark.stop()
      delete(warehouse)
    } finally super.afterAll()
  }

  describe("OPENIVM INSIGHTS SQL commands") {
    it("returns status first and SHOW submits no Spark job") {
      val requestId = s"sql-show-${UUID.randomUUID()}"
      spark
        .sql(
          s"OPENIVM INSIGHTS BEGIN REQUEST '$requestId' RUN 'run-show' NODE 'model.openivm.show'"
        )
        .collect()
      try {
        val jobGroup = s"insights-show-${UUID.randomUUID()}"
        spark.sparkContext.setJobGroup(jobGroup, "SHOW OPENIVM INSIGHTS driver-only guard")
        val rows =
          try {
            spark
              .sql(s"SHOW OPENIVM INSIGHTS FOR REQUEST '$requestId' AFTER 0 LIMIT 100")
              .collect()
              .toSeq
          } finally spark.sparkContext.clearJobGroup()

        rows should not be empty
        rows.head.getAs[String]("record_type") shouldBe OpenIvmInsightsContract.RecordType.Status
        rows.head.getAs[String]("capture_status") shouldBe OpenIvmInsightsContract.CaptureStatus.Running
        rows.tail.map(_.getAs[String]("event_type")) should contain(
          OpenIvmInsightsContract.EventType.RequestStarted
        )
        spark.sparkContext.statusTracker.getJobIdsForGroup(jobGroup) shouldBe empty

        spark
          .sql(s"OPENIVM INSIGHTS END REQUEST '$requestId' STATUS SUCCEEDED")
          .collect()
        val complete = spark
          .sql(s"SHOW OPENIVM INSIGHTS FOR REQUEST '$requestId' AFTER 0 LIMIT 100")
          .collect()
        complete.head.getAs[String]("capture_status") shouldBe OpenIvmInsightsContract.CaptureStatus.Complete
        complete.head.getAs[Boolean]("terminal") shouldBe true
      } finally {
        val state = OpenIvmInsightsBroker.page(spark, requestId, 0L, 100)
        if (!state.status.terminal)
          OpenIvmInsightsBroker.end(spark, requestId, succeeded = false)
        spark.sql(s"OPENIVM INSIGHTS RELEASE REQUEST '$requestId'").collect()
      }
    }

    it("returns one missing status row instead of throwing") {
      val rows = spark
        .sql("SHOW OPENIVM INSIGHTS FOR REQUEST 'missing-sql-request' AFTER 12 LIMIT 10")
        .collect()
      rows.length shouldBe 1
      rows.head.getAs[String]("record_type") shouldBe OpenIvmInsightsContract.RecordType.Status
      rows.head.getAs[String]("capture_status") shouldBe OpenIvmInsightsContract.CaptureStatus.Missing
      rows.head.getAs[Long]("next_sequence") shouldBe 12L
      rows.head.getAs[Boolean]("has_more") shouldBe false
    }

    it("records model context, a typed MV query-hash decision, and safe failure identity") {
      val requestId = s"sql-context-${UUID.randomUUID()}"
      spark
        .sql(
          s"""OPENIVM INSIGHTS BEGIN REQUEST '$requestId'
             |RUN 'run-context'
             |NODE 'model.analytics.sales_summary'
             |MATERIALIZATION 'materialized_view'
             |TARGET 'analytics.sales_summary'""".stripMargin
        )
        .collect()
      try {
        spark
          .sql(
            """OPENIVM INSIGHTS ANNOTATE MATERIALIZED VIEW QUERY HASH
              |TARGET 'analytics.sales_summary'
              |OLD '0123456789abcdef0123456789abcdef'
              |NEW 'fedcba9876543210fedcba9876543210'
              |POLICY 'rebuild'
              |DECISION 'rebuild'
              |REASON 'query_hash_changed'""".stripMargin
          )
          .collect()
        spark
          .sql(
            s"""OPENIVM INSIGHTS END REQUEST '$requestId' STATUS FAILED
               |ERROR_CLASS 'org.apache.spark.SparkException'
               |ERROR_CODE 'SPARK_JOB_CANCELLED'""".stripMargin
          )
          .collect()

        val events = OpenIvmInsightsBroker.page(spark, requestId, 0L, 100).events
        val started = events
          .find(_.eventType == OpenIvmInsightsContract.EventType.RequestStarted)
          .getOrElse(fail("Missing request-start event"))
        val annotation = events
          .find(_.code == OpenIvmInsightsContract.Code.MvQueryHashDecisionAnnotated)
          .getOrElse(fail("Missing query-hash annotation event"))
        val failed = events
          .find(_.eventType == OpenIvmInsightsContract.EventType.RequestFailed)
          .getOrElse(fail("Missing request-failed event"))

        val startedDetails = mapper.readTree(started.detailsJson.get)
        startedDetails.path(OpenIvmInsightsContract.DetailField.ExecutionMode).asText() shouldBe
          OpenIvmInsightsContract.ExecutionMode.MaterializedView
        startedDetails.path(OpenIvmInsightsContract.DetailField.Materialization).asText() shouldBe
          OpenIvmInsightsContract.Materialization.MaterializedView
        startedDetails.path(OpenIvmInsightsContract.DetailField.TargetRelation).asText() shouldBe
          "analytics.sales_summary"

        annotation.operation shouldBe OpenIvmInsightsContract.Operation.Create
        annotation.materializedView shouldBe Some("analytics.sales_summary")
        annotation.status shouldBe Some("rebuild")
        val annotationDetails = mapper.readTree(annotation.detailsJson.get)
        annotationDetails.path(OpenIvmInsightsContract.DetailField.OldQueryHash).asText() shouldBe
          "0123456789abcdef0123456789abcdef"
        annotationDetails.path(OpenIvmInsightsContract.DetailField.NewQueryHash).asText() shouldBe
          "fedcba9876543210fedcba9876543210"
        annotationDetails.path(OpenIvmInsightsContract.DetailField.Policy).asText() shouldBe "rebuild"
        annotationDetails.path(OpenIvmInsightsContract.DetailField.Decision).asText() shouldBe "rebuild"
        annotationDetails.path(OpenIvmInsightsContract.DetailField.Reason).asText() shouldBe
          "query_hash_changed"
        annotationDetails.path(OpenIvmInsightsContract.DetailField.BranchCode).asText() shouldBe
          OpenIvmInsightsContract.BranchCode.M7

        val failedDetails = mapper.readTree(failed.detailsJson.get)
        failedDetails.path(OpenIvmInsightsContract.DetailField.ErrorClass).asText() shouldBe
          "org.apache.spark.SparkException"
        failedDetails.path(OpenIvmInsightsContract.DetailField.ErrorCode).asText() shouldBe
          "SPARK_JOB_CANCELLED"
      } finally {
        val page = OpenIvmInsightsBroker.page(spark, requestId, 0L, 100)
        if (!page.status.terminal) OpenIvmInsightsBroker.end(spark, requestId, succeeded = false)
        OpenIvmInsightsBroker.release(spark, requestId)
      }
    }

    it("rejects unsafe query-hash annotation values before emitting an event") {
      val requestId = s"unsafe-annotation-${UUID.randomUUID()}"
      spark
        .sql(
          s"""OPENIVM INSIGHTS BEGIN REQUEST '$requestId'
             |RUN 'run-unsafe-annotation'
             |NODE 'model.analytics.unsafe_annotation'
             |MATERIALIZATION 'materialized_view'
             |TARGET 'analytics.unsafe_annotation_mv'""".stripMargin
        )
        .collect()
      try {
        val error = intercept[IllegalArgumentException] {
          spark
            .sql(
              """OPENIVM INSIGHTS ANNOTATE MATERIALIZED VIEW QUERY HASH
                |TARGET 'analytics.unsafe_annotation_mv'
                |OLD '0123456789abcdef0123456789abcdef'
                |NEW 'SELECT_secret_value'
                |POLICY 'fail'
                |DECISION 'rejected'
                |REASON 'query_hash_changed'""".stripMargin
            )
            .collect()
        }
        error.getMessage should include(OpenIvmInsightsContract.FailureCode.ConfigInvalid)
        OpenIvmInsightsBroker
          .page(spark, requestId, 0L, 100)
          .events
          .map(_.code) should not contain OpenIvmInsightsContract.Code.MvQueryHashDecisionAnnotated
      } finally {
        val page = OpenIvmInsightsBroker.page(spark, requestId, 0L, 100)
        if (!page.status.terminal) OpenIvmInsightsBroker.end(spark, requestId, succeeded = false)
        OpenIvmInsightsBroker.release(spark, requestId)
      }
    }

    it("emits stable M4 details for a compiler-native full refresh without regular-Spark ownership") {
      val requestId = s"native-full-${UUID.randomUUID()}"
      val target    = "analytics.native_full_mv"
      OpenIvmInsightsBroker.begin(
        spark,
        requestId,
        "run-native-full",
        "model.analytics.native_full",
        Some(OpenIvmInsightsContract.Materialization.MaterializedView),
        Some(target)
      )
      try {
        OpenIvmMaterializedViewInsightEvents.createCompleted(
          spark,
          operationId = "native-full-operation",
          materializedView = target,
          classification = OpenIvmMaterializedViewInsightEvents.Classification(
            compileRefreshType = "FULL_REFRESH",
            effectiveRefreshType = "FULL_REFRESH",
            reason = "compiler_selected_full_refresh"
          )
        )
        OpenIvmInsightsBroker.end(spark, requestId, succeeded = true)

        val events = OpenIvmInsightsBroker.page(spark, requestId, 0L, 100).events
        val branch = events
          .find(_.code == OpenIvmInsightsContract.BranchCode.M4)
          .getOrElse(fail("Missing M4 materialized-view branch"))
        branch.operation shouldBe OpenIvmInsightsContract.Operation.Create
        branch.status shouldBe Some(OpenIvmInsightsContract.BranchCode.Labels(OpenIvmInsightsContract.BranchCode.M4))
        val details = mapper.readTree(branch.detailsJson.get)
        details.path(OpenIvmInsightsContract.DetailField.BranchCode).asText() shouldBe
          OpenIvmInsightsContract.BranchCode.M4
        details.path("branch_label").asText() shouldBe "NATIVE_FULL_REFRESH"
        details.path(OpenIvmInsightsContract.DetailField.ExecutionMode).asText() shouldBe
          OpenIvmInsightsContract.ExecutionMode.MaterializedView
        details.path(OpenIvmInsightsContract.DetailField.Materialization).asText() shouldBe
          OpenIvmInsightsContract.Materialization.MaterializedView
        details.path(OpenIvmInsightsContract.DetailField.TargetRelation).asText() shouldBe target
        details.path(OpenIvmInsightsContract.DetailField.Reason).asText() shouldBe
          "compiler_selected_full_refresh"
        details.path("compile_refresh_type").asText() shouldBe "FULL_REFRESH"
        details.path("effective_refresh_type").asText() shouldBe "FULL_REFRESH"
        events.map(_.code) should contain noneOf (
          OpenIvmInsightsContract.Code.RegularSparkCompleted,
          OpenIvmInsightsContract.Code.RegularSparkFailed
        )
      } finally {
        val page = OpenIvmInsightsBroker.page(spark, requestId, 0L, 100)
        if (!page.status.terminal) OpenIvmInsightsBroker.end(spark, requestId, succeeded = false)
        OpenIvmInsightsBroker.release(spark, requestId)
      }
    }

    it("emits failed action state without failing the query-log collector") {
      val requestId = s"failed-action-${UUID.randomUUID()}"
      OpenIvmInsightsBroker.begin(spark, requestId, "run-failed-action", "model.failed.action")
      try {
        val log = RefreshSqlLog.start(
          spark,
          "failed_action_refresh",
          "default.failed_action_mv",
          RefreshSqlLog.ModeRefresh
        )
        noException should be thrownBy {
          log.record(
            category = "rewritten_stmt",
            stmtOrder = 0,
            attemptIdx = 0,
            stmtKind = "merge",
            sql = "SELECT raw_sql_must_not_escape",
            durationMs = 3L,
            succeeded = false
          )
        }
        log.finish("refresh_failed")
        OpenIvmInsightsBroker.end(spark, requestId, succeeded = false)

        val failedAction = OpenIvmInsightsBroker
          .page(spark, requestId, 0L, 100)
          .events
          .find(_.eventType == OpenIvmInsightsContract.EventType.ActionFailed)
          .getOrElse(fail("Missing failed action event"))
        failedAction.status shouldBe Some("failed")
        failedAction.code shouldBe OpenIvmInsightsContract.Code.IncrementalProgramStatement
        failedAction.detailsJson.mkString should not include "raw_sql_must_not_escape"
      } finally {
        val page = OpenIvmInsightsBroker.page(spark, requestId, 0L, 100)
        if (!page.status.terminal) OpenIvmInsightsBroker.end(spark, requestId, succeeded = false)
        OpenIvmInsightsBroker.release(spark, requestId)
      }
    }

    it("emits failed lifecycle-step state when a timed body throws") {
      val requestId = s"failed-step-${UUID.randomUUID()}"
      OpenIvmInsightsBroker.begin(spark, requestId, "run-failed-step", "model.failed.step")
      try {
        val profile = RefreshProfile.start(spark, "default.failed_step_mv", RefreshProfile.Mode.Refresh)
        val log = RefreshSqlLog.start(
          spark,
          profile.refreshId,
          profile.viewName,
          RefreshSqlLog.ModeRefresh
        )
        intercept[IllegalStateException] {
          profile.timeStep("synthetic_failed_step", "safe_detail=true") {
            throw new IllegalStateException("synthetic failure")
          }
        }
        profile.completeSpan("refresh_failed", Thread.currentThread().getName)
        profile.flush()
        log.finish("refresh_failed")
        OpenIvmInsightsBroker.end(spark, requestId, succeeded = false)

        val failedStep = OpenIvmInsightsBroker
          .page(spark, requestId, 0L, 100)
          .events
          .find(_.eventType == OpenIvmInsightsContract.EventType.LifecycleStepFailed)
          .getOrElse(fail("Missing failed lifecycle-step event"))
        failedStep.status shouldBe Some("failed")
        failedStep.code shouldBe OpenIvmInsightsContract.Code.LifecycleStepFailed
      } finally {
        RefreshProfile.flushActive()
        val page = OpenIvmInsightsBroker.page(spark, requestId, 0L, 100)
        if (!page.status.terminal) OpenIvmInsightsBroker.end(spark, requestId, succeeded = false)
        OpenIvmInsightsBroker.release(spark, requestId)
      }
    }
  }

  private def delete(file: File): Unit = {
    if (file.isDirectory) Option(file.listFiles()).foreach(_.foreach(delete))
    file.delete()
    ()
  }
}
