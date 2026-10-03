package org.openivm.spark.parity

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.spark.sql.Row
import org.openivm.spark.common.FeatureGate
import org.openivm.spark.insights.OpenIvmInsightsContract
import org.openivm.spark.parity.base.{InterceptMode, IvmParitySpecBase}

import scala.util.control.NonFatal

class OpenIvmInsightsSpec extends IvmParitySpecBase("openivm-insights") with InterceptMode {

  override protected def extraSparkConf: Map[String, String] = Map(
    FeatureGate.QueryLogEnabledKey                   -> "true",
    FeatureGate.ProfileRefreshKey                    -> "false",
    FeatureGate.UnifiedRefreshIntelligenceEnabledKey -> "true"
  )

  private val mapper = new ObjectMapper()

  describe("request-scoped SQL insights") {
    it("captures create classification, lifecycle, actions, query-log linkage, and no raw SQL") {
      sql("CREATE TABLE ins_create_src(id INT) USING DELTA")
      sql("INSERT INTO ins_create_src VALUES (1), (2)")
      val secret = "insights_secret_sql_marker"

      val capture = runCaptured("create") {
        sql(
          s"CREATE MATERIALIZED VIEW ins_create_mv AS " +
            s"SELECT id, '$secret' AS marker FROM ins_create_src"
        ).collect()
      }

      capture.failure shouldBe None
      capture.status.getAs[String]("capture_status") shouldBe OpenIvmInsightsContract.CaptureStatus.Complete
      capture.status.getAs[String]("run_id") shouldBe "run-create"
      capture.status.getAs[String]("dbt_node_id") shouldBe "model.openivm.create"
      eventTypes(capture) should contain allOf (
        OpenIvmInsightsContract.EventType.OperationStarted,
        OpenIvmInsightsContract.EventType.DecisionCompleted,
        OpenIvmInsightsContract.EventType.LifecycleStepCompleted,
        OpenIvmInsightsContract.EventType.ActionCompleted,
        OpenIvmInsightsContract.EventType.OperationCompleted
      )
      codes(capture) should contain allOf (
        OpenIvmInsightsContract.Code.ClassificationCompleted,
        OpenIvmInsightsContract.Code.InitialMaterialization,
        OpenIvmInsightsContract.Code.CatalogPublication,
        OpenIvmInsightsContract.Code.QueryBodyObserved
      )
      val actionDetails = details(capture).filter(_.has("query_logging_enabled"))
      actionDetails should not be empty
      actionDetails.exists(_.path("query_logging_enabled").asBoolean()) shouldBe true
      capture.rows.flatMap(_.toSeq).filter(_ != null).mkString("\n") should not include secret
      capture.rows.flatMap(_.toSeq).filter(_ != null).mkString("\n").toLowerCase should not include
        "select id"
    }

    it("captures refresh source observations, unified route reasons, and incremental actions") {
      sql("CREATE TABLE ins_refresh_src(id INT, amount INT) USING DELTA")
      sql("INSERT INTO ins_refresh_src VALUES (1, 10)")
      sql(
        "CREATE MATERIALIZED VIEW ins_refresh_mv AS " +
          "SELECT id, SUM(amount) AS total FROM ins_refresh_src GROUP BY id"
      ).collect()
      sql("INSERT INTO ins_refresh_src VALUES (1, 5), (2, 7)")

      val capture = runCaptured("refresh") {
        sql("REFRESH MATERIALIZED VIEW ins_refresh_mv").collect()
      }

      capture.failure shouldBe None
      codes(capture) should contain allOf (
        OpenIvmInsightsContract.Code.ClassificationCompleted,
        OpenIvmInsightsContract.Code.SourceVersionsObserved,
        OpenIvmInsightsContract.Code.PendingDeltasObserved,
        OpenIvmInsightsContract.Code.RefreshRouteSelected,
        OpenIvmInsightsContract.Code.SourceChangesMaterialized,
        OpenIvmInsightsContract.Code.IncrementalProgramStatement,
        OpenIvmInsightsContract.Code.OperationCompleted
      )
      val pending = event(capture, OpenIvmInsightsContract.Code.PendingDeltasObserved)
      pending.getAs[String]("status") shouldBe "pending"
      mapper.readTree(pending.getAs[String]("details_json")).path("pending_delta_count").asInt() should be > 0
      val route = mapper.readTree(
        event(capture, OpenIvmInsightsContract.Code.RefreshRouteSelected).getAs[String]("details_json")
      )
      route.path("reasons").isArray shouldBe true
      route.path("runtime_delta_size").path("total_rows").asLong() should be > 0L
      assertMvCorrect(
        "ins_refresh_mv",
        "SELECT id, SUM(amount) AS total FROM ins_refresh_src GROUP BY id"
      )
      assertTerminalHasHighestOperationSequence(capture)
    }

    it("captures an explicit successful no-op refresh outcome") {
      sql("CREATE TABLE ins_noop_src(id INT) USING DELTA")
      sql("INSERT INTO ins_noop_src VALUES (1)")
      sql("CREATE MATERIALIZED VIEW ins_noop_mv AS SELECT id FROM ins_noop_src").collect()

      val capture = runCaptured("noop") {
        sql("REFRESH MATERIALIZED VIEW ins_noop_mv").collect()
      }

      capture.failure shouldBe None
      event(capture, OpenIvmInsightsContract.Code.PendingDeltasObserved)
        .getAs[String]("status") shouldBe "empty"
      val completed = terminalOperation(capture)
      completed.getAs[String]("event_type") shouldBe OpenIvmInsightsContract.EventType.OperationCompleted
      completed.getAs[String]("status") shouldBe "no_pending_deltas"
      event(capture, OpenIvmInsightsContract.Code.PendingDeltasObserved)
        .getAs[String]("operation") shouldBe OpenIvmInsightsContract.Operation.Refresh
      assertTerminalHasHighestOperationSequence(capture)
    }

    it("attributes idempotent CREATE pending-delta observations to create") {
      sql("CREATE TABLE ins_idempotent_src(id INT) USING DELTA")
      sql("INSERT INTO ins_idempotent_src VALUES (1)")
      sql("CREATE MATERIALIZED VIEW ins_idempotent_mv AS SELECT id FROM ins_idempotent_src").collect()

      val capture = runCaptured("idempotent-create") {
        sql(
          "CREATE MATERIALIZED VIEW IF NOT EXISTS ins_idempotent_mv AS " +
            "SELECT id FROM ins_idempotent_src"
        ).collect()
      }

      capture.failure shouldBe None
      val pending = event(capture, OpenIvmInsightsContract.Code.PendingDeltasObserved)
      pending.getAs[String]("operation") shouldBe OpenIvmInsightsContract.Operation.Create
      terminalOperation(capture).getAs[String]("status") shouldBe "create_already_exists"
    }

    it("captures operation failure while preserving the original SQL failure") {
      sql("CREATE TABLE ins_fail_src(id INT) USING DELTA")
      sql("INSERT INTO ins_fail_src VALUES (1)")
      sql("CREATE MATERIALIZED VIEW ins_fail_mv AS SELECT id FROM ins_fail_src").collect()

      val capture = runCaptured("failure") {
        sql("CREATE MATERIALIZED VIEW ins_fail_mv AS SELECT id FROM ins_fail_src").collect()
      }

      capture.failure should not be empty
      capture.status.getAs[String]("capture_status") shouldBe OpenIvmInsightsContract.CaptureStatus.Failed
      val failed = terminalOperation(capture)
      failed.getAs[String]("event_type") shouldBe OpenIvmInsightsContract.EventType.OperationFailed
      failed.getAs[String]("code") shouldBe OpenIvmInsightsContract.Code.OperationFailed
      failed.getAs[String]("message").toLowerCase should not include "select"
    }

    it("classifies schema drift as a terminal refresh failure") {
      sql("CREATE TABLE ins_drift_src(id INT, value INT) USING DELTA")
      sql("INSERT INTO ins_drift_src VALUES (1, 10)")
      sql("CREATE MATERIALIZED VIEW ins_drift_mv AS SELECT id FROM ins_drift_src").collect()
      sql("ALTER TABLE ins_drift_src ADD COLUMNS (added INT)")
      sql("INSERT INTO ins_drift_src VALUES (2, 20, 30)")

      val capture = runCaptured("schema-drift") {
        sql("REFRESH MATERIALIZED VIEW ins_drift_mv").collect()
      }

      capture.failure should not be empty
      val failed = terminalOperation(capture)
      failed.getAs[String]("event_type") shouldBe OpenIvmInsightsContract.EventType.OperationFailed
      failed.getAs[String]("status") shouldBe "schema_drift"
      assertTerminalHasHighestOperationSequence(capture)
    }

    it("captures drop planning and every materialized-view cleanup domain") {
      sql("CREATE TABLE ins_drop_src(id INT) USING DELTA")
      sql("INSERT INTO ins_drop_src VALUES (1)")
      sql("CREATE MATERIALIZED VIEW ins_drop_mv AS SELECT id FROM ins_drop_src").collect()

      val capture = runCaptured("drop") {
        sql("DROP MATERIALIZED VIEW ins_drop_mv").collect()
      }

      capture.failure shouldBe None
      codes(capture) should contain allOf (
        OpenIvmInsightsContract.Code.DropCascadePlan,
        OpenIvmInsightsContract.Code.MaterializedTableRemoved,
        OpenIvmInsightsContract.Code.StoragePathRemoved,
        OpenIvmInsightsContract.Code.ChangePropagationRowsRemoved,
        OpenIvmInsightsContract.Code.CdfWatermarkStateRemoved,
        OpenIvmInsightsContract.Code.ViewDeltaNamespaceRemoved,
        OpenIvmInsightsContract.Code.CatalogEntryRemoved,
        OpenIvmInsightsContract.Code.OperationCompleted
      )
      mapper
        .readTree(event(capture, OpenIvmInsightsContract.Code.DropCascadePlan).getAs[String]("details_json"))
        .path(OpenIvmInsightsContract.DetailField.Reason)
        .asText() shouldBe "explicit_drop"
      terminalOperation(capture).getAs[String]("status") shouldBe "drop_completed"
    }

    it("keeps EXPLAIN preflight compatible and adds stable graph fields and events") {
      sql("CREATE TABLE ins_preflight_src(id INT) USING DELTA")

      var explainJson = ""
      val capture = runCaptured("preflight") {
        explainJson = sql(
          "EXPLAIN CREATE MATERIALIZED VIEW ins_preflight_mv AS SELECT id FROM ins_preflight_src"
        ).head().getString(0)
      }

      capture.failure shouldBe None
      val explain = mapper.readTree(explainJson)
      explain.path("view").asText() shouldBe "ins_preflight_mv"
      explain.path("graph_version").asInt() shouldBe 1
      explain.path("operation").asText() shouldBe "preflight"
      explain.path("materialized_view").asText() shouldBe "ins_preflight_mv"
      explain.path("direct_dependencies").isArray shouldBe true
      explain.path("cascade_capability").asText() should not be empty
      capture.events.map(_.getAs[String]("operation")).toSet should contain(
        OpenIvmInsightsContract.Operation.Preflight
      )
      codes(capture) should contain allOf (
        OpenIvmInsightsContract.Code.ClassificationCompleted,
        OpenIvmInsightsContract.Code.OperationCompleted
      )
    }
  }

  private final class Capture(val rows: Seq[Row], val failure: Option[Throwable]) {
    val status: Row      = rows.head
    val events: Seq[Row] = rows.tail.filter(_.getAs[String]("record_type") == OpenIvmInsightsContract.RecordType.Event)
  }

  private def runCaptured(name: String)(body: => Unit): Capture = {
    val requestId = s"insights-$name"
    sql(
      s"OPENIVM INSIGHTS BEGIN REQUEST '$requestId' RUN 'run-$name' NODE 'model.openivm.$name'"
    ).collect()
    var failure = Option.empty[Throwable]
    try body
    catch {
      case NonFatal(error) => failure = Some(error)
    }
    val status = if (failure.isEmpty) "SUCCEEDED" else "FAILED"
    sql(s"OPENIVM INSIGHTS END REQUEST '$requestId' STATUS $status").collect()
    try {
      val rows = sql(s"SHOW OPENIVM INSIGHTS FOR REQUEST '$requestId' AFTER 0 LIMIT 1000").collect().toSeq
      new Capture(rows, failure)
    } finally {
      sql(s"OPENIVM INSIGHTS RELEASE REQUEST '$requestId'").collect()
    }
  }

  private def codes(capture: Capture): Set[String] =
    capture.events.map(_.getAs[String]("code")).toSet

  private def eventTypes(capture: Capture): Set[String] =
    capture.events.map(_.getAs[String]("event_type")).toSet

  private def event(capture: Capture, code: String): Row =
    capture.events.find(_.getAs[String]("code") == code).getOrElse(fail(s"Missing insight event code $code"))

  private def terminalOperation(capture: Capture): Row =
    capture.events
      .find(row =>
        row.getAs[java.lang.Boolean]("terminal") == java.lang.Boolean.TRUE &&
          row.getAs[String]("operation") != OpenIvmInsightsContract.Operation.Request
      )
      .getOrElse(fail("Missing terminal operation event"))

  private def assertTerminalHasHighestOperationSequence(capture: Capture): Unit = {
    val terminal    = terminalOperation(capture)
    val operationId = terminal.getAs[String]("operation_id")
    val sequences = capture.events
      .filter(_.getAs[String]("operation_id") == operationId)
      .map(_.getAs[Long]("sequence"))
    terminal.getAs[Long]("sequence") shouldBe sequences.max
  }

  private def details(capture: Capture): Seq[JsonNode] =
    capture.events.flatMap(row => Option(row.getAs[String]("details_json")).map(mapper.readTree))
}
