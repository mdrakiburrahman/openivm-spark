package org.openivm.spark.parity

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.spark.sql.Row
import org.openivm.spark.common.{FeatureGate, MvCatalog, MvMetadata, RefreshTypeCode}
import org.openivm.spark.insights.OpenIvmInsightsContract
import org.openivm.spark.parity.base.{InterceptMode, IvmParitySpecBase}

import java.io.File
import java.util.UUID
import scala.util.control.NonFatal

class OpenIvmMaterializedViewBranchesSpec extends IvmParitySpecBase("openivm-mv-branches") with InterceptMode {

  private val mapper = new ObjectMapper()

  override protected def extraSparkConf: Map[String, String] =
    Map(
      FeatureGate.StreamingCheckpointArchiveUriKey ->
        new File(warehouseDir, "checkpoint-archive").toURI.toString
    )

  describe("materialized-view insight branches") {
    it("emits M1-M3 while preserving incremental create and refresh behavior") {
      sql("CREATE TABLE mvbr_inc_src(id INT, amount INT) USING DELTA")
      sql("INSERT INTO mvbr_inc_src VALUES (1, 10)")

      val create = runCaptured("incremental-create", "mvbr_inc_mv") {
        sql(
          "CREATE MATERIALIZED VIEW mvbr_inc_mv AS " +
            "SELECT id, SUM(amount) AS total FROM mvbr_inc_src GROUP BY id"
        ).collect()
      }
      create.failure shouldBe None
      branchDetails(create, OpenIvmInsightsContract.BranchCode.M1)
        .path(OpenIvmInsightsContract.DetailField.ExecutionMode)
        .asText() shouldBe OpenIvmInsightsContract.ExecutionMode.MaterializedView

      val meta = mvMeta("mvbr_inc_mv")
      meta.refreshType should not be RefreshTypeCode.FullRefresh
      meta.refreshTypeName should not be RefreshTypeCode.FullRefreshName

      sql("INSERT INTO mvbr_inc_src VALUES (1, 5), (2, 7)")
      val refresh = runCaptured("incremental-refresh", "mvbr_inc_mv") {
        refreshMv("mvbr_inc_mv")
      }
      refresh.failure shouldBe None
      branchDetails(refresh, OpenIvmInsightsContract.BranchCode.M2)
        .path(OpenIvmInsightsContract.DetailField.Reason)
        .asText() should not be empty
      mvMeta("mvbr_inc_mv").refreshType shouldBe meta.refreshType

      val noPending = runCaptured("no-pending", "mvbr_inc_mv") {
        refreshMv("mvbr_inc_mv")
      }
      noPending.failure shouldBe None
      branchDetails(noPending, OpenIvmInsightsContract.BranchCode.M3)
        .path(OpenIvmInsightsContract.DetailField.Reason)
        .asText() shouldBe "no_pending_deltas"

      assertMvCorrect(
        "mvbr_inc_mv",
        "SELECT id, SUM(amount) AS total FROM mvbr_inc_src GROUP BY id"
      )
    }

    it("emits M5 with the exact persisted demotion reason") {
      sql("CREATE TABLE mvbr_demote_src(grp STRING, amount INT) USING DELTA")
      sql("INSERT INTO mvbr_demote_src VALUES ('a', 10), ('a', 5), ('b', 7)")

      val capture = runCaptured("demoted-create", "mvbr_demote_mv") {
        sql(
          "CREATE MATERIALIZED VIEW mvbr_demote_mv AS " +
            "SELECT grp, SUM(amount) AS total FROM mvbr_demote_src " +
            "GROUP BY grp HAVING COUNT(*) > 0"
        ).collect()
      }

      capture.failure shouldBe None
      val meta    = mvMeta("mvbr_demote_mv")
      val details = branchDetails(capture, OpenIvmInsightsContract.BranchCode.M5)
      meta.refreshType shouldBe RefreshTypeCode.FullRefresh
      meta.properties(MvMetadata.CompileRefreshTypeKey) should not be RefreshTypeCode.FullRefreshName
      details.path(OpenIvmInsightsContract.DetailField.Reason).asText() shouldBe
        meta.properties(MvMetadata.RefreshReasonKey)
      details.path("effective_refresh_type").asText() shouldBe RefreshTypeCode.FullRefreshName
      assertMvCorrect(
        "mvbr_demote_mv",
        "SELECT grp, SUM(amount) AS total FROM mvbr_demote_src GROUP BY grp HAVING COUNT(*) > 0"
      )
    }

    it("emits M6 for a verified signed-delta recompute without relabeling its numeric type") {
      val create = runCaptured("signed-create", "mvbr_signed_mv") {
        sql(
          "CREATE MATERIALIZED VIEW mvbr_signed_mv AS " +
            "SELECT CAST(CURRENT_TIMESTAMP() AS TIMESTAMP) AS observed_at"
        ).collect()
      }
      create.failure shouldBe None
      branchDetails(create, OpenIvmInsightsContract.BranchCode.M6)
        .path(OpenIvmInsightsContract.DetailField.Reason)
        .asText() shouldBe "signed_delta_recompute_verified"

      val meta = mvMeta("mvbr_signed_mv")
      meta.refreshType shouldBe RefreshTypeCode.FullRefresh
      meta.refreshTypeName shouldBe RefreshTypeCode.SignedDeltaRecomputeName
      meta.emitsCascadeViewDelta shouldBe true

      Thread.sleep(20L)
      val refresh = runCaptured("signed-refresh", "mvbr_signed_mv") {
        refreshMv("mvbr_signed_mv")
      }
      refresh.failure shouldBe None
      branchDetails(refresh, OpenIvmInsightsContract.BranchCode.M6)
        .path("effective_refresh_type")
        .asText() shouldBe RefreshTypeCode.SignedDeltaRecomputeName
      mvMeta("mvbr_signed_mv").refreshType shouldBe RefreshTypeCode.FullRefresh
    }

    it("keeps M7 annotation, cascade cleanup, and replacement CREATE in one request") {
      sql("CREATE TABLE mvbr_cascade_src(id INT) USING DELTA")
      sql("INSERT INTO mvbr_cascade_src VALUES (1), (2)")
      sql("CREATE MATERIALIZED VIEW mvbr_cascade_root AS SELECT id FROM mvbr_cascade_src").collect()
      sql("CREATE MATERIALIZED VIEW mvbr_cascade_child AS SELECT id FROM mvbr_cascade_root").collect()

      val capture = runCaptured("query-change-rebuild", "mvbr_cascade_root") {
        sql(
          """OPENIVM INSIGHTS ANNOTATE MATERIALIZED VIEW QUERY HASH
            |TARGET 'mvbr_cascade_root'
            |OLD '0123456789abcdef0123456789abcdef'
            |NEW 'fedcba9876543210fedcba9876543210'
            |POLICY 'rebuild'
            |DECISION 'rebuild'
            |REASON 'query_hash_changed'""".stripMargin
        ).collect()
        sql("DROP MATERIALIZED VIEW mvbr_cascade_root").collect()
        sql(
          "CREATE MATERIALIZED VIEW mvbr_cascade_root AS " +
            "SELECT id, id + 1 AS shifted_id FROM mvbr_cascade_src"
        ).collect()
      }

      capture.failure shouldBe None
      val annotation = event(capture, OpenIvmInsightsContract.Code.MvQueryHashDecisionAnnotated)
      details(annotation).path(OpenIvmInsightsContract.DetailField.BranchCode).asText() shouldBe
        OpenIvmInsightsContract.BranchCode.M7
      details(annotation).path(OpenIvmInsightsContract.DetailField.Reason).asText() shouldBe
        "query_hash_changed"

      val cascade        = event(capture, OpenIvmInsightsContract.Code.DropCascadePlan)
      val cascadeDetails = details(cascade)
      cascadeDetails.path(OpenIvmInsightsContract.DetailField.Reason).asText() shouldBe "query_hash_changed"
      cascadeDetails.path("dependent_count").asInt() shouldBe 1
      val dependentPlan = cascadeDetails.path("dependents").get(0)
      dependentPlan.path("order").asInt() shouldBe 0
      dependentPlan.path("name").asText() should endWith("mvbr_cascade_child")
      dependentPlan.path("caused_by").asText() should not be empty
      dependentPlan.path(OpenIvmInsightsContract.DetailField.Reason).asText() shouldBe
        "query_hash_changed"
      val dependent = event(capture, OpenIvmInsightsContract.Code.StreamingDescendantCleanupCompleted)
      details(dependent).path("caused_by").asText() should not be empty

      val replacement = branchEvent(capture, OpenIvmInsightsContract.BranchCode.M7)
      replacement.getAs[Long]("sequence") should be > cascade.getAs[Long]("sequence")
      mvMeta("mvbr_cascade_root").refreshType should not be RefreshTypeCode.FullRefresh
      MvCatalog.lookup(spark, spark.sessionState.sqlParser.parseTableIdentifier("mvbr_cascade_child")) shouldBe None
      assertMvCorrect(
        "mvbr_cascade_root",
        "SELECT id, id + 1 AS shifted_id FROM mvbr_cascade_src"
      )
    }

    it("emits M8 before a fail-policy rejection without dropping the MV") {
      sql("CREATE TABLE mvbr_reject_src(id INT) USING DELTA")
      sql("INSERT INTO mvbr_reject_src VALUES (1)")
      sql("CREATE MATERIALIZED VIEW mvbr_reject_mv AS SELECT id FROM mvbr_reject_src").collect()

      val capture = runCaptured("query-change-rejected", "mvbr_reject_mv") {
        sql(
          """OPENIVM INSIGHTS ANNOTATE MATERIALIZED VIEW QUERY HASH
            |TARGET 'mvbr_reject_mv'
            |OLD '0123456789abcdef0123456789abcdef'
            |NEW 'abcdef0123456789abcdef0123456789'
            |POLICY 'fail'
            |DECISION 'rejected'
            |REASON 'query_hash_changed'""".stripMargin
        ).collect()
        throw new IllegalStateException("simulated dbt fail-policy rejection")
      }

      capture.failure should not be empty
      val annotation        = event(capture, OpenIvmInsightsContract.Code.MvQueryHashDecisionAnnotated)
      val annotationDetails = details(annotation)
      annotationDetails.path(OpenIvmInsightsContract.DetailField.BranchCode).asText() shouldBe
        OpenIvmInsightsContract.BranchCode.M8
      annotationDetails.path(OpenIvmInsightsContract.DetailField.Policy).asText() shouldBe "fail"
      annotationDetails.path(OpenIvmInsightsContract.DetailField.Decision).asText() shouldBe "rejected"
      annotationDetails.path(OpenIvmInsightsContract.DetailField.Reason).asText() shouldBe
        "query_hash_changed"
      capture.events.map(_.getAs[String]("code")) should not contain OpenIvmInsightsContract.Code.DropCascadePlan
      mvMeta("mvbr_reject_mv").refreshType should not be RefreshTypeCode.FullRefresh
      assertMvCorrect("mvbr_reject_mv", "SELECT id FROM mvbr_reject_src")
    }
  }

  private final class Capture(val rows: Seq[Row], val failure: Option[Throwable]) {
    val events: Seq[Row] = rows.tail.filter(
      _.getAs[String]("record_type") == OpenIvmInsightsContract.RecordType.Event
    )
  }

  private def runCaptured(name: String, target: String)(body: => Unit): Capture = {
    val requestId = s"mv-branches-$name-${UUID.randomUUID().toString.take(8)}"
    sql(
      s"""OPENIVM INSIGHTS BEGIN REQUEST '$requestId'
         |RUN 'run-$name'
         |NODE 'model.openivm.$name'
         |MATERIALIZATION 'materialized_view'
         |TARGET '$target'""".stripMargin
    ).collect()
    var failure = Option.empty[Throwable]
    try body
    catch {
      case NonFatal(error) => failure = Some(error)
    }
    val status = if (failure.isEmpty) "SUCCEEDED" else "FAILED"
    sql(s"OPENIVM INSIGHTS END REQUEST '$requestId' STATUS $status").collect()
    try {
      val result = new Capture(
        sql(s"SHOW OPENIVM INSIGHTS FOR REQUEST '$requestId' AFTER 0 LIMIT 2000").collect().toSeq,
        failure
      )
      result.events.map(_.getAs[String]("code")) should contain noneOf (
        OpenIvmInsightsContract.Code.RegularSparkCompleted,
        OpenIvmInsightsContract.Code.RegularSparkFailed
      )
      result
    } finally {
      sql(s"OPENIVM INSIGHTS RELEASE REQUEST '$requestId'").collect()
    }
  }

  private def mvMeta(name: String) =
    MvCatalog
      .lookup(spark, spark.sessionState.sqlParser.parseTableIdentifier(name))
      .getOrElse(fail(s"Missing materialized view $name"))

  private def event(capture: Capture, code: String): Row =
    capture.events.find(_.getAs[String]("code") == code).getOrElse(fail(s"Missing insight event $code"))

  private def branchEvent(capture: Capture, branchCode: String): Row = {
    val result = capture.events.reverse
      .filter(_.getAs[String]("code") == branchCode)
      .find(row => details(row).path(OpenIvmInsightsContract.DetailField.BranchCode).asText() == branchCode)
      .getOrElse(fail(s"Missing materialized-view branch $branchCode"))
    val branchDetails = details(result)
    branchDetails.path("branch_label").asText() shouldBe OpenIvmInsightsContract.BranchCode.Labels(branchCode)
    branchDetails.path(OpenIvmInsightsContract.DetailField.ExecutionMode).asText() shouldBe
      OpenIvmInsightsContract.ExecutionMode.MaterializedView
    branchDetails.path(OpenIvmInsightsContract.DetailField.Materialization).asText() shouldBe
      OpenIvmInsightsContract.Materialization.MaterializedView
    branchDetails.path(OpenIvmInsightsContract.DetailField.TargetRelation).asText() shouldBe
      result.getAs[String]("materialized_view")
    branchDetails.path(OpenIvmInsightsContract.DetailField.Reason).asText() should not be empty
    result
  }

  private def branchDetails(capture: Capture, branchCode: String): JsonNode =
    details(branchEvent(capture, branchCode))

  private def details(row: Row): JsonNode =
    mapper.readTree(row.getAs[String]("details_json"))
}
