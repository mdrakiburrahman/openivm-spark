package org.openivm.spark.commands

import org.openivm.spark.common.RefreshTypeCode
import org.openivm.spark.insights.OpenIvmInsightsContract
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class OpenIvmMaterializedViewInsightEventsSpec extends AnyFunSpec with Matchers {

  private val Incremental = OpenIvmMaterializedViewInsightEvents.Classification(
    compileRefreshType = "AGGREGATE_GROUP",
    effectiveRefreshType = "AGGREGATE_GROUP",
    reason = "kept"
  )

  describe("materialized-view branch normalization") {
    it("maps incremental create, refresh, and empty refresh outcomes to M1-M3") {
      OpenIvmMaterializedViewInsightEvents
        .branchForClassification(OpenIvmInsightsContract.Operation.Create, Incremental) shouldBe
        OpenIvmMaterializedViewInsightEvents.Branch(OpenIvmInsightsContract.BranchCode.M1, "kept")

      OpenIvmMaterializedViewInsightEvents
        .branchForRefreshOutcome("incremental_executed", Incremental) shouldBe
        Some(OpenIvmMaterializedViewInsightEvents.Branch(OpenIvmInsightsContract.BranchCode.M2, "kept"))

      OpenIvmMaterializedViewInsightEvents
        .branchForRefreshOutcome("no_pending_deltas", Incremental) shouldBe
        Some(
          OpenIvmMaterializedViewInsightEvents.Branch(
            OpenIvmInsightsContract.BranchCode.M3,
            "no_pending_deltas"
          )
        )
    }

    it("keeps every supported incremental strategy on M1/M2 instead of relabeling it FULL_REFRESH") {
      Seq(
        "AGGREGATE_GROUP",
        "SIMPLE_AGGREGATE",
        "SIMPLE_PROJECTION",
        "AGGREGATE_HAVING",
        "WINDOW_PARTITION",
        "GROUP_RECOMPUTE",
        "DISTINCT_INCREMENTAL"
      ).foreach { refreshType =>
        val classification = OpenIvmMaterializedViewInsightEvents.Classification(
          compileRefreshType = refreshType,
          effectiveRefreshType = refreshType,
          reason = s"${refreshType.toLowerCase(java.util.Locale.ROOT)}_kept"
        )

        val create = OpenIvmMaterializedViewInsightEvents.branchForClassification(
          OpenIvmInsightsContract.Operation.Create,
          classification
        )
        val refresh = OpenIvmMaterializedViewInsightEvents
          .branchForRefreshOutcome("incremental_executed", classification)
          .getOrElse(fail(s"Missing refresh branch for $refreshType"))

        create.code shouldBe OpenIvmInsightsContract.BranchCode.M1
        refresh.code shouldBe OpenIvmInsightsContract.BranchCode.M2
        create.reason shouldBe classification.reason
        refresh.reason shouldBe classification.reason
        create.code should not be OpenIvmInsightsContract.BranchCode.M5
        refresh.code should not be OpenIvmInsightsContract.BranchCode.M5
      }
    }

    it("distinguishes native full refresh from an incremental demotion and preserves its reason") {
      val nativeFull = OpenIvmMaterializedViewInsightEvents.Classification(
        compileRefreshType = RefreshTypeCode.FullRefreshName,
        effectiveRefreshType = RefreshTypeCode.FullRefreshName,
        reason = "no_real_delta"
      )
      val demoted = OpenIvmMaterializedViewInsightEvents.Classification(
        compileRefreshType = "AGGREGATE_HAVING",
        effectiveRefreshType = RefreshTypeCode.FullRefreshName,
        reason = "having_pred_hidden_agg"
      )

      OpenIvmMaterializedViewInsightEvents
        .branchForClassification(OpenIvmInsightsContract.Operation.Create, nativeFull) shouldBe
        OpenIvmMaterializedViewInsightEvents.Branch(
          OpenIvmInsightsContract.BranchCode.M4,
          "no_real_delta"
        )
      OpenIvmMaterializedViewInsightEvents
        .branchForClassification(OpenIvmInsightsContract.Operation.Create, demoted) shouldBe
        OpenIvmMaterializedViewInsightEvents.Branch(
          OpenIvmInsightsContract.BranchCode.M5,
          "having_pred_hidden_agg"
        )
    }

    it("maps a verified signed full-refresh companion to M6") {
      val signed = OpenIvmMaterializedViewInsightEvents.Classification(
        compileRefreshType = RefreshTypeCode.FullRefreshName,
        effectiveRefreshType = RefreshTypeCode.SignedDeltaRecomputeName,
        reason = "signed_delta_recompute_verified"
      )

      OpenIvmMaterializedViewInsightEvents
        .branchForClassification(OpenIvmInsightsContract.Operation.Refresh, signed) shouldBe
        OpenIvmMaterializedViewInsightEvents.Branch(
          OpenIvmInsightsContract.BranchCode.M6,
          "signed_delta_recompute_verified"
        )
    }

    it("does not assign a successful branch to failed refresh outcomes") {
      OpenIvmMaterializedViewInsightEvents
        .branchForRefreshOutcome("refresh_failed", Incremental) shouldBe None
    }
  }
}
