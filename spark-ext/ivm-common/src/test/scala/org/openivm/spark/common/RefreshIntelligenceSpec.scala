package org.openivm.spark.common

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class RefreshIntelligenceSpec extends AnyFunSpec with Matchers {

  describe("RefreshIntelligence") {
    it("chooses SKIP when runtime materialized deltas are empty") {
      val facts = WorkloadFacts(
        tableStats = Map("db.orders" -> WorkloadTableStats(rowCount = Some(1000L))),
        deltaStats = Map("db.orders" -> WorkloadDeltaStats(rowCount = Some(900L)))
      )
      val estimate = RefreshCostModel.estimate(facts)

      val decision = RefreshIntelligence.decide(
        facts,
        estimate,
        Some(RuntimeDeltaSize(Map("db.orders" -> 0L)))
      )

      decision.route shouldBe RefreshDecisionRoute.Skip
      decision.reasons should contain("runtime_delta_empty_wins")
      decision.toJson should include("\"route\":\"SKIP\"")
    }

    it("chooses FULL_RECOMPUTE when compile-time cost says the delta is broad") {
      val facts = WorkloadFacts(
        tableStats = Map("db.orders" -> WorkloadTableStats(rowCount = Some(1000L))),
        deltaStats = Map("db.orders" -> WorkloadDeltaStats(rowCount = Some(600L)))
      )
      val estimate = RefreshCostModel.estimate(facts)

      val decision = RefreshIntelligence.decide(
        facts,
        estimate,
        Some(RuntimeDeltaSize(Map("db.orders" -> 5L)))
      )

      decision.route shouldBe RefreshDecisionRoute.FullRecompute
      decision.reasons should contain("compile_cost_model_prefers_full_recompute")
      estimate.hint should include("route=FULL_RECOMPUTE")
    }

    it("chooses INCREMENTAL when runtime deltas are non-empty and compile cost is narrow") {
      val facts = WorkloadFacts(
        tableStats = Map("db.orders" -> WorkloadTableStats(rowCount = Some(1000L))),
        deltaStats = Map("db.orders" -> WorkloadDeltaStats(rowCount = Some(10L)))
      )
      val estimate = RefreshCostModel.estimate(facts)

      val decision = RefreshIntelligence.decide(
        facts,
        estimate,
        Some(RuntimeDeltaSize(Map("db.orders" -> 10L)))
      )

      decision.route shouldBe RefreshDecisionRoute.Incremental
      decision.reasons should contain("compile_cost_model_prefers_incremental")
      decision.toJson should include("\"total_rows\":10")
    }
  }
}
