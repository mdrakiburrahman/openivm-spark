package org.openivm.spark.common

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class MvLayoutPolicySpec extends AnyFunSpec with Matchers {

  import MvLayoutPolicy._

  private val allOff: Config = Config(
    simpleProjection = false,
    aggregateGroup = false,
    window = false,
    recompute = false,
    legacyWindowClusterPrune = false,
    fullRefreshOptimizeWrite = false,
    dataSkippingStatsColumns = false,
    checkpointInterval = None
  )

  private val allTypes = Seq(
    RefreshTypeCode.AggregateGroup,
    RefreshTypeCode.SimpleAggregate,
    RefreshTypeCode.SimpleProjection,
    RefreshTypeCode.FullRefresh,
    RefreshTypeCode.AggregateHaving,
    RefreshTypeCode.WindowPartition,
    RefreshTypeCode.GroupRecompute,
    RefreshTypeCode.DistinctIncremental,
    RefreshTypeCode.SemiAntiRecompute
  )

  describe("MvLayoutPolicy default-OFF baseline") {
    it("returns Empty for every refresh type when all flags are OFF") {
      allTypes.foreach { rt =>
        resolve(allOff, rt, Seq("k1", "k2")) shouldBe Empty
      }
    }

    it("preserves the legacy windowClusterPrune path for WINDOW_PARTITION") {
      val cfg = allOff.copy(legacyWindowClusterPrune = true)
      resolve(cfg, RefreshTypeCode.WindowPartition, Seq("part")).clusterColumns shouldBe Seq("part")
      // legacy flag does not leak into other types
      resolve(cfg, RefreshTypeCode.SimpleProjection, Seq("c")).clusterColumns shouldBe empty
    }
  }

  describe("per-type CLUSTER BY levers") {
    it("simpleProjection clusters SIMPLE_PROJECTION only") {
      val cfg = allOff.copy(simpleProjection = true)
      resolve(cfg, RefreshTypeCode.SimpleProjection, Seq("a", "b")).clusterColumns shouldBe Seq("a", "b")
      resolve(cfg, RefreshTypeCode.AggregateGroup, Seq("a")).clusterColumns shouldBe empty
    }

    it("aggregateGroup clusters AGGREGATE_GROUP and AGGREGATE_HAVING") {
      val cfg = allOff.copy(aggregateGroup = true)
      resolve(cfg, RefreshTypeCode.AggregateGroup, Seq("g")).clusterColumns shouldBe Seq("g")
      resolve(cfg, RefreshTypeCode.AggregateHaving, Seq("g")).clusterColumns shouldBe Seq("g")
      resolve(cfg, RefreshTypeCode.WindowPartition, Seq("g")).clusterColumns shouldBe empty
    }

    it("window clusters WINDOW_PARTITION via the new flag") {
      val cfg = allOff.copy(window = true)
      resolve(cfg, RefreshTypeCode.WindowPartition, Seq("p")).clusterColumns shouldBe Seq("p")
    }

    it("recompute clusters GROUP_RECOMPUTE and DISTINCT_INCREMENTAL") {
      val cfg = allOff.copy(recompute = true)
      resolve(cfg, RefreshTypeCode.GroupRecompute, Seq("k")).clusterColumns shouldBe Seq("k")
      resolve(cfg, RefreshTypeCode.DistinctIncremental, Seq("k")).clusterColumns shouldBe Seq("k")
    }

    it("emits no CLUSTER BY when probe keys are empty even if the flag is ON") {
      val cfg = allOff.copy(aggregateGroup = true)
      resolve(cfg, RefreshTypeCode.AggregateGroup, Nil).clusterColumns shouldBe empty
    }

    it("caps clustering at 4 columns (Delta liquid-clustering limit)") {
      val cfg = allOff.copy(simpleProjection = true)
      resolve(cfg, RefreshTypeCode.SimpleProjection, Seq("a", "b", "c", "d", "e", "f")).clusterColumns shouldBe
        Seq("a", "b", "c", "d")
    }

    it("de-duplicates and drops blank probe keys") {
      val cfg = allOff.copy(aggregateGroup = true)
      resolve(cfg, RefreshTypeCode.AggregateGroup, Seq("g", "", "g", "h")).clusterColumns shouldBe Seq("g", "h")
    }
  }

  describe("extra TBLPROPERTIES levers") {
    it("checkpointInterval / dataSkippingStatsColumns are neutral until set") {
      resolve(allOff, RefreshTypeCode.SimpleProjection, Seq("a")).extraTblProperties shouldBe empty
    }

    it("indexes data-skipping stats on the probe key when enabled") {
      val cfg = allOff.copy(dataSkippingStatsColumns = true)
      resolve(cfg, RefreshTypeCode.AggregateGroup, Seq("g1", "g2")).extraTblProperties should contain(
        "'delta.dataSkippingStatsColumns' = 'g1,g2'"
      )
      // no stats property when there is no probe key
      resolve(cfg, RefreshTypeCode.FullRefresh, Nil).extraTblProperties should not contain
        "'delta.dataSkippingStatsColumns' = ''"
    }

    it("emits delta.checkpointInterval when configured") {
      val cfg = allOff.copy(checkpointInterval = Some(50))
      resolve(cfg, RefreshTypeCode.SimpleProjection, Seq("a")).extraTblProperties should contain(
        "'delta.checkpointInterval' = '50'"
      )
    }

    it("optimizeWrite applies to FULL_REFRESH / SIMPLE_AGGREGATE only") {
      val cfg = allOff.copy(fullRefreshOptimizeWrite = true)
      resolve(cfg, RefreshTypeCode.FullRefresh, Nil).extraTblProperties should contain(
        "'delta.autoOptimize.optimizeWrite' = 'true'"
      )
      resolve(cfg, RefreshTypeCode.SimpleAggregate, Nil).extraTblProperties should contain(
        "'delta.autoOptimize.optimizeWrite' = 'true'"
      )
      resolve(cfg, RefreshTypeCode.SimpleProjection, Seq("a")).extraTblProperties should not contain
        "'delta.autoOptimize.optimizeWrite' = 'true'"
    }
  }
}
