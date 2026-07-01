package org.openivm.spark.common

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class RefreshCostModelSpec extends AnyFunSpec with Matchers {

  describe("RefreshCostModel.estimate") {
    it("keeps SIMPLE_PROJECTION insert-only tiny deltas on the incremental path") {
      val estimate = RefreshCostModel.estimate(
        RefreshTypeCode.SimpleProjection,
        facts(sourceRows = 1000000L, deltaRows = 1000L, shape = DeltaShape.InsertOnly),
        mvRowCount = 1000000L,
        sourceTables = Seq("default.users")
      )

      estimate.affectedFraction shouldBe 0.001d +- 0.0000001d
      estimate.incrementalCost shouldBe 2000.0d +- 0.0000001d
      estimate.fullRecomputeCost shouldBe 2000000.0d +- 0.0000001d
      estimate.recommendFullRefresh shouldBe false
      estimate.rationale should include("incremental refresh")
    }

    it("recommends full refresh for WINDOW_PARTITION when most partitions are affected") {
      val estimate = RefreshCostModel.estimate(
        RefreshTypeCode.WindowPartition,
        facts(sourceRows = 1000000L, deltaRows = 900000L, shape = DeltaShape.General),
        mvRowCount = 1000000L,
        sourceTables = Seq("default.users")
      )

      estimate.affectedFraction shouldBe 0.9d +- 0.0000001d
      estimate.incrementalCost should be > estimate.fullRecomputeCost
      estimate.recommendFullRefresh shouldBe true
      estimate.rationale should include("full refresh")
    }

    it("keeps low-affected WINDOW_PARTITION refreshes incremental") {
      val estimate = RefreshCostModel.estimate(
        RefreshTypeCode.WindowPartition,
        facts(sourceRows = 1000000L, deltaRows = 1000L, shape = DeltaShape.General),
        mvRowCount = 1000000L,
        sourceTables = Seq("default.users")
      )

      estimate.affectedFraction shouldBe 0.001d +- 0.0000001d
      estimate.incrementalCost should be < estimate.fullRecomputeCost
      estimate.recommendFullRefresh shouldBe false
    }

    it("increases affected fraction and incremental cost monotonically with larger deltas") {
      val small = RefreshCostModel.estimate(
        RefreshTypeCode.AggregateGroup,
        facts(sourceRows = 1000000L, deltaRows = 100L, shape = DeltaShape.General),
        mvRowCount = 100000L,
        sourceTables = Seq("default.users")
      )
      val large = RefreshCostModel.estimate(
        RefreshTypeCode.AggregateGroup,
        facts(sourceRows = 1000000L, deltaRows = 10000L, shape = DeltaShape.General),
        mvRowCount = 100000L,
        sourceTables = Seq("default.users")
      )

      large.affectedFraction should be > small.affectedFraction
      large.incrementalCost should be > small.incrementalCost
      small.recommendFullRefresh shouldBe false
    }

    it("treats unchanged sources as zero affected work") {
      val estimate = RefreshCostModel.estimate(
        RefreshTypeCode.SimpleProjection,
        facts(sourceRows = 1000L, deltaRows = 1000L, shape = DeltaShape.Unchanged),
        mvRowCount = 1000L,
        sourceTables = Seq("default.users")
      )

      estimate.affectedFraction shouldBe 0.0d
      estimate.incrementalCost shouldBe 0.0d
      estimate.recommendFullRefresh shouldBe false
    }

    it("falls back to MV row count when source table row counts are missing") {
      val estimate = RefreshCostModel.estimate(
        RefreshTypeCode.SimpleProjection,
        WorkloadFacts(
          deltaShape = Map("default.users" -> DeltaShape.InsertOnly),
          deltaStats = Map("default.users" -> WorkloadDeltaStats(rowCount = Some(50L)))
        ),
        mvRowCount = 1000L,
        sourceTables = Seq("default.users")
      )

      estimate.affectedFraction shouldBe 0.05d +- 0.0000001d
      estimate.fullRecomputeCost shouldBe 1000.0d +- 0.0000001d
      estimate.recommendFullRefresh shouldBe false
    }
  }

  private def facts(sourceRows: Long, deltaRows: Long, shape: DeltaShape): WorkloadFacts =
    WorkloadFacts(
      tableStats = Map("default.users" -> WorkloadTableStats(rowCount = Some(sourceRows))),
      deltaShape = Map("default.users" -> shape),
      deltaStats = Map("default.users" -> WorkloadDeltaStats(rowCount = Some(deltaRows)))
    )
}
