package org.openivm.spark.common

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class RefreshTypeCodeSpec extends AnyFunSpec with Matchers {

  private val incrementalCascadeTypes = Seq(
    RefreshTypeCode.AggregateGroup,
    RefreshTypeCode.AggregateHaving,
    RefreshTypeCode.SimpleAggregate,
    RefreshTypeCode.SimpleProjection,
    RefreshTypeCode.WindowPartition,
    RefreshTypeCode.GroupRecompute
  )

  private val neverCascadeTypes = Seq(
    RefreshTypeCode.TopK,
    RefreshTypeCode.DistinctIncremental,
    RefreshTypeCode.SemiAntiRecompute
  )

  describe("RefreshTypeCode.emitsCascadeViewDelta") {
    it("accepts the refresh types whose compiled shape always carries a signed view-delta") {
      incrementalCascadeTypes.foreach { rt =>
        withClue(s"refresh_type=$rt: ")(RefreshTypeCode.emitsCascadeViewDelta(rt) shouldBe true)
      }
    }

    it("fails closed for FULL_REFRESH and the non-cascade complement set") {
      RefreshTypeCode.emitsCascadeViewDelta(RefreshTypeCode.FullRefresh) shouldBe false
      neverCascadeTypes.foreach { rt =>
        withClue(s"refresh_type=$rt: ")(RefreshTypeCode.emitsCascadeViewDelta(rt) shouldBe false)
      }
    }
  }

  describe("RefreshTypeCode.mayEmitCascadeViewDelta") {
    it("permits FULL_REFRESH, which the static capability withholds") {
      RefreshTypeCode.mayEmitCascadeViewDelta(RefreshTypeCode.FullRefresh) shouldBe true
      RefreshTypeCode.emitsCascadeViewDelta(RefreshTypeCode.FullRefresh) shouldBe false
    }

    it("keeps the always-cascade types permitted") {
      incrementalCascadeTypes.foreach { rt =>
        withClue(s"refresh_type=$rt: ")(RefreshTypeCode.mayEmitCascadeViewDelta(rt) shouldBe true)
      }
    }

    it("still refuses the refresh types that can never carry a signed view-delta") {
      neverCascadeTypes.foreach { rt =>
        withClue(s"refresh_type=$rt: ")(RefreshTypeCode.mayEmitCascadeViewDelta(rt) shouldBe false)
      }
    }
  }

  describe("reported strategy names") {
    it("keeps the wire value the benchmark refresh-type guard treats as a lost incremental path") {
      RefreshTypeCode.FullRefreshName shouldBe "FULL_REFRESH"
    }

    it("reports a verified full-refresh companion under a distinct, non-FULL strategy name") {
      RefreshTypeCode.SignedDeltaRecomputeName shouldBe "SIGNED_DELTA_RECOMPUTE"
      RefreshTypeCode.SignedDeltaRecomputeName should not be RefreshTypeCode.FullRefreshName
      // The guard normalizes only FULL/FULL_REFRESH to FULL; anything else
      // passes through, so the cascade strategy must not alias either spelling.
      Set("FULL", "FULL_REFRESH") should not contain RefreshTypeCode.SignedDeltaRecomputeName
    }
  }
}
