package org.openivm.spark.common

import org.apache.spark.SparkConf
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class RefreshCostModelSpec extends AnyFunSpec with Matchers {
  describe("RefreshCostModel") {
    it("defaults to hand-picked coefficients unless calibrated loading is enabled") {
      RefreshCostModel.fromConf(new SparkConf(false)).coefficients shouldBe RefreshCostModel.DefaultCoefficients
      RefreshCostModel
        .fromConf(
          new SparkConf(false).set(RefreshCostModel.CalibratedEnabledKey, "true")
        )
        .coefficients shouldBe RefreshCostModel.DefaultCoefficients
    }

    it("loads calibrated coefficients and predicts wall-clock milliseconds") {
      val model = RefreshCostModel.fromJson(
        """{
          |  "schema_version": 1,
          |  "target": "wall_clock_ms",
          |  "ridge_alpha": 0.25,
          |  "r2": 0.75,
          |  "intercept": 10.0,
          |  "weights": {
          |    "records_read": 0.5,
          |    "records_written": 2.0,
          |    "files_scanned": 3.0,
          |    "refresh_type_5": 7.0
          |  }
          |}""".stripMargin
      )

      model.coefficients.r2 shouldBe Some(0.75d)
      model.coefficients.ridgeAlpha shouldBe Some(0.25d)
      model.predictWallClockMs(
        RefreshCostFeatures(recordsRead = 4.0d, recordsWritten = 5.0d, filesScanned = 6.0d, refreshType = 5)
      ) shouldBe 47.0d
    }

    it("loads calibrated coefficients from a configured resource when enabled") {
      val model = RefreshCostModel.fromConf(
        new SparkConf(false)
          .set(RefreshCostModel.CalibratedEnabledKey, "true")
          .set(RefreshCostModel.CoefficientsResourceKey, "openivm/refresh_cost_model_test_coefficients.json")
      )

      model.coefficients.intercept shouldBe 12.0d
      model.predictWallClockMs(RefreshCostFeatures(recordsRead = 2.0d)) shouldBe 13.0d
    }

    it("derives the model feature set from WorkloadFacts") {
      val facts = WorkloadFacts(
        tableStats = Map("base" -> WorkloadTableStats(rowCount = Some(100L), numFiles = Some(4L))),
        deltaStats = Map("base" -> WorkloadDeltaStats(rowCount = Some(7L), numFiles = Some(2L)))
      )

      RefreshCostFeatures.fromWorkloadFacts(RefreshTypeCode.SimpleProjection, facts) shouldBe
        RefreshCostFeatures(recordsRead = 7.0d, recordsWritten = 7.0d, filesScanned = 2.0d, refreshType = 2)
    }
  }
}
