package org.openivm.spark.common

import org.apache.spark.SparkConf
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class FeatureGateSpec extends AnyFunSpec with Matchers {

  describe("FeatureGate.adaptiveBroadcastEnabled") {
    it("defaults ON and honours an explicit OFF flag") {
      FeatureGate.adaptiveBroadcastEnabled(new SparkConf(false)) shouldBe true
      FeatureGate.adaptiveBroadcastEnabled(
        new SparkConf(false).set(FeatureGate.AdaptiveBroadcastEnabledKey, "false")
      ) shouldBe false
    }
  }

  describe("FeatureGate.adaptiveBroadcastThresholdBytes") {
    it("prefers an explicit positive thresholdBytes override over everything") {
      val conf = new SparkConf(false).set(FeatureGate.AdaptiveBroadcastThresholdKey, "12345")
      FeatureGate.adaptiveBroadcastThresholdBytes(conf, sessionStaticBytes = Some(999999999L)) shouldBe 12345L
    }

    it("inherits the session static broadcast budget when positive and no override is set") {
      FeatureGate.adaptiveBroadcastThresholdBytes(
        new SparkConf(false),
        sessionStaticBytes = Some(104857600L)
      ) shouldBe 104857600L
    }

    it("falls back to the 100 MiB default when the static budget is absent or non-positive") {
      val conf = new SparkConf(false)
      FeatureGate.adaptiveBroadcastThresholdBytes(conf, sessionStaticBytes = None) shouldBe
        FeatureGate.AdaptiveBroadcastDefaultBytes
      FeatureGate.adaptiveBroadcastThresholdBytes(conf, sessionStaticBytes = Some(-1L)) shouldBe
        FeatureGate.AdaptiveBroadcastDefaultBytes
      FeatureGate.AdaptiveBroadcastDefaultBytes shouldBe 104857600L
    }

    it("ignores a non-positive explicit override and inherits instead") {
      val conf = new SparkConf(false).set(FeatureGate.AdaptiveBroadcastThresholdKey, "-1")
      FeatureGate.adaptiveBroadcastThresholdBytes(conf, sessionStaticBytes = Some(50L)) shouldBe 50L
    }
  }

  describe("FeatureGate.runtimeFilterConfOverrides") {
    it("enables bloom + semi-join pushdown by default and lowers the scan-size threshold") {
      val o = FeatureGate.runtimeFilterConfOverrides(new SparkConf(false))
      o("spark.sql.optimizer.runtime.bloomFilter.enabled") shouldBe "true"
      o("spark.sql.optimizer.runtimeFilter.semiJoinReduction.enabled") shouldBe "true"
      o("spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold") shouldBe "1MB"
    }

    it("is empty when the gate is flipped off") {
      val conf = new SparkConf(false).set(FeatureGate.RuntimeFilterEnabledKey, "false")
      FeatureGate.runtimeFilterConfOverrides(conf) shouldBe empty
    }
  }

  describe("FeatureGate.selectiveBroadcastEnabled") {
    it("defaults OFF and honours an explicit ON flag") {
      FeatureGate.selectiveBroadcastEnabled(new SparkConf(false)) shouldBe false
      FeatureGate.selectiveBroadcastEnabled(
        new SparkConf(false).set(FeatureGate.SelectiveBroadcastEnabledKey, "true")
      ) shouldBe true
    }
  }

  describe("FeatureGate.semiJoinPruneEnabled") {
    it("defaults ON and honours an explicit OFF flag") {
      FeatureGate.semiJoinPruneEnabled(new SparkConf(false)) shouldBe true
      FeatureGate.semiJoinPruneEnabled(
        new SparkConf(false).set(FeatureGate.SemiJoinPruneEnabledKey, "false")
      ) shouldBe false
    }
  }

  describe("FeatureGate.uniqueJoinSimplifyEnabled") {
    it("defaults OFF and honours an explicit ON flag") {
      FeatureGate.uniqueJoinSimplifyEnabled(new SparkConf(false)) shouldBe false
      FeatureGate.uniqueJoinSimplifyEnabled(
        new SparkConf(false).set(FeatureGate.UniqueJoinSimplifyEnabledKey, "true")
      ) shouldBe true
    }
  }

  describe("FeatureGate.fkTermPruneEnabled") {
    it("defaults OFF and honours an explicit ON flag") {
      FeatureGate.fkTermPruneEnabled(new SparkConf(false)) shouldBe false
      FeatureGate.fkTermPruneEnabled(
        new SparkConf(false).set(FeatureGate.FkTermPruneEnabledKey, "true")
      ) shouldBe true
    }
  }

  describe("FeatureGate.scd2RangeAccelEnabled") {
    it("defaults ON and honours an explicit OFF flag") {
      FeatureGate.scd2RangeAccelEnabled(new SparkConf(false)) shouldBe true
      FeatureGate.scd2RangeAccelEnabled(
        new SparkConf(false).set(FeatureGate.Scd2RangeAccelEnabledKey, "false")
      ) shouldBe false
    }
  }

  describe("FeatureGate.windowClusterPruneEnabled") {
    it("defaults OFF and honours an explicit ON flag") {
      FeatureGate.windowClusterPruneEnabled(new SparkConf(false)) shouldBe false
      FeatureGate.windowClusterPruneEnabled(
        new SparkConf(false).set(FeatureGate.WindowClusterPruneEnabledKey, "true")
      ) shouldBe true
    }
  }
}
