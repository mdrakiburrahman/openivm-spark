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

  describe("FeatureGate.regularNtermLiteralPruneEnabled") {
    it("defaults ON and honours an explicit OFF flag") {
      FeatureGate.regularNtermLiteralPruneEnabled(new SparkConf(false)) shouldBe true
      FeatureGate.regularNtermLiteralPruneEnabled(
        new SparkConf(false).set(FeatureGate.RegularNtermLiteralPruneEnabledKey, "false")
      ) shouldBe false
    }
  }

  describe("FeatureGate.skewFanoutEnabled") {
    it("defaults OFF and honours explicit strategy thresholds") {
      val base = new SparkConf(false)
      FeatureGate.skewFanoutEnabled(base) shouldBe false
      FeatureGate.skewFanoutNarrowDeltaRows(base) shouldBe FeatureGate.SkewFanoutDefaultNarrowDeltaRows
      FeatureGate.skewFanoutNarrowOverlapRatio(base) shouldBe FeatureGate.SkewFanoutDefaultNarrowOverlapRatio

      val tuned = new SparkConf(false)
        .set(FeatureGate.SkewFanoutEnabledKey, "true")
        .set(FeatureGate.SkewFanoutNarrowDeltaRowsKey, "123")
        .set(FeatureGate.SkewFanoutNarrowOverlapRatioKey, "0.01")
      FeatureGate.skewFanoutEnabled(tuned) shouldBe true
      FeatureGate.skewFanoutNarrowDeltaRows(tuned) shouldBe 123L
      FeatureGate.skewFanoutNarrowOverlapRatio(tuned) shouldBe 0.01d
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

  describe("FeatureGate.driverAdmission") {
    it("defaults OFF and leaves maxConcurrent effectively unbounded unless explicitly set") {
      val conf = new SparkConf(false)

      FeatureGate.driverAdmissionEnabled(conf) shouldBe false
      FeatureGate.driverAdmissionMaxConcurrent(conf) shouldBe Int.MaxValue
      FeatureGate.driverAdmissionEnabled(
        new SparkConf(false).set(FeatureGate.DriverAdmissionEnabledKey, "true")
      ) shouldBe true
      FeatureGate.driverAdmissionMaxConcurrent(
        new SparkConf(false).set(FeatureGate.DriverAdmissionMaxConcurrentKey, "7")
      ) shouldBe 7
    }
  }

  describe("FeatureGate.createCatalogPublicationMaxConcurrent") {
    it("preserves 32-way capacity by default and clamps explicit bounds to 2 through 32") {
      FeatureGate.createCatalogPublicationMaxConcurrent(new SparkConf(false)) shouldBe 32
      FeatureGate.createCatalogPublicationMaxConcurrent(
        new SparkConf(false).set(FeatureGate.CreateCatalogPublicationMaxConcurrentKey, "7")
      ) shouldBe 7
      FeatureGate.createCatalogPublicationMaxConcurrent(
        new SparkConf(false).set(FeatureGate.CreateCatalogPublicationMaxConcurrentKey, "1")
      ) shouldBe 2
      FeatureGate.createCatalogPublicationMaxConcurrent(
        new SparkConf(false).set(FeatureGate.CreateCatalogPublicationMaxConcurrentKey, "64")
      ) shouldBe 32
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

  describe("FeatureGate.scd2RangeJoinAccelEnabled") {
    it("defaults OFF and honours an explicit ON flag") {
      FeatureGate.scd2RangeJoinAccelEnabled(new SparkConf(false)) shouldBe false
      FeatureGate.scd2RangeJoinAccelEnabled(
        new SparkConf(false).set(FeatureGate.Scd2RangeJoinAccelEnabledKey, "true")
      ) shouldBe true
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

  describe("FeatureGate.windowRunningIncrementalEnabled") {
    it("defaults OFF and honours an explicit ON flag") {
      FeatureGate.windowRunningIncrementalEnabled(new SparkConf(false)) shouldBe false
      FeatureGate.windowRunningIncrementalEnabled(
        new SparkConf(false).set(FeatureGate.WindowRunningIncrementalEnabledKey, "true")
      ) shouldBe true
    }
  }

  describe("FeatureGate.statePath") {
    it("defaults to None when unset") {
      FeatureGate.statePath(new SparkConf(false)) shouldBe None
    }

    it("returns the trimmed override when set") {
      FeatureGate.statePath(
        new SparkConf(false).set(FeatureGate.StatePathKey, "  /lakehouse/default/Files/_openivm  ")
      ) shouldBe Some("/lakehouse/default/Files/_openivm")
    }

    it("treats a blank override as unset") {
      FeatureGate.statePath(new SparkConf(false).set(FeatureGate.StatePathKey, "   ")) shouldBe None
    }
  }

  describe("FeatureGate.stateSyncUri") {
    it("defaults to None and returns the trimmed URI when set") {
      FeatureGate.stateSyncUri(new SparkConf(false)) shouldBe None
      FeatureGate.stateSyncUri(
        new SparkConf(false).set(FeatureGate.StateSyncUriKey, "  abfss://ws@onelake/lh/Files/_openivm  ")
      ) shouldBe Some("abfss://ws@onelake/lh/Files/_openivm")
    }
  }

  describe("FeatureGate.autoCompactSupported") {
    it("rejects exactly one clustering column (Delta hilbert clustering asserts cols.size > 1)") {
      FeatureGate.autoCompactSupported(Seq("region")) shouldBe false
    }

    it("allows an unclustered table — Delta falls back to CompactionStrategy") {
      FeatureGate.autoCompactSupported(Nil) shouldBe true
    }

    it("allows two or more clustering columns — the hilbert curve is well defined") {
      FeatureGate.autoCompactSupported(Seq("region", "day")) shouldBe true
      FeatureGate.autoCompactSupported(Seq("region", "day", "sku")) shouldBe true
    }
  }
}
