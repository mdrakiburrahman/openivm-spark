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
}
