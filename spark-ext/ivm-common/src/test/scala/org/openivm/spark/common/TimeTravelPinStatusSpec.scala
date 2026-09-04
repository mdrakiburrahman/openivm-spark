package org.openivm.spark.common

import org.openivm.spark.telemetry.KvLogValue
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

/** Unit tests for the [[TimeTravelPinStatus]] / [[TimeTravelPinReason]]
  * telemetry vocabulary.
  *
  * The contract these lock down is fail-closed normalization: a downstream
  * campaign guard decides whether a materialized result may be attributed to a
  * pinned source manifest purely from this status, so a value it does not
  * recognise must never soften into `NOT_APPLICABLE` ("no pin here") or vanish
  * into a missing field.
  */
class TimeTravelPinStatusSpec extends AnyFunSpec with Matchers {

  describe("TimeTravelPinStatus.normalize") {
    it("accepts the contract values case- and whitespace-insensitively") {
      TimeTravelPinStatus.normalize("applied") shouldBe Some(TimeTravelPinStatus.Applied)
      TimeTravelPinStatus.normalize("  Not_Applicable  ") shouldBe Some(TimeTravelPinStatus.NotApplicable)
      TimeTravelPinStatus.normalize("COMPILE_FAILED") shouldBe Some(TimeTravelPinStatus.CompileFailed)
    }

    it("reports an unknown, blank or absent value as not-a-contract-value") {
      TimeTravelPinStatus.normalize("MAYBE") shouldBe None
      TimeTravelPinStatus.normalize("  ") shouldBe None
      TimeTravelPinStatus.normalize(null) shouldBe None
    }
  }

  describe("TimeTravelPinStatus.normalizeOrRefuse") {
    it("passes contract values through unchanged") {
      TimeTravelPinStatus.normalizeOrRefuse("applied") shouldBe Some(TimeTravelPinStatus.Applied)
      TimeTravelPinStatus.normalizeOrRefuse(TimeTravelPinStatus.NotApplicable) shouldBe
        Some(TimeTravelPinStatus.NotApplicable)
    }

    it("refuses a present-but-unrecognised value instead of dropping or softening it") {
      TimeTravelPinStatus.normalizeOrRefuse("MAYBE") shouldBe Some(TimeTravelPinStatus.CompileFailed)
      TimeTravelPinStatus.normalizeOrRefuse("APPLIED_ISH") shouldBe Some(TimeTravelPinStatus.CompileFailed)
      TimeTravelPinStatus.normalizeOrRefuse("MAYBE") should not be Some(TimeTravelPinStatus.NotApplicable)
    }

    it("leaves a genuinely absent value absent") {
      TimeTravelPinStatus.normalizeOrRefuse(null) shouldBe None
      TimeTravelPinStatus.normalizeOrRefuse("   ") shouldBe None
    }
  }

  describe("TimeTravelPinStatus.isApplied") {
    it("is true only for the exact contract value") {
      TimeTravelPinStatus.isApplied(TimeTravelPinStatus.Applied) shouldBe true
      TimeTravelPinStatus.isApplied("applied") shouldBe false
      TimeTravelPinStatus.isApplied(TimeTravelPinStatus.NotApplicable) shouldBe false
    }
  }

  describe("TimeTravelPinReason") {
    it("normalizes known tokens and drops unknown ones") {
      TimeTravelPinReason.normalize("  PINS_RESOLVED ") shouldBe Some(TimeTravelPinReason.PinsResolved)
      TimeTravelPinReason.normalize("who_knows") shouldBe None
      TimeTravelPinReason.normalize(null) shouldBe None
    }

    it("keeps every token loggable verbatim in the single-quoted KV format") {
      TimeTravelPinReason.All.foreach { reason =>
        KvLogValue.isSafe(reason) shouldBe true
        reason should fullyMatch regex "[a-z][a-z_]*"
      }
      TimeTravelPinStatus.All.foreach(status => KvLogValue.isSafe(status) shouldBe true)
    }
  }
}
