package org.openivm.spark.common

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

/**
 * Pure parsing coverage for the symbolic `startingVersion` strategies
 * (issue #63). Resolution against a live `DeltaLog` is covered by the
 * streaming-table specs in `ivm-extension`.
 */
class DeltaStreamStartingVersionSpec extends AnyFunSpec with Matchers {

  it("recognizes the three symbolic strategies case-insensitively") {
    DeltaStreamStartingVersion.parse("earliest") shouldBe Some(DeltaStreamStartingVersion.Earliest)
    DeltaStreamStartingVersion.parse("Earliest") shouldBe Some(DeltaStreamStartingVersion.Earliest)
    DeltaStreamStartingVersion.parse("EARLIESTAVAILABLE") shouldBe Some(DeltaStreamStartingVersion.EarliestAvailable)
    DeltaStreamStartingVersion.parse("earliestAvailable") shouldBe Some(DeltaStreamStartingVersion.EarliestAvailable)
    DeltaStreamStartingVersion.parse("latestInclusive") shouldBe Some(DeltaStreamStartingVersion.LatestInclusive)
    DeltaStreamStartingVersion.parse("LATESTINCLUSIVE") shouldBe Some(DeltaStreamStartingVersion.LatestInclusive)
  }

  it("trims surrounding whitespace before matching") {
    DeltaStreamStartingVersion.parse("  latestInclusive  ") shouldBe Some(DeltaStreamStartingVersion.LatestInclusive)
  }

  it("leaves numeric literals unresolved so Delta's own parsing applies") {
    DeltaStreamStartingVersion.parse("0") shouldBe None
    DeltaStreamStartingVersion.parse("53174") shouldBe None
  }

  it("leaves native 'latest' unresolved so Delta's own exclusive semantics apply") {
    DeltaStreamStartingVersion.parse("latest") shouldBe None
    DeltaStreamStartingVersion.parse("Latest") shouldBe None
  }

  it("rejects unrelated text") {
    DeltaStreamStartingVersion.parse("earliest-ish") shouldBe None
    DeltaStreamStartingVersion.parse("") shouldBe None
    DeltaStreamStartingVersion.parse(null) shouldBe None
  }

  it("exposes a stable canonical display name per strategy") {
    DeltaStreamStartingVersion.Earliest.canonicalName shouldBe "earliest"
    DeltaStreamStartingVersion.EarliestAvailable.canonicalName shouldBe "earliestAvailable"
    DeltaStreamStartingVersion.LatestInclusive.canonicalName shouldBe "latestInclusive"
  }
}
