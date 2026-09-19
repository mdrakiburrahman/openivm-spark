package org.openivm.spark.telemetry

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

/** Unit tests for [[KvLogValue]].
  *
  * The hazard these lock down is a `TIMESTAMP AS OF '2024-01-01'` pin clause
  * reaching an `[openivm-mv]` classification line verbatim. Those lines are not
  * JSON: readers tokenize them with a `(\w+)='([^']*)'` scanner, so an embedded
  * quote closes the field early and the reader silently keeps a TRUNCATED pin —
  * dropping the very value a pin audit exists to check. The regex used here is
  * deliberately the same shape as the downstream consumer's.
  */
class KvLogValueSpec extends AnyFunSpec with Matchers {

  private val KvRe = """(\w+)='([^']*)'""".r

  private def parse(line: String): Map[String, String] =
    KvRe.findAllMatchIn(line).map(m => m.group(1) -> m.group(2)).toMap

  describe("sanitize") {
    it("replaces single quotes so a value can never close its own field") {
      KvLogValue.sanitize("db.t=TIMESTAMP AS OF '2024-01-01'") shouldBe
        """db.t=TIMESTAMP AS OF "2024-01-01""""
    }

    it("flattens control characters that would break line-oriented reading") {
      KvLogValue.sanitize("a\nb\tc\rd") shouldBe "a b c d"
    }

    it("leaves an already-safe value untouched and treats null as empty") {
      KvLogValue.sanitize("db.t=VERSION AS OF 366") shouldBe "db.t=VERSION AS OF 366"
      KvLogValue.sanitize(null) shouldBe ""
      KvLogValue.isSafe("db.t=VERSION AS OF 366") shouldBe true
      KvLogValue.isSafe("db.t=TIMESTAMP AS OF '2024-01-01'") shouldBe false
      KvLogValue.isSafe(null) shouldBe false
    }
  }

  describe("render / renderIfPresent") {
    it("emits a single-quoted field") {
      KvLogValue.render("time_travel_pin_status", "APPLIED") shouldBe "time_travel_pin_status='APPLIED'"
    }

    it("omits an absent or blank optional field entirely") {
      KvLogValue.renderIfPresent("time_travel_pins", null) shouldBe ""
      KvLogValue.renderIfPresent("time_travel_pins", "  ") shouldBe ""
      KvLogValue.renderIfPresent("time_travel_pins", "db.t=VERSION AS OF 1") shouldBe
        " time_travel_pins='db.t=VERSION AS OF 1'"
    }
  }

  describe("downstream KV parsing") {
    it("keeps every following field correctly bound when a pin carries quotes") {
      val line =
        "[openivm-mv] view='`db`.`mv`' " +
          KvLogValue.render("time_travel_pin_status", "APPLIED") +
          KvLogValue.renderIfPresent("time_travel_pin_reason", "pins_resolved") +
          KvLogValue.renderIfPresent("time_travel_pins", "db.t=TIMESTAMP AS OF '2024-01-01'") +
          " upstream_snapshot_trigger='db.upstream'"

      val fields = parse(line)
      fields("time_travel_pin_status") shouldBe "APPLIED"
      fields("time_travel_pin_reason") shouldBe "pins_resolved"
      fields("time_travel_pins") shouldBe """db.t=TIMESTAMP AS OF "2024-01-01""""
      fields("upstream_snapshot_trigger") shouldBe "db.upstream"
    }

    it("demonstrates the truncation an unsanitized clause would cause") {
      val unsafe =
        "[openivm-mv] time_travel_pins='db.t=TIMESTAMP AS OF '2024-01-01'' " +
          "upstream_snapshot_trigger='db.upstream'"

      val fields = parse(unsafe)
      // The value closes its own field: the reader keeps only the prefix, and
      // the pinned VALUE — the part a pin audit is about — is lost.
      fields("time_travel_pins") shouldBe "db.t=TIMESTAMP AS OF "
      fields("time_travel_pins") should not include "2024-01-01"

      parse(
        "[openivm-mv] " + KvLogValue.render("time_travel_pins", "db.t=TIMESTAMP AS OF '2024-01-01'")
      )("time_travel_pins") shouldBe """db.t=TIMESTAMP AS OF "2024-01-01""""
    }
  }
}
