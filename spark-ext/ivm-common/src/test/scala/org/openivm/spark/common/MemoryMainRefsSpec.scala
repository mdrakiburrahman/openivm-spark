package org.openivm.spark.common

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

/** Unit tests for [[MemoryMainRefs]].
  *
  * The hazard these lock down is a prefix collision between two source names.
  * Rewriting `memory.main.<short>` with a plain `String.replace` per source —
  * over an unordered collection, as the compiler's initial-load reattach used
  * to do — also matches the prefix of a longer source name, which either
  * injects a snapshot pin into the middle of an identifier or consumes the
  * longer name so ITS pin is never re-attached and the CTAS silently loads live
  * rows.
  */
class MemoryMainRefsSpec extends AnyFunSpec with Matchers {

  /** The rewrite the compiler's initial load performs. */
  private def reattach(sql: String, qualified: Map[String, String], pins: Map[String, String]): String =
    MemoryMainRefs.rewrite(sql) { short =>
      val relation = qualified.getOrElse(short, short)
      pins.get(short).fold(relation)(clause => s"$relation $clause")
    }

  describe("rewrite") {
    it("never lets a shorter pinned source bleed into a longer one") {
      val sql = "SELECT c.id, a.city FROM memory.main.customer c JOIN memory.main.customer_address a ON a.id = c.id"
      val out = reattach(
        sql,
        qualified = Map("customer" -> "db.customer", "customer_address" -> "db.customer_address"),
        pins = Map("customer" -> "VERSION AS OF 3")
      )
      out shouldBe
        "SELECT c.id, a.city FROM db.customer VERSION AS OF 3 c JOIN db.customer_address a ON a.id = c.id"
      out should not include "3_address"
    }

    it("keeps the pin of the LONGER source when only it is pinned") {
      val sql = "SELECT c.id, a.city FROM memory.main.customer c JOIN memory.main.customer_address a ON a.id = c.id"
      val out = reattach(
        sql,
        qualified = Map("customer" -> "db.customer", "customer_address" -> "db.customer_address"),
        pins = Map("customer_address" -> "VERSION AS OF 7")
      )
      out shouldBe
        "SELECT c.id, a.city FROM db.customer c JOIN db.customer_address VERSION AS OF 7 a ON a.id = c.id"
      """(?i)db\.customer\s+VERSION""".r.findFirstIn(out) shouldBe None
    }

    it("pins both sides of a prefix pair independently") {
      val out = reattach(
        "SELECT 1 FROM memory.main.customer_address a JOIN memory.main.customer c ON c.id = a.id",
        qualified = Map("customer" -> "db.customer", "customer_address" -> "db.customer_address"),
        pins = Map("customer" -> "VERSION AS OF 3", "customer_address" -> "VERSION AS OF 7")
      )
      out shouldBe
        "SELECT 1 FROM db.customer_address VERSION AS OF 7 a JOIN db.customer VERSION AS OF 3 c ON c.id = a.id"
    }

    it("leaves openivm's internal relations as bare short names") {
      reattach(
        "INSERT INTO memory.main.openivm_delta_mv SELECT * FROM memory.main.customer",
        qualified = Map("customer" -> "db.customer"),
        pins = Map.empty
      ) shouldBe "INSERT INTO openivm_delta_mv SELECT * FROM db.customer"
    }

    it("rewrites the backticked multipart spelling too") {
      reattach(
        "SELECT * FROM `memory`.`main`.`customer_address`",
        qualified = Map("customer_address" -> "db.customer_address"),
        pins = Map("customer_address" -> "VERSION AS OF 7")
      ) shouldBe "SELECT * FROM db.customer_address VERSION AS OF 7"
    }

    it("does not match a `memory` segment inside a longer identifier") {
      val sql = "SELECT * FROM my_memory.main.customer"
      reattach(sql, qualified = Map("customer" -> "db.customer"), pins = Map.empty) shouldBe sql
    }

    it("keeps regex metacharacters in the replacement inert") {
      MemoryMainRefs.rewrite("SELECT * FROM memory.main.t")(_ => "db.`t$1\\x`") shouldBe
        "SELECT * FROM db.`t$1\\x`"
    }

    it("is a no-op for SQL without the prefix, and null-safe") {
      MemoryMainRefs.rewrite("SELECT 1")(_ => "x") shouldBe "SELECT 1"
      MemoryMainRefs.rewrite(null)(_ => "x") shouldBe null
    }
  }
}
