package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.apache.spark.sql.catalyst.TableIdentifier
import org.openivm.spark.common.MvCatalog

/** Slice of `ParserSpec` covering REFRESH idempotency, INSERT+DELETE-in-the-
  * same-window net-zero deltas, DROP+recreate with a different body, and
  * `CREATE … IF NOT EXISTS` idempotency.
  *
  * Forked-JVM isolation: own SparkSession + warehouse dir per spec.
  */
abstract class ParserRefreshScenarios extends IvmParitySpecBase("parser-refresh") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  protected def mvExists(name: String): Boolean =
    MvCatalog.lookup(spark, TableIdentifier(name)).isDefined

  // ──────────────────────────────────────────────────────────────────────────
  // (10) Idempotent back-to-back REFRESH (parser.test Test 28).
  // ──────────────────────────────────────────────────────────────────────────
  describe("(10) REFRESH idempotency") {

    it("multiple back-to-back REFRESH calls with no DML in between leave the MV unchanged") {
      sql("CREATE TABLE IF NOT EXISTS idem_p10(id INT, grp STRING, val INT) USING DELTA")
      sql("INSERT INTO idem_p10 VALUES (1,'a',10),(2,'a',20)")
      sql(
        "CREATE MATERIALIZED VIEW mv_idem_p10 AS " +
          "SELECT grp, min(val) AS lo, max(val) AS hi, sum(val) AS total, count(val) AS cnt " +
          "FROM idem_p10 GROUP BY grp"
      )

      sql("INSERT INTO idem_p10 VALUES (3,'a',5)")
      refreshMv("mv_idem_p10")
      val snap1 = spark.table("mv_idem_p10").collect().toSet
      refreshMv("mv_idem_p10")
      refreshMv("mv_idem_p10")
      val snap2 = spark.table("mv_idem_p10").collect().toSet
      snap2 shouldBe snap1
      assertMvCorrect(
        "mv_idem_p10",
        "SELECT grp, min(val) AS lo, max(val) AS hi, sum(val) AS total, count(val) AS cnt " +
          "FROM idem_p10 GROUP BY grp"
      )
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (11) INSERT+DELETE of same id in one cycle (parser.test Test 29) —
  //     net effect must be zero.
  // ──────────────────────────────────────────────────────────────────────────
  describe("(11) INSERT then DELETE the same row before refresh") {

    it("net delta is zero; MV is unchanged from the pre-batch state") {
      sql("CREATE TABLE IF NOT EXISTS oscill_p11(id INT, grp STRING, val INT) USING DELTA")
      sql("INSERT INTO oscill_p11 VALUES (1,'a',10),(2,'a',20)")
      sql(
        "CREATE MATERIALIZED VIEW mv_oscill_p11 AS " +
          "SELECT grp, min(val) AS lo, max(val) AS hi, sum(val) AS total, count(val) AS cnt " +
          "FROM oscill_p11 GROUP BY grp"
      )
      val before = spark.table("mv_oscill_p11").collect().toSet

      sql("INSERT INTO oscill_p11 VALUES (99,'a',1)")
      sql("DELETE FROM oscill_p11 WHERE id = 99")
      refreshMv("mv_oscill_p11")

      spark.table("mv_oscill_p11").collect().toSet shouldBe before
      assertMvCorrect(
        "mv_oscill_p11",
        "SELECT grp, min(val) AS lo, max(val) AS hi, sum(val) AS total, count(val) AS cnt " +
          "FROM oscill_p11 GROUP BY grp"
      )
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (12) DROP MATERIALIZED VIEW removes the catalog row and lets a fresh MV
  //     of the same name be created with a different SELECT body.
  // ──────────────────────────────────────────────────────────────────────────
  describe("(12) DROP + recreate MV with a different body") {

    it("a fresh CREATE after DROP succeeds and uses the new body") {
      sql(
        "CREATE TABLE IF NOT EXISTS recreate_p12(id INT, val INT, category STRING) USING DELTA"
      )
      sql("INSERT INTO recreate_p12 VALUES (1,10,'a'),(2,20,'b')")
      sql("CREATE MATERIALIZED VIEW mv_recreate_p12 AS SELECT id, val FROM recreate_p12")
      assertMvCorrect("mv_recreate_p12", "SELECT id, val FROM recreate_p12")

      sql("DROP MATERIALIZED VIEW mv_recreate_p12")
      mvExists("mv_recreate_p12") shouldBe false

      // Re-create with an aggregate body.
      sql(
        "CREATE MATERIALIZED VIEW mv_recreate_p12 AS " +
          "SELECT category, SUM(val) AS total FROM recreate_p12 GROUP BY category"
      )
      assertMvCorrect(
        "mv_recreate_p12",
        "SELECT category, SUM(val) AS total FROM recreate_p12 GROUP BY category"
      )

      sql("INSERT INTO recreate_p12 VALUES (3, 30, 'a')")
      refreshMv("mv_recreate_p12")
      assertMvCorrect(
        "mv_recreate_p12",
        "SELECT category, SUM(val) AS total FROM recreate_p12 GROUP BY category"
      )
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (13) IF NOT EXISTS guard.
  // ──────────────────────────────────────────────────────────────────────────
  describe("(13) IF NOT EXISTS makes CREATE idempotent") {

    it("a second CREATE … IF NOT EXISTS with the same name is a no-op") {
      sql("CREATE TABLE IF NOT EXISTS ine_p13(id INT, val INT) USING DELTA")
      sql("INSERT INTO ine_p13 VALUES (1,10)")
      sql("CREATE MATERIALIZED VIEW mv_ine_p13 AS SELECT id, val FROM ine_p13")
      val before = spark.table("mv_ine_p13").collect().toSet

      // Second CREATE IF NOT EXISTS — must NOT throw and must not modify the MV.
      sql(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS mv_ine_p13 AS SELECT id, val + 99 FROM ine_p13"
      )
      spark.table("mv_ine_p13").collect().toSet shouldBe before
    }
  }
}
