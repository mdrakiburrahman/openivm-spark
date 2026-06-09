package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** Second half of the auto_refresh parity port — empty-delta no-op, MIN/MAX
  * aggregate refresh, multi-step DML, refresh-mode dial, filter selectivity
  * and mixed DML on a selective filter. See [[AutoRefreshBasicSpec]] for the
  * basic projection / aggregate scenarios.
  */
abstract class AutoRefreshFilterScenarios extends IvmParitySpecBase("auto-refresh-filter") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  protected def insertSeq(table: String, rows: Seq[String]): Unit =
    sql(s"INSERT INTO $table VALUES ${rows.mkString(", ")}")

  // ============================================================================
  // (8) Empty delta — REFRESH is a no-op
  // ============================================================================
  describe("(8) Empty delta — REFRESH is a no-op") {
    it("REFRESH on an MV with no pending DML leaves contents unchanged") {
      sql("CREATE TABLE IF NOT EXISTS noop_ar(id INT, val INT) USING DELTA")
      insertSeq("noop_ar", (1 to 10).map(i => s"($i, ${i * 10})"))
      sql("CREATE MATERIALIZED VIEW mv_noop_ar AS SELECT id, val FROM noop_ar")

      val before = spark.table("mv_noop_ar").collect().toSet
      refreshMv("mv_noop_ar")
      val after = spark.table("mv_noop_ar").collect().toSet
      after shouldBe before
      assertMvCorrect("mv_noop_ar", "SELECT id, val FROM noop_ar")
    }
  }

  // ============================================================================
  // (9) MIN/MAX aggregate — small INSERT + DELETE current MIN
  // ============================================================================
  describe("(9) MIN/MAX aggregate — INSERT new min, DELETE it, recompute MIN") {
    it("aggregate stays correct after INSERT (new min) and subsequent DELETE (current min)") {
      sql("CREATE TABLE IF NOT EXISTS minmax_ar(id INT, grp STRING, val INT) USING DELTA")
      val rows = (1 to 60).map { i =>
        val g = i % 3 match {
          case 0 => "a"; case 1 => "b"; case _ => "c"
        }
        s"($i, '$g', $i)"
      }
      insertSeq("minmax_ar", rows)

      sql(
        "CREATE MATERIALIZED VIEW mv_minmax_ar AS " +
          "SELECT grp, MIN(val) AS lo, MAX(val) AS hi, COUNT(val) AS cnt FROM minmax_ar GROUP BY grp"
      )

      // Insert a new row that becomes the new MIN for group 'a'
      sql("INSERT INTO minmax_ar VALUES (61, 'a', 0)")
      refreshMv("mv_minmax_ar")
      assertMvCorrect(
        "mv_minmax_ar",
        "SELECT grp, MIN(val) AS lo, MAX(val) AS hi, COUNT(val) AS cnt FROM minmax_ar GROUP BY grp"
      )

      // Delete that new row — recompute MIN must restore the prior value
      sql("DELETE FROM minmax_ar WHERE id = 61")
      refreshMv("mv_minmax_ar")
      assertMvCorrect(
        "mv_minmax_ar",
        "SELECT grp, MIN(val) AS lo, MAX(val) AS hi, COUNT(val) AS cnt FROM minmax_ar GROUP BY grp"
      )
    }
  }

  // ============================================================================
  // (10) Cross-check — multi-step INSERT/DELETE/large INSERT
  // ============================================================================
  describe("(10) Multi-step INSERT / DELETE / large INSERT — MV correct at each step") {
    it("aggregate stays bag-equal to the live view body across three refresh cycles") {
      sql("CREATE TABLE IF NOT EXISTS xcheck_ar(id INT, cat STRING, amount INT) USING DELTA")
      val rows = (1 to 80).map(i => s"($i, '${('A' + (i % 4)).toChar}', ${i * 10})")
      insertSeq("xcheck_ar", rows)

      sql(
        "CREATE MATERIALIZED VIEW mv_xcheck_ar AS " +
          "SELECT cat, SUM(amount) AS total, COUNT(amount) AS n FROM xcheck_ar GROUP BY cat"
      )

      // (1) Small insert
      sql("INSERT INTO xcheck_ar VALUES (81, 'A', 9999)")
      refreshMv("mv_xcheck_ar")
      assertMvCorrect(
        "mv_xcheck_ar",
        "SELECT cat, SUM(amount) AS total, COUNT(amount) AS n FROM xcheck_ar GROUP BY cat"
      )

      // (2) Delete a whole category
      sql("DELETE FROM xcheck_ar WHERE cat = 'D'")
      refreshMv("mv_xcheck_ar")
      assertMvCorrect(
        "mv_xcheck_ar",
        "SELECT cat, SUM(amount) AS total, COUNT(amount) AS n FROM xcheck_ar GROUP BY cat"
      )

      // (3) Large batch into a single category
      insertSeq("xcheck_ar", (1000 to 1299).map(i => s"($i, 'A', 1)"))
      refreshMv("mv_xcheck_ar")
      assertMvCorrect(
        "mv_xcheck_ar",
        "SELECT cat, SUM(amount) AS total, COUNT(amount) AS n FROM xcheck_ar GROUP BY cat"
      )
    }
  }

  // ============================================================================
  // (11) Mode dial out of scope — manual REFRESH always works on Spark
  // ============================================================================
  describe("(11) openivm_refresh_mode dial is out of scope — manual REFRESH always works") {
    it("regardless of internal path choice, result equals live view body") {
      sql("CREATE TABLE IF NOT EXISTS mode_ar(id INT, grp STRING, val INT) USING DELTA")
      insertSeq(
        "mode_ar",
        (1 to 50).map(i => s"($i, '${if (i % 2 == 0) "even" else "odd"}', $i)")
      )
      sql(
        "CREATE MATERIALIZED VIEW mv_mode_ar AS " +
          "SELECT grp, SUM(val) AS total, COUNT(val) AS cnt FROM mode_ar GROUP BY grp"
      )

      // 1 row insert
      sql("INSERT INTO mode_ar VALUES (51, 'even', 999)")
      refreshMv("mv_mode_ar")
      assertMvCorrect(
        "mv_mode_ar",
        "SELECT grp, SUM(val) AS total, COUNT(val) AS cnt FROM mode_ar GROUP BY grp"
      )

      // 300 row insert
      insertSeq("mode_ar", (100 to 399).map(i => s"($i, 'odd', 1)"))
      refreshMv("mv_mode_ar")
      assertMvCorrect(
        "mv_mode_ar",
        "SELECT grp, SUM(val) AS total, COUNT(val) AS cnt FROM mode_ar GROUP BY grp"
      )
    }
  }

  // ============================================================================
  // (12) Filter selectivity — selective predicate + matching inserts
  // ============================================================================
  describe("(12) Highly selective filter — inserts that pass") {
    it("100 base rows pass filter; 100 more inserts pass filter; MV reaches 200 rows") {
      sql("CREATE TABLE IF NOT EXISTS sel_base_ar(id INT, val INT) USING DELTA")
      insertSeq("sel_base_ar", (1 to 10000).map(i => s"($i, $i)"))

      sql(
        "CREATE MATERIALIZED VIEW mv_sel_ar AS SELECT id, val FROM sel_base_ar WHERE val > 9900"
      )
      spark.table("mv_sel_ar").count() shouldBe 100L

      insertSeq("sel_base_ar", (10001 to 10100).map(i => s"($i, $i)"))
      refreshMv("mv_sel_ar")
      spark.table("mv_sel_ar").count() shouldBe 200L
      assertMvCorrect("mv_sel_ar", "SELECT id, val FROM sel_base_ar WHERE val > 9900")
    }
  }

  // ============================================================================
  // (13) Filter selectivity — inserts that fail predicate (MV unchanged)
  // ============================================================================
  describe("(13) Inserts that fail the filter leave the MV unchanged") {
    it("INSERTing non-passing rows is reflected as no MV growth after REFRESH") {
      sql("CREATE TABLE IF NOT EXISTS sel_fail_ar(id INT, val INT) USING DELTA")
      insertSeq("sel_fail_ar", (1 to 1000).map(i => s"($i, $i)"))
      sql(
        "CREATE MATERIALIZED VIEW mv_selfail_ar AS SELECT id, val FROM sel_fail_ar WHERE val > 9900"
      )
      val before = spark.table("mv_selfail_ar").count()

      // Insert 200 rows all with val = 0 — fail the predicate
      insertSeq("sel_fail_ar", (2001 to 2200).map(i => s"($i, 0)"))
      refreshMv("mv_selfail_ar")
      val after = spark.table("mv_selfail_ar").count()
      after shouldBe before
      assertMvCorrect("mv_selfail_ar", "SELECT id, val FROM sel_fail_ar WHERE val > 9900")
    }
  }

  // ============================================================================
  // (14) Mixed DML on a selective filter
  // ============================================================================
  describe("(14) Mixed DML on a selective filter — insert pass, insert fail, delete pass") {
    it("MV correctly reflects every transition under bidirectional EXCEPT ALL") {
      sql("CREATE TABLE IF NOT EXISTS sel_mix_ar(id INT, val INT) USING DELTA")
      insertSeq("sel_mix_ar", (1 to 10000).map(i => s"($i, $i)"))
      sql(
        "CREATE MATERIALIZED VIEW mv_selmix_ar AS SELECT id, val FROM sel_mix_ar WHERE val > 9900"
      )

      // pass-insert
      sql("INSERT INTO sel_mix_ar VALUES (30001, 99999)")
      // fail-insert
      sql("INSERT INTO sel_mix_ar VALUES (30002, 1)")
      // delete a passing row
      sql("DELETE FROM sel_mix_ar WHERE id = 9950")

      refreshMv("mv_selmix_ar")
      assertMvCorrect("mv_selmix_ar", "SELECT id, val FROM sel_mix_ar WHERE val > 9900")
    }
  }
}
