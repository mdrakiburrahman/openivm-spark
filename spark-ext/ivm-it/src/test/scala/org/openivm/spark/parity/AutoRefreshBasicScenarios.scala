package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** First half of the auto_refresh parity port — basic projection / aggregate
  * IVM behavior (small / large delta, deletes, multi-cycle). See
  * [[AutoRefreshFilterSpec]] for empty-delta, MIN/MAX, mode-dial and
  * filter-selectivity scenarios.
  */
abstract class AutoRefreshBasicScenarios extends IvmParitySpecBase("auto-refresh-basic") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  protected def insertSeq(table: String, rows: Seq[String]): Unit =
    sql(s"INSERT INTO $table VALUES ${rows.mkString(", ")}")

  // ============================================================================
  // (1) Out-of-scope documentation: REFRESH EVERY scheduling has no Spark analogue
  // ============================================================================
  describe("(1) Out-of-scope: REFRESH EVERY scheduling daemon (PLAN §12)") {
    it("CREATE MATERIALIZED VIEW … REFRESH EVERY '5 minutes' is not supported syntax") {
      val ex = intercept[Exception] {
        sql(
          "CREATE MATERIALIZED VIEW mv_ar_doc REFRESH EVERY '5 minutes' AS SELECT 1 AS x"
        )
      }
      ex.getMessage should not be null
    }
  }

  // ============================================================================
  // (2) Projection — small delta (openivm picks IVM)
  // ============================================================================
  describe("(2) Projection MV — small delta → IVM path on openivm (Spark routes equivalently)") {
    it("100 base rows + 1 INSERT + REFRESH → 101 rows, correct contents") {
      sql("CREATE TABLE IF NOT EXISTS proj_base_ar(id INT, name STRING) USING DELTA")
      val initial = (1 to 100).map(i => s"($i, 'name_$i')")
      insertSeq("proj_base_ar", initial)

      sql("CREATE MATERIALIZED VIEW mv_proj_ar AS SELECT id, name FROM proj_base_ar")
      spark.table("mv_proj_ar").count() shouldBe 100L

      sql("INSERT INTO proj_base_ar VALUES (101, 'new_name')")
      refreshMv("mv_proj_ar")
      spark.table("mv_proj_ar").count() shouldBe 101L
      assertMvCorrect("mv_proj_ar", "SELECT id, name FROM proj_base_ar")

      // Spot-check the new row
      val row = spark.table("mv_proj_ar").where("id = 101").collect()
      row.length shouldBe 1
      row.head.getString(row.head.fieldIndex("name")) shouldBe "new_name"
    }
  }

  // ============================================================================
  // (3) Projection — large delta (openivm picks full recompute)
  // ============================================================================
  describe("(3) Projection MV — large delta (openivm switches to full recompute)") {
    it("101 base rows + 300 INSERT + REFRESH → 401 rows, MV correct") {
      sql("CREATE TABLE IF NOT EXISTS proj_base_ar2(id INT, name STRING) USING DELTA")
      insertSeq("proj_base_ar2", (1 to 100).map(i => s"($i, 'name_$i')"))
      sql("CREATE MATERIALIZED VIEW mv_proj_ar2 AS SELECT id, name FROM proj_base_ar2")

      // Add 1 row, refresh.
      sql("INSERT INTO proj_base_ar2 VALUES (101, 'new_name')")
      refreshMv("mv_proj_ar2")

      // Now the "large delta" — 300 rows.
      insertSeq("proj_base_ar2", (102 to 401).map(i => s"($i, 'batch_$i')"))
      refreshMv("mv_proj_ar2")

      spark.table("mv_proj_ar2").count() shouldBe 401L
      assertMvCorrect("mv_proj_ar2", "SELECT id, name FROM proj_base_ar2")
    }
  }

  // ============================================================================
  // (4) Aggregate — small delta (IVM in openivm)
  // ============================================================================
  describe("(4) Aggregate MV — small INSERT delta") {
    it("200 base rows + 2 INSERTs into the same group + REFRESH → group totals correct") {
      sql("CREATE TABLE IF NOT EXISTS agg_base_ar(id INT, grp INT, val INT) USING DELTA")
      insertSeq("agg_base_ar", (1 to 200).map(i => s"($i, ${i % 10}, ${i * 3})"))

      sql(
        "CREATE MATERIALIZED VIEW mv_agg_ar AS " +
          "SELECT grp, SUM(val) AS total, COUNT(val) AS cnt FROM agg_base_ar GROUP BY grp"
      )
      spark.table("mv_agg_ar").count() shouldBe 10L

      sql("INSERT INTO agg_base_ar VALUES (201, 0, 999), (202, 0, 1)")
      refreshMv("mv_agg_ar")

      assertMvCorrect(
        "mv_agg_ar",
        "SELECT grp, SUM(val) AS total, COUNT(val) AS cnt FROM agg_base_ar GROUP BY grp"
      )
    }
  }

  // ============================================================================
  // (5) Projection — delete-majority triggers recompute in openivm
  // ============================================================================
  describe("(5) Projection MV — DELETE majority then REFRESH") {
    it("100 base rows → DELETE 80 → REFRESH → 20 rows, MV correct") {
      sql("CREATE TABLE IF NOT EXISTS del_base_ar(id INT, val INT) USING DELTA")
      insertSeq("del_base_ar", (1 to 100).map(i => s"($i, ${i * 10})"))

      sql("CREATE MATERIALIZED VIEW mv_del_ar AS SELECT id, val FROM del_base_ar")
      spark.table("mv_del_ar").count() shouldBe 100L

      sql("DELETE FROM del_base_ar WHERE id > 20")
      refreshMv("mv_del_ar")

      spark.table("mv_del_ar").count() shouldBe 20L
      assertMvCorrect("mv_del_ar", "SELECT id, val FROM del_base_ar")
    }
  }

  // ============================================================================
  // (6) Aggregate — DELETE half the rows, MV is correct
  // ============================================================================
  describe("(6) Aggregate MV — DELETE half (large delta on aggregate)") {
    it("50 rows split even/odd → DELETE all even → REFRESH → only 'odd' group remains") {
      sql("CREATE TABLE IF NOT EXISTS agg_del_ar(id INT, grp STRING, val INT) USING DELTA")
      val rows = (1 to 50).map(i => s"($i, '${if (i % 2 == 0) "even" else "odd"}', $i)")
      insertSeq("agg_del_ar", rows)

      sql(
        "CREATE MATERIALIZED VIEW mv_agg_del_ar AS " +
          "SELECT grp, SUM(val) AS total, COUNT(val) AS cnt FROM agg_del_ar GROUP BY grp"
      )

      sql("DELETE FROM agg_del_ar WHERE grp = 'even'")
      refreshMv("mv_agg_del_ar")

      assertMvCorrect(
        "mv_agg_del_ar",
        "SELECT grp, SUM(val) AS total, COUNT(val) AS cnt FROM agg_del_ar GROUP BY grp"
      )

      // Only 'odd' group should remain
      val rowsRemaining = spark.table("mv_agg_del_ar").collect()
      rowsRemaining.length shouldBe 1
    }
  }

  // ============================================================================
  // (7) Multi-cycle — three refresh cycles, MV correct after each
  // ============================================================================
  describe("(7) Multi-cycle REFRESH — small + small + large") {
    it("each REFRESH between varying deltas keeps the MV correct") {
      sql("CREATE TABLE IF NOT EXISTS multi_ar(id INT, grp INT, val INT) USING DELTA")
      insertSeq("multi_ar", (1 to 100).map(i => s"($i, ${i % 5}, $i)"))

      sql(
        "CREATE MATERIALIZED VIEW mv_multi_ar AS SELECT grp, SUM(val) AS total FROM multi_ar GROUP BY grp"
      )

      // Cycle 1
      sql("INSERT INTO multi_ar VALUES (101, 0, 500)")
      refreshMv("mv_multi_ar")
      assertMvCorrect("mv_multi_ar", "SELECT grp, SUM(val) AS total FROM multi_ar GROUP BY grp")

      // Cycle 2
      sql("INSERT INTO multi_ar VALUES (102, 1, 1000)")
      refreshMv("mv_multi_ar")
      assertMvCorrect("mv_multi_ar", "SELECT grp, SUM(val) AS total FROM multi_ar GROUP BY grp")

      // Cycle 3 — large batch
      insertSeq("multi_ar", (200 to 499).map(i => s"($i, ${i % 5}, ${i * 2})"))
      refreshMv("mv_multi_ar")
      assertMvCorrect("mv_multi_ar", "SELECT grp, SUM(val) AS total FROM multi_ar GROUP BY grp")
    }
  }
}
