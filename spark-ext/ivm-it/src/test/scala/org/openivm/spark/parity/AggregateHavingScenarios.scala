package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** Comprehensive parity tests for RefreshType 4 — AGGREGATE_HAVING.
  *
  * OpenIVM classifies any `GROUP BY … HAVING <pred>` view as AGGREGATE_HAVING.
  * Its data table stores ALL groups (no HAVING) so a group can be re-promoted
  * into the HAVING-passing set when its aggregate later crosses the threshold.
  * The user-facing view applies the HAVING predicate at read time.
  *
  * These tests exercise:
  *   1.  SUM-threshold HAVING                    — basic.
  *   2.  COUNT(*)-threshold HAVING               — count-based predicate.
  *   3.  AVG BETWEEN range HAVING                — average within bounds.
  *   4.  Composite GROUP BY + multi-predicate    — AND/OR over two columns.
  *   5.  Multiple aggregates in SELECT; HAVING on a subset.
  *   6.  DELETE that pulls a group out of the HAVING set.
  *   7.  INSERT that promotes a group into the HAVING set (incremental — the
  *       MV must not "forget" groups whose sum was previously below threshold).
  *   8.  Net-zero INSERT + DELETE in one batch — MV unchanged.
  *   9.  Mixed batched DML on overlapping groups.
  *  10.  NULL group keys and NULL aggregate inputs.
  *  11.  Empty result table after refresh (every group fails predicate).
  *
  * Each test verifies the MV against a fresh re-computation of the view body
  * via bidirectional EXCEPT ALL (mirrors AggregateGroupSpec).
  */
abstract class AggregateHavingScenarios extends IvmParitySpecBase("aggregate-having") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  // ── Shape 1: SUM-threshold HAVING ─────────────────────────────────────────

  describe("(1) HAVING SUM(amount) > X — basic threshold") {
    it("incremental refresh tracks SUM threshold correctly across INSERTs") {
      sql("CREATE TABLE IF NOT EXISTS sales_h1(region STRING, amount INT) USING DELTA")
      sql("INSERT INTO sales_h1 VALUES ('a', 100), ('a', 200), ('b', 50), ('c', 10)")
      sql(
        "CREATE MATERIALIZED VIEW mv_h1 AS " +
          "SELECT region, SUM(amount) AS total FROM sales_h1 " +
          "GROUP BY region HAVING SUM(amount) > 60"
      )
      sql("INSERT INTO sales_h1 VALUES ('b', 100), ('c', 200)")
      refreshMv("mv_h1")
      assertMvCorrect(
        "mv_h1",
        "SELECT region, SUM(amount) AS total FROM sales_h1 " +
          "GROUP BY region HAVING SUM(amount) > 60"
      )
    }
  }

  // ── Shape 2: COUNT(*)-based HAVING ─────────────────────────────────────────

  describe("(2) HAVING COUNT(*) >= K — count-based predicate") {
    it("incremental refresh tracks COUNT(*) threshold correctly") {
      sql("CREATE TABLE IF NOT EXISTS sales_h2(region STRING, amount INT) USING DELTA")
      sql(
        "INSERT INTO sales_h2 VALUES " +
          "('a', 10), ('a', 20), ('a', 30), ('b', 50), ('c', 70), ('c', 80)"
      )
      sql(
        "CREATE MATERIALIZED VIEW mv_h2 AS " +
          "SELECT region, COUNT(*) AS cnt FROM sales_h2 GROUP BY region HAVING COUNT(*) >= 3"
      )
      sql("INSERT INTO sales_h2 VALUES ('c', 90), ('b', 60), ('b', 75)")
      refreshMv("mv_h2")
      assertMvCorrect(
        "mv_h2",
        "SELECT region, COUNT(*) AS cnt FROM sales_h2 GROUP BY region HAVING COUNT(*) >= 3"
      )
    }
  }

  // ── Shape 3: AVG BETWEEN — range predicate ─────────────────────────────────

  describe("(3) HAVING AVG(amount) BETWEEN A AND B — range predicate") {
    // Uses exact-integer values so the AVG decomposition (SUM/COUNT) yields
    // bit-exact equality against Spark's native AVG.
    it("incremental refresh tracks AVG range predicate") {
      sql("CREATE TABLE IF NOT EXISTS sales_h3(region STRING, amount DOUBLE) USING DELTA")
      sql(
        "INSERT INTO sales_h3 VALUES " +
          "('a', 10.0), ('a', 30.0), ('b', 100.0), ('b', 200.0), ('c', 5.0), ('c', 15.0)"
      )
      sql(
        "CREATE MATERIALIZED VIEW mv_h3 AS " +
          "SELECT region, AVG(amount) AS avg_amt FROM sales_h3 " +
          "GROUP BY region HAVING AVG(amount) BETWEEN 10.0 AND 50.0"
      )
      // Push 'a' avg from 20.0 (passes) to 40.0 (still passes).
      // Push 'c' avg from 10.0 (passes) to 5.0 (still passes after adding two more 0.0).
      // 'b' stays at 150.0 (fails).
      sql("INSERT INTO sales_h3 VALUES ('a', 50.0), ('a', 50.0), ('d', 25.0), ('d', 35.0)")
      refreshMv("mv_h3")
      assertMvCorrect(
        "mv_h3",
        "SELECT region, AVG(amount) AS avg_amt FROM sales_h3 " +
          "GROUP BY region HAVING AVG(amount) BETWEEN 10.0 AND 50.0"
      )
    }
  }

  // ── Shape 4: Composite GROUP BY + AND/OR HAVING ───────────────────────────

  describe("(4a) Composite GROUP BY (k1, k2) HAVING two predicates joined by AND") {
    it("incremental refresh tracks AND-combined HAVING predicates") {
      sql(
        "CREATE TABLE IF NOT EXISTS sales_h4a(region STRING, sku INT, qty INT) USING DELTA"
      )
      sql(
        "INSERT INTO sales_h4a VALUES " +
          "('a', 1, 10), ('a', 1, 20), ('a', 2, 5), ('b', 1, 100), " +
          "('b', 2, 50), ('b', 2, 50), ('c', 3, 200)"
      )
      sql(
        "CREATE MATERIALIZED VIEW mv_h4a AS " +
          "SELECT region, sku, SUM(qty) AS total, COUNT(*) AS cnt " +
          "FROM sales_h4a GROUP BY region, sku " +
          "HAVING SUM(qty) > 25 AND COUNT(*) >= 2"
      )
      sql("INSERT INTO sales_h4a VALUES ('a', 2, 30), ('a', 2, 40), ('c', 3, 50)")
      refreshMv("mv_h4a")
      assertMvCorrect(
        "mv_h4a",
        "SELECT region, sku, SUM(qty) AS total, COUNT(*) AS cnt " +
          "FROM sales_h4a GROUP BY region, sku " +
          "HAVING SUM(qty) > 25 AND COUNT(*) >= 2"
      )
    }
  }

  describe("(4b) Composite GROUP BY (k1, k2) HAVING two predicates joined by OR") {
    it("incremental refresh tracks OR-combined HAVING predicates") {
      sql(
        "CREATE TABLE IF NOT EXISTS sales_h4b(region STRING, sku INT, qty INT) USING DELTA"
      )
      sql(
        "INSERT INTO sales_h4b VALUES " +
          "('a', 1, 10), ('a', 1, 20), ('b', 2, 5), ('b', 2, 5), ('b', 2, 5), " +
          "('c', 3, 1000), ('d', 4, 1)"
      )
      sql(
        "CREATE MATERIALIZED VIEW mv_h4b AS " +
          "SELECT region, sku, SUM(qty) AS total, COUNT(*) AS cnt " +
          "FROM sales_h4b GROUP BY region, sku " +
          "HAVING SUM(qty) > 100 OR COUNT(*) >= 3"
      )
      // 'b' originally has cnt=3 → passes via OR branch. Drop one row → cnt=2 → still fails one branch.
      // 'a' total is 30, cnt=2 → fails both. Add huge qty → total > 100 → passes via SUM branch.
      sql("DELETE FROM sales_h4b WHERE region = 'b' AND qty = 5 AND sku = 2")
      sql("INSERT INTO sales_h4b VALUES ('a', 1, 200)")
      refreshMv("mv_h4b")
      assertMvCorrect(
        "mv_h4b",
        "SELECT region, sku, SUM(qty) AS total, COUNT(*) AS cnt " +
          "FROM sales_h4b GROUP BY region, sku " +
          "HAVING SUM(qty) > 100 OR COUNT(*) >= 3"
      )
    }
  }

  // ── Shape 5: Multiple aggregates, HAVING on a subset ──────────────────────

  describe("(5) Multiple aggregates in SELECT, HAVING references one") {
    it("HAVING on SUM works while AVG/COUNT projections also stay in sync") {
      sql("CREATE TABLE IF NOT EXISTS sales_h5(region STRING, amount INT) USING DELTA")
      sql(
        "INSERT INTO sales_h5 VALUES " +
          "('a', 10), ('a', 30), ('b', 5), ('b', 5), ('c', 100), ('c', 100)"
      )
      sql(
        "CREATE MATERIALIZED VIEW mv_h5 AS " +
          "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt, AVG(amount) AS avg_amt " +
          "FROM sales_h5 GROUP BY region HAVING SUM(amount) > 30"
      )
      sql("INSERT INTO sales_h5 VALUES ('b', 50), ('b', 50), ('d', 200)")
      refreshMv("mv_h5")
      assertMvCorrect(
        "mv_h5",
        "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt, AVG(amount) AS avg_amt " +
          "FROM sales_h5 GROUP BY region HAVING SUM(amount) > 30"
      )
    }
  }

  // ── Shape 6: Row transitions OUT of HAVING set after DELETE ───────────────

  describe("(6) Row transitions OUT after DELETE — group sum falls below threshold") {
    it("group disappears from MV when its sum falls below the HAVING threshold") {
      sql("CREATE TABLE IF NOT EXISTS sales_h6(region STRING, amount INT) USING DELTA")
      sql(
        "INSERT INTO sales_h6 VALUES " +
          "('a', 100), ('a', 50), ('b', 80), ('b', 20), ('c', 200)"
      )
      sql(
        "CREATE MATERIALIZED VIEW mv_h6 AS " +
          "SELECT region, SUM(amount) AS total FROM sales_h6 " +
          "GROUP BY region HAVING SUM(amount) > 75"
      )
      // 'b' starts at 100 (passes). Remove the 80 row → 'b' sum = 20 (fails).
      sql("DELETE FROM sales_h6 WHERE region = 'b' AND amount = 80")
      refreshMv("mv_h6")
      assertMvCorrect(
        "mv_h6",
        "SELECT region, SUM(amount) AS total FROM sales_h6 " +
          "GROUP BY region HAVING SUM(amount) > 75"
      )
    }
  }

  // ── Shape 7: Row transitions INTO HAVING set after INSERT ─────────────────

  describe("(7) Row transitions IN after INSERT — group sum rises above threshold") {
    it("previously-failing group appears in MV after INSERT pushes it above threshold") {
      sql("CREATE TABLE IF NOT EXISTS sales_h7(region STRING, amount INT) USING DELTA")
      // 'c' starts below threshold (fails); after INSERT it crosses above.
      sql(
        "INSERT INTO sales_h7 VALUES ('a', 100), ('a', 50), ('b', 200), ('c', 20)"
      )
      sql(
        "CREATE MATERIALIZED VIEW mv_h7 AS " +
          "SELECT region, SUM(amount) AS total FROM sales_h7 " +
          "GROUP BY region HAVING SUM(amount) > 60"
      )
      // Sanity: 'c' should not be in MV pre-refresh
      assertMvCorrect(
        "mv_h7",
        "SELECT region, SUM(amount) AS total FROM sales_h7 " +
          "GROUP BY region HAVING SUM(amount) > 60"
      )
      // Promote 'c': add 50 more, raising sum to 70 → passes.
      sql("INSERT INTO sales_h7 VALUES ('c', 50)")
      refreshMv("mv_h7")
      assertMvCorrect(
        "mv_h7",
        "SELECT region, SUM(amount) AS total FROM sales_h7 " +
          "GROUP BY region HAVING SUM(amount) > 60"
      )
    }
  }

  // ── Shape 8: Net-zero INSERT + DELETE — MV unchanged ──────────────────────

  describe("(8) Net-zero INSERT+DELETE within one batch — MV unchanged") {
    it("MV equals base table when inserted rows are also deleted in same batch") {
      sql("CREATE TABLE IF NOT EXISTS sales_h8(region STRING, amount INT) USING DELTA")
      sql("INSERT INTO sales_h8 VALUES ('a', 100), ('a', 50), ('b', 200)")
      sql(
        "CREATE MATERIALIZED VIEW mv_h8 AS " +
          "SELECT region, SUM(amount) AS total FROM sales_h8 " +
          "GROUP BY region HAVING SUM(amount) > 100"
      )
      val before = spark.table("mv_h8").select("region", "total").collect().toSet
      // Insert and then delete the same row — net delta = 0.
      sql("INSERT INTO sales_h8 VALUES ('z', 999)")
      sql("DELETE FROM sales_h8 WHERE region = 'z'")
      refreshMv("mv_h8")
      assertMvCorrect(
        "mv_h8",
        "SELECT region, SUM(amount) AS total FROM sales_h8 " +
          "GROUP BY region HAVING SUM(amount) > 100"
      )
      val after = spark.table("mv_h8").select("region", "total").collect().toSet
      after shouldBe before
    }
  }

  // ── Shape 9: Mixed batched DML on overlapping groups ──────────────────────

  describe("(9) Mixed batched DML — INSERT + DELETE + UPDATE on overlapping groups") {
    it("single REFRESH reconciles all conflicting deltas correctly") {
      sql("CREATE TABLE IF NOT EXISTS sales_h9(region STRING, amount INT) USING DELTA")
      sql(
        "INSERT INTO sales_h9 VALUES " +
          "('a', 100), ('a', 50), ('b', 60), ('b', 20), ('c', 300), ('d', 10)"
      )
      sql(
        "CREATE MATERIALIZED VIEW mv_h9 AS " +
          "SELECT region, SUM(amount) AS total FROM sales_h9 " +
          "GROUP BY region HAVING SUM(amount) > 75"
      )
      // INSERT pushes 'd' from 10 (fails) into passing territory.
      sql("INSERT INTO sales_h9 VALUES ('d', 200), ('e', 500), ('a', 25)")
      // DELETE drops one 'b' row → sum=60 (still below 75).
      sql("DELETE FROM sales_h9 WHERE region = 'b' AND amount = 20")
      // DELETE removes 'c' entirely (passes → disappears).
      sql("DELETE FROM sales_h9 WHERE region = 'c'")
      refreshMv("mv_h9")
      assertMvCorrect(
        "mv_h9",
        "SELECT region, SUM(amount) AS total FROM sales_h9 " +
          "GROUP BY region HAVING SUM(amount) > 75"
      )
    }
  }

  // ── Shape 10: NULL group keys and NULL aggregate inputs ───────────────────

  describe("(10) NULL group keys and NULL aggregate inputs") {
    it("NULL key is a valid group; NULL amounts are ignored by SUM per SQL semantics") {
      sql("CREATE TABLE IF NOT EXISTS sales_h10(region STRING, amount INT) USING DELTA")
      sql(
        "INSERT INTO sales_h10 VALUES " +
          "('a', 100), ('a', NULL), (NULL, 50), (NULL, 80), ('b', NULL), ('b', NULL)"
      )
      sql(
        "CREATE MATERIALIZED VIEW mv_h10 AS " +
          "SELECT region, SUM(amount) AS total FROM sales_h10 " +
          "GROUP BY region HAVING SUM(amount) > 60"
      )
      sql("INSERT INTO sales_h10 VALUES (NULL, 40), ('c', 200), ('c', NULL)")
      refreshMv("mv_h10")
      assertMvCorrect(
        "mv_h10",
        "SELECT region, SUM(amount) AS total FROM sales_h10 " +
          "GROUP BY region HAVING SUM(amount) > 60"
      )
    }
  }

  // ── Shape 11: Empty result table after refresh ────────────────────────────

  describe("(11) All groups fail HAVING after refresh — MV is empty") {
    it("MV correctly reports empty when every group's aggregate falls below threshold") {
      sql("CREATE TABLE IF NOT EXISTS sales_h11(region STRING, amount INT) USING DELTA")
      sql("INSERT INTO sales_h11 VALUES ('a', 1000), ('b', 800)")
      sql(
        "CREATE MATERIALIZED VIEW mv_h11 AS " +
          "SELECT region, SUM(amount) AS total FROM sales_h11 " +
          "GROUP BY region HAVING SUM(amount) > 500"
      )
      // Delete enough to pull every group below 500.
      sql("DELETE FROM sales_h11 WHERE region = 'a'")
      sql("DELETE FROM sales_h11 WHERE region = 'b'")
      // Add small groups so the underlying table is non-empty but no group passes.
      sql("INSERT INTO sales_h11 VALUES ('a', 10), ('b', 20), ('c', 30)")
      refreshMv("mv_h11")
      assertMvCorrect(
        "mv_h11",
        "SELECT region, SUM(amount) AS total FROM sales_h11 " +
          "GROUP BY region HAVING SUM(amount) > 500"
      )
      // Explicit: zero rows.
      spark.table("mv_h11").select("region", "total").count() shouldBe 0L
    }
  }
}
