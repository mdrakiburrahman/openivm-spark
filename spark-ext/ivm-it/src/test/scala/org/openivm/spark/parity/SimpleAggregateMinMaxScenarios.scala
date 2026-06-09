package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** MIN/MAX + edge-case slice of the SIMPLE_AGGREGATE parity coverage. Covers
  * scalar MIN/MAX via SIMPLE_AGGREGATE, empty-table behavior, INSERT+DELETE
  * cancellation, mixed batched DML, and NULL-input handling. See
  * [[SimpleAggregateSumSpec]] for SUM/COUNT/AVG/STDDEV/VARIANCE tests.
  */
abstract class SimpleAggregateMinMaxScenarios extends IvmParitySpecBase("simple-aggregate-min-max") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  // ── (7) MIN/MAX scalar aggregates — SIMPLE_AGGREGATE path ────────────────

  describe("(7) Scalar MIN and MAX aggregates (SIMPLE_AGGREGATE, not FULL_REFRESH)") {
    it("multi-scalar MV with MAX refreshes correctly via SIMPLE_AGGREGATE incremental path") {
      sql("CREATE TABLE IF NOT EXISTS sa_sales_7(id INT, amount INT) USING DELTA")
      sql("INSERT INTO sa_sales_7 VALUES (1, 100), (2, 200), (3, 50)")
      sql(
        "CREATE MATERIALIZED VIEW mv_sa_7 AS " +
          "SELECT SUM(amount) AS sum_amount, COUNT(*) AS count_star, AVG(amount) AS avg_amount, MAX(amount) AS most FROM sa_sales_7"
      )
      assertMvCorrect(
        "mv_sa_7",
        "SELECT SUM(amount) AS sum_amount, COUNT(*) AS count_star, AVG(amount) AS avg_amount, MAX(amount) AS most FROM sa_sales_7"
      )
      sql("INSERT INTO sa_sales_7 VALUES (4, 350)")
      refreshMv("mv_sa_7")
      assertMvCorrect(
        "mv_sa_7",
        "SELECT SUM(amount) AS sum_amount, COUNT(*) AS count_star, AVG(amount) AS avg_amount, MAX(amount) AS most FROM sa_sales_7"
      )
    }

    it("scalar MIN MV refreshes correctly via SIMPLE_AGGREGATE incremental path") {
      sql("CREATE TABLE IF NOT EXISTS sa_sales_7b(id INT, amount INT) USING DELTA")
      sql("INSERT INTO sa_sales_7b VALUES (1, 100), (2, 200)")
      sql(
        "CREATE MATERIALIZED VIEW mv_sa_7b AS SELECT MIN(amount) AS least FROM sa_sales_7b"
      )
      assertMvCorrect("mv_sa_7b", "SELECT MIN(amount) AS least FROM sa_sales_7b")
      sql("INSERT INTO sa_sales_7b VALUES (3, 50)")
      refreshMv("mv_sa_7b")
      assertMvCorrect("mv_sa_7b", "SELECT MIN(amount) AS least FROM sa_sales_7b")
    }
  }

  // ── (8) Empty base table → 1 NULL-aggregate row → INSERT → REFRESH ────────

  describe("(8) Empty base table → single NULL-aggregate row → INSERT → REFRESH") {
    it("initial MV over empty table has 1 row with NULL SUM; INSERT+REFRESH propagates") {
      sql("CREATE TABLE IF NOT EXISTS sa_sales_8(id INT, amount INT) USING DELTA")
      sql(
        "CREATE MATERIALIZED VIEW mv_sa_8 AS SELECT SUM(amount) AS total FROM sa_sales_8"
      )

      // SQL semantics: scalar SUM over empty input → 1 row with NULL value
      val emptyExpected = sql("SELECT SUM(amount) AS total FROM sa_sales_8").collect()
      emptyExpected should have length 1
      emptyExpected.head.isNullAt(0) shouldBe true

      // Initial MV must match
      assertMvCorrect("mv_sa_8", "SELECT SUM(amount) AS total FROM sa_sales_8")

      // After INSERT and REFRESH the MV must reflect the new data
      sql("INSERT INTO sa_sales_8 VALUES (1, 100), (2, 200)")
      refreshMv("mv_sa_8")
      assertMvCorrect("mv_sa_8", "SELECT SUM(amount) AS total FROM sa_sales_8")
    }
  }

  // ── (9) INSERT then DELETE-back of same row → net-zero delta ─────────────

  describe("(9) INSERT then DELETE-back of same row → zero net delta") {
    it("MV value is unchanged after a cancelling INSERT+DELETE pair") {
      sql("CREATE TABLE IF NOT EXISTS sa_sales_9(id INT, amount INT) USING DELTA")
      sql("INSERT INTO sa_sales_9 VALUES (1, 100), (2, 200)")
      sql(
        "CREATE MATERIALIZED VIEW mv_sa_9 AS SELECT SUM(amount) AS total FROM sa_sales_9"
      )
      // INSERT a row then immediately DELETE it — net delta is zero
      sql("INSERT INTO sa_sales_9 VALUES (3, 50)")
      sql("DELETE FROM sa_sales_9 WHERE id = 3")
      refreshMv("mv_sa_9")
      // base table is back to (1,100),(2,200) → total = 300
      assertMvCorrect("mv_sa_9", "SELECT SUM(amount) AS total FROM sa_sales_9")
    }
  }

  // ── (10) Mixed batched DML before single REFRESH ───────────────────────────

  describe("(10) Mixed batched DML (INSERT + DELETE + UPDATE) → single REFRESH") {
    it("MV equals view body after mixed DML and a single REFRESH") {
      sql("CREATE TABLE IF NOT EXISTS sa_sales_10(id INT, amount INT) USING DELTA")
      sql("INSERT INTO sa_sales_10 VALUES (1, 100), (2, 200), (3, 50)")
      sql(
        "CREATE MATERIALIZED VIEW mv_sa_10 AS " +
          "SELECT SUM(amount) AS total, COUNT(*) AS n FROM sa_sales_10"
      )
      sql("INSERT INTO sa_sales_10 VALUES (4, 75), (5, 125)")
      sql("DELETE FROM sa_sales_10 WHERE id = 2")
      sql("UPDATE sa_sales_10 SET amount = 300 WHERE id = 3")
      refreshMv("mv_sa_10")
      assertMvCorrect(
        "mv_sa_10",
        "SELECT SUM(amount) AS total, COUNT(*) AS n FROM sa_sales_10"
      )
    }
  }

  // ── (11) NULL aggregate inputs — SUM/COUNT ignore NULLs ──────────────────

  describe("(11) NULL aggregate inputs — SUM and COUNT ignore NULLs") {
    it("SUM ignores NULL amounts; incremental refresh preserves that behavior") {
      sql("CREATE TABLE IF NOT EXISTS sa_sales_11(id INT, amount INT) USING DELTA")
      sql("INSERT INTO sa_sales_11 VALUES (1, 100), (2, NULL), (3, 50), (4, NULL)")
      sql(
        "CREATE MATERIALIZED VIEW mv_sa_11 AS SELECT SUM(amount) AS total FROM sa_sales_11"
      )
      assertMvCorrect("mv_sa_11", "SELECT SUM(amount) AS total FROM sa_sales_11")
      // Add more NULLs and one real value
      sql("INSERT INTO sa_sales_11 VALUES (5, NULL), (6, 75)")
      refreshMv("mv_sa_11")
      assertMvCorrect("mv_sa_11", "SELECT SUM(amount) AS total FROM sa_sales_11")
    }
  }
}
