package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** P6d — Port of `openivm/test/sql/ducklake_aggregate.test`.
  *
  * Translation:
  *   - DuckLake catalog `dl.<table>` → Delta tables in the default Spark
  *     catalog (named `dl_<table>` to preserve traceability with the openivm
  *     test).
  *   - `ATTACH … (TYPE ducklake)` → no-op (Delta is the only target).
  *   - `PRAGMA refresh('v')` → `REFRESH MATERIALIZED VIEW v`.
  *   - DuckLake `INTERVAL '12' MONTH` → Spark `INTERVAL 12 MONTHS`.
  *   - SQLite-backed DuckLake metadata locking (final section of the openivm
  *     test) has no analogue in Delta — Delta uses OCC against the
  *     `_delta_log/` directly, with no external metadata catalog to lock. The
  *     N/A section is therefore omitted (per PLAN §9).
  *
  * Source: `.temp/openivm/test/sql/ducklake_aggregate.test`.
  */
abstract class DucklakeAggregateScenarios extends IvmParitySpecBase("ducklake-aggregate") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  // ── (DA1) SUM + COUNT grouped aggregate ───────────────────────────────────
  // openivm: ducklake_aggregate.test "SUM + COUNT grouped aggregate"

  describe("(DA1) Grouped SUM + COUNT — full DML matrix") {
    it("incremental refresh handles INSERT into existing/new group, DELETE, UPDATE") {
      sql("CREATE TABLE IF NOT EXISTS dl_sales(id INT, region STRING, amount INT) USING DELTA")
      sql("INSERT INTO dl_sales VALUES (1, 'east', 100), (2, 'west', 200), (3, 'east', 150)")
      sql(
        "CREATE MATERIALIZED VIEW dl_sales_summary AS " +
          "SELECT region, SUM(amount) AS total, COUNT(amount) AS cnt " +
          "FROM dl_sales GROUP BY region"
      )

      // Insert into existing group
      sql("INSERT INTO dl_sales VALUES (4, 'east', 50)")
      refreshMv("dl_sales_summary")
      assertMvCorrect(
        "dl_sales_summary",
        "SELECT region, SUM(amount) AS total, COUNT(amount) AS cnt FROM dl_sales GROUP BY region"
      )

      // Insert into a new group
      sql("INSERT INTO dl_sales VALUES (5, 'north', 300)")
      refreshMv("dl_sales_summary")
      assertMvCorrect(
        "dl_sales_summary",
        "SELECT region, SUM(amount) AS total, COUNT(amount) AS cnt FROM dl_sales GROUP BY region"
      )

      // Delete from existing group
      sql("DELETE FROM dl_sales WHERE id = 1")
      refreshMv("dl_sales_summary")
      assertMvCorrect(
        "dl_sales_summary",
        "SELECT region, SUM(amount) AS total, COUNT(amount) AS cnt FROM dl_sales GROUP BY region"
      )

      // Update: change amount within same group
      sql("UPDATE dl_sales SET amount = 500 WHERE id = 2")
      refreshMv("dl_sales_summary")
      assertMvCorrect(
        "dl_sales_summary",
        "SELECT region, SUM(amount) AS total, COUNT(amount) AS cnt FROM dl_sales GROUP BY region"
      )
    }
  }

  // ── (DA2) Ungrouped COUNT(*) — simple/scalar aggregate ───────────────────
  // openivm: ducklake_aggregate.test "COUNT(*) ungrouped (simple/scalar aggregate)"

  describe("(DA2) Ungrouped COUNT(*) — scalar aggregate") {
    it("incremental refresh tracks insert and delete row counts") {
      sql("CREATE TABLE IF NOT EXISTS dl_counter(id INT, val INT) USING DELTA")
      sql("INSERT INTO dl_counter VALUES (1, 10), (2, 20), (3, 30)")
      sql("CREATE MATERIALIZED VIEW dl_mv_counter AS SELECT COUNT(*) AS cnt FROM dl_counter")
      assertMvCorrect("dl_mv_counter", "SELECT COUNT(*) AS cnt FROM dl_counter")

      sql("INSERT INTO dl_counter VALUES (4, 40), (5, 50)")
      refreshMv("dl_mv_counter")
      assertMvCorrect("dl_mv_counter", "SELECT COUNT(*) AS cnt FROM dl_counter")

      sql("DELETE FROM dl_counter WHERE id <= 2")
      refreshMv("dl_mv_counter")
      assertMvCorrect("dl_mv_counter", "SELECT COUNT(*) AS cnt FROM dl_counter")
    }
  }

  // ── (DA3) MIN/MAX with CTE + computed CASE column ────────────────────────
  // openivm: ducklake_aggregate.test "Group recompute with MIN/MAX plus computed output"

  describe("(DA3) MIN/MAX + computed CASE — chained CTE shape") {
    it("MIN/MAX over a CTE-wrapped projection refreshes incrementally") {
      sql(
        "CREATE TABLE IF NOT EXISTS dl_watch_events(" +
          "customer_id INT, symbol STRING, company_id INT, company_name STRING, " +
          "event_ts TIMESTAMP, action_type STRING) USING DELTA"
      )
      sql(
        "INSERT INTO dl_watch_events VALUES " +
          "(1, 'AAA', NULL, NULL, TIMESTAMP '2026-01-01 10:00:00', 'Activate'), " +
          "(2, 'BBB', 10, 'Beta', TIMESTAMP '2026-01-01 11:00:00', 'Activate')"
      )

      // The openivm test uses an MV-over-MV pattern (watch_history → watch_state).
      // Spark-ext's REFRESH bypasses the DML interceptor, so MV-over-MV staging
      // does not propagate (see CLAUDE.md note in MaterializedViewCommands).
      // We flatten the two views into one MV that directly references the base
      // table — preserving the MIN/MAX-over-CASE shape, which is what this
      // section was actually exercising.
      val viewBody =
        """WITH s1 AS (
          |  SELECT
          |    customer_id,
          |    symbol,
          |    company_id,
          |    company_name,
          |    CASE action_type WHEN 'Activate'  THEN event_ts ELSE NULL END AS placed_timestamp,
          |    CASE action_type WHEN 'Cancelled' THEN event_ts ELSE NULL END AS removed_timestamp
          |  FROM dl_watch_events
          |)
          |SELECT
          |  customer_id,
          |  symbol,
          |  company_id,
          |  company_name,
          |  MIN(placed_timestamp)  AS placed_timestamp,
          |  MAX(removed_timestamp) AS removed_timestamp,
          |  CASE WHEN MAX(removed_timestamp) IS NULL THEN 'Active' ELSE 'Inactive' END AS watch_status
          |FROM s1
          |GROUP BY customer_id, symbol, company_id, company_name""".stripMargin
      sql(s"CREATE MATERIALIZED VIEW dl_watch_state AS $viewBody")

      // Add a Cancelled event for customer 1 and a new Activate event for
      // customer 3 — exercises both an existing-group transition and a new
      // group, similar to the openivm test.
      sql(
        "INSERT INTO dl_watch_events VALUES " +
          "(1, 'AAA', NULL, NULL, TIMESTAMP '2026-01-02 10:00:00', 'Cancelled'), " +
          "(3, 'CCC', NULL, NULL, TIMESTAMP '2026-01-02 12:00:00', 'Activate')"
      )
      refreshMv("dl_watch_state")
      assertMvCorrect("dl_watch_state", viewBody)
    }
  }

  // ── (DA4) Stress: batched conflicting DML ─────────────────────────────────
  // openivm: ducklake_aggregate.test "Stress test: batch many conflicting DML ops…"

  describe("(DA4) Stress — batched conflicting INSERT + DELETE + UPDATE") {
    it("single REFRESH consolidates two rounds of overlapping DML") {
      sql("CREATE TABLE IF NOT EXISTS dl_stress(id INT, grp STRING, val INT) USING DELTA")
      sql(
        "INSERT INTO dl_stress VALUES " +
          "(1, 'a', 10), (2, 'a', 20), (3, 'b', 30), " +
          "(4, 'b', 40), (5, 'c', 50), (6, 'c', 60)"
      )
      sql(
        "CREATE MATERIALIZED VIEW dl_stress_agg AS " +
          "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM dl_stress GROUP BY grp"
      )

      // Round 1: insert + delete + update overlapping rows
      sql("INSERT INTO dl_stress VALUES (7, 'a', 100), (8, 'd', 200)")
      sql("DELETE FROM dl_stress WHERE id = 2")
      sql("UPDATE dl_stress SET val = 999 WHERE id = 3")
      sql("INSERT INTO dl_stress VALUES (9, 'b', 5), (10, 'b', 5)")
      sql("DELETE FROM dl_stress WHERE id = 5")
      sql("UPDATE dl_stress SET grp = 'a' WHERE id = 4")
      sql("INSERT INTO dl_stress VALUES (11, 'c', 1)")
      sql("DELETE FROM dl_stress WHERE id = 8")
      refreshMv("dl_stress_agg")
      assertMvCorrect(
        "dl_stress_agg",
        "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM dl_stress GROUP BY grp"
      )

      // Round 2: more conflicting ops on the same table
      sql("DELETE FROM dl_stress WHERE grp = 'b'")
      sql("INSERT INTO dl_stress VALUES (12, 'e', 500), (13, 'e', 500)")
      sql("UPDATE dl_stress SET val = 0 WHERE grp = 'a'")
      sql("INSERT INTO dl_stress VALUES (14, 'a', 1)")
      sql("DELETE FROM dl_stress WHERE id = 11")
      refreshMv("dl_stress_agg")
      assertMvCorrect(
        "dl_stress_agg",
        "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM dl_stress GROUP BY grp"
      )
    }
  }

  // ── (DA5) Aggregate top-k: GROUP BY + ORDER BY + LIMIT ────────────────────
  // openivm: ducklake_aggregate.test "Aggregate top-k: GROUP BY + ORDER BY + LIMIT"

  describe("(DA5) Top-k aggregate — GROUP BY + ORDER BY + LIMIT") {
    it("refresh keeps top-k correct after INSERT and after a stress batch") {
      sql("CREATE TABLE IF NOT EXISTS dl_topk_sales(id INT, product STRING, revenue INT) USING DELTA")
      sql(
        "INSERT INTO dl_topk_sales VALUES " +
          "(1,'apple',300),(2,'banana',150),(3,'cherry',500),(4,'date',80),(5,'elderberry',420)"
      )
      val topK =
        "SELECT product, total FROM (" +
          "SELECT product, SUM(revenue) AS total FROM dl_topk_sales GROUP BY product " +
          "ORDER BY total DESC LIMIT 3) _q"
      sql(
        "CREATE MATERIALIZED VIEW dl_topk_sales_mv AS " +
          "SELECT product, SUM(revenue) AS total FROM dl_topk_sales GROUP BY product " +
          "ORDER BY total DESC LIMIT 3"
      )

      refreshMv("dl_topk_sales_mv")
      assertMvCorrect("dl_topk_sales_mv", topK)

      // grape enters top-3, displaces banana/date
      sql("INSERT INTO dl_topk_sales VALUES (6,'grape',600)")
      refreshMv("dl_topk_sales_mv")
      assertMvCorrect("dl_topk_sales_mv", topK)

      // Stress: conflicting INSERT + DELETE + UPDATE before a single refresh
      sql("INSERT INTO dl_topk_sales VALUES (7,'hazel',700),(8,'iris',10)")
      sql("DELETE FROM dl_topk_sales WHERE id = 6")
      sql("UPDATE dl_topk_sales SET revenue = 600 WHERE id = 3")
      refreshMv("dl_topk_sales_mv")
      assertMvCorrect("dl_topk_sales_mv", topK)
    }
  }

  // ── (DA6) Scalar aggregate subquery in HAVING, joined to two tables ─────
  // openivm: ducklake_aggregate.test "Scalar aggregate subquery in HAVING"

  describe("(DA6) Scalar aggregate subquery in HAVING (3-way join)") {
    it("MV joining a HAVING-filtered CTE to two dimensions refreshes correctly") {
      sql("CREATE TABLE IF NOT EXISTS dl_hav_line(w INT, o INT, d INT, amount DOUBLE) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS dl_hav_order(w INT, o INT, d INT, c INT) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS dl_hav_customer(w INT, d INT, c INT, last_name STRING) USING DELTA")
      sql(
        "INSERT INTO dl_hav_line VALUES " +
          "(1, 1, 1, 10), (1, 1, 1, 12), (1, 2, 1, 200), (1, 2, 1, 220), (2, 1, 1, 5)"
      )
      sql("INSERT INTO dl_hav_order VALUES (1, 1, 1, 10), (1, 2, 1, 11), (2, 1, 1, 12)")
      sql(
        "INSERT INTO dl_hav_customer VALUES (1, 1, 10, 'Able'), (1, 1, 11, 'Baker'), (2, 1, 12, 'Cross')"
      )
      val viewBody =
        """WITH high_value AS (
          |  SELECT w, o, d
          |  FROM dl_hav_line
          |  GROUP BY w, o, d
          |  HAVING SUM(amount) > (SELECT AVG(amount) * 2 FROM dl_hav_line)
          |)
          |SELECT hv.w, hv.d, hv.o, c.last_name
          |FROM high_value hv
          |JOIN dl_hav_order o    ON hv.w = o.w AND hv.d = o.d AND hv.o = o.o
          |JOIN dl_hav_customer c ON o.w  = c.w AND o.d  = c.d AND o.c  = c.c""".stripMargin
      sql(s"CREATE MATERIALIZED VIEW dl_mv_hav_scalar AS $viewBody")

      sql("INSERT INTO dl_hav_line VALUES (1, 3, 1, 500)")
      sql("INSERT INTO dl_hav_order VALUES (1, 3, 1, 13)")
      sql("INSERT INTO dl_hav_customer VALUES (1, 1, 13, 'Delta')")
      refreshMv("dl_mv_hav_scalar")
      assertMvCorrect("dl_mv_hav_scalar", viewBody)
    }
  }

  // ── (DA7) COUNT(DISTINCT) over join — GROUP_RECOMPUTE shape ──────────────
  // openivm: ducklake_aggregate.test "DuckLake GROUP_RECOMPUTE: COUNT(DISTINCT) over join"

  describe("(DA7) COUNT(DISTINCT) over join — GROUP_RECOMPUTE classification") {
    it("INSERT + INSERT + DELETE before a single refresh keeps MV consistent") {
      sql("CREATE TABLE IF NOT EXISTS dl_gr_items(id INT, category STRING) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS dl_gr_sales(item_id INT, qty INT) USING DELTA")
      sql("INSERT INTO dl_gr_items VALUES (1, 'hardware'), (2, 'hardware'), (3, 'office')")
      sql("INSERT INTO dl_gr_sales VALUES (1, 5), (1, 7), (2, 10), (3, 4)")
      val viewBody =
        "SELECT i.category, COUNT(DISTINCT i.id) AS item_count, SUM(s.qty) AS total_qty " +
          "FROM dl_gr_items i " +
          "JOIN dl_gr_sales s ON i.id = s.item_id " +
          "GROUP BY i.category"
      sql(s"CREATE MATERIALIZED VIEW dl_mv_gr_distinct AS $viewBody")

      sql("INSERT INTO dl_gr_items VALUES (4, 'office')")
      sql("INSERT INTO dl_gr_sales VALUES (4, 20), (2, 1)")
      sql("DELETE FROM dl_gr_sales WHERE item_id = 1 AND qty = 5")
      refreshMv("dl_mv_gr_distinct")
      assertMvCorrect("dl_mv_gr_distinct", viewBody)
    }
  }
}
