package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.openivm.spark.common.MvCatalog

/** Port of `openivm/test/sql/ducklake_cte.test`.
  *
  * Translation:
  *   - DuckLake catalog `dl.<table>` → Delta tables in the default Spark
  *     catalog (named `dl_<table>` to preserve traceability with the openivm
  *     test).
  *   - `ATTACH … (TYPE ducklake)` → no-op.
  *   - `PRAGMA refresh('v')` → `REFRESH MATERIALIZED VIEW v`.
  *   - openivm's "MV definition does not contain AT VERSION" assertion verifies
  *     that the stored view query does not pin a DuckLake snapshot. In Delta
  *     the equivalent assertion is that the stored `querySql` does not
  *     reference Delta time-travel syntax (`VERSION AS OF` / `TIMESTAMP AS OF`)
  *     — openivm-spark stores the original query unchanged.
  *
  * Source: `.temp/openivm/test/sql/ducklake_cte.test`.
  */
abstract class DucklakeCteScenarios extends IvmParitySpecBase("ducklake-cte") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  protected def storedQuerySql(name: String): String = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    MvCatalog.lookup(spark, id).getOrElse(fail(s"MV $name not in catalog")).querySql
  }

  // ── (DC1) Stored MV definition must not pin a snapshot version ────────────
  // openivm: ducklake_cte.test "MV definition should NOT contain AT VERSION"

  describe("(DC1) Stored querySql does not reference a Delta time-travel pin") {
    it("MvCatalog records the original user query without VERSION AS OF") {
      sql("CREATE TABLE IF NOT EXISTS dl_products(id INT, name STRING, price DECIMAL(10,2)) USING DELTA")
      sql("INSERT INTO dl_products VALUES (1, 'Widget', 9.99), (2, 'Gadget', 19.99)")
      sql(
        "CREATE MATERIALIZED VIEW dl_mv_prices AS " +
          "SELECT name, SUM(price) AS total, COUNT(*) AS cnt FROM dl_products GROUP BY name"
      )

      val storedSql = storedQuerySql("dl_mv_prices")
      withClue(s"Stored MV SQL: $storedSql") {
        // Equivalent of openivm's "sql_string LIKE '%dl.main.products%'" — the
        // MV must reference its source table by name.
        storedSql.toLowerCase should include("dl_products")
        // Equivalent of openivm's "sql_string NOT LIKE '%AT (VERSION%'" — no
        // Delta time-travel pin should leak into the stored query.
        storedSql.toUpperCase should not include "VERSION AS OF"
        storedSql.toUpperCase should not include "TIMESTAMP AS OF"
        storedSql.toUpperCase should not include "AT (VERSION"
      }
    }
  }

  // ── (DC2) CTE round-trip — incremental refresh after INSERT ──────────────
  // openivm: ducklake_cte.test "Verify incremental refresh produces correct results"
  // (the CTE SQL round-trips correctly through compile → store → refresh)

  describe("(DC2) CTE round-trip — INSERT then refresh") {
    it("incremental refresh produces correct grouped aggregate after CTE compilation") {
      sql("CREATE TABLE IF NOT EXISTS dl_products2(id INT, name STRING, price DECIMAL(10,2)) USING DELTA")
      sql("INSERT INTO dl_products2 VALUES (1, 'Widget', 9.99), (2, 'Gadget', 19.99)")
      sql(
        "CREATE MATERIALIZED VIEW dl_mv_prices2 AS " +
          "SELECT name, SUM(price) AS total, COUNT(*) AS cnt FROM dl_products2 GROUP BY name"
      )

      sql("INSERT INTO dl_products2 VALUES (3, 'Widget', 5.00)")
      refreshMv("dl_mv_prices2")
      assertMvCorrect(
        "dl_mv_prices2",
        "SELECT name, SUM(price) AS total, COUNT(*) AS cnt FROM dl_products2 GROUP BY name"
      )
    }
  }

  // ── (DC3) Filter + aggregate combined ────────────────────────────────────
  // openivm: ducklake_cte.test "Combined operators: filter + aggregate over DuckLake"

  describe("(DC3) Combined filter + aggregate") {
    it("incremental refresh keeps WHERE-filtered groups consistent") {
      sql(
        "CREATE TABLE IF NOT EXISTS dl_orders_cte(id INT, region STRING, amount INT, status STRING) USING DELTA"
      )
      sql(
        "INSERT INTO dl_orders_cte VALUES " +
          "(1, 'east', 100, 'active'), (2, 'west', 200, 'active'), (3, 'east', 150, 'cancelled')"
      )
      sql(
        "CREATE MATERIALIZED VIEW dl_mv_active AS " +
          "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt " +
          "FROM dl_orders_cte WHERE status = 'active' GROUP BY region"
      )

      assertMvCorrect(
        "dl_mv_active",
        "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt " +
          "FROM dl_orders_cte WHERE status = 'active' GROUP BY region"
      )

      sql("INSERT INTO dl_orders_cte VALUES (4, 'east', 50, 'active'), (5, 'west', 75, 'cancelled')")
      refreshMv("dl_mv_active")
      assertMvCorrect(
        "dl_mv_active",
        "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt " +
          "FROM dl_orders_cte WHERE status = 'active' GROUP BY region"
      )
    }
  }

  // ── (DC4) Repeated aggregate CTE under cross join ────────────────────────
  // openivm: ducklake_cte.test "Repeated aggregate CTE under join"
  // openivm classifies this as GROUP_RECOMPUTE (type 6); we let the spark-ext
  // classifier choose its own assembler and verify only end-state correctness.

  describe("(DC4) Repeated aggregate CTE under CROSS JOIN") {
    it("CTE reused twice (once directly, once via subquery aggregate) refreshes correctly") {
      sql("CREATE TABLE IF NOT EXISTS dl_cte_reuse(a INT, b INT) USING DELTA")
      sql("INSERT INTO dl_cte_reuse VALUES (1, 10), (1, 20), (2, 5)")
      val viewBody =
        """WITH s AS (
          |  SELECT a, SUM(b) AS sb
          |  FROM dl_cte_reuse
          |  GROUP BY a
          |)
          |SELECT s.a, s.sb, g.av
          |FROM s
          |CROSS JOIN (SELECT AVG(sb) AS av FROM s) g""".stripMargin
      sql(s"CREATE MATERIALIZED VIEW dl_mv_cte_reuse AS $viewBody")

      // Insert a new row that changes the per-group SUM and (via the
      // re-aggregation through the CROSS JOIN) the global AVG.
      sql("INSERT INTO dl_cte_reuse VALUES (2, 15)")
      refreshMv("dl_mv_cte_reuse")
      assertMvCorrect("dl_mv_cte_reuse", viewBody)
    }
  }
}
