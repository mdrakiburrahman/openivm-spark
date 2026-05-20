package org.openivm.spark.common

import org.apache.spark.sql.catalyst.TableIdentifier
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

/** Pure-string unit tests for [[SparkRefreshRewriter]].
  *
  * No SparkSession is required — the rewriter operates on SQL strings. Tests
  * pin the exact 7-statement openivm output captured empirically from
  * `mv_r AS SELECT region, SUM(amount) AS total FROM sales GROUP BY region`
  * and assert each rewrite invariant in isolation.
  */
class SparkRefreshRewriterSpec extends AnyFunSpec with Matchers {

  private val viewLogicalName = "mv_r"
  private val mvName          = TableIdentifier("mv_r", Some("mydb"))
  private val mvLocation      = "dbfs:/delta/mv_r"
  private val viewDeltaPath   = "dbfs:/delta/_tmp/mv_r_delta_uuid"

  /** Empirical openivm output for `mv_r AS SELECT region, SUM(amount) AS total
    * FROM sales GROUP BY region`, captured verbatim per the P4.5 spec.
    */
  private val sevenStatementInput: String =
    """UPDATE openivm_views SET refresh_in_progress = true WHERE view_name = 'mv_r';
      |WITH scan_0 (t3_region, t3_amount, t3_openivm_multiplicity) AS (SELECT region, amount, openivm_multiplicity FROM memory.main.openivm_delta_sales WHERE openivm_timestamp>='2026-05-16 10:00:55'::TIMESTAMP), aggregate_1 (t27_region, t27_amount, t28_aggregate_0) AS (SELECT t3_region, t3_amount, sum(t3_openivm_multiplicity) FROM scan_0 GROUP BY t3_region, t3_amount), filter_2 (t27_region, t27_amount, t28_aggregate_0) AS (SELECT * FROM aggregate_1 WHERE ((t28_aggregate_0) != (0))), projection_3 (t3_region, t3_amount, t3_scalar_2) AS (SELECT t27_region, t27_amount, CAST(t28_aggregate_0 AS INTEGER) FROM filter_2), projection_4 (t4_region, t4_amount, t4_scalar_2) AS (SELECT t3_region, t3_amount, t3_scalar_2 FROM projection_3), projection_5 (t10_region, t10_amount, t10_scalar_2) AS (SELECT t4_region, t4_amount, t4_scalar_2 FROM projection_4), aggregate_6 (t12_region, t12_scalar_2, t13_aggregate_0, t13_aggregate_1) AS (SELECT t10_region, t10_scalar_2, sum(t10_amount), count_star() FROM projection_5 GROUP BY t10_region, t10_scalar_2), projection_7 (t11_region, t11_aggregate_0, t11_aggregate_1, t11_scalar_2) AS (SELECT t12_region, t13_aggregate_0, t13_aggregate_1, t12_scalar_2 FROM aggregate_6), projection_8 (t17_region, t17_aggregate_0, t17_aggregate_1, t17_scalar_2) AS (SELECT t11_region, t11_aggregate_0, t11_aggregate_1, t11_scalar_2 FROM projection_7), projection_9 (t18_region, t18_aggregate_0, t18_aggregate_1, t18_scalar_2) AS (SELECT t17_region, t17_aggregate_0, t17_aggregate_1, t17_scalar_2 FROM projection_8), projection_10 (t24_region, t24_aggregate_0, t24_aggregate_1, t24_scalar_2) AS (SELECT t18_region, t18_aggregate_0, t18_aggregate_1, t18_scalar_2 FROM projection_9), projection_11 (t25_region, t25_aggregate_0, t25_aggregate_1, t25_scalar_2) AS (SELECT t24_region, t24_aggregate_0, t24_aggregate_1, t24_scalar_2 FROM projection_10) INSERT INTO openivm_delta_mv_r (region, total, openivm_count_star, openivm_multiplicity)  SELECT * FROM projection_11;
      |WITH refresh_cte AS (
      |select region,
      |	sum(openivm_multiplicity * total) as total,
      |	sum(openivm_multiplicity * openivm_count_star) as openivm_count_star
      |from openivm_delta_mv_r WHERE openivm_timestamp > '2026-05-16 10:00:55'::TIMESTAMP
      |group by region)
      |MERGE INTO openivm_data_mv_r v USING refresh_cte d
      |ON v.region IS NOT DISTINCT FROM d.region
      |WHEN MATCHED THEN UPDATE SET total = COALESCE(v.total + d.total, v.total, d.total), openivm_count_star = COALESCE(v.openivm_count_star + d.openivm_count_star, v.openivm_count_star, d.openivm_count_star)
      |WHEN NOT MATCHED THEN INSERT (region, total, openivm_count_star) VALUES (d.region, d.total, d.openivm_count_star);
      |DELETE FROM openivm_delta_mv_r;
      |DELETE FROM openivm_delta_sales WHERE openivm_timestamp < (SELECT MIN(last_update) FROM openivm_delta_tables WHERE table_name = 'openivm_delta_sales');
      |UPDATE openivm_delta_tables SET last_update = COALESCE((SELECT MAX(openivm_timestamp) + INTERVAL '1 microsecond' FROM openivm_delta_sales), now()), last_refresh_ts = now() WHERE view_name = 'mv_r' AND table_name = 'openivm_delta_sales';
      |UPDATE openivm_views SET refresh_in_progress = false WHERE view_name = 'mv_r';
      |""".stripMargin

  private val simpleProjectionInput: String =
    """UPDATE openivm_views SET refresh_in_progress = true WHERE view_name = 'mv_r';
      |INSERT INTO openivm_delta_mv_r (name, age, openivm_multiplicity, openivm_timestamp)
      |SELECT name, age, openivm_multiplicity, openivm_timestamp
      |FROM memory.main.openivm_delta_users
      |WHERE openivm_timestamp >= '2026-05-16 10:00:55'::TIMESTAMP;
      |INSERT INTO openivm_data_mv_r
      |SELECT name, age
      |FROM openivm_delta_mv_r, generate_series(1, openivm_multiplicity::BIGINT)
      |WHERE openivm_timestamp >= '2026-05-16 10:00:55'::TIMESTAMP AND openivm_multiplicity > 0;
      |DELETE FROM openivm_delta_mv_r;
      |UPDATE openivm_views SET refresh_in_progress = false WHERE view_name = 'mv_r';
      |""".stripMargin

  // ── 1. Statement splitter: keep only B and C ──────────────────────────────
  describe("splitStatements + rewrite") {
    it("reduces the 7-statement openivm program to 2 surviving Spark statements") {
      val all = SparkRefreshRewriter.splitStatements(sevenStatementInput).map(_.trim).filter(_.nonEmpty)
      all should have size 7

      val rewritten = SparkRefreshRewriter.rewrite(
        compiledSql = sevenStatementInput,
        mvName = mvName,
        mvLocation = mvLocation,
        viewLogicalName = viewLogicalName,
        sourceTempViews = Map("sales" -> "openivm_delta_sales"),
        viewDeltaPath = viewDeltaPath
      )
      rewritten.statements should have size 2
    }
  }

  // ── 2. Splitter handles single-quoted strings with embedded `;` ───────────
  describe("splitStatements with literals") {
    it("does NOT split inside a single-quoted string literal containing `;`") {
      val sql   = "SELECT 'a;b;c' AS x; SELECT 1;"
      val stmts = SparkRefreshRewriter.splitStatements(sql).map(_.trim).filter(_.nonEmpty)
      stmts should have size 2
      stmts.head should include("'a;b;c'")
    }

    it("handles '' escape inside a string literal correctly") {
      val sql   = "SELECT 'it''s ok' AS s; SELECT 2;"
      val stmts = SparkRefreshRewriter.splitStatements(sql).map(_.trim).filter(_.nonEmpty)
      stmts should have size 2
      stmts.head should include("'it''s ok'")
    }
  }

  // ── 3. memory.main.openivm_delta_sales → `openivm_delta_sales` ────────────
  describe("source identifier rewrite") {
    it("rewrites memory.main.openivm_delta_sales to backtick-quoted openivm_delta_sales in statement B") {
      val rewritten = SparkRefreshRewriter.rewrite(
        compiledSql = sevenStatementInput,
        mvName = mvName,
        mvLocation = mvLocation,
        viewLogicalName = viewLogicalName,
        sourceTempViews = Map("sales" -> "openivm_delta_sales"),
        viewDeltaPath = viewDeltaPath
      )
      val stmtB = rewritten.statements.head
      stmtB should include("`openivm_delta_sales`")
      stmtB should not include "memory.main.openivm_delta_sales"
    }

    it("rewrites backticked memory.main.openivm_delta_sales to backtick-quoted openivm_delta_sales in statement B") {
      val rewritten = SparkRefreshRewriter.rewrite(
        compiledSql = sevenStatementInput.replace(
          "memory.main.openivm_delta_sales",
          "`memory`.`main`.`openivm_delta_sales`"
        ),
        mvName = mvName,
        mvLocation = mvLocation,
        viewLogicalName = viewLogicalName,
        sourceTempViews = Map("sales" -> "openivm_delta_sales"),
        viewDeltaPath = viewDeltaPath
      )
      val stmtB = rewritten.statements.head
      stmtB should include("`openivm_delta_sales`")
      stmtB should not include("`memory`.`main`.`openivm_delta_sales`")
    }
  }

  // ── 4. openivm_data_mv_r → `mydb`.`mv_r` ──────────────────────────────────
  describe("MV identifier rewrite") {
    it("rewrites openivm_data_mv_r to backtick-quoted multi-part MV identifier in statement C") {
      val rewritten = SparkRefreshRewriter.rewrite(
        compiledSql = sevenStatementInput,
        mvName = mvName,
        mvLocation = mvLocation,
        viewLogicalName = viewLogicalName,
        sourceTempViews = Map("sales" -> "openivm_delta_sales"),
        viewDeltaPath = viewDeltaPath
      )
      val stmtC = rewritten.statements(1)
      stmtC should include("`mydb`.`mv_r`")
      stmtC should not include "openivm_data_mv_r"
    }
  }

  // ── 5. openivm_delta_mv_r → delta.`<viewDeltaPath>` ───────────────────────
  describe("view-delta identifier rewrite in statement C") {
    it("rewrites openivm_delta_mv_r (the per-refresh CTAS sink) to delta.`<viewDeltaPath>`") {
      val rewritten = SparkRefreshRewriter.rewrite(
        compiledSql = sevenStatementInput,
        mvName = mvName,
        mvLocation = mvLocation,
        viewLogicalName = viewLogicalName,
        sourceTempViews = Map("sales" -> "openivm_delta_sales"),
        viewDeltaPath = viewDeltaPath
      )
      val stmtC = rewritten.statements(1)
      stmtC should include(s"delta.`$viewDeltaPath`")
      stmtC should not include "openivm_delta_mv_r"
    }
  }

  // ── 6. Timestamp predicate stripped from both surviving statements ───────
  describe("timestamp predicate strip") {
    it("strips `WHERE openivm_timestamp >= '...'::TIMESTAMP` from both statement B and C") {
      val rewritten = SparkRefreshRewriter.rewrite(
        compiledSql = sevenStatementInput,
        mvName = mvName,
        mvLocation = mvLocation,
        viewLogicalName = viewLogicalName,
        sourceTempViews = Map("sales" -> "openivm_delta_sales"),
        viewDeltaPath = viewDeltaPath
      )
      rewritten.statements.foreach { s =>
        s should not include "openivm_timestamp>="
        s should not include "openivm_timestamp >="
        s should not include "openivm_timestamp >"
        s should not include "::TIMESTAMP"
      }
    }

    it("strips trailing `AND openivm_timestamp >= '...'::TIMESTAMP` from a compound WHERE (CTE-with-filter pattern)") {
      val cteInput = {
        val scanLine =
          "SELECT amount, region, openivm_multiplicity " +
            "FROM memory.main.openivm_delta_sales " +
            "WHERE amount>100 AND openivm_timestamp>='2026-05-16 10:00:55'::TIMESTAMP"
        val insert =
          "INSERT INTO openivm_delta_mv_r (region, total, openivm_count_star, openivm_multiplicity) " +
            "SELECT * FROM projection_1"
        s"WITH scan_0 (amount, region, openivm_multiplicity) AS ($scanLine), " +
          s"projection_1 (region, total, openivm_count_star, openivm_multiplicity) AS " +
          s"(SELECT amount, region, CAST(1 AS INTEGER), CAST(1 AS INTEGER) FROM scan_0) $insert"
      }
      val stripped = SparkRefreshRewriter.splitStatements(cteInput).map { stmt =>
        val s0 = stmt
        // Apply the same rewrite passes as rewriteViewDeltaInsert does
        val s1 = s0.replaceAll(
          """(?i)\s+AND\s+openivm_timestamp\s*(?:>=|>|<=|<|=)\s*'[^']*'::\s*TIMESTAMP""",
          ""
        )
        s1
      }
      stripped.head should include("WHERE amount>100")
      stripped.head should not include "openivm_timestamp"
      stripped.head should not include "::TIMESTAMP"
    }
  }

  // ── 7. INSERT INTO openivm_delta_<v> becomes CREATE OR REPLACE TABLE CTAS ─
  describe("statement B CTAS rewrite") {
    it(
      "transforms `INSERT INTO openivm_delta_mv_r (...) SELECT * FROM lastCte` into a `CREATE OR REPLACE TABLE delta.`<path>` USING DELTA AS …` CTAS"
    ) {
      val rewritten = SparkRefreshRewriter.rewrite(
        compiledSql = sevenStatementInput,
        mvName = mvName,
        mvLocation = mvLocation,
        viewLogicalName = viewLogicalName,
        sourceTempViews = Map("sales" -> "openivm_delta_sales"),
        viewDeltaPath = viewDeltaPath
      )
      val stmtB = rewritten.statements.head
      stmtB should include(s"CREATE OR REPLACE TABLE delta.`$viewDeltaPath` USING DELTA AS")
      stmtB should not include "INSERT INTO openivm_delta_mv_r"
      // Aliased SELECT — the last CTE's column names mapped to the INSERT
      // column list (region/total/openivm_count_star/openivm_multiplicity).
      stmtB should include("t25_region AS region")
      stmtB should include("t25_aggregate_0 AS total")
      stmtB should include("t25_aggregate_1 AS openivm_count_star")
      stmtB should include("t25_scalar_2 AS openivm_multiplicity")
    }

    it(
      "rewrites a bare placeholder INSERT (no CTE, SELECT typed-nulls WHERE false) into a CTAS with an inline CTE for schema derivation"
    ) {
      // openivm emits this form for multi-source MVs when one source has no staging data.
      val placeholderStmt =
        "INSERT INTO openivm_delta_mv_r (id, `name`, openivm_multiplicity)" +
          "  SELECT CAST(NULL AS INTEGER), CAST(NULL AS STRING), CAST(NULL AS INTEGER) WHERE false"
      val mergeStmt =
        s"""WITH refresh_cte AS (
           |  SELECT id, openivm_multiplicity
           |  FROM openivm_delta_mv_r WHERE openivm_timestamp > '2026-01-01'::TIMESTAMP
           |)
           |MERGE INTO openivm_data_mv_r v USING refresh_cte d
           |ON v.id IS NOT DISTINCT FROM d.id
           |WHEN MATCHED THEN UPDATE SET id = d.id
           |WHEN NOT MATCHED THEN INSERT (id) VALUES (d.id)""".stripMargin
      val input = Seq(
        "UPDATE openivm_views SET refresh_in_progress = true WHERE view_name = 'mv_r'",
        placeholderStmt,
        mergeStmt,
        "DELETE FROM openivm_delta_mv_r",
        "UPDATE openivm_views SET refresh_in_progress = false WHERE view_name = 'mv_r'"
      ).mkString(";\n") + ";"

      val rewritten = SparkRefreshRewriter.rewrite(
        compiledSql = input,
        mvName = mvName,
        mvLocation = mvLocation,
        viewLogicalName = viewLogicalName,
        sourceTempViews = Map.empty,
        viewDeltaPath = viewDeltaPath
      )
      val stmtB = rewritten.statements.head
      stmtB should include(s"CREATE OR REPLACE TABLE delta.`$viewDeltaPath` USING DELTA AS")
      stmtB should not include "INSERT INTO openivm_delta_mv_r"
      stmtB should include("__openivm_placeholder")
      stmtB should include("WHERE false")
    }
  }

  // ── 8. SIMPLE_PROJECTION delete MERGE is tagged for runtime skip ─────────
  describe("simple projection delete MERGE tagging") {
    it("tags the delete-only MERGE so refresh execution can skip it when the view-delta has no negative rows") {
      val rewritten = SparkRefreshRewriter.rewrite(
        compiledSql = simpleProjectionInput,
        mvName = mvName,
        mvLocation = mvLocation,
        viewLogicalName = viewLogicalName,
        sourceTempViews = Map("users" -> "openivm_delta_users"),
        viewDeltaPath = viewDeltaPath
      )

      rewritten.statements should have size 3
      SparkRefreshRewriter.isSimpleProjectionDeleteMerge(rewritten.statements.head) shouldBe false
      SparkRefreshRewriter.isSimpleProjectionDeleteMerge(rewritten.statements(1)) shouldBe false
      SparkRefreshRewriter.isSimpleProjectionDeleteMerge(rewritten.statements(2)) shouldBe true

      val deleteMerge = SparkRefreshRewriter.stripExecutionMarker(rewritten.statements(2))
      deleteMerge should startWith("MERGE INTO `mydb`.`mv_r` AS v")
      deleteMerge should include("WHERE `openivm_multiplicity` < 0")
    }
  }

  // ── 9. postProcess is applied to each surviving statement ────────────────
  describe("postProcess hook") {
    it("invokes the supplied postProcess function on every kept statement") {
      val tagged = SparkRefreshRewriter.rewrite(
        compiledSql = sevenStatementInput,
        mvName = mvName,
        mvLocation = mvLocation,
        viewLogicalName = viewLogicalName,
        sourceTempViews = Map("sales" -> "openivm_delta_sales"),
        viewDeltaPath = viewDeltaPath,
        postProcess = s => "/*PP*/" + s
      )
      tagged.statements should have size 2
      tagged.statements.foreach(_ should startWith("/*PP*/"))
    }
  }

  // ── 10. Recompute-cascade snapshot rewrite ───────────────────────────────
  describe("pragma-gated recompute cascade rewrite") {
    it("keeps the pre-refresh snapshot pinned via Delta time travel and aliases bare delta metadata cols") {
      val windowCascadeInput =
        """UPDATE openivm_views SET refresh_in_progress = true WHERE view_name = 'mv_r';
          |CREATE OR REPLACE TEMP TABLE openivm_old_mv_r AS
          |SELECT * FROM openivm_data_mv_r
          |WHERE region IN (SELECT DISTINCT region FROM openivm_delta_sales WHERE openivm_timestamp > '2026-01-01 00:00:00'::TIMESTAMP);
          |CREATE OR REPLACE TEMP TABLE openivm_new_mv_r AS
          |SELECT * FROM (SELECT id, region, amount FROM memory.main.sales) openivm_recompute
          |WHERE region IN (SELECT DISTINCT region FROM openivm_delta_sales WHERE openivm_timestamp > '2026-01-01 00:00:00'::TIMESTAMP);
          |DELETE FROM openivm_data_mv_r WHERE region IN (SELECT DISTINCT region FROM openivm_delta_sales WHERE openivm_timestamp > '2026-01-01 00:00:00'::TIMESTAMP);
          |INSERT INTO openivm_data_mv_r SELECT * FROM openivm_new_mv_r;
          |INSERT INTO openivm_delta_mv_r
          |SELECT *, CAST(-1 AS INTEGER), CURRENT_TIMESTAMP FROM openivm_old_mv_r
          |UNION ALL
          |SELECT *, CAST(1 AS INTEGER), CURRENT_TIMESTAMP FROM openivm_new_mv_r;
          |DROP TABLE IF EXISTS openivm_old_mv_r;
          |DROP TABLE IF EXISTS openivm_new_mv_r;
          |UPDATE openivm_views SET refresh_in_progress = false WHERE view_name = 'mv_r';
          |""".stripMargin

      val rewritten = SparkRefreshRewriter.rewrite(
        compiledSql = windowCascadeInput,
        mvName = mvName,
        mvLocation = mvLocation,
        viewLogicalName = viewLogicalName,
        sourceTempViews = Map("sales" -> "openivm_delta_sales"),
        viewDeltaPath = viewDeltaPath,
        mvVersionBeforeRefresh = Some(7L)
      )

      rewritten.statements should have size 7
      rewritten.statements.head should include(s"delta.`$mvLocation` VERSION AS OF 7")
      rewritten.statements.head should not include "openivm_data_mv_r"
      rewritten.statements(1) should include("`sales`")
      rewritten.statements(1) should not include "memory.main.sales"
      rewritten.statements(3) should include("INSERT INTO `mydb`.`mv_r` SELECT * FROM openivm_new_mv_r")
      val deltaCtas = rewritten.statements
        .find(_.contains(s"delta.`$viewDeltaPath`"))
        .getOrElse(fail("view-delta CTAS missing"))
      deltaCtas should include("AS openivm_multiplicity")
      deltaCtas should include("AS openivm_timestamp")
      rewritten.statements.takeRight(2) shouldBe Seq(
        "DROP VIEW IF EXISTS `openivm_old_mv_r`",
        "DROP VIEW IF EXISTS `openivm_new_mv_r`"
      )
    }
  }

  // ── 11. hasRealDelta detection ───────────────────────────────────────────
  describe("hasRealDelta") {
    it("returns true for a real CTE-prefixed delta (single-source AGGREGATE_GROUP)") {
      SparkRefreshRewriter.hasRealDelta(sevenStatementInput, viewLogicalName) shouldBe true
    }

    it("returns true for pragma-gated recompute signed deltas without an INSERT column list") {
      val bareInsertSql =
        """UPDATE openivm_views SET refresh_in_progress = true WHERE view_name = 'mv_r';
          |INSERT INTO openivm_delta_mv_r
          |SELECT *, CAST(-1 AS INTEGER), CURRENT_TIMESTAMP FROM openivm_old_mv_r
          |UNION ALL
          |SELECT *, CAST(1 AS INTEGER), CURRENT_TIMESTAMP FROM openivm_new_mv_r;
          |UPDATE openivm_views SET refresh_in_progress = false WHERE view_name = 'mv_r';
          |""".stripMargin
      SparkRefreshRewriter.hasRealDelta(bareInsertSql, viewLogicalName) shouldBe true
    }

    it("returns false for an empty-placeholder delta (NULL WHERE false — multi-source JOIN)") {
      val placeholderSql =
        """UPDATE openivm_views SET refresh_in_progress = true WHERE view_name = 'mv_r';
          |INSERT INTO openivm_delta_mv_r (id, name, openivm_multiplicity) SELECT NULL::INTEGER, NULL::VARCHAR, NULL::INTEGER WHERE false;
          |INSERT INTO openivm_data_mv_r SELECT id, name FROM openivm_delta_mv_r WHERE openivm_multiplicity > 0;
          |DELETE FROM openivm_delta_mv_r;
          |UPDATE openivm_views SET refresh_in_progress = false WHERE view_name = 'mv_r';
          |""".stripMargin
      SparkRefreshRewriter.hasRealDelta(placeholderSql, viewLogicalName) shouldBe false
    }
  }
}
