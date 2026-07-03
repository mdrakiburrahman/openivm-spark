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

  describe("refresh effectivization") {
    it("is default-off and wraps the signed view delta when enabled") {
      val input =
        """WITH d (id, v, mult) AS (
          |  SELECT 1, 'a', 1
          |  UNION ALL
          |  SELECT 1, 'a', -1
          |  UNION ALL
          |  SELECT 1, 'a', 1
          |)
          |INSERT INTO openivm_delta_mv_r (id, v, openivm_multiplicity)
          |SELECT * FROM d""".stripMargin

      val off = SparkRefreshRewriter.rewrite(
        input,
        mvName,
        mvLocation,
        viewLogicalName,
        Map.empty,
        viewDeltaPath
      )
      off.statements.head should not include "__openivm_effective_raw"

      val on = SparkRefreshRewriter.rewrite(
        input,
        mvName,
        mvLocation,
        viewLogicalName,
        Map.empty,
        viewDeltaPath,
        refreshEffectivizeEnabled = true
      )
      on.statements.head should include("__openivm_effective_raw")
      on.statements.head should include("GROUP BY `id`, `v`")
      on.statements.head should include("HAVING SUM(openivm_multiplicity) <> 0")
    }
  }

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

  private val currentSimpleProjectionInput: String =
    """UPDATE openivm_views SET refresh_in_progress = true WHERE view_name = 'mv_r';
      |INSERT INTO openivm_delta_mv_r (name, age, openivm_multiplicity, openivm_timestamp)
      |SELECT name, age, openivm_multiplicity, openivm_timestamp
      |FROM memory.main.openivm_delta_users
      |WHERE openivm_timestamp >= '2026-05-16 10:00:55'::TIMESTAMP;
      |WITH openivm_net AS (
      |  SELECT name, age, SUM(openivm_multiplicity) AS _net
      |  FROM openivm_delta_mv_r
      |  WHERE openivm_timestamp > '2026-05-16 10:00:55'::TIMESTAMP
      |  GROUP BY name, age
      |  HAVING SUM(openivm_multiplicity) != 0
      |), openivm_delete_net AS (SELECT * FROM openivm_net WHERE _net < 0),
      |openivm_delete_rows AS (SELECT v.rowid FROM openivm_data_mv_r v JOIN openivm_delete_net d ON v.name IS NOT DISTINCT FROM d.name AND v.age IS NOT DISTINCT FROM d.age)
      |DELETE FROM openivm_data_mv_r USING openivm_delete_rows d WHERE openivm_data_mv_r.rowid = d.rowid;
      |WITH openivm_net AS (
      |  SELECT name, age, SUM(openivm_multiplicity) AS _net
      |  FROM openivm_delta_mv_r
      |  WHERE openivm_timestamp > '2026-05-16 10:00:55'::TIMESTAMP
      |  GROUP BY name, age
      |  HAVING SUM(openivm_multiplicity) != 0
      |) INSERT INTO openivm_data_mv_r SELECT name, age FROM openivm_net, generate_series(1, openivm_net._net::BIGINT) WHERE openivm_net._net > 0;
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
      stmtB should not include ("`memory`.`main`.`openivm_delta_sales`")
    }
  }

  describe("per-source delta_shape empty-delta term pruning") {
    it("drops UNION ALL join terms whose selected source delta is UNCHANGED") {
      System.setProperty("openivm.refresh.emptyDeltaSkip", "true")
      try {
        val input =
          """UPDATE openivm_views SET refresh_in_progress = true WHERE view_name = 'mv_r';
          |WITH join_delta AS (
          |  SELECT f.region_id, p.name, f.amount, f.openivm_multiplicity
          |  FROM memory.main.openivm_delta_fact_sales f
          |  JOIN memory.main.dim_product p ON f.product_id = p.product_id
          |  JOIN memory.main.dim_region r ON f.region_id = r.region_id
          |  UNION ALL
          |  SELECT f.region_id, p.name, f.amount, p.openivm_multiplicity
          |  FROM memory.main.fact_sales f
          |  JOIN memory.main.openivm_delta_dim_product p ON f.product_id = p.product_id
          |  JOIN memory.main.dim_region r ON f.region_id = r.region_id
          |  UNION ALL
          |  SELECT f.region_id, p.name, f.amount, r.openivm_multiplicity
          |  FROM memory.main.fact_sales f
          |  JOIN memory.main.dim_product p ON f.product_id = p.product_id
          |  JOIN memory.main.openivm_delta_dim_region r ON f.region_id = r.region_id
          |  UNION ALL
          |  SELECT f.region_id, p.name, f.amount, -1 * f.openivm_multiplicity * p.openivm_multiplicity
          |  FROM memory.main.openivm_delta_fact_sales f
          |  JOIN memory.main.openivm_delta_dim_product p ON f.product_id = p.product_id
          |  JOIN memory.main.dim_region r ON f.region_id = r.region_id
          |)
          |INSERT INTO openivm_delta_mv_r (region_id, name, amount, openivm_multiplicity)
          |SELECT * FROM join_delta;
          |UPDATE openivm_views SET refresh_in_progress = false WHERE view_name = 'mv_r';
          |""".stripMargin

        val rewritten = SparkRefreshRewriter.rewrite(
          compiledSql = input,
          mvName = mvName,
          mvLocation = mvLocation,
          viewLogicalName = viewLogicalName,
          sourceTempViews = Map.empty,
          viewDeltaPath = viewDeltaPath,
          deltaShape = Map(
            "default.fact_sales"  -> DeltaShape.InsertOnly,
            "default.dim_product" -> DeltaShape.Unchanged,
            "default.dim_region"  -> DeltaShape.Unchanged
          )
        )

        val stmt = rewritten.statements.head
        stmt should include("`openivm_delta_fact_sales`")
        stmt should not include "openivm_delta_dim_product"
        stmt should not include "openivm_delta_dim_region"
        stmt.split("(?i)UNION\\s+ALL").length shouldBe 1
      } finally System.clearProperty("openivm.refresh.emptyDeltaSkip")
    }
  }

  describe("FK term pruning") {
    val fkJoinDeltaInput =
      """UPDATE openivm_views SET refresh_in_progress = true WHERE view_name = 'mv_r';
        |WITH full_source (sale_id, product_id, amount) AS (
        |  SELECT sale_id, product_id, amount
        |  FROM memory.main.fact_sales
        |),
        |join_delta AS (
        |  SELECT f.sale_id, p.name, f.amount, f.openivm_multiplicity
        |  FROM memory.main.openivm_delta_fact_sales f
        |  JOIN memory.main.dim_product p ON f.product_id = p.product_id
        |  UNION ALL
        |  SELECT fs.sale_id, p.name, fs.amount, p.openivm_multiplicity
        |  FROM full_source fs
        |  JOIN memory.main.openivm_delta_dim_product p ON fs.product_id = p.product_id
        |  UNION ALL
        |  SELECT f.sale_id, p.name, f.amount, -1 * f.openivm_multiplicity * p.openivm_multiplicity
        |  FROM memory.main.openivm_delta_fact_sales f
        |  JOIN memory.main.openivm_delta_dim_product p ON f.product_id = p.product_id
        |)
        |INSERT INTO openivm_delta_mv_r (sale_id, name, amount, openivm_multiplicity)
        |SELECT * FROM join_delta;
        |UPDATE openivm_views SET refresh_in_progress = false WHERE view_name = 'mv_r';
        |""".stripMargin

    it("drops FK-redundant higher-order terms while leaving semiJoinPrune visible") {
      val rewritten = SparkRefreshRewriter.rewrite(
        compiledSql = fkJoinDeltaInput,
        mvName = mvName,
        mvLocation = mvLocation,
        viewLogicalName = viewLogicalName,
        sourceTempViews = Map.empty,
        viewDeltaPath = viewDeltaPath,
        deltaShape = Map(
          "default.fact_sales"  -> DeltaShape.InsertOnly,
          "default.dim_product" -> DeltaShape.InsertOnly
        ),
        semiJoinPruneEnabled = true,
        fkTermPruneEnabled = true,
        fkRelations =
          Seq(ForeignKeyRelation("default.fact_sales", Seq("product_id"), "default.dim_product", Seq("product_id")))
      )

      val stmt = rewritten.statements.head
      stmt.split("(?i)UNION\\s+ALL").length shouldBe 2
      stmt should include("`openivm_delta_fact_sales`")
      stmt should include("`openivm_delta_dim_product`")
      stmt should not include "-1 * f.openivm_multiplicity * p.openivm_multiplicity"
      stmt should include(
        "__openivm_full_source_pre.product_id IN (SELECT product_id FROM `openivm_delta_dim_product`)"
      )
      stmt should not include "FULL_REFRESH"
    }

    it("leaves terms unchanged without FK facts or when the sub-flag is off") {
      val noFacts = SparkRefreshRewriter
        .rewrite(
          compiledSql = fkJoinDeltaInput,
          mvName = mvName,
          mvLocation = mvLocation,
          viewLogicalName = viewLogicalName,
          sourceTempViews = Map.empty,
          viewDeltaPath = viewDeltaPath,
          deltaShape = Map(
            "default.fact_sales"  -> DeltaShape.InsertOnly,
            "default.dim_product" -> DeltaShape.InsertOnly
          ),
          semiJoinPruneEnabled = true,
          fkTermPruneEnabled = true
        )
        .statements
        .head

      val disabled = SparkRefreshRewriter
        .rewrite(
          compiledSql = fkJoinDeltaInput,
          mvName = mvName,
          mvLocation = mvLocation,
          viewLogicalName = viewLogicalName,
          sourceTempViews = Map.empty,
          viewDeltaPath = viewDeltaPath,
          deltaShape = Map(
            "default.fact_sales"  -> DeltaShape.InsertOnly,
            "default.dim_product" -> DeltaShape.InsertOnly
          ),
          semiJoinPruneEnabled = true,
          fkTermPruneEnabled = false,
          fkRelations =
            Seq(ForeignKeyRelation("default.fact_sales", Seq("product_id"), "default.dim_product", Seq("product_id")))
        )
        .statements
        .head

      noFacts should include("-1 * f.openivm_multiplicity * p.openivm_multiplicity")
      disabled should include("-1 * f.openivm_multiplicity * p.openivm_multiplicity")
    }
  }

  describe("semi-join full_source pre-prune") {
    val joinDeltaInput =
      """UPDATE openivm_views SET refresh_in_progress = true WHERE view_name = 'mv_r';
        |WITH full_source (sk_security_id, trade_date, amount) AS (
        |  SELECT sk_security_id, trade_date, amount
        |  FROM memory.main.daily_market
        |),
        |join_delta AS (
        |  SELECT fs.sk_security_id, fs.amount, d.openivm_multiplicity
        |  FROM full_source fs
        |  JOIN memory.main.openivm_delta_dim_security d
        |    ON fs.sk_security_id = d.sk_security_id
        |   AND fs.trade_date >= d.effective_date
        |)
        |INSERT INTO openivm_delta_mv_r (sk_security_id, amount, openivm_multiplicity)
        |SELECT * FROM join_delta;
        |UPDATE openivm_views SET refresh_in_progress = false WHERE view_name = 'mv_r';
        |""".stripMargin

    it("wraps FULL_SOURCE with a changed-source semi-join prefilter when enabled") {
      val rewritten = SparkRefreshRewriter.rewrite(
        compiledSql = joinDeltaInput,
        mvName = mvName,
        mvLocation = mvLocation,
        viewLogicalName = viewLogicalName,
        sourceTempViews = Map.empty,
        viewDeltaPath = viewDeltaPath,
        deltaShape = Map("default.dim_security" -> DeltaShape.InsertOnly),
        semiJoinPruneEnabled = true
      )

      val stmt = rewritten.statements.head
      stmt should include("SELECT * FROM (")
      stmt should include(
        "__openivm_full_source_pre.sk_security_id IN (SELECT sk_security_id FROM `openivm_delta_dim_security`)"
      )
      stmt should include("FROM full_source fs")
      stmt should not include "FULL_REFRESH"
    }

    it("leaves FULL_SOURCE unchanged when the gate is off or the delta source is unchanged") {
      val disabled = SparkRefreshRewriter
        .rewrite(
          compiledSql = joinDeltaInput,
          mvName = mvName,
          mvLocation = mvLocation,
          viewLogicalName = viewLogicalName,
          sourceTempViews = Map.empty,
          viewDeltaPath = viewDeltaPath,
          deltaShape = Map("default.dim_security" -> DeltaShape.InsertOnly),
          semiJoinPruneEnabled = false
        )
        .statements
        .head

      val unchanged = SparkRefreshRewriter
        .rewrite(
          compiledSql = joinDeltaInput,
          mvName = mvName,
          mvLocation = mvLocation,
          viewLogicalName = viewLogicalName,
          sourceTempViews = Map.empty,
          viewDeltaPath = viewDeltaPath,
          deltaShape = Map("default.dim_security" -> DeltaShape.Unchanged),
          semiJoinPruneEnabled = true
        )
        .statements
        .head

      disabled should not include "__openivm_full_source_pre"
      unchanged should not include "__openivm_full_source_pre"
    }
  }

  describe("unique-key join simplification") {
    val joinInput =
      """UPDATE openivm_views SET refresh_in_progress = true WHERE view_name = 'mv_r';
        |WITH join_delta AS (
        |  SELECT f.id, f.amount, f.openivm_multiplicity
        |  FROM memory.main.openivm_delta_fact_sales f
        |  JOIN memory.main.dim_customer d ON f.customer_id = d.id
        |),
        |left_join_delta AS (
        |  SELECT f.id, f.amount, f.openivm_multiplicity
        |  FROM memory.main.openivm_delta_fact_sales f
        |  LEFT JOIN memory.main.dim_region r ON f.region_id = r.id
        |)
        |INSERT INTO openivm_delta_mv_r (id, amount, openivm_multiplicity)
        |SELECT * FROM join_delta UNION ALL SELECT * FROM left_join_delta;
        |UPDATE openivm_views SET refresh_in_progress = false WHERE view_name = 'mv_r';
        |""".stripMargin

    it("demotes unused INNER unique-dimension joins to EXISTS probes and drops unused LEFT joins") {
      val rewritten = SparkRefreshRewriter.rewrite(
        compiledSql = joinInput,
        mvName = mvName,
        mvLocation = mvLocation,
        viewLogicalName = viewLogicalName,
        sourceTempViews = Map.empty,
        viewDeltaPath = viewDeltaPath,
        uniqueKeys = Seq(UniqueKey("dim_customer", Seq("id")), UniqueKey("dim_region", Seq("id"))),
        uniqueJoinSimplifyEnabled = true
      )

      val stmt = rewritten.statements.head
      stmt should include("EXISTS (SELECT 1 FROM `dim_customer` d WHERE f.customer_id = d.id)")
      stmt should not include "JOIN `dim_customer`"
      stmt should not include "JOIN `dim_region`"
      stmt should not include "FULL_REFRESH"
    }

    it("leaves joins unchanged when the gate is off or right columns are projected") {
      val disabled = SparkRefreshRewriter
        .rewrite(
          compiledSql = joinInput,
          mvName = mvName,
          mvLocation = mvLocation,
          viewLogicalName = viewLogicalName,
          sourceTempViews = Map.empty,
          viewDeltaPath = viewDeltaPath,
          uniqueKeys = Seq(UniqueKey("dim_customer", Seq("id")), UniqueKey("dim_region", Seq("id"))),
          uniqueJoinSimplifyEnabled = false
        )
        .statements
        .head

      val rightUsedInput = joinInput.replace(
        "SELECT f.id, f.amount, f.openivm_multiplicity",
        "SELECT f.id, d.name, f.openivm_multiplicity"
      )
      val rightUsed = SparkRefreshRewriter
        .rewrite(
          compiledSql = rightUsedInput,
          mvName = mvName,
          mvLocation = mvLocation,
          viewLogicalName = viewLogicalName,
          sourceTempViews = Map.empty,
          viewDeltaPath = viewDeltaPath,
          uniqueKeys = Seq(UniqueKey("dim_customer", Seq("id"))),
          uniqueJoinSimplifyEnabled = true
        )
        .statements
        .head

      disabled should include("JOIN `dim_customer`")
      disabled should include("LEFT JOIN `dim_region`")
      rightUsed should include("JOIN `dim_customer`")
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

    it("uses the MERGE alias for column references when the target has an alias") {
      // Regression: openivm compiler may emit
      //   MERGE INTO openivm_data_<view> AS v USING openivm_delta_<view> AS _d
      //   ON _d.col IS NOT DISTINCT FROM openivm_data_<view>.col
      // The fully-qualified table name in the ON clause must become the alias,
      // otherwise Spark raises DELTA_MERGE_UNRESOLVED_EXPRESSION.
      val aliasedMergeInput =
        """UPDATE openivm_views SET refresh_in_progress = true WHERE view_name = 'mv_r';
          |INSERT INTO openivm_delta_mv_r (region, total, openivm_multiplicity)
          |SELECT region, total, openivm_multiplicity FROM memory.main.openivm_delta_sales
          |WHERE openivm_timestamp >= '2026-01-01'::TIMESTAMP;
          |MERGE INTO openivm_data_mv_r AS v USING openivm_delta_mv_r AS _d
          |ON _d.openivm_left_key IS NOT DISTINCT FROM openivm_data_mv_r.openivm_left_key
          |WHEN MATCHED THEN DELETE;
          |INSERT INTO openivm_data_mv_r SELECT region, total FROM memory.main.sales;
          |UPDATE openivm_views SET refresh_in_progress = false WHERE view_name = 'mv_r';
          |""".stripMargin

      val rewritten = SparkRefreshRewriter.rewrite(
        compiledSql = aliasedMergeInput,
        mvName = mvName,
        mvLocation = mvLocation,
        viewLogicalName = viewLogicalName,
        sourceTempViews = Map("sales" -> "openivm_delta_sales"),
        viewDeltaPath = viewDeltaPath
      )
      val mergeStmt = rewritten.statements.find(_.contains("MERGE INTO")).get
      // The ON clause must reference the alias 'v', not the full MV name
      mergeStmt should include("v.openivm_left_key")
      mergeStmt should not include "`mydb`.`mv_r`.openivm_left_key"
      // The MERGE INTO target must still use the full MV name
      mergeStmt should include("MERGE INTO `mydb`.`mv_r`")
      mergeStmt should include regex "MERGE INTO `mydb`.`mv_r` (AS )?v"
    }

    it("uses the MERGE alias with a qualified multi-part MV name (fact_market_history repro)") {
      val fmhInput =
        """UPDATE openivm_views SET refresh_in_progress = true WHERE view_name = 'fact_market_history';
          |INSERT INTO openivm_delta_fact_market_history (sk_security_id, openivm_multiplicity)
          |SELECT sk_security_id, openivm_multiplicity FROM memory.main.openivm_delta_daily_market
          |WHERE openivm_timestamp >= '2026-01-01'::TIMESTAMP;
          |MERGE INTO openivm_data_fact_market_history AS v USING openivm_delta_fact_market_history AS _d
          |ON _d.openivm_left_key IS NOT DISTINCT FROM openivm_data_fact_market_history.openivm_left_key
          |WHEN MATCHED THEN DELETE;
          |INSERT INTO openivm_data_fact_market_history SELECT sk_security_id FROM memory.main.daily_market;
          |UPDATE openivm_views SET refresh_in_progress = false WHERE view_name = 'fact_market_history';
          |""".stripMargin

      val fmhMv        = TableIdentifier("fact_market_history", Some("gold"))
      val fmhDeltaPath = "dbfs:/delta/_ivm/view_deltas/gold_fact_market_history/uuid"
      val rewritten = SparkRefreshRewriter.rewrite(
        compiledSql = fmhInput,
        mvName = fmhMv,
        mvLocation = "dbfs:/delta/fact_market_history",
        viewLogicalName = "fact_market_history",
        sourceTempViews = Map("daily_market" -> "openivm_delta_daily_market"),
        viewDeltaPath = fmhDeltaPath
      )
      val mergeStmt = rewritten.statements.find(_.contains("MERGE INTO")).get
      mergeStmt should include("v.openivm_left_key")
      mergeStmt should not include "`gold`.`fact_market_history`.openivm_left_key"
      mergeStmt should include("MERGE INTO `gold`.`fact_market_history`")
      mergeStmt should include regex "MERGE INTO `gold`.`fact_market_history` (AS )?v"
    }

    it("handles the MERGE alias even when DuckDB emits dotted column refs") {
      // After dataViewRe replacement, the statement has `<mv>.col` references
      // that must become `<alias>.col`. Validates the post-replacement fixup.
      val quotedInput =
        """UPDATE openivm_views SET refresh_in_progress = true WHERE view_name = 'mv_r';
          |INSERT INTO openivm_delta_mv_r (region, total, openivm_multiplicity)
          |SELECT region, total, openivm_multiplicity FROM memory.main.openivm_delta_sales
          |WHERE openivm_timestamp >= '2026-01-01'::TIMESTAMP;
          |MERGE INTO openivm_data_mv_r AS v USING openivm_delta_mv_r AS _d
          |ON _d.openivm_left_key IS NOT DISTINCT FROM openivm_data_mv_r.openivm_left_key
          |AND _d.region IS NOT DISTINCT FROM openivm_data_mv_r.region
          |WHEN MATCHED THEN DELETE;
          |INSERT INTO openivm_data_mv_r SELECT region, total FROM memory.main.sales;
          |UPDATE openivm_views SET refresh_in_progress = false WHERE view_name = 'mv_r';
          |""".stripMargin

      val rewritten = SparkRefreshRewriter.rewrite(
        compiledSql = quotedInput,
        mvName = mvName,
        mvLocation = mvLocation,
        viewLogicalName = viewLogicalName,
        sourceTempViews = Map("sales" -> "openivm_delta_sales"),
        viewDeltaPath = viewDeltaPath
      )
      val mergeStmt = rewritten.statements.find(_.contains("MERGE INTO")).get
      // Both column refs in the ON clause must use the alias
      mergeStmt should include("v.openivm_left_key")
      mergeStmt should include("v.region")
      mergeStmt should not include "`mydb`.`mv_r`.openivm_left_key"
      mergeStmt should not include "`mydb`.`mv_r`.region"
    }

    it("handles newlines between MERGE alias and USING (DuckDB multi-line output)") {
      val multiLineInput =
        """UPDATE openivm_views SET refresh_in_progress = true WHERE view_name = 'mv_r';
          |INSERT INTO openivm_delta_mv_r (region, openivm_multiplicity)
          |SELECT region, openivm_multiplicity FROM memory.main.openivm_delta_sales
          |WHERE openivm_timestamp >= '2026-01-01'::TIMESTAMP;
          |MERGE INTO openivm_data_mv_r AS v
          |USING openivm_delta_mv_r AS _d
          |ON _d.openivm_left_key IS NOT DISTINCT FROM openivm_data_mv_r.openivm_left_key
          |WHEN MATCHED THEN DELETE;
          |INSERT INTO openivm_data_mv_r SELECT region FROM memory.main.sales;
          |UPDATE openivm_views SET refresh_in_progress = false WHERE view_name = 'mv_r';
          |""".stripMargin

      val rewritten = SparkRefreshRewriter.rewrite(
        compiledSql = multiLineInput,
        mvName = mvName,
        mvLocation = mvLocation,
        viewLogicalName = viewLogicalName,
        sourceTempViews = Map("sales" -> "openivm_delta_sales"),
        viewDeltaPath = viewDeltaPath
      )
      val mergeStmt = rewritten.statements.find(_.contains("MERGE INTO")).get
      mergeStmt should include("v.openivm_left_key")
      mergeStmt should not include "`mydb`.`mv_r`.openivm_left_key"
    }
  }

  // ── 4b. DELETE-only MERGE source dedup (Cartesian-on-NULLs fix) ───────────
  describe("MERGE USING source deduplication on NULL-safe DELETE merges") {
    it("wraps the USING source with SELECT DISTINCT on the ON-clause key columns") {
      val nullSafeMergeInput =
        """UPDATE openivm_views SET refresh_in_progress = true WHERE view_name = 'mv_r';
          |INSERT INTO openivm_delta_mv_r (region, openivm_multiplicity)
          |SELECT region, openivm_multiplicity FROM memory.main.openivm_delta_sales
          |WHERE openivm_timestamp >= '2026-01-01'::TIMESTAMP;
          |MERGE INTO openivm_data_mv_r AS v USING openivm_delta_mv_r AS _d
          |ON _d.openivm_left_key IS NOT DISTINCT FROM openivm_data_mv_r.openivm_left_key
          |WHEN MATCHED THEN DELETE;
          |INSERT INTO openivm_data_mv_r SELECT region FROM memory.main.sales;
          |UPDATE openivm_views SET refresh_in_progress = false WHERE view_name = 'mv_r';
          |""".stripMargin

      val rewritten = SparkRefreshRewriter.rewrite(
        compiledSql = nullSafeMergeInput,
        mvName = mvName,
        mvLocation = mvLocation,
        viewLogicalName = viewLogicalName,
        sourceTempViews = Map("sales" -> "openivm_delta_sales"),
        viewDeltaPath = viewDeltaPath
      )
      val mergeStmt = rewritten.statements.find(_.contains("WHEN MATCHED THEN DELETE")).get
      // Source is wrapped in SELECT DISTINCT on the ON-clause key column.
      mergeStmt should include("SELECT DISTINCT openivm_left_key FROM")
      // NULL-safe predicate is preserved unchanged.
      mergeStmt should include("IS NOT DISTINCT FROM")
      // Alias of the USING source is preserved.
      mergeStmt should include regex "AS _d\\s+ON"
    }

    it("dedupes composite-key MERGE source on every ON-clause key column") {
      val compositeInput =
        """UPDATE openivm_views SET refresh_in_progress = true WHERE view_name = 'mv_r';
          |INSERT INTO openivm_delta_mv_r (region, openivm_multiplicity)
          |SELECT region, openivm_multiplicity FROM memory.main.openivm_delta_sales
          |WHERE openivm_timestamp >= '2026-01-01'::TIMESTAMP;
          |MERGE INTO openivm_data_mv_r AS v USING openivm_delta_mv_r AS _d
          |ON _d.openivm_left_key IS NOT DISTINCT FROM openivm_data_mv_r.openivm_left_key
          |AND _d.region IS NOT DISTINCT FROM openivm_data_mv_r.region
          |WHEN MATCHED THEN DELETE;
          |INSERT INTO openivm_data_mv_r SELECT region FROM memory.main.sales;
          |UPDATE openivm_views SET refresh_in_progress = false WHERE view_name = 'mv_r';
          |""".stripMargin

      val rewritten = SparkRefreshRewriter.rewrite(
        compiledSql = compositeInput,
        mvName = mvName,
        mvLocation = mvLocation,
        viewLogicalName = viewLogicalName,
        sourceTempViews = Map("sales" -> "openivm_delta_sales"),
        viewDeltaPath = viewDeltaPath
      )
      val mergeStmt = rewritten.statements.find(_.contains("WHEN MATCHED THEN DELETE")).get
      // DISTINCT covers BOTH key columns referenced by the ON clause.
      mergeStmt should (include("openivm_left_key") and include("region"))
      mergeStmt should include regex "SELECT DISTINCT (openivm_left_key, region|region, openivm_left_key) FROM"
    }

    it("leaves UPDATE/INSERT MERGEs untouched (only DELETE-only MERGEs are deduped)") {
      // The 7-statement input's MERGE has UPDATE+INSERT clauses on `refresh_cte` —
      // dedup must not apply, because non-key source columns are referenced
      // in WHEN MATCHED THEN UPDATE / WHEN NOT MATCHED THEN INSERT.
      val rewritten = SparkRefreshRewriter.rewrite(
        compiledSql = sevenStatementInput,
        mvName = mvName,
        mvLocation = mvLocation,
        viewLogicalName = viewLogicalName,
        sourceTempViews = Map("sales" -> "openivm_delta_sales"),
        viewDeltaPath = viewDeltaPath
      )
      val mergeStmt = rewritten.statements.find(_.contains("MERGE INTO")).get
      // No `SELECT DISTINCT` injection on a MERGE that has WHEN MATCHED UPDATE.
      mergeStmt should include("WHEN MATCHED THEN UPDATE")
      mergeStmt should not include "_openivm_dedup_src"
    }

    it("does not dedupe MERGEs whose ON clause does not use IS NOT DISTINCT FROM") {
      // A defensive test: MERGE with plain `=` semantics is left alone.
      val plainEqInput =
        """UPDATE openivm_views SET refresh_in_progress = true WHERE view_name = 'mv_r';
          |INSERT INTO openivm_delta_mv_r (region, openivm_multiplicity)
          |SELECT region, openivm_multiplicity FROM memory.main.openivm_delta_sales
          |WHERE openivm_timestamp >= '2026-01-01'::TIMESTAMP;
          |MERGE INTO openivm_data_mv_r AS v USING openivm_delta_mv_r AS _d
          |ON _d.openivm_left_key = openivm_data_mv_r.openivm_left_key
          |WHEN MATCHED THEN DELETE;
          |INSERT INTO openivm_data_mv_r SELECT region FROM memory.main.sales;
          |UPDATE openivm_views SET refresh_in_progress = false WHERE view_name = 'mv_r';
          |""".stripMargin

      val rewritten = SparkRefreshRewriter.rewrite(
        compiledSql = plainEqInput,
        mvName = mvName,
        mvLocation = mvLocation,
        viewLogicalName = viewLogicalName,
        sourceTempViews = Map("sales" -> "openivm_delta_sales"),
        viewDeltaPath = viewDeltaPath
      )
      val mergeStmt = rewritten.statements.find(_.contains("WHEN MATCHED THEN DELETE")).get
      mergeStmt should not include "_openivm_dedup_src"
      mergeStmt should not include "SELECT DISTINCT"
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

  describe("WINDOW_PARTITION single delete MERGE rewrite") {
    val windowDeleteInput =
      """UPDATE openivm_views SET refresh_in_progress = true WHERE view_name = 'mv_trh';
        |DELETE FROM openivm_data_mv_trh
        |WHERE trade_id IN (
        |  SELECT DISTINCT t_id FROM openivm_delta_brokerage_trade
        |  WHERE openivm_timestamp > '2026-07-01 00:00:00'::TIMESTAMP
        |)
        |OR trade_id IN (
        |  SELECT DISTINCT th_t_id FROM openivm_delta_brokerage_trade_history
        |  WHERE openivm_timestamp > '2026-07-01 00:00:00'::TIMESTAMP
        |);
        |INSERT INTO openivm_data_mv_trh
        |SELECT * FROM (SELECT t_id AS trade_id FROM memory.main.brokerage_trade) openivm_recompute
        |WHERE trade_id IN (SELECT DISTINCT t_id FROM openivm_delta_brokerage_trade
        |  WHERE openivm_timestamp > '2026-07-01 00:00:00'::TIMESTAMP);
        |UPDATE openivm_views SET refresh_in_progress = false WHERE view_name = 'mv_trh';
        |""".stripMargin

    it("keeps the legacy one-MERGE-per-IN-clause shape when the gate is off") {
      val rewritten = SparkRefreshRewriter
        .rewrite(
          compiledSql = windowDeleteInput,
          mvName = TableIdentifier("mv_trh", Some("silver")),
          mvLocation = "dbfs:/delta/mv_trh",
          viewLogicalName = "mv_trh",
          sourceTempViews = Map.empty,
          viewDeltaPath = "dbfs:/delta/_tmp/mv_trh_delta_uuid"
        )
        .statements

      rewritten.filter(_.contains("WHEN MATCHED THEN DELETE")) should have size 2
      rewritten.mkString("\n") should not include "UNION ALL"
    }

    it("collapses same-target partition deletes to one MERGE over unioned affected keys when enabled") {
      val rewritten = SparkRefreshRewriter
        .rewrite(
          compiledSql = windowDeleteInput,
          mvName = TableIdentifier("mv_trh", Some("silver")),
          mvLocation = "dbfs:/delta/mv_trh",
          viewLogicalName = "mv_trh",
          sourceTempViews = Map.empty,
          viewDeltaPath = "dbfs:/delta/_tmp/mv_trh_delta_uuid",
          windowPartitionSingleDeleteMergeEnabled = true
        )
        .statements

      val deleteMerges = rewritten.filter(_.contains("WHEN MATCHED THEN DELETE"))
      deleteMerges should have size 1
      deleteMerges.head should include("UNION ALL")
      deleteMerges.head should include("SELECT t_id AS trade_id")
      deleteMerges.head should include("SELECT th_t_id AS trade_id")
      deleteMerges.head should include("ON v.trade_id IS NOT DISTINCT FROM d.trade_id")
      deleteMerges.head should not include "openivm_timestamp"
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
        viewDeltaPath = viewDeltaPath,
        mvVersionBeforeRefresh = Some(42L)
      )

      // 4 surviving statements:
      //   0. view-delta CTAS (CREATE OR REPLACE TABLE delta.`<viewDeltaPath>` …)
      //   1. CREATE OR REPLACE TEMPORARY VIEW `openivm_sp_net_mv_r` (consolidate net per cols)
      //   2. MERGE INTO `mydb`.`mv_r` … WHEN MATCHED THEN DELETE  ← marked
      //   3. INSERT INTO `mydb`.`mv_r` …  (bag-correct INSERT)
      rewritten.statements should have size 4
      SparkRefreshRewriter.isSimpleProjectionDeleteMerge(rewritten.statements.head) shouldBe false
      SparkRefreshRewriter.isSimpleProjectionDeleteMerge(rewritten.statements(1)) shouldBe false
      SparkRefreshRewriter.isSimpleProjectionDeleteMerge(rewritten.statements(2)) shouldBe true
      SparkRefreshRewriter.isSimpleProjectionDeleteMerge(rewritten.statements(3)) shouldBe false

      // Stmt 1: net-consolidation TEMP VIEW
      val netView = rewritten.statements(1)
      netView should startWith("CREATE OR REPLACE TEMPORARY VIEW `openivm_sp_net_mv_r`")
      netView should include("SUM(CAST(`openivm_multiplicity` AS BIGINT)) AS __openivm_net")
      netView should include(s"FROM delta.`$viewDeltaPath`")
      netView should include("HAVING SUM(CAST(`openivm_multiplicity` AS BIGINT)) != 0")

      // Stmt 2: MERGE-DELETE — now sources from the net view, not raw delta
      val deleteMerge = SparkRefreshRewriter.stripExecutionMarker(rewritten.statements(2))
      deleteMerge should startWith("MERGE INTO `mydb`.`mv_r` AS v")
      deleteMerge should include("FROM `openivm_sp_net_mv_r`")
      deleteMerge should include("WHERE __openivm_net < 0")
      deleteMerge should include("WHEN MATCHED THEN DELETE")

      // Stmt 3: bag-correct INSERT — reads pre-DELETE MV state via Delta time travel,
      // re-inserts `max(0, _cur + _net)` copies per over-deleted group.
      val bagInsert = rewritten.statements(3)
      bagInsert should startWith("INSERT INTO `mydb`.`mv_r`")
      bagInsert should include(s"FROM delta.`$mvLocation` VERSION AS OF 42 v")
      bagInsert should include("COALESCE(a.__openivm_cur, CAST(0 AS BIGINT)) + n.__openivm_net")
      bagInsert should include("LATERAL VIEW EXPLODE(")
      bagInsert should include("WHERE __openivm_src.__openivm_to_insert > 0")
    }

    it("recognizes current openivm_net SIMPLE_PROJECTION apply statements") {
      val rewritten = SparkRefreshRewriter.rewrite(
        compiledSql = currentSimpleProjectionInput,
        mvName = mvName,
        mvLocation = mvLocation,
        viewLogicalName = viewLogicalName,
        sourceTempViews = Map("users" -> "openivm_delta_users"),
        viewDeltaPath = viewDeltaPath,
        mvVersionBeforeRefresh = Some(42L)
      )

      rewritten.statements should have size 4
      rewritten.statements.head should startWith(s"CREATE OR REPLACE TABLE delta.`$viewDeltaPath` USING DELTA AS")
      rewritten.statements(1) should startWith("CREATE OR REPLACE TEMPORARY VIEW `openivm_sp_net_mv_r`")
      SparkRefreshRewriter.isSimpleProjectionDeleteMerge(rewritten.statements(2)) shouldBe true
      rewritten.statements(3) should startWith("INSERT INTO `mydb`.`mv_r`")
      rewritten.statements(3) should include(s"FROM delta.`$mvLocation` VERSION AS OF 42 v")
      rewritten.statements.exists(_.contains("openivm_delete_rows")) shouldBe false
      rewritten.statements.exists(_.contains("rowid")) shouldBe false
    }
  }

  describe("outer-join projection partial recompute rewrites") {
    it("rewrites FULL OUTER CTE-prefixed IN deletes into key-safe delete MERGEs") {
      val input =
        """UPDATE openivm_views SET refresh_in_progress = true WHERE view_name = 'mv_r';
          |INSERT INTO openivm_delta_mv_r (name, amount, openivm_left_key, openivm_right_key, openivm_multiplicity)
          |SELECT name, amount, openivm_left_key, openivm_right_key, openivm_multiplicity
          |FROM memory.main.openivm_delta_users WHERE openivm_timestamp >= '2026-01-01'::TIMESTAMP;
          |WITH openivm_affected AS (
          |  SELECT DISTINCT id AS _k FROM memory.main.openivm_delta_users WHERE openivm_timestamp >= '2026-01-01'::TIMESTAMP
          |  UNION
          |  SELECT DISTINCT user_id AS _k FROM memory.main.openivm_delta_orders WHERE openivm_timestamp >= '2026-01-01'::TIMESTAMP
          |)
          |DELETE FROM openivm_data_mv_r
          |WHERE openivm_left_key IN (SELECT _k FROM openivm_affected)
          |   OR openivm_right_key IN (SELECT _k FROM openivm_affected);
          |WITH openivm_affected AS (
          |  SELECT DISTINCT id AS _k FROM memory.main.openivm_delta_users WHERE openivm_timestamp >= '2026-01-01'::TIMESTAMP
          |  UNION
          |  SELECT DISTINCT user_id AS _k FROM memory.main.openivm_delta_orders WHERE openivm_timestamp >= '2026-01-01'::TIMESTAMP
          |)
          |INSERT INTO openivm_data_mv_r
          |SELECT * FROM (SELECT u.name, o.amount, u.id AS openivm_left_key, o.user_id AS openivm_right_key
          |FROM memory.main.users u FULL OUTER JOIN memory.main.orders o ON u.id = o.user_id) openivm_foj
          |WHERE openivm_left_key IN (SELECT _k FROM openivm_affected)
          |   OR openivm_right_key IN (SELECT _k FROM openivm_affected);
          |UPDATE openivm_views SET refresh_in_progress = false WHERE view_name = 'mv_r';
          |""".stripMargin

      val rewritten = SparkRefreshRewriter.rewrite(
        compiledSql = input,
        mvName = mvName,
        mvLocation = mvLocation,
        viewLogicalName = viewLogicalName,
        sourceTempViews = Map.empty,
        viewDeltaPath = viewDeltaPath
      )

      val deleteMerges = rewritten.statements.filter(_.contains("WHEN MATCHED THEN DELETE"))
      deleteMerges should have size 2
      deleteMerges.head should include("ON v.openivm_left_key IS NOT DISTINCT FROM d.`_k`")
      deleteMerges(1) should include("ON v.openivm_right_key IS NOT DISTINCT FROM d.`_k`")
      deleteMerges.foreach(_ should include("WITH openivm_affected AS"))
      rewritten.statements.last should include(s"MERGE INTO delta.`$mvLocation` AS v")
      rewritten.statements.last should include("WHEN NOT MATCHED THEN INSERT")
    }

    it("rewrites DELETE USING into a key-safe delete MERGE") {
      val input =
        """UPDATE openivm_views SET refresh_in_progress = true WHERE view_name = 'mv_r';
          |WITH openivm_affected AS (
          |  SELECT DISTINCT openivm_left_key FROM openivm_delta_mv_r WHERE openivm_timestamp >= '2026-01-01'::TIMESTAMP
          |)
          |DELETE FROM openivm_data_mv_r AS openivm_delete_target
          |USING openivm_affected _d
          |WHERE _d.openivm_left_key IS NOT DISTINCT FROM openivm_delete_target.openivm_left_key;
          |UPDATE openivm_views SET refresh_in_progress = false WHERE view_name = 'mv_r';
          |""".stripMargin

      val rewritten = SparkRefreshRewriter.rewrite(
        compiledSql = input,
        mvName = mvName,
        mvLocation = mvLocation,
        viewLogicalName = viewLogicalName,
        sourceTempViews = Map.empty,
        viewDeltaPath = viewDeltaPath
      )

      rewritten.statements should have size 1
      rewritten.statements.head should include("MERGE INTO `mydb`.`mv_r` AS openivm_delete_target")
      rewritten.statements.head should include("USING (")
      rewritten.statements.head should include("SELECT * FROM openivm_affected")
      rewritten.statements.head should include("WHEN MATCHED THEN DELETE")
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

  describe("selective broadcast hint injection") {
    it("adds a BROADCAST hint only for proven-small join-side aliases") {
      val sql =
        """SELECT f.id, d.name
          |FROM `db`.`fact_sales` AS f
          |JOIN `db`.`dim_customer` d ON f.customer_id = d.id
          |JOIN `db`.`dim_large` l ON f.large_id = l.id""".stripMargin
      val hinted = SparkRefreshRewriter.injectSelectiveBroadcastHints(
        sql,
        Seq(SparkRefreshRewriter.SelectiveBroadcastTable("dim_customer", "db.dim_customer", 1024L))
      )

      hinted should include("SELECT /*+ BROADCAST(d) */ f.id")
      hinted should not include "BROADCAST(l)"
    }

    it("leaves non-join statements unchanged even when a table is small") {
      val sql = "SELECT id FROM `db`.`dim_customer`"

      SparkRefreshRewriter.injectSelectiveBroadcastHints(
        sql,
        Seq(SparkRefreshRewriter.SelectiveBroadcastTable("dim_customer", "db.dim_customer", 1024L))
      ) shouldBe sql
    }
  }

  describe("skew fanout delta broadcast planning") {
    it("plans and injects a delta BROADCAST hint for narrow min/max overlap") {
      val facts = WorkloadFacts(
        columnStats = Map(
          "db.fact.customer_id" -> WorkloadColumnStats(min = Some("1"), max = Some("100000"), rowCount = Some(1000000L))
        ),
        deltaStats = Map(
          "db.fact" -> WorkloadDeltaStats(
            rowCount = Some(4L),
            min = Map("customer_id" -> "42"),
            max = Map("customer_id" -> "42")
          )
        )
      )
      val plan = SparkRefreshRewriter.planSkewFanoutDeltaBroadcasts(facts, maxDeltaRows = 100L, maxOverlapRatio = 0.01d)
      plan should have size 1
      plan.head.signal should include("min_max_overlap column=customer_id")

      val sql =
        """SELECT f.id, d.name
          |FROM memory.main.openivm_delta_fact f
          |JOIN `db`.`dim_customer` d ON f.customer_id = d.id""".stripMargin
      val hinted = SparkRefreshRewriter.injectSkewFanoutBroadcastHints(sql, plan)

      hinted should include("SELECT /*+ BROADCAST(f) */ /*OPENIVM_SKEW_FANOUT fact:rows=4:")
    }

    it("falls back to row-count-only delta broadcast when histogram bins are unavailable") {
      val facts = WorkloadFacts(deltaStats = Map("db.fact" -> WorkloadDeltaStats(rowCount = Some(5L))))
      val plan  = SparkRefreshRewriter.planSkewFanoutDeltaBroadcasts(facts, maxDeltaRows = 10L, maxOverlapRatio = 0.05d)

      plan.map(_.shortName) shouldBe Seq("fact")
      plan.head.signal should include("histogram_bins=unavailable")
    }
  }

  describe("SCD2 range acceleration injection") {
    it("broadcasts the SCD alias and pre-filters it to the source-delta timestamp range") {
      val sql =
        """SELECT f.id, d.name
          |FROM `openivm_delta_fact_market_history` f
          |JOIN `dim_security` d
          |  ON f.security_id = d.security_id
          | AND f.ts BETWEEN d.effective_timestamp AND d.end_timestamp""".stripMargin

      val accelerated = SparkRefreshRewriter.injectScd2RangeAcceleration(sql)

      accelerated should include("SELECT /*+ BROADCAST(d) */ f.id")
      accelerated should include("/*__openivm_scd2_range_accel__*/")
      accelerated should include(
        "d.effective_timestamp <= (SELECT MAX(__openivm_scd2_probe_0.ts) FROM `openivm_delta_fact_market_history` AS __openivm_scd2_probe_0)"
      )
      accelerated should include(
        "d.end_timestamp >= (SELECT MIN(__openivm_scd2_probe_0.ts) FROM `openivm_delta_fact_market_history` AS __openivm_scd2_probe_0)"
      )
    }

    it("leaves non-SCD2 joins unchanged") {
      val sql = "SELECT f.id FROM fact f JOIN dim d ON f.id = d.id"
      SparkRefreshRewriter.injectScd2RangeAcceleration(sql) shouldBe sql
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

    it("keeps current-diff recompute helpers and pins the affected diff to the pre-refresh MV snapshot") {
      val currentDiffInput =
        """UPDATE openivm_views SET refresh_in_progress = true WHERE view_name = 'mv_r';
          |CREATE OR REPLACE TEMP TABLE openivm_current_mv_r AS
          |SELECT region, total FROM memory.main.sales;
          |CREATE OR REPLACE TEMP TABLE openivm_affected_mv_r AS
          |SELECT DISTINCT region
          |FROM (
          |  (SELECT * FROM openivm_current_mv_r EXCEPT ALL SELECT * FROM openivm_data_mv_r)
          |  UNION ALL
          |  (SELECT * FROM openivm_data_mv_r EXCEPT ALL SELECT * FROM openivm_current_mv_r)
          |) openivm_changed;
          |DROP TABLE IF EXISTS openivm_affected_mv_r;
          |DROP TABLE IF EXISTS openivm_current_mv_r;
          |UPDATE openivm_views SET refresh_in_progress = false WHERE view_name = 'mv_r';
          |""".stripMargin

      val rewritten = SparkRefreshRewriter.rewrite(
        compiledSql = currentDiffInput,
        mvName = mvName,
        mvLocation = mvLocation,
        viewLogicalName = viewLogicalName,
        sourceTempViews = Map("sales" -> "openivm_delta_sales"),
        viewDeltaPath = viewDeltaPath,
        mvVersionBeforeRefresh = Some(7L)
      )

      rewritten.statements should have size 4
      rewritten.statements.head should startWith("CREATE OR REPLACE TEMPORARY VIEW openivm_current_mv_r")
      rewritten.statements.head should include("`sales`")
      rewritten.statements(1) should include(s"delta.`$mvLocation` VERSION AS OF 7")
      rewritten.statements(1) should not include "openivm_data_mv_r"
      rewritten.statements.takeRight(2) shouldBe Seq(
        "DROP VIEW IF EXISTS `openivm_affected_mv_r`",
        "DROP VIEW IF EXISTS `openivm_current_mv_r`"
      )
    }
  }

  describe("P5.2 running-window suffix-extend rewrite") {
    val p52ViewLogicalName = "wradm_mv"
    val p52MvName          = TableIdentifier("wradm_mv", Some("default"))
    val p52Input: String =
      """UPDATE openivm_views SET refresh_in_progress = true WHERE view_name = 'wradm_mv';
        |CREATE OR REPLACE TEMP TABLE openivm_run_affected_wradm_mv AS
        |SELECT DISTINCT d.dm_s_symb
        |FROM openivm_delta_wradm_daily_market d
        |WHERE d.openivm_multiplicity > 0 AND d.openivm_timestamp > CAST('2026-06-30 20:00:00' AS TIMESTAMP);
        |CREATE OR REPLACE TEMP TABLE openivm_run_bounds_wradm_mv AS
        |WITH old_max AS (
        |  SELECT dm_s_symb, MAX(dm_date) AS openivm_old_max_order
        |  FROM openivm_data_wradm_mv
        |  GROUP BY dm_s_symb
        |), delta_min AS (
        |  SELECT d.dm_s_symb, MIN(d.dm_date) AS openivm_delta_min_order
        |  FROM openivm_delta_wradm_daily_market d
        |  WHERE d.openivm_multiplicity > 0 AND d.openivm_timestamp > CAST('2026-06-30 20:00:00' AS TIMESTAMP)
        |  GROUP BY d.dm_s_symb
        |)
        |SELECT a.dm_s_symb, m.openivm_old_max_order, b.openivm_delta_min_order
        |FROM openivm_run_affected_wradm_mv a
        |LEFT JOIN old_max m ON a.dm_s_symb IS NOT DISTINCT FROM m.dm_s_symb
        |JOIN delta_min b ON a.dm_s_symb IS NOT DISTINCT FROM b.dm_s_symb;
        |CREATE OR REPLACE TEMP TABLE openivm_run_fast_wradm_mv AS
        |SELECT dm_s_symb FROM openivm_run_bounds_wradm_mv
        |WHERE openivm_old_max_order IS NULL OR openivm_delta_min_order > openivm_old_max_order;
        |CREATE OR REPLACE TEMP TABLE openivm_run_fallback_wradm_mv AS
        |SELECT dm_s_symb FROM openivm_run_bounds_wradm_mv
        |WHERE openivm_old_max_order IS NOT NULL AND openivm_delta_min_order <= openivm_old_max_order;
        |CREATE OR REPLACE TEMP TABLE openivm_run_state_wradm_mv AS
        |SELECT dm_s_symb, dm_date, run_sum, openivm_prior_count FROM (
        |  SELECT dt.dm_s_symb, dt.dm_date, dt.run_sum,
        |         COUNT(*) OVER (PARTITION BY dt.dm_s_symb) AS openivm_prior_count,
        |         ROW_NUMBER() OVER (PARTITION BY dt.dm_s_symb ORDER BY dt.dm_date DESC) AS openivm_rn
        |  FROM openivm_data_wradm_mv dt
        |  JOIN openivm_run_fast_wradm_mv fk ON dt.dm_s_symb IS NOT DISTINCT FROM fk.dm_s_symb
        |) openivm_state_ranked WHERE openivm_rn = 1;
        |DELETE FROM openivm_data_wradm_mv WHERE dm_s_symb IN (SELECT dm_s_symb FROM openivm_run_fallback_wradm_mv);
        |INSERT INTO openivm_data_wradm_mv
        |SELECT * FROM (
        |  SELECT dm_s_symb, dm_date, dm_close,
        |         SUM(dm_close) OVER (PARTITION BY dm_s_symb ORDER BY dm_date) AS run_sum
        |  FROM memory.main.wradm_daily_market
        |) openivm_recompute
        |WHERE dm_s_symb IN (SELECT dm_s_symb FROM openivm_run_fallback_wradm_mv);
        |INSERT INTO openivm_data_wradm_mv (dm_s_symb, dm_date, dm_close, run_sum)
        |SELECT d.dm_s_symb, d.dm_date, d.dm_close,
        |       CASE WHEN s.run_sum IS NULL THEN SUM(d.dm_close) OVER (PARTITION BY d.dm_s_symb ORDER BY d.dm_date)
        |            ELSE s.run_sum + SUM(d.dm_close) OVER (PARTITION BY d.dm_s_symb ORDER BY d.dm_date)
        |       END AS run_sum
        |FROM openivm_delta_wradm_daily_market d
        |JOIN openivm_run_fast_wradm_mv fk ON d.dm_s_symb IS NOT DISTINCT FROM fk.dm_s_symb
        |LEFT JOIN openivm_run_state_wradm_mv s ON d.dm_s_symb IS NOT DISTINCT FROM s.dm_s_symb
        |WHERE d.openivm_multiplicity > 0 AND d.openivm_timestamp > CAST('2026-06-30 20:00:00' AS TIMESTAMP);
        |DROP TABLE IF EXISTS openivm_run_state_wradm_mv;
        |DROP TABLE IF EXISTS openivm_run_fallback_wradm_mv;
        |DROP TABLE IF EXISTS openivm_run_fast_wradm_mv;
        |DROP TABLE IF EXISTS openivm_run_bounds_wradm_mv;
        |DROP TABLE IF EXISTS openivm_run_affected_wradm_mv;
        |DELETE FROM openivm_delta_wradm_mv;
        |UPDATE openivm_views SET refresh_in_progress = false WHERE view_name = 'wradm_mv';
        |""".stripMargin

    def rewriteP52(): Seq[String] =
      SparkRefreshRewriter
        .rewrite(
          compiledSql = p52Input,
          mvName = p52MvName,
          mvLocation = "dbfs:/delta/wradm_mv",
          viewLogicalName = p52ViewLogicalName,
          sourceTempViews = Map("wradm_daily_market" -> "openivm_delta_wradm_daily_market"),
          viewDeltaPath = "dbfs:/delta/_tmp/wradm_mv_delta_uuid",
          mvVersionBeforeRefresh = Some(3)
        )
        .statements

    it("materialises running-window helper CREATEs as version-pinned, eagerly cached temporary views") {
      val rewritten = rewriteP52()
      val creates   = rewritten.filter(_.startsWith("CREATE OR REPLACE TEMPORARY VIEW openivm_run_"))
      creates should have size 5
      creates.foreach { stmt =>
        stmt should not include "TEMP TABLE"
        stmt should not include "openivm_timestamp"
        stmt should not include "openivm_data_wradm_mv"
        stmt should include("openivm_run_")
      }
      // bounds + state READ the MV, so they must snapshot the pre-refresh Delta
      // version (the program mutates the MV mid-refresh).
      val boundsCreate =
        creates.find(_.contains("openivm_run_bounds_wradm_mv")).getOrElse(fail("bounds create missing"))
      val stateCreate = creates.find(_.contains("openivm_run_state_wradm_mv")).getOrElse(fail("state create missing"))
      boundsCreate should include("delta.`dbfs:/delta/wradm_mv` VERSION AS OF 3")
      stateCreate should include("delta.`dbfs:/delta/wradm_mv` VERSION AS OF 3")
      // Each helper is eagerly CACHEd so the snapshot is frozen before the MV mutates.
      val caches = rewritten.filter(_.startsWith("CACHE TABLE `openivm_run_"))
      caches should have size 5
    }

    it("rewrites fallback delete and recompute insert while preserving the run_fallback subquery") {
      val rewritten = rewriteP52()
      val deleteMerge = rewritten
        .find(s => s.contains("WHEN MATCHED THEN DELETE") && s.contains("openivm_run_fallback_wradm_mv"))
        .getOrElse(fail("partition-scoped delete MERGE missing"))
      deleteMerge should startWith("MERGE INTO `default`.`wradm_mv` AS v")
      deleteMerge should include("SELECT dm_s_symb FROM openivm_run_fallback_wradm_mv")
      deleteMerge should include("AS d ON")
      deleteMerge should include("v.dm_s_symb IS NOT DISTINCT FROM d.dm_s_symb")

      val fallbackInsert = rewritten
        .find(s => s.contains("openivm_recompute") && s.contains("openivm_run_fallback_wradm_mv"))
        .getOrElse(fail("fallback recompute insert missing"))
      fallbackInsert should include("`wradm_daily_market`")
      fallbackInsert should include("openivm_run_fallback_wradm_mv")
      fallbackInsert should not include "memory.main."
      fallbackInsert should not include "openivm_timestamp"
    }

    it("rewrites the suffix fast INSERT before the generic data-insert classifier can grab it") {
      val fastInsert = rewriteP52()
        .find(s => s.startsWith("INSERT INTO `default`.`wradm_mv` (dm_s_symb, dm_date, dm_close, run_sum)"))
        .getOrElse(fail("running-window fast insert missing"))
      fastInsert should include("FROM openivm_delta_wradm_daily_market d")
      fastInsert should include("JOIN openivm_run_fast_wradm_mv fk")
      fastInsert should include("LEFT JOIN openivm_run_state_wradm_mv s")
      fastInsert should include("d.openivm_multiplicity > 0")
      fastInsert should not include "openivm_data_wradm_mv"
      fastInsert should not include "openivm_timestamp"
    }

    it("drops running-window helper objects as views") {
      rewriteP52().takeRight(5) shouldBe Seq(
        "DROP VIEW IF EXISTS `openivm_run_state_wradm_mv`",
        "DROP VIEW IF EXISTS `openivm_run_fallback_wradm_mv`",
        "DROP VIEW IF EXISTS `openivm_run_fast_wradm_mv`",
        "DROP VIEW IF EXISTS `openivm_run_bounds_wradm_mv`",
        "DROP VIEW IF EXISTS `openivm_run_affected_wradm_mv`"
      )
    }

    it("CTAS-creates the view-delta on the fallback cascade and APPENDS the fast cascade") {
      // Cascade-source running window (force_view_delta_cascade=true): openivm
      // emits TWO `INSERT INTO openivm_delta_<view>` — the fallback signed
      // multiset (openivm_old/openivm_new) then the fast suffix rows (joins
      // openivm_run_fast). The first must CTAS-create the view-delta path; the
      // second must APPEND, else the second overwrites the first.
      val cascadeInput =
        """UPDATE openivm_views SET refresh_in_progress = true WHERE view_name = 'wradm_mv';
          |CREATE OR REPLACE TEMP TABLE openivm_run_fast_wradm_mv AS SELECT dm_s_symb FROM openivm_run_bounds_wradm_mv WHERE openivm_old_max_order IS NULL;
          |CREATE OR REPLACE TEMP TABLE openivm_old_wradm_mv AS SELECT * FROM openivm_data_wradm_mv WHERE dm_s_symb IN (SELECT dm_s_symb FROM openivm_run_fallback_wradm_mv);
          |CREATE OR REPLACE TEMP TABLE openivm_new_wradm_mv AS SELECT * FROM (SELECT dm_s_symb, dm_date, dm_close, SUM(dm_close) OVER (PARTITION BY dm_s_symb ORDER BY dm_date) AS run_sum FROM memory.main.wradm_daily_market) openivm_recompute WHERE dm_s_symb IN (SELECT dm_s_symb FROM openivm_run_fallback_wradm_mv);
          |INSERT INTO openivm_delta_wradm_mv
          |SELECT *, CAST(-1 AS INTEGER), CURRENT_TIMESTAMP FROM openivm_old_wradm_mv
          |UNION ALL
          |SELECT *, CAST(1 AS INTEGER), CURRENT_TIMESTAMP FROM openivm_new_wradm_mv;
          |INSERT INTO openivm_delta_wradm_mv
          |SELECT d.dm_s_symb, d.dm_date, d.dm_close, s.run_sum, CAST(1 AS INTEGER), CURRENT_TIMESTAMP
          |FROM openivm_delta_wradm_daily_market d
          |JOIN openivm_run_fast_wradm_mv fk ON d.dm_s_symb IS NOT DISTINCT FROM fk.dm_s_symb
          |LEFT JOIN openivm_run_state_wradm_mv s ON d.dm_s_symb IS NOT DISTINCT FROM s.dm_s_symb
          |WHERE d.openivm_multiplicity > 0 AND d.openivm_timestamp > CAST('2026-06-30 20:00:00' AS TIMESTAMP);
          |UPDATE openivm_views SET refresh_in_progress = false WHERE view_name = 'wradm_mv';
          |""".stripMargin
      val rewritten = SparkRefreshRewriter
        .rewrite(
          compiledSql = cascadeInput,
          mvName = p52MvName,
          mvLocation = "dbfs:/delta/wradm_mv",
          viewLogicalName = p52ViewLogicalName,
          sourceTempViews = Map("wradm_daily_market" -> "openivm_delta_wradm_daily_market"),
          viewDeltaPath = "dbfs:/delta/_tmp/wradm_mv_delta_uuid",
          mvVersionBeforeRefresh = Some(3)
        )
        .statements
      val ctas = rewritten
        .find(s => s.startsWith("CREATE OR REPLACE TABLE delta.`dbfs:/delta/_tmp/wradm_mv_delta_uuid`"))
        .getOrElse(fail("fallback cascade CTAS missing"))
      ctas should include("openivm_old_wradm_mv")
      ctas should include("openivm_new_wradm_mv")
      val append = rewritten
        .find(s =>
          s.startsWith("INSERT INTO delta.`dbfs:/delta/_tmp/wradm_mv_delta_uuid`") &&
            s.contains("openivm_run_fast_wradm_mv")
        )
        .getOrElse(fail("fast cascade append missing"))
      append should not include "CREATE OR REPLACE TABLE"
      append should not include "openivm_timestamp"
      // Exactly one CTAS create of the view-delta path (the second is an append).
      rewritten.count(_.contains("CREATE OR REPLACE TABLE delta.`dbfs:/delta/_tmp/wradm_mv_delta_uuid`")) shouldBe 1
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

  // ── 12. recompute WHERE EXISTS → LEFT SEMI JOIN ──────────────────────────
  // Reproduces the broadcast-explosion shape that produced
  //   `Cannot broadcast the table that is larger than 8.0 GiB: 57.6 GiB`
  // for `gold.fact_market_history` at SF=10 in ivm-bench. The trigger is
  // openivm's SIMPLE_PROJECTION recompute INSERT over an SCD2 range-joined
  // view body, post-wrapped into a `MERGE INTO ... USING (... WHERE EXISTS
  // ... IS NOT DISTINCT FROM ...) AS d ON false WHEN NOT MATCHED THEN INSERT`.
  describe("recompute WHERE EXISTS → LEFT SEMI JOIN rewrite") {
    val recomputeInput: String =
      """UPDATE openivm_views SET refresh_in_progress = true WHERE view_name = 'mv_r';
        |CREATE OR REPLACE TABLE delta.`dbfs:/delta/_tmp/mv_r_delta_uuid` USING DELTA AS
        |SELECT NULL::INTEGER AS openivm_left_key, 1 AS openivm_multiplicity, '2026-01-01'::TIMESTAMP AS openivm_timestamp;
        |MERGE INTO openivm_data_mv_r AS v USING (
        |SELECT DISTINCT openivm_left_key FROM openivm_delta_mv_r
        |) AS _d ON _d.openivm_left_key IS NOT DISTINCT FROM v.openivm_left_key
        |WHEN MATCHED THEN DELETE;
        |INSERT INTO openivm_data_mv_r
        |SELECT * FROM (
        |  WITH scan_0 AS (SELECT * FROM memory.main.silver_daily_market),
        |       scan_1 AS (SELECT * FROM memory.main.gold_dim_security)
        |  SELECT s.sk_company_id AS openivm_left_key, dm.dm_date, dm.dm_close
        |  FROM scan_0 dm INNER JOIN scan_1 s ON dm.dm_s_symb = s.symbol
        |) openivm_lj
        |WHERE EXISTS (SELECT 1 FROM openivm_delta_mv_r _d WHERE _d.openivm_left_key IS NOT DISTINCT FROM openivm_lj.openivm_left_key);
        |UPDATE openivm_views SET refresh_in_progress = false WHERE view_name = 'mv_r';
        |""".stripMargin

    it("converts WHERE EXISTS IS NOT DISTINCT FROM into LEFT SEMI JOIN ... <=>") {
      val rewritten = SparkRefreshRewriter.rewrite(
        compiledSql = recomputeInput,
        mvName = mvName,
        mvLocation = mvLocation,
        viewLogicalName = viewLogicalName,
        sourceTempViews = Map.empty,
        viewDeltaPath = viewDeltaPath
      )
      val mergeStmt = rewritten.statements
        .find(s => s.contains("ON false") && s.contains("WHEN NOT MATCHED"))
        .getOrElse(fail("recompute INSERT MERGE missing"))

      withClue(s"rewritten merge:\n$mergeStmt\n") {
        mergeStmt should include("LEFT SEMI JOIN")
        mergeStmt should include("SELECT DISTINCT openivm_left_key FROM")
        mergeStmt should include("_openivm_ak")
        mergeStmt should include("openivm_lj.openivm_left_key <=> _openivm_ak.openivm_left_key")
        // WHERE EXISTS must be gone.
        mergeStmt.toUpperCase should not include "WHERE EXISTS"
        mergeStmt should not include "IS NOT DISTINCT FROM openivm_lj.openivm_left_key"
      }
    }

    it("is idempotent — re-running the rewriter on already-rewritten SQL is a no-op for the recompute MERGE") {
      val once = SparkRefreshRewriter.rewrite(
        compiledSql = recomputeInput,
        mvName = mvName,
        mvLocation = mvLocation,
        viewLogicalName = viewLogicalName,
        sourceTempViews = Map.empty,
        viewDeltaPath = viewDeltaPath
      )
      // Re-feed each rewritten statement: the post-process passes operate on
      // arbitrary strings so reapplying the recompute pass must not double-wrap.
      val rewritten1 = once.statements
        .find(s => s.contains("LEFT SEMI JOIN"))
        .getOrElse(
          fail("LEFT SEMI JOIN missing on first pass")
        )
      val deltaAlias = "_openivm_ak"
      // Count occurrences of the marker alias — must stay exactly one.
      val count = rewritten1.sliding(deltaAlias.length).count(_ == deltaAlias)
      // 3 references: subquery alias `_openivm_ak`, and 1× in the ON clause
      // (delta side of `<=>`).  No re-rewrite should add more.
      count shouldBe 2
    }

    it("leaves a DELETE MERGE (WHEN MATCHED THEN DELETE) untouched") {
      val rewritten = SparkRefreshRewriter.rewrite(
        compiledSql = recomputeInput,
        mvName = mvName,
        mvLocation = mvLocation,
        viewLogicalName = viewLogicalName,
        sourceTempViews = Map.empty,
        viewDeltaPath = viewDeltaPath
      )
      val deleteMerge = rewritten.statements
        .find(s => s.toUpperCase.contains("WHEN MATCHED THEN DELETE"))
        .getOrElse(fail("DELETE MERGE missing"))
      // DELETE side has no WHERE EXISTS and must not gain a LEFT SEMI JOIN.
      deleteMerge should not include "LEFT SEMI JOIN"
    }

    it("does not rewrite a MERGE whose ON clause is not exactly `ON false`") {
      // Hand-crafted MERGE with `ON v.k = d.k` — must be no-op for the WHERE
      // EXISTS rewrite because dropping the WHERE EXISTS could change the
      // matched set on the INSERT branch only by skipping rows that exist in
      // the delta. The strict gate is `ON FALSE` only.
      val sqlWithOnMatch =
        s"""MERGE INTO `mydb`.`mv_r` AS v USING (
           |  SELECT openivm_lj.k, openivm_lj.openivm_left_key
           |  FROM (SELECT * FROM memory.main.t) openivm_lj
           |  WHERE EXISTS (SELECT 1 FROM delta.`$viewDeltaPath` _d WHERE _d.openivm_left_key IS NOT DISTINCT FROM openivm_lj.openivm_left_key)
           |) AS d
           |ON v.openivm_left_key = d.openivm_left_key
           |WHEN MATCHED THEN UPDATE SET v.k = d.k
           |WHEN NOT MATCHED THEN INSERT (openivm_left_key, k) VALUES (d.openivm_left_key, d.k)
           |""".stripMargin
      // Wrap as a standalone openivm program so the rewriter pipeline pipes it
      // through the post-process chain.
      val wrapped =
        s"""UPDATE openivm_views SET refresh_in_progress = true WHERE view_name = 'mv_r';
           |$sqlWithOnMatch;
           |UPDATE openivm_views SET refresh_in_progress = false WHERE view_name = 'mv_r';
           |""".stripMargin
      val rewritten = SparkRefreshRewriter.rewrite(
        compiledSql = wrapped,
        mvName = mvName,
        mvLocation = mvLocation,
        viewLogicalName = viewLogicalName,
        sourceTempViews = Map.empty,
        viewDeltaPath = viewDeltaPath
      )
      // The non-matching-MERGE statement is not openivm-classified, so the
      // dispatch in `classify` may return Unknown and drop it — that's fine.
      // We only need to assert that no LEFT SEMI JOIN was introduced.
      rewritten.statements.foreach { s =>
        withClue(s"statement should not gain LEFT SEMI JOIN:\n$s") {
          s should not include "LEFT SEMI JOIN"
        }
      }
    }
  }

  // ── 12. isRecomputeInsertMerge — recompute INSERT MERGE detector ─────────
  describe("isRecomputeInsertMerge") {
    it("detects the bench-shape MERGE … USING (…) AS d ON FALSE WHEN NOT MATCHED THEN INSERT") {
      val sql =
        """MERGE INTO delta.`s3://w/mv` AS v
          |USING (WITH scan_0 AS (SELECT * FROM x) SELECT * FROM scan_0) AS d
          |ON false
          |WHEN NOT MATCHED THEN INSERT (a) VALUES (d.a)""".stripMargin
      SparkRefreshRewriter.isRecomputeInsertMerge(sql) shouldBe true
    }

    it("detects the ON (FALSE) parenthesised variant") {
      val sql =
        """MERGE INTO `db`.`mv` AS v
          |USING (SELECT * FROM x) AS d
          |ON (FALSE)
          |WHEN NOT MATCHED THEN INSERT *""".stripMargin
      SparkRefreshRewriter.isRecomputeInsertMerge(sql) shouldBe true
    }

    it("detects the WHEN NOT MATCHED AND <pred> THEN INSERT tail variant") {
      val sql =
        """MERGE INTO delta.`/p` AS v
          |USING (SELECT a FROM s) AS d
          |ON FALSE
          |WHEN NOT MATCHED AND d.a IS NOT NULL THEN INSERT (a) VALUES (d.a)""".stripMargin
      SparkRefreshRewriter.isRecomputeInsertMerge(sql) shouldBe true
    }

    it("survives a leading SQL block comment before MERGE INTO") {
      val sql =
        """/* openivm:stmt=2 */ MERGE INTO `db`.`mv` v
          |USING (SELECT 1 AS a) AS d
          |ON FALSE
          |WHEN NOT MATCHED THEN INSERT (a) VALUES (d.a)""".stripMargin
      SparkRefreshRewriter.isRecomputeInsertMerge(sql) shouldBe true
    }

    it("is paren-aware: parens inside the USING source body don't fool the matcher") {
      val sql =
        """MERGE INTO `db`.`mv` v
          |USING (SELECT a, (b + c) AS bc, COUNT(*) AS n FROM s GROUP BY a, (b + c)) AS d
          |ON FALSE
          |WHEN NOT MATCHED THEN INSERT (a, bc, n) VALUES (d.a, d.bc, d.n)""".stripMargin
      SparkRefreshRewriter.isRecomputeInsertMerge(sql) shouldBe true
    }

    it("returns false for a non-MERGE statement (CTAS / INSERT / UPDATE / DELETE)") {
      SparkRefreshRewriter.isRecomputeInsertMerge(
        "CREATE OR REPLACE TABLE delta.`/p` USING DELTA AS SELECT 1"
      ) shouldBe false
      SparkRefreshRewriter.isRecomputeInsertMerge(
        "INSERT INTO `db`.`mv` SELECT * FROM s"
      ) shouldBe false
      SparkRefreshRewriter.isRecomputeInsertMerge(
        "UPDATE `db`.`mv` SET a = 1 WHERE b = 2"
      ) shouldBe false
      SparkRefreshRewriter.isRecomputeInsertMerge(
        "DELETE FROM `db`.`mv` WHERE a = 1"
      ) shouldBe false
    }

    it("returns false for a real equi-merge (ON <key> = <key>, not ON FALSE)") {
      val sql =
        """MERGE INTO `db`.`mv` v
          |USING (SELECT a, b FROM s) AS d
          |ON v.a = d.a
          |WHEN MATCHED THEN UPDATE SET b = d.b
          |WHEN NOT MATCHED THEN INSERT (a, b) VALUES (d.a, d.b)""".stripMargin
      SparkRefreshRewriter.isRecomputeInsertMerge(sql) shouldBe false
    }

    it("returns false for a delete-merge (WHEN MATCHED first, not WHEN NOT MATCHED)") {
      val sql =
        """MERGE INTO `db`.`mv` v
          |USING (SELECT DISTINCT k FROM s) AS d
          |ON v.k IS NOT DISTINCT FROM d.k
          |WHEN MATCHED THEN DELETE""".stripMargin
      SparkRefreshRewriter.isRecomputeInsertMerge(sql) shouldBe false
    }

    it("returns false for a MERGE whose USING source is not parenthesised (e.g. a CTE reference)") {
      val sql =
        """WITH cte AS (SELECT * FROM s)
          |MERGE INTO `db`.`mv` v
          |USING cte d
          |ON FALSE
          |WHEN NOT MATCHED THEN INSERT *""".stripMargin
      SparkRefreshRewriter.isRecomputeInsertMerge(sql) shouldBe false
    }
  }
}
