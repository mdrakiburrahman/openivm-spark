package org.openivm.spark.parity

import org.apache.spark.sql.catalyst.TableIdentifier
import org.openivm.spark.common._
import org.openivm.spark.parity.base.IvmParitySpecBase

abstract class SemiJoinPruneFkTermComposeScenarios extends IvmParitySpecBase("semijoin-fkterm-compose") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  override protected def extraSparkConf: Map[String, String] =
    Map(
      "spark.openivm.queryLog.enabled"                   -> "true",
      "spark.openivm.refresh.semiJoinPrune.enabled"      -> "true",
      "spark.openivm.refresh.fkTermPrune.enabled"        -> "true",
      "spark.openivm.refresh.scd2RangeAccel.enabled"     -> "true",
      "spark.openivm.refresh.uniqueJoinSimplify.enabled" -> "false"
    )

  override def beforeAll(): Unit = {
    super.beforeAll()
    RefreshSqlLogCatalog.ensureTables(spark)
  }

  private def mvRefreshType(name: String): Int = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    MvCatalog.lookup(spark, id).getOrElse(fail(s"MV $name not found in catalog")).refreshType
  }

  private def clearLog(): Unit = RefreshSqlLogCatalog.removeAll(spark)

  private def rewrittenSqlText: String =
    sql("SHOW OPENIVM QUERY LOG")
      .collect()
      .filter(row => row.getString(6) == "rewritten_stmt")
      .map(row => row.getString(9))
      .mkString("\n")

  describe("FK term pruning composes with semiJoinPrune") {
    it("keeps the star join incremental, correct, and still semi-pruned") {
      sql("CREATE TABLE sfk_dim_product(product_id INT, name STRING) USING DELTA")
      sql(
        "CREATE TABLE sfk_dim_security(security_id INT, symbol STRING, effective_date DATE, end_date DATE) USING DELTA"
      )
      sql(
        "CREATE TABLE sfk_fact_sales(" +
          "sale_id INT, product_id INT, security_id INT, trade_date DATE, amount INT) USING DELTA " +
          "TBLPROPERTIES ('spark.openivm.fk.product_id' = 'sfk_dim_product(product_id)')"
      )

      sql("INSERT INTO sfk_dim_product VALUES (1, 'Widget'), (2, 'Gadget')")
      sql(
        "INSERT INTO sfk_dim_security VALUES " +
          "(10, 'AAA', DATE '2020-01-01', DATE '2020-12-31'), " +
          "(20, 'BBB', DATE '2020-01-01', DATE '2020-12-31')"
      )
      sql(
        "INSERT INTO sfk_fact_sales VALUES " +
          "(1, 1, 10, DATE '2020-02-01', 100), (2, 2, 20, DATE '2020-03-01', 200)"
      )

      sql(
        "CREATE MATERIALIZED VIEW sfk_mv_star AS " +
          "SELECT f.sale_id, p.name, s.symbol, f.amount " +
          "FROM sfk_fact_sales f " +
          "JOIN sfk_dim_product p ON f.product_id = p.product_id " +
          "JOIN sfk_dim_security s ON f.security_id = s.security_id " +
          " AND f.trade_date >= s.effective_date AND f.trade_date < s.end_date"
      )
      mvRefreshType("sfk_mv_star") should not be RefreshTypeCode.FullRefresh
      assertMvCorrect("sfk_mv_star", starQuery)

      sql("INSERT INTO sfk_dim_product VALUES (3, 'Thingamajig')")
      sql(
        "INSERT INTO sfk_dim_security VALUES (30, 'CCC', DATE '2020-01-01', DATE '2020-12-31')"
      )
      sql("INSERT INTO sfk_fact_sales VALUES (3, 3, 30, DATE '2020-04-01', 300)")
      clearLog()
      refreshMv("sfk_mv_star")

      mvRefreshType("sfk_mv_star") should not be RefreshTypeCode.FullRefresh
      assertMvCorrect("sfk_mv_star", starQuery)

      val logSql = rewrittenSqlText
      logSql should not include "FULL_REFRESH"
      assertFkTermPruneAndSemiJoinCompose()
    }
  }

  private def assertFkTermPruneAndSemiJoinCompose(): Unit = {
    val facts = WorkloadFactsRegistry.forRefresh().discover(spark, Seq("sfk_fact_sales", "sfk_dim_product"))
    facts.fkRelations should contain(
      ForeignKeyRelation("sfk_fact_sales", Seq("product_id"), "sfk_dim_product", Seq("product_id"))
    )

    val compiled =
      """UPDATE openivm_views SET refresh_in_progress = true WHERE view_name = 'sfk_mv_star';
        |WITH full_source (sale_id, product_id, amount) AS (
        |  SELECT sale_id, product_id, amount FROM memory.main.sfk_fact_sales
        |),
        |join_delta AS (
        |  SELECT f.sale_id, p.name, f.amount, f.openivm_multiplicity
        |  FROM memory.main.openivm_delta_sfk_fact_sales f
        |  JOIN memory.main.sfk_dim_product p ON f.product_id = p.product_id
        |  UNION ALL
        |  SELECT fs.sale_id, p.name, fs.amount, p.openivm_multiplicity
        |  FROM full_source fs
        |  JOIN memory.main.openivm_delta_sfk_dim_product p ON fs.product_id = p.product_id
        |  UNION ALL
        |  SELECT f.sale_id, p.name, f.amount, -1 * f.openivm_multiplicity * p.openivm_multiplicity
        |  FROM memory.main.openivm_delta_sfk_fact_sales f
        |  JOIN memory.main.openivm_delta_sfk_dim_product p ON f.product_id = p.product_id
        |)
        |INSERT INTO openivm_delta_sfk_mv_star (sale_id, name, amount, openivm_multiplicity)
        |SELECT * FROM join_delta;
        |UPDATE openivm_views SET refresh_in_progress = false WHERE view_name = 'sfk_mv_star';
        |""".stripMargin

    val rewritten = SparkRefreshRewriter
      .rewrite(
        compiledSql = compiled,
        mvName = TableIdentifier("sfk_mv_star"),
        mvLocation = warehouseDir + "/sfk_mv_star",
        viewLogicalName = "sfk_mv_star",
        sourceTempViews = Map.empty,
        viewDeltaPath = warehouseDir + "/_ivm/rewrite_probe",
        deltaShape = Map(
          "default.sfk_fact_sales"  -> DeltaShape.InsertOnly,
          "default.sfk_dim_product" -> DeltaShape.InsertOnly
        ),
        semiJoinPruneEnabled = true,
        fkTermPruneEnabled = true,
        fkRelations = facts.fkRelations
      )
      .statements
      .head

    rewritten.split("(?i)UNION\\s+ALL").length shouldBe 2
    rewritten should include("__openivm_full_source_pre")
    rewritten should not include "-1 * f.openivm_multiplicity * p.openivm_multiplicity"
  }

  private def starQuery: String =
    "SELECT f.sale_id, p.name, s.symbol, f.amount " +
      "FROM sfk_fact_sales f " +
      "JOIN sfk_dim_product p ON f.product_id = p.product_id " +
      "JOIN sfk_dim_security s ON f.security_id = s.security_id " +
      " AND f.trade_date >= s.effective_date AND f.trade_date < s.end_date"
}
