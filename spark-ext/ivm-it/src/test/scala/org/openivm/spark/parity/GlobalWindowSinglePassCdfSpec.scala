package org.openivm.spark.parity

import org.openivm.spark.common.{FeatureGate, MvCatalog, RefreshSqlLogCatalog, RefreshTypeCode}
import org.openivm.spark.parity.base.{CdfMode, IvmParitySpecBase}

class GlobalWindowSinglePassCdfSpec extends IvmParitySpecBase("global-window-single-pass") with CdfMode {
  override protected def extraSparkConf: Map[String, String] = Map(
    FeatureGate.WindowSinglePassReplaceEnabledKey -> "true",
    FeatureGate.QueryLogEnabledKey                -> "true",
    "spark.openivm.catalogPreservesColumnCase"    -> "false"
  )

  Seq(false, true).foreach { backing =>
    it(s"replaces global ranks once and feeds CDF consumers with backing=$backing") {
      val prefix    = if (backing) "gw_backed" else "gw_direct"
      val source    = s"${prefix}_sales"
      val view      = s"${prefix}_rank"
      val consumer  = s"${prefix}_consumer"
      val rankAlias = if (backing) "Rank" else "rank"
      sql(s"CREATE TABLE $source(id INT, region STRING, amount INT) USING DELTA")
      sql(s"INSERT INTO $source VALUES (0, 'unknown', NULL), (1, 'east', 10), (2, 'east', 20), (3, 'west', 15)")
      val query = s"SELECT region, total, DENSE_RANK() OVER (ORDER BY total DESC) AS $rankAlias " +
        s"FROM (SELECT region, SUM(amount) AS total FROM $source GROUP BY region) t"
      sql(s"CREATE MATERIALIZED VIEW $view AS $query")
      assertMvCorrect(view, query)
      val id   = spark.sessionState.sqlParser.parseTableIdentifier(view)
      val meta = MvCatalog.lookup(spark, id).getOrElse(fail("missing global window metadata"))
      meta.refreshType shouldBe RefreshTypeCode.WindowPartition
      meta.usesBackingDataTable shouldBe backing

      def checkRefresh(): Unit = {
        val before = mvDataVersion(view)
        RefreshSqlLogCatalog.removeAll(spark)
        refreshMv(view)
        assertMvCorrect(view, query)
        val statements = sql("SHOW OPENIVM QUERY LOG").collect().map(_.getString(9)).toSeq
        withClue(statements.mkString("\n")) {
          mvDataVersion(view) shouldBe before + 1
          statements.count(_.contains("REPLACE WHERE true")) shouldBe 1
          statements.mkString("\n") should not include "/_ivm/view_deltas/"
          statements.exists(_.trim.toUpperCase.startsWith("DELETE FROM")) shouldBe false
        }
      }

      sql(s"INSERT INTO $source VALUES (4, 'west', 30), (5, 'north', 45)")
      checkRefresh()
      val downstream = s"SELECT region, total, rank FROM $view"
      sql(s"CREATE MATERIALIZED VIEW $consumer AS $downstream")
      assertMvCorrect(consumer, downstream)
      sql(s"UPDATE $source SET amount = 100 WHERE id = 1")
      sql(s"DELETE FROM $source WHERE id = 3")
      sql(s"INSERT INTO $source VALUES (6, 'south', 100), (7, 'transient', 1)")
      sql(s"UPDATE $source SET amount = 2 WHERE id = 7")
      sql(s"DELETE FROM $source WHERE id = 7")
      checkRefresh()
      refreshMv(consumer)
      assertMvCorrect(consumer, downstream)

      sql(s"DELETE FROM $source")
      checkRefresh()
      refreshMv(consumer)
      assertMvCorrect(consumer, downstream)
      val emptyVersion = mvDataVersion(view)
      refreshMv(view)
      assertMvCorrect(view, query)
      mvDataVersion(view) shouldBe emptyVersion
    }
  }

  it("retains the native program when single-pass replacement is disabled") {
    restartSpark(extraSparkConf + (FeatureGate.WindowSinglePassReplaceEnabledKey -> "false"))
    sql("CREATE TABLE gw_control_sales(id INT, region STRING, amount INT) USING DELTA")
    sql("INSERT INTO gw_control_sales VALUES (1, 'east', 10), (2, 'west', 20)")
    val query = "SELECT region, total, DENSE_RANK() OVER (ORDER BY total DESC) AS rank " +
      "FROM (SELECT region, SUM(amount) AS total FROM gw_control_sales GROUP BY region) t"
    sql(s"CREATE MATERIALIZED VIEW gw_control AS $query")
    sql("INSERT INTO gw_control_sales VALUES (3, 'east', 30)")
    val before = mvDataVersion("gw_control")
    RefreshSqlLogCatalog.removeAll(spark)
    refreshMv("gw_control")
    assertMvCorrect("gw_control", query)
    mvDataVersion("gw_control") shouldBe before + 2
    val statements = sql("SHOW OPENIVM QUERY LOG").collect().map(_.getString(9)).mkString("\n")
    statements should include("/_ivm/view_deltas/")
    statements should not include "REPLACE WHERE true"
  }
}
