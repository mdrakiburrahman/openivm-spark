package org.openivm.spark.parity

import org.openivm.spark.common.{FeatureGate, MvCatalog, RefreshSqlLogCatalog, RefreshTypeCode}
import org.openivm.spark.parity.base.{CdfMode, IvmParitySpecBase}

class GlobalWindowSinglePassCdfSpec       extends GlobalWindowSinglePassCdfBase(backing = false)
class BackedGlobalWindowSinglePassCdfSpec extends GlobalWindowSinglePassCdfBase(backing = true)

abstract class GlobalWindowSinglePassCdfBase(backing: Boolean)
    extends IvmParitySpecBase(s"global-window-single-pass-$backing")
    with CdfMode {
  override protected def extraSparkConf: Map[String, String] = Map(
    FeatureGate.WindowSinglePassReplaceEnabledKey -> "true",
    FeatureGate.QueryLogEnabledKey                -> "true",
    "spark.openivm.catalogPreservesColumnCase"    -> "false"
  )

  it("replaces global ranks once, without a cascade table, and preserves CDF consumers") {
    sql("CREATE TABLE gw_sales(id INT, region STRING, amount INT) USING DELTA")
    sql("INSERT INTO gw_sales VALUES (1, 'east', 10), (2, 'east', 20), (3, 'west', 15)")
    val rankAlias = if (backing) "Rank" else "rank"
    val query = s"SELECT region, total, DENSE_RANK() OVER (ORDER BY total DESC) AS $rankAlias " +
      "FROM (SELECT region, SUM(amount) AS total FROM gw_sales GROUP BY region) t"
    sql(s"CREATE MATERIALIZED VIEW gw_rank AS $query")

    val id   = spark.sessionState.sqlParser.parseTableIdentifier("gw_rank")
    val meta = MvCatalog.lookup(spark, id).get
    meta.refreshType shouldBe RefreshTypeCode.WindowPartition
    meta.usesBackingDataTable shouldBe backing

    def checkRefresh(): Unit = {
      val before = mvDataVersion("gw_rank")
      RefreshSqlLogCatalog.removeAll(spark)
      refreshMv("gw_rank")
      assertMvCorrect("gw_rank", query)
      val statements = sql("SHOW OPENIVM QUERY LOG").collect().map(_.getString(9)).toSeq
      withClue(statements.mkString("\n")) {
        mvDataVersion("gw_rank") shouldBe before + 1
        statements.count(_.contains("REPLACE WHERE true")) shouldBe 1
      }
      statements.mkString("\n") should not include "/_ivm/view_deltas/"
      statements.exists(_.trim.toUpperCase.startsWith("DELETE FROM")) shouldBe false
    }

    sql("INSERT INTO gw_sales VALUES (4, 'west', 30), (5, 'north', 45)")
    checkRefresh()

    // A consumer created after the terminal optimization starts from the new
    // snapshot, then observes mixed mutations and rank changes through CDF.
    val downstream = "SELECT region, total, rank FROM gw_rank"
    sql(s"CREATE MATERIALIZED VIEW gw_consumer AS $downstream")
    sql("UPDATE gw_sales SET amount = 100 WHERE id = 1")
    sql("DELETE FROM gw_sales WHERE id = 3")
    sql("INSERT INTO gw_sales VALUES (6, 'south', 100)")
    checkRefresh()
    refreshMv("gw_consumer")
    assertMvCorrect("gw_consumer", downstream)

    sql("DELETE FROM gw_sales")
    checkRefresh()
    refreshMv("gw_consumer")
    assertMvCorrect("gw_consumer", downstream)
    spark.table("gw_rank").count() shouldBe 0L
    val emptyVersion = mvDataVersion("gw_rank")
    refreshMv("gw_rank")
    mvDataVersion("gw_rank") shouldBe emptyVersion
  }

  it("retains the native delete/insert and cascade program when disabled") {
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
