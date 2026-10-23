package org.openivm.spark.parity

import org.openivm.spark.common.{FeatureGate, MvCatalog, StagingCatalog}
import org.openivm.spark.parity.base.{InterceptMode, IvmParitySpecBase}

class WindowSinglePassReplaceInterceptSpec
    extends IvmParitySpecBase("window-single-pass-replace-intercept")
    with InterceptMode {

  override protected def extraSparkConf: Map[String, String] =
    Map(
      FeatureGate.WindowSinglePassReplaceEnabledKey -> "true",
      FeatureGate.QueryLogEnabledKey -> "true"
    )

  it("preserves the native global-window cascade in interception mode") {
    sql("CREATE TABLE wspri_global_sales(id INT, region STRING, amount INT) USING DELTA")
    sql("INSERT INTO wspri_global_sales VALUES (1, 'east', 10), (2, 'west', 20)")
    val query = "SELECT region, total, DENSE_RANK() OVER (ORDER BY total DESC) AS rank " +
      "FROM (SELECT region, SUM(amount) AS total FROM wspri_global_sales GROUP BY region) t"
    sql(s"CREATE MATERIALIZED VIEW wspri_global AS $query")
    val downstream = "SELECT region, total, rank FROM wspri_global"
    sql(s"CREATE MATERIALIZED VIEW wspri_global_consumer AS $downstream")
    sql("INSERT INTO wspri_global_sales VALUES (3, 'east', 30), (4, 'transient', 1)")
    sql("UPDATE wspri_global_sales SET amount = 40 WHERE id = 1")
    sql("DELETE FROM wspri_global_sales WHERE id IN (2, 4)")
    val before = mvDataVersion("wspri_global")
    refreshMv("wspri_global")
    assertMvCorrect("wspri_global", query)
    val statements = sql("SHOW OPENIVM QUERY LOG").collect().map(_.getString(9)).mkString("\n")
    mvDataVersion("wspri_global") shouldBe before + 2
    statements should include("/_ivm/view_deltas/")
    statements should not include "REPLACE WHERE true"
    refreshMv("wspri_global_consumer")
    assertMvCorrect("wspri_global_consumer", downstream)
  }

  describe("WINDOW_PARTITION single-pass no-op replacement") {
    it("does not stage a downstream cascade when conflicting DML leaves the window bag unchanged") {
      sql("CREATE TABLE wspri_sales(id INT, region STRING, amount INT) USING DELTA")
      sql("INSERT INTO wspri_sales VALUES (1, 'east', 10), (2, 'east', 20), (3, 'west', 30)")
      val viewSql =
        "SELECT id, region, amount, " +
          "ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount, id) AS rn FROM wspri_sales"
      sql(s"CREATE MATERIALIZED VIEW wspri_mv AS $viewSql")
      val downstreamSql = "SELECT id, region, amount, rn FROM wspri_mv"
      sql(s"CREATE MATERIALIZED VIEW wspri_downstream AS $downstreamSql")

      sql("INSERT INTO wspri_sales VALUES (4, 'east', 40), (5, 'west', 50)")
      sql("UPDATE wspri_sales SET amount = 11 WHERE id = 1")
      sql("DELETE FROM wspri_sales WHERE id IN (4, 5)")
      sql("UPDATE wspri_sales SET amount = 10 WHERE id = 1")
      sql("DELETE FROM wspri_sales WHERE id = 3")
      sql("INSERT INTO wspri_sales VALUES (3, 'west', 30)")

      refreshMv("wspri_mv")
      assertMvCorrect("wspri_mv", viewSql)
      assertMvCorrect("wspri_downstream", downstreamSql)

      val downstreamId = spark.sessionState.sqlParser.parseTableIdentifier("wspri_downstream")
      val downstreamMeta =
        MvCatalog.lookup(spark, downstreamId).getOrElse(fail("missing wspri_downstream metadata"))
      StagingCatalog.collectFor(spark, "wspri_downstream", downstreamMeta.sourceTables) shouldBe empty
    }
  }
}
