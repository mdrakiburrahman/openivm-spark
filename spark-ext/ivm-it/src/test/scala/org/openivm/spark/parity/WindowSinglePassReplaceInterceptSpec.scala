package org.openivm.spark.parity

import org.openivm.spark.common.{FeatureGate, MvCatalog, StagingCatalog}
import org.openivm.spark.parity.base.{InterceptMode, IvmParitySpecBase}

class WindowSinglePassReplaceInterceptSpec
    extends IvmParitySpecBase("window-single-pass-replace-intercept")
    with InterceptMode {

  override protected def extraSparkConf: Map[String, String] =
    Map(FeatureGate.WindowSinglePassReplaceEnabledKey -> "true")

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
