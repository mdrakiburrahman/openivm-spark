package org.openivm.spark.parity

import org.openivm.spark.common.{FeatureGate, MvCatalog, RefreshTypeCode}
import org.openivm.spark.parity.base.{CdfMode, IvmParitySpecBase}

class WindowSinglePassReplaceCdfSpec extends IvmParitySpecBase("window-single-pass-replace") with CdfMode {

  override protected def extraSparkConf: Map[String, String] =
    Map(
      FeatureGate.WindowSinglePassReplaceEnabledKey -> "true",
      FeatureGate.QueryLogEnabledKey                -> "true"
    )

  describe("WINDOW_PARTITION single-pass replacement with a named affected-key view") {
    it("creates the affected-key view before collecting keys and remains bag-equal") {
      sql("CREATE TABLE wspr_sales(id INT, region STRING, amount INT) USING DELTA")
      sql(
        "INSERT INTO wspr_sales VALUES " +
          "(1, 'east', 10), (3, 'east', 30), (4, 'west', 5), (5, 'west', 15)"
      )
      val viewSql =
        "SELECT id, region, amount, " +
          "ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount, id) AS rn FROM wspr_sales"
      sql(s"CREATE MATERIALIZED VIEW wspr_mv AS $viewSql")

      val id = spark.sessionState.sqlParser.parseTableIdentifier("wspr_mv")
      MvCatalog.lookup(spark, id).getOrElse(fail("missing wspr_mv metadata")).refreshType shouldBe
        RefreshTypeCode.WindowPartition

      // Batch conflicting inserts, updates, and deletes before one refresh so
      // affected-key consolidation and replacement see the complete CDF batch.
      sql("INSERT INTO wspr_sales VALUES (2, 'east', 20), (6, 'west', 25), (7, 'north', 1)")
      sql("UPDATE wspr_sales SET amount = 22 WHERE id = 2")
      sql("DELETE FROM wspr_sales WHERE id = 6")
      sql("UPDATE wspr_sales SET amount = 2 WHERE id = 5")

      refreshMv("wspr_mv")

      assertMvCorrect("wspr_mv", viewSql)
      val refreshSql = sql("SHOW OPENIVM QUERY LOG").collect().map(_.getString(9)).mkString("\n")
      refreshSql should include("REPLACE WHERE")
      refreshSql should not include "WHEN MATCHED THEN DELETE"
    }
  }
}
