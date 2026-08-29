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
      val refreshStatements = sql("SHOW OPENIVM QUERY LOG").collect().map(_.getString(9)).toSeq
      val cascadeIdx = refreshStatements.indexWhere { statement =>
        statement.contains("CREATE OR REPLACE TABLE delta.") && statement.contains("/_ivm/view_deltas/")
      }
      val replacementIdx = refreshStatements.indexWhere(_.contains("REPLACE WHERE"))

      cascadeIdx should be >= 0
      replacementIdx should be > cascadeIdx
      refreshStatements(replacementIdx) should include("/_ivm/view_deltas/")
      refreshStatements(replacementIdx) should include("WHERE `openivm_multiplicity` > 0")
      refreshStatements(replacementIdx) should not include "openivm_new_wspr_mv"
      refreshStatements.mkString("\n") should not include "WHEN MATCHED THEN DELETE"
    }

    it("does not commit or cascade when a mixed CDF batch leaves the window bag unchanged") {
      sql("CREATE TABLE wspr_noop_sales(id INT, region STRING, amount INT) USING DELTA")
      sql(
        "INSERT INTO wspr_noop_sales VALUES " +
          "(1, 'east', 10), (2, 'east', 20), (3, 'west', 30)"
      )
      val viewSql =
        "SELECT id, region, amount, " +
          "ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount, id) AS rn FROM wspr_noop_sales"
      sql(s"CREATE MATERIALIZED VIEW wspr_noop_mv AS $viewSql")
      val downstreamSql = "SELECT id, region, amount, rn FROM wspr_noop_mv"
      sql(s"CREATE MATERIALIZED VIEW wspr_noop_downstream AS $downstreamSql")

      val upstreamVersion   = mvDataVersion("wspr_noop_mv")
      val downstreamVersion = mvDataVersion("wspr_noop_downstream")

      // Exercise several conflicting operations before one refresh while
      // restoring the exact source bag.
      sql("INSERT INTO wspr_noop_sales VALUES (4, 'east', 40), (5, 'west', 50)")
      sql("UPDATE wspr_noop_sales SET amount = 11 WHERE id = 1")
      sql("DELETE FROM wspr_noop_sales WHERE id IN (4, 5)")
      sql("UPDATE wspr_noop_sales SET amount = 10 WHERE id = 1")
      sql("DELETE FROM wspr_noop_sales WHERE id = 3")
      sql("INSERT INTO wspr_noop_sales VALUES (3, 'west', 30)")

      refreshMv("wspr_noop_mv")
      assertMvCorrect("wspr_noop_mv", viewSql)
      mvDataVersion("wspr_noop_mv") shouldBe upstreamVersion

      refreshMv("wspr_noop_downstream")
      assertMvCorrect("wspr_noop_downstream", downstreamSql)
      mvDataVersion("wspr_noop_downstream") shouldBe downstreamVersion
    }
  }
}
