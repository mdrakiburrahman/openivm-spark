package org.openivm.spark.parity

import org.openivm.spark.common.{FeatureGate, MvCatalog, RefreshTypeCode}
import org.openivm.spark.parity.base.IvmParitySpecBase

abstract class UniqueJoinSimplifyScenarios extends IvmParitySpecBase("unique-join-simplify") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  override protected def extraSparkConf: Map[String, String] =
    Map(FeatureGate.UniqueJoinSimplifyEnabledKey -> "true")

  private def mvRefreshType(name: String): Int = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    MvCatalog.lookup(spark, id).getOrElse(fail(s"MV $name not found in catalog")).refreshType
  }

  describe("uniqueness-driven join simplification") {
    it("keeps INNER probe and LEFT unused-right rewrites result-invariant across refreshes") {
      sql("CREATE TABLE IF NOT EXISTS ujs_fact(id INT, customer_id INT, region_id INT, amount INT) USING DELTA")
      sql(
        "CREATE TABLE IF NOT EXISTS ujs_customer(id INT, name STRING) USING DELTA " +
          "TBLPROPERTIES ('spark.openivm.unique_key' = 'id')"
      )
      sql(
        "CREATE TABLE IF NOT EXISTS ujs_region(id INT, label STRING) USING DELTA " +
          "TBLPROPERTIES ('spark.openivm.unique_key' = 'id')"
      )

      sql("INSERT INTO ujs_customer VALUES (10, 'Alice'), (20, 'Bob')")
      sql("INSERT INTO ujs_region VALUES (1, 'North'), (2, 'South')")
      sql("INSERT INTO ujs_fact VALUES (1, 10, 1, 100), (2, 20, 2, 200), (3, 30, 9, 300)")

      val innerBody =
        "SELECT f.id, f.amount FROM ujs_fact f JOIN ujs_customer c ON f.customer_id = c.id"
      val leftBody =
        "SELECT f.id, f.region_id, f.amount FROM ujs_fact f LEFT JOIN ujs_region r ON f.region_id = r.id"

      sql(s"CREATE MATERIALIZED VIEW ujs_mv_inner AS $innerBody")
      sql(s"CREATE MATERIALIZED VIEW ujs_mv_left AS $leftBody")
      mvRefreshType("ujs_mv_inner") should not be RefreshTypeCode.FullRefresh
      mvRefreshType("ujs_mv_left") should not be RefreshTypeCode.FullRefresh
      assertMvCorrect("ujs_mv_inner", innerBody)
      assertMvCorrect("ujs_mv_left", leftBody)

      sql("INSERT INTO ujs_customer VALUES (30, 'Charlie')")
      sql("DELETE FROM ujs_region WHERE id = 1")
      refreshMv("ujs_mv_inner")
      refreshMv("ujs_mv_left")
      assertMvCorrect("ujs_mv_inner", innerBody)
      assertMvCorrect("ujs_mv_left", leftBody)

      sql("INSERT INTO ujs_fact VALUES (4, 10, 2, 400), (5, 40, 7, 500)")
      refreshMv("ujs_mv_inner")
      refreshMv("ujs_mv_left")
      assertMvCorrect("ujs_mv_inner", innerBody)
      assertMvCorrect("ujs_mv_left", leftBody)

      sql("DROP MATERIALIZED VIEW ujs_mv_inner")
      sql("DROP MATERIALIZED VIEW ujs_mv_left")
      sql("DROP TABLE ujs_fact")
      sql("DROP TABLE ujs_customer")
      sql("DROP TABLE ujs_region")
    }
  }
}
