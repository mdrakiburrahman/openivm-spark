package org.openivm.spark.parity

import org.openivm.spark.common.{FeatureGate, MvCatalog, RefreshSqlLogCatalog, RefreshTypeCode}
import org.openivm.spark.parity.base.{InterceptMode, IvmParitySpecBase}

class FingerprintGuardSpec extends FingerprintGuardScenarios with InterceptMode

abstract class FingerprintGuardScenarios extends IvmParitySpecBase("fingerprint-guard") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  override protected def extraSparkConf: Map[String, String] =
    Map(
      FeatureGate.FingerprintGuardEnabledKey -> "true",
      FeatureGate.QueryLogEnabledKey         -> "true"
    )

  private val ColCategory = 6

  private def clearLog(): Unit =
    RefreshSqlLogCatalog.removeAll(spark)

  private def loggedFullRefresh: Boolean =
    sql("SHOW OPENIVM QUERY LOG").collect().exists(_.getString(ColCategory) == "full_refresh_stmt")

  describe("E6 query fingerprint guard") {
    it("routes a same-name changed body through FULL_REFRESH while leaving unchanged deterministic MVs incremental") {
      sql("CREATE TABLE IF NOT EXISTS fg_sales(id INT, amount INT) USING DELTA")
      sql("INSERT INTO fg_sales VALUES (1, 10), (2, 20)")
      sql("CREATE MATERIALIZED VIEW fg_mv AS SELECT id, amount FROM fg_sales WHERE amount > 0")

      sql("INSERT INTO fg_sales VALUES (3, 30)")
      clearLog()
      refreshMv("fg_mv")
      loggedFullRefresh shouldBe false
      MvCatalog.lookup(spark, org.apache.spark.sql.catalyst.TableIdentifier("fg_mv")).get.refreshType shouldBe
        RefreshTypeCode.SimpleProjection
      assertMvCorrect("fg_mv", "SELECT id, amount FROM fg_sales WHERE amount > 0")

      sql("DROP MATERIALIZED VIEW fg_mv")
      sql("CREATE MATERIALIZED VIEW fg_mv AS SELECT id, amount * 2 AS amount FROM fg_sales WHERE amount > 0")
      sql("INSERT INTO fg_sales VALUES (4, 40)")
      clearLog()
      refreshMv("fg_mv")
      loggedFullRefresh shouldBe true
      assertMvCorrect("fg_mv", "SELECT id, amount * 2 AS amount FROM fg_sales WHERE amount > 0")
    }

    it("routes non-deterministic MV bodies through FULL_REFRESH") {
      sql("CREATE TABLE IF NOT EXISTS fg_nd_src(id INT) USING DELTA")
      sql("INSERT INTO fg_nd_src VALUES (1)")
      sql("CREATE MATERIALIZED VIEW fg_nd_mv AS SELECT id, random() AS sample FROM fg_nd_src")

      sql("INSERT INTO fg_nd_src VALUES (2)")
      clearLog()
      refreshMv("fg_nd_mv")
      loggedFullRefresh shouldBe true
    }
  }
}
