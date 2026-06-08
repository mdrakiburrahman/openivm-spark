package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.openivm.spark.common.{MvCatalog, RefreshTypeCode}

abstract class WindowIgnoreNullsForwardFillScenarios extends IvmParitySpecBase("window-ignore-nulls-forward-fill") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  protected def mvRefreshType(name: String): Int =
    MvCatalog
      .list(spark)
      .find(_.name.table == name)
      .getOrElse(fail(s"MV $name not found in catalog"))
      .refreshType

  describe("last_value(expr, true) forward-fill over nullable rows") {
    it("stays WindowPartition and bag-equal after initial load and refresh") {
      sql("CREATE TABLE winnf_customer_src (customer_id INT, effective_ts TIMESTAMP, status STRING) USING DELTA")
      sql(
        "INSERT INTO winnf_customer_src VALUES " +
          "(1, TIMESTAMP'2024-01-01 00:00:00', 'bronze'), " +
          "(1, TIMESTAMP'2024-02-01 00:00:00', NULL), " +
          "(1, TIMESTAMP'2024-03-01 00:00:00', 'silver'), " +
          "(2, TIMESTAMP'2024-01-15 00:00:00', NULL), " +
          "(2, TIMESTAMP'2024-02-15 00:00:00', 'starter')"
      )

      val mvName = "winnf_mv_customer"
      val viewSql =
        "SELECT customer_id, effective_ts, status, " +
          "last_value(status, true) OVER (PARTITION BY customer_id ORDER BY effective_ts) AS carried_status, " +
          "coalesce(status, last_value(status, true) OVER (PARTITION BY customer_id ORDER BY effective_ts)) AS filled_status " +
          "FROM winnf_customer_src"
      sql(s"CREATE MATERIALIZED VIEW $mvName AS $viewSql")

      mvRefreshType(mvName) shouldBe RefreshTypeCode.WindowPartition
      assertMvCorrect(mvName, viewSql)

      sql(
        "INSERT INTO winnf_customer_src VALUES " +
          "(1, TIMESTAMP'2024-04-01 00:00:00', NULL), " +
          "(2, TIMESTAMP'2024-03-15 00:00:00', NULL), " +
          "(3, TIMESTAMP'2024-01-20 00:00:00', 'new')"
      )
      refreshMv(mvName)

      mvRefreshType(mvName) shouldBe RefreshTypeCode.WindowPartition
      assertMvCorrect(mvName, viewSql)
    }
  }
}
