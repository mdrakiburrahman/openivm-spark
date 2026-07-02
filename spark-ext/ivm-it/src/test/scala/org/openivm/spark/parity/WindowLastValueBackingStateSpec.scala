package org.openivm.spark.parity

import org.openivm.spark.common.{FeatureGate, MvCatalog, RefreshTypeCode}
import org.openivm.spark.parity.base.{InterceptMode, IvmParitySpecBase}

class WindowLastValueBackingStateSpec extends IvmParitySpecBase("window-last-value-backing-state") with InterceptMode {

  override protected def extraSparkConf: Map[String, String] =
    Map(FeatureGate.WindowLastValueBackingStateEnabledKey -> "true")

  describe("LAST_VALUE IGNORE NULLS backing-state guarded WINDOW_PARTITION") {
    it("stays incremental and bag-correct for suffix, backdated insert, and delete batches") {
      sql(
        """CREATE TABLE IF NOT EXISTS lvbs_customers(
          |  customer_id INT,
          |  tax_id STRING,
          |  first_name STRING,
          |  effective_timestamp TIMESTAMP,
          |  status STRING,
          |  account_id INT
          |) USING DELTA""".stripMargin
      )
      sql(
        """INSERT INTO lvbs_customers VALUES
          |  (1, 'tax-1', 'Ada', TIMESTAMP '2020-01-01 00:00:00', 'A', 10),
          |  (1, NULL,    NULL,  TIMESTAMP '2020-02-01 00:00:00', 'A', 11),
          |  (2, 'tax-2', 'Grace', TIMESTAMP '2020-01-01 00:00:00', 'A', 20)
          |""".stripMargin
      )

      val viewSql =
        """SELECT
          |  customer_id,
          |  effective_timestamp,
          |  status,
          |  account_id,
          |  coalesce(tax_id, last_value(tax_id, true) OVER (
          |    PARTITION BY customer_id ORDER BY effective_timestamp, status, account_id)) AS tax_id,
          |  coalesce(first_name, last_value(first_name, true) OVER (
          |    PARTITION BY customer_id ORDER BY effective_timestamp, status, account_id)) AS first_name
          |FROM lvbs_customers""".stripMargin

      sql(s"CREATE MATERIALIZED VIEW lvbs_mv AS $viewSql")
      val id = spark.sessionState.sqlParser.parseTableIdentifier("lvbs_mv")
      MvCatalog.lookup(spark, id).getOrElse(fail("missing lvbs_mv metadata")).refreshType shouldBe
        RefreshTypeCode.WindowPartition
      assertMvCorrect("lvbs_mv", viewSql)

      sql(
        """INSERT INTO lvbs_customers VALUES
          |  (1, NULL, 'Ada', TIMESTAMP '2020-03-01 00:00:00', 'A', 12),
          |  (3, 'tax-3', 'Katherine', TIMESTAMP '2020-01-01 00:00:00', 'A', 30)
          |""".stripMargin
      )
      refreshMv("lvbs_mv")
      MvCatalog.lookup(spark, id).get.refreshType shouldBe RefreshTypeCode.WindowPartition
      assertMvCorrect("lvbs_mv", viewSql)

      sql("INSERT INTO lvbs_customers VALUES (1, 'tax-0', 'Augusta', TIMESTAMP '2019-12-01 00:00:00', 'A', 9)")
      refreshMv("lvbs_mv")
      MvCatalog.lookup(spark, id).get.refreshType shouldBe RefreshTypeCode.WindowPartition
      assertMvCorrect("lvbs_mv", viewSql)

      sql(
        """DELETE FROM lvbs_customers
          |WHERE customer_id = 1 AND effective_timestamp = TIMESTAMP '2020-03-01 00:00:00'""".stripMargin
      )
      refreshMv("lvbs_mv")
      MvCatalog.lookup(spark, id).get.refreshType shouldBe RefreshTypeCode.WindowPartition
      assertMvCorrect("lvbs_mv", viewSql)

      spark.table("lvbs_mv__ivm_last_value_state").count() shouldBe 3L
    }
  }
}
