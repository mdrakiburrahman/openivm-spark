package org.openivm.spark.parity

import org.openivm.spark.common.{FeatureGate, MvCatalog, RefreshTypeCode}
import org.openivm.spark.parity.base.{InterceptMode, IvmParitySpecBase}

class WindowLastValueSnapshotCacheSpec extends IvmParitySpecBase("window-last-value-cache") with InterceptMode {

  override protected def extraSparkConf: Map[String, String] =
    Map(
      FeatureGate.WindowSnapshotCacheEnabledKey -> "true",
      FeatureGate.QueryLogEnabledKey            -> "true"
    )

  describe("dim_customer-shaped LAST_VALUE forward-fill WINDOW_PARTITION") {
    it("uses incremental WINDOW_PARTITION with cached single-pass snapshot and stays bag-equal") {
      sql(
        """CREATE TABLE IF NOT EXISTS dimc_customers(
          |  customer_id INT,
          |  tax_id STRING,
          |  first_name STRING,
          |  last_name STRING,
          |  postal_code STRING,
          |  address_line1 STRING,
          |  address_line2 STRING,
          |  effective_timestamp TIMESTAMP,
          |  status STRING,
          |  account_id INT
          |) USING DELTA""".stripMargin
      )
      sql(
        """INSERT INTO dimc_customers VALUES
          |  (1, 'tax-1', 'Ada', 'Lovelace', '10001', '1 Main', NULL, TIMESTAMP '2020-01-01 00:00:00', 'A', 10),
          |  (1, NULL,    NULL,  'Lovelace', '10001', '1 Main', NULL, TIMESTAMP '2020-02-01 00:00:00', 'A', 11),
          |  (2, 'tax-2', 'Grace','Hopper',   '10002', '2 Main', NULL, TIMESTAMP '2020-01-01 00:00:00', 'A', 20)
          |""".stripMargin
      )

      val viewSql =
        """SELECT
          |  md5(cast(concat(coalesce(cast(customer_id AS STRING), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(effective_timestamp AS STRING), '_dbt_utils_surrogate_key_null_')) AS STRING)) AS sk_customer_id,
          |  customer_id,
          |  coalesce(tax_id, last_value(tax_id, true) OVER (
          |    PARTITION BY customer_id ORDER BY effective_timestamp, status, account_id)) AS tax_id,
          |  coalesce(first_name, last_value(first_name, true) OVER (
          |    PARTITION BY customer_id ORDER BY effective_timestamp, status, account_id)) AS first_name,
          |  last_name,
          |  effective_timestamp,
          |  status,
          |  account_id
          |FROM dimc_customers""".stripMargin

      sql(s"CREATE MATERIALIZED VIEW dimc_mv AS $viewSql")
      val id   = spark.sessionState.sqlParser.parseTableIdentifier("dimc_mv")
      val meta = MvCatalog.lookup(spark, id).getOrElse(fail("missing dimc_mv metadata"))
      meta.refreshType shouldBe RefreshTypeCode.WindowPartition

      sql(
        """INSERT INTO dimc_customers VALUES
          |  (1, NULL, 'Ada', 'Lovelace', '10001', '1 Main', NULL, TIMESTAMP '2020-03-01 00:00:00', 'A', 12),
          |  (3, 'tax-3', 'Katherine', 'Johnson', '10003', '3 Main', NULL, TIMESTAMP '2020-01-01 00:00:00', 'A', 30)
          |""".stripMargin
      )
      refreshMv("dimc_mv")

      MvCatalog.lookup(spark, id).get.refreshType shouldBe RefreshTypeCode.WindowPartition
      assertMvCorrect("dimc_mv", viewSql)

      val refreshSql = sql("SHOW OPENIVM QUERY LOG").collect().map(_.getString(9)).mkString("\n")
      refreshSql should include("CACHE TABLE `openivm_new_dimc_mv`")
      refreshSql should include("REPLACE WHERE")
    }
  }
}
