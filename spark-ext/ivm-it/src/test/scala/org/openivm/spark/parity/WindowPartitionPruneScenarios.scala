package org.openivm.spark.parity

import org.openivm.spark.common.{FeatureGate, RefreshTypeCode}
import org.openivm.spark.parity.base.IvmParitySpecBase

abstract class WindowPartitionPruneScenarios extends IvmParitySpecBase("window-partition-prune") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  override protected def extraSparkConf: Map[String, String] =
    Map(FeatureGate.WindowPartitionPruneEnabledKey -> "true")

  private def deltaPartitionColumns(tableName: String): Seq[String] = {
    val escaped = tableName.replace("`", "``")
    spark
      .sql(s"DESCRIBE DETAIL `$escaped`")
      .select("partitionColumns")
      .head()
      .getAs[Seq[String]]("partitionColumns")
  }

  private def mvRefreshType(name: String): Int = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    org.openivm.spark.common.MvCatalog.lookup(spark, id).getOrElse(fail(s"MV $name not found in catalog")).refreshType
  }

  describe("WINDOW_PARTITION data-table partition pruning") {
    it("partitions the Delta data table by the window key and stays correct after a single-partition INSERT") {
      sql("CREATE TABLE wpp_sales(id INT, region STRING, amount INT) USING DELTA")
      sql(
        "INSERT INTO wpp_sales VALUES " +
          "(1,'east',10), (2,'east',30), (3,'east',20), " +
          "(4,'west',5), (5,'west',15), (6,'west',25)"
      )
      val viewSql =
        "SELECT id, region, amount, " +
          "ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount) AS rn FROM wpp_sales"

      sql(s"CREATE MATERIALIZED VIEW wpp_mv_sales AS $viewSql")

      mvRefreshType("wpp_mv_sales") shouldBe RefreshTypeCode.WindowPartition
      deltaPartitionColumns("wpp_mv_sales") shouldBe Seq("region")

      sql("INSERT INTO wpp_sales VALUES (7,'east',25)")
      refreshMv("wpp_mv_sales")

      assertMvCorrect("wpp_mv_sales", viewSql)
    }
  }
}
