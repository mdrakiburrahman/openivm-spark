package org.openivm.spark.parity

import org.apache.spark.sql.delta.DeltaLog
import org.openivm.spark.common.{FeatureGate, RefreshTypeCode}
import org.openivm.spark.parity.base.IvmParitySpecBase

abstract class WindowPartitionPruneScenarios extends IvmParitySpecBase("window-partition-prune") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  override protected def extraSparkConf: Map[String, String] =
    Map(FeatureGate.WindowClusterPruneEnabledKey -> "true")

  private def deltaClusteringMetadata(tableName: String): String = {
    val escaped = tableName.replace("`", "``")
    val detail  = spark.sql(s"DESCRIBE DETAIL `$escaped`")
    val describeClustering =
      if (detail.schema.fieldNames.contains("clusteringColumns"))
        Option(detail.select("clusteringColumns").head().getAs[Seq[String]]("clusteringColumns"))
          .getOrElse(Seq.empty)
          .mkString(",")
      else ""
    val id = spark.sessionState.sqlParser.parseTableIdentifier(tableName)
    val configClustering = DeltaLog
      .forTable(spark, id)
      .update()
      .metadata
      .configuration
      .filter { case (key, _) => key.toLowerCase(java.util.Locale.ROOT).contains("clustering") }
      .toSeq
      .sortBy(_._1)
      .map { case (key, value) => s"$key=$value" }
      .mkString(";")
    s"$describeClustering;$configClustering"
  }

  private def mvRefreshType(name: String): Int = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    org.openivm.spark.common.MvCatalog.lookup(spark, id).getOrElse(fail(s"MV $name not found in catalog")).refreshType
  }

  describe("WINDOW_PARTITION data-table cluster pruning") {
    it("liquid-clusters the Delta data table by the window key and stays correct after a single-partition INSERT") {
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
      deltaClusteringMetadata("wpp_mv_sales").toLowerCase(java.util.Locale.ROOT) should include("region")

      sql("INSERT INTO wpp_sales VALUES (7,'east',25)")
      refreshMv("wpp_mv_sales")

      assertMvCorrect("wpp_mv_sales", viewSql)
    }
  }
}
