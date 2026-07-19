package org.openivm.spark.parity

import java.util.Locale

import org.apache.spark.sql.delta.DeltaLog
import org.openivm.spark.common.{MvCatalog, RefreshTypeCode}
import org.openivm.spark.parity.base.{InterceptMode, IvmParitySpecBase}

/** Integration coverage for `CREATE MATERIALIZED VIEW ... CLUSTER BY (...)` (#24).
  *
  * Verifies the user-supplied `CLUSTER BY` columns are (a) injected into the MV's
  * Delta `CREATE TABLE ... USING DELTA CLUSTER BY (...)` CTAS, (b) persisted in
  * `MvMetadata` under `_ivm_cluster_cols`, (c) leave `OPTIMIZE <mv>` working as a
  * plain Delta command (openivm does NOT intercept OPTIMIZE), and — critically —
  * (d) do NOT regress the MV's incremental refresh classification to FULL_REFRESH.
  *
  * All table / MV names are prefixed `cbc_` to avoid Delta-warehouse collisions
  * with sibling specs running in parallel forks.
  */
class ClusterByCreateSpec extends IvmParitySpecBase("cluster-by-create") with InterceptMode {

  private def mvRefreshType(name: String): Int = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    MvCatalog.lookup(spark, id).getOrElse(fail(s"MV $name not found in catalog")).refreshType
  }

  private def mvClusterColumns(name: String): Seq[String] = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    MvCatalog.lookup(spark, id).getOrElse(fail(s"MV $name not found in catalog")).clusterColumns
  }

  /** Concatenated DESCRIBE DETAIL `clusteringColumns` + Delta metadata clustering
    * config, lower-cased — mirrors WindowPartitionPruneScenarios so the assertion
    * is robust to which surface a given Delta build reports clustering on.
    */
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
      .filter { case (key, _) => key.toLowerCase(Locale.ROOT).contains("clustering") }
      .toSeq
      .sortBy(_._1)
      .map { case (key, value) => s"$key=$value" }
      .mkString(";")
    s"$describeClustering;$configClustering".toLowerCase(Locale.ROOT)
  }

  describe("CREATE MATERIALIZED VIEW ... CLUSTER BY") {
    it("clusters the Delta data table and persists the CLUSTER BY columns in metadata") {
      sql("CREATE TABLE cbc_sales (region STRING, day STRING, amount INT) USING DELTA")
      sql("INSERT INTO cbc_sales VALUES ('east','d1',10), ('west','d1',20), ('east','d2',30)")

      val viewBody = "SELECT region, day, SUM(amount) AS total FROM cbc_sales GROUP BY region, day"
      sql(s"CREATE MATERIALIZED VIEW cbc_mv_agg CLUSTER BY (region) AS $viewBody")

      mvClusterColumns("cbc_mv_agg") shouldBe Seq("region")
      deltaClusteringMetadata("cbc_mv_agg") should include("region")
      assertMvCorrect("cbc_mv_agg", viewBody)
    }

    it("supports a multi-column CLUSTER BY in declaration order") {
      sql("CREATE TABLE cbc_multi (region STRING, day STRING, amount INT) USING DELTA")
      sql("INSERT INTO cbc_multi VALUES ('east','d1',10), ('west','d2',20)")

      val viewBody = "SELECT region, day, SUM(amount) AS total FROM cbc_multi GROUP BY region, day"
      sql(s"CREATE MATERIALIZED VIEW cbc_mv_multi CLUSTER BY (region, day) AS $viewBody")

      mvClusterColumns("cbc_mv_multi") shouldBe Seq("region", "day")
      val clustering = deltaClusteringMetadata("cbc_mv_multi")
      clustering should include("region")
      clustering should include("day")
      assertMvCorrect("cbc_mv_multi", viewBody)
    }

    it("leaves OPTIMIZE <mv> working as a plain Delta command and preserves correctness") {
      sql("CREATE TABLE cbc_opt (region STRING, day STRING, amount INT) USING DELTA")
      sql(
        "INSERT INTO cbc_opt VALUES " +
          "('east','d1',10), ('west','d1',20), ('east','d2',30), " +
          "('west','d2',40), ('north','d1',50), ('south','d2',60)"
      )

      val viewBody = "SELECT region, day, SUM(amount) AS total FROM cbc_opt GROUP BY region, day"
      // Two clustering columns: Delta's OPTIMIZE clustering uses a Hilbert curve
      // that requires >= 2 dimensions (a single-column CLUSTER BY is a documented
      // Delta limitation, not an openivm concern).
      sql(s"CREATE MATERIALIZED VIEW cbc_mv_opt CLUSTER BY (region, day) AS $viewBody")

      // OPTIMIZE is NOT intercepted by openivm — it falls through to Delta.
      noException should be thrownBy spark.sql("OPTIMIZE cbc_mv_opt").collect()
      assertMvCorrect("cbc_mv_opt", viewBody)
    }

    it("does NOT regress incremental refresh to FULL_REFRESH and stays correct after DML") {
      sql("CREATE TABLE cbc_reg (region STRING, amount INT) USING DELTA")
      sql("INSERT INTO cbc_reg VALUES ('east', 10), ('west', 20)")

      val viewBody = "SELECT region, SUM(amount) AS total FROM cbc_reg GROUP BY region"
      sql(s"CREATE MATERIALIZED VIEW cbc_mv_clustered CLUSTER BY (region) AS $viewBody")
      sql(s"CREATE MATERIALIZED VIEW cbc_mv_plain AS $viewBody")

      // Clustering is a physical-layout hint only: it must not change the
      // classification, and in particular must never demote to FULL_REFRESH (3).
      mvRefreshType("cbc_mv_clustered") shouldBe RefreshTypeCode.AggregateGroup
      mvRefreshType("cbc_mv_clustered") shouldBe mvRefreshType("cbc_mv_plain")
      mvRefreshType("cbc_mv_clustered") should not be RefreshTypeCode.FullRefresh

      sql("INSERT INTO cbc_reg VALUES ('east', 5), ('north', 40)")
      refreshMv("cbc_mv_clustered")
      assertMvCorrect("cbc_mv_clustered", viewBody)

      sql("DELETE FROM cbc_reg WHERE region = 'west'")
      refreshMv("cbc_mv_clustered")
      assertMvCorrect("cbc_mv_clustered", viewBody)
    }
  }
}
