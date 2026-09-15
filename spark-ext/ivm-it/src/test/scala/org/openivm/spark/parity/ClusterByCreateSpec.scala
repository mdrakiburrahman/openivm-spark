package org.openivm.spark.parity

import java.util.Locale

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.clustering.ClusteringMetadataDomain
import org.openivm.spark.common.{
  BatchVerdict,
  DeltaCommitClassifier,
  MvCatalog,
  RefreshSqlLogAsyncFlusher,
  RefreshSqlLogCatalog,
  RefreshTypeCode
}
import org.openivm.spark.parity.base.{InterceptMode, IvmParitySpecBase}
import org.openivm.spark.telemetry.metrics.OpenIvmMetrics

/** Integration coverage for `CREATE MATERIALIZED VIEW ... CLUSTER BY (...)` (#24).
  *
  * Verifies the user-supplied `CLUSTER BY` columns are (a) injected into the MV's
  * Delta `CREATE TABLE ... USING DELTA CLUSTER BY (...)` CTAS, (b) persisted in
  * `MvMetadata` under `_ivm_cluster_cols`, (c) leave `OPTIMIZE` on physical MV data working as a
  * plain Delta command (openivm does NOT intercept OPTIMIZE), and — critically —
  * (d) do NOT regress the MV's incremental refresh classification to FULL_REFRESH.
  *
  * All table / MV names are prefixed `cbc_` to avoid Delta-warehouse collisions
  * with sibling specs running in parallel forks.
  */
class ClusterByCreateSpec extends IvmParitySpecBase("cluster-by-create") with InterceptMode {

  override protected def extraSparkConf: Map[String, String] =
    Map("spark.openivm.queryLog.enabled" -> "true")

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
    val location = mvDataLocation(tableName)
    val escaped  = location.replace("`", "``")
    val detail   = spark.sql(s"DESCRIBE DETAIL delta.`$escaped`")
    val describeClustering =
      if (detail.schema.fieldNames.contains("clusteringColumns"))
        Option(detail.select("clusteringColumns").head().getAs[Seq[String]]("clusteringColumns"))
          .getOrElse(Seq.empty)
          .mkString(",")
      else ""
    val configClustering = DeltaLog
      .forTable(spark, location)
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

  private def deltaClusteringColumns(tableName: String): Seq[String] =
    ClusteringMetadataDomain
      .fromSnapshot(DeltaLog.forTable(spark, mvDataLocation(tableName)).update())
      .map(_.clusteringColumns.map(_.mkString(".")).map(_.toLowerCase(Locale.ROOT)))
      .getOrElse(Seq.empty)

  private def deltaMetadataId(tableName: String): String =
    DeltaLog.forTable(spark, mvDataLocation(tableName)).update().metadata.id

  private def deltaSchemaJson(tableName: String): String =
    DeltaLog.forTable(spark, mvDataLocation(tableName)).update().metadata.schema.json

  private def refreshSqlText: String =
    sql("SHOW OPENIVM QUERY LOG").collect().map(_.getString(9)).mkString("\n")

  private def clearQueryLog(): Unit = {
    RefreshSqlLogAsyncFlusher.awaitQuiescence(30000L) shouldBe true
    RefreshSqlLogCatalog.removeAll(spark)
  }

  private def assertDataFrameWriterReplaceWhereLogged(): Unit = {
    val text = refreshSqlText
    text should include("DataFrameWriter.format(\"delta\")")
    text should include(".option(\"replaceWhere\", \"true\")")
    text should not include "REPLACE WHERE true"
  }

  private def replaceWhereWriterCounter(suffix: String): Long =
    OpenIvmMetrics.counter(s"refresh.sql_stmt.replace_where_writer.$suffix").getCount

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

    it("leaves OPTIMIZE on physical MV data working as a plain Delta command and preserves correctness") {
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
      val location = mvDataLocation("cbc_mv_opt").replace("`", "``")
      noException should be thrownBy spark.sql(s"OPTIMIZE delta.`$location`").collect()
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

    it("preserves single-column Delta clustering metadata after an ordinary full-refresh rewrite") {
      sql("CREATE TABLE cbc_refresh_one (region STRING, amount INT) USING DELTA")
      sql("INSERT INTO cbc_refresh_one VALUES ('east', 10), ('west', 20)")

      val viewBody = "SELECT region, amount, current_timestamp() AS observed_at FROM cbc_refresh_one"
      val expected = "SELECT region, amount FROM cbc_refresh_one"
      sql(s"CREATE MATERIALIZED VIEW cbc_mv_refresh_one CLUSTER BY (region) AS $viewBody")

      mvRefreshType("cbc_mv_refresh_one") shouldBe RefreshTypeCode.FullRefresh
      val beforeId         = deltaMetadataId("cbc_mv_refresh_one")
      val beforeSchemaJson = deltaSchemaJson("cbc_mv_refresh_one")
      val beforeVersion    = mvDataVersion("cbc_mv_refresh_one")
      val metricsUnavailableBefore =
        replaceWhereWriterCounter("writer_metrics_unavailable")
      val rowsReadBefore    = replaceWhereWriterCounter("rows_read")
      val rowsWrittenBefore = replaceWhereWriterCounter("rows_written")
      deltaClusteringColumns("cbc_mv_refresh_one") shouldBe Seq("region")

      clearQueryLog()
      refreshMv("cbc_mv_refresh_one")

      replaceWhereWriterCounter("writer_metrics_unavailable") shouldBe metricsUnavailableBefore + 1L
      replaceWhereWriterCounter("rows_read") shouldBe rowsReadBefore
      replaceWhereWriterCounter("rows_written") shouldBe rowsWrittenBefore
      DeltaCommitClassifier.classify(spark, mvDataLocation("cbc_mv_refresh_one"), beforeVersion) shouldBe
        BatchVerdict.Replace
      deltaMetadataId("cbc_mv_refresh_one") shouldBe beforeId
      deltaSchemaJson("cbc_mv_refresh_one") shouldBe beforeSchemaJson
      deltaClusteringColumns("cbc_mv_refresh_one") shouldBe Seq("region")
      deltaClusteringMetadata("cbc_mv_refresh_one") should include("region")
      assertDataFrameWriterReplaceWhereLogged()
      assertMvCorrect("cbc_mv_refresh_one", expected)
    }

    it("preserves multi-column Delta clustering metadata after an ordinary full-refresh rewrite") {
      sql("CREATE TABLE cbc_refresh_multi (region STRING, day STRING, amount INT) USING DELTA")
      sql("INSERT INTO cbc_refresh_multi VALUES ('east','d1',10), ('west','d2',20)")

      val viewBody =
        "SELECT region, day, amount, current_timestamp() AS observed_at FROM cbc_refresh_multi"
      val expected = "SELECT region, day, amount FROM cbc_refresh_multi"
      sql(s"CREATE MATERIALIZED VIEW cbc_mv_refresh_multi CLUSTER BY (region, day) AS $viewBody")

      mvRefreshType("cbc_mv_refresh_multi") shouldBe RefreshTypeCode.FullRefresh
      val beforeId         = deltaMetadataId("cbc_mv_refresh_multi")
      val beforeSchemaJson = deltaSchemaJson("cbc_mv_refresh_multi")
      val beforeVersion    = mvDataVersion("cbc_mv_refresh_multi")
      deltaClusteringColumns("cbc_mv_refresh_multi") shouldBe Seq("region", "day")

      clearQueryLog()
      refreshMv("cbc_mv_refresh_multi")

      DeltaCommitClassifier.classify(spark, mvDataLocation("cbc_mv_refresh_multi"), beforeVersion) shouldBe
        BatchVerdict.Replace
      deltaMetadataId("cbc_mv_refresh_multi") shouldBe beforeId
      deltaSchemaJson("cbc_mv_refresh_multi") shouldBe beforeSchemaJson
      deltaClusteringColumns("cbc_mv_refresh_multi") shouldBe Seq("region", "day")
      val clustering = deltaClusteringMetadata("cbc_mv_refresh_multi")
      clustering should include("region")
      clustering should include("day")
      assertDataFrameWriterReplaceWhereLogged()
      assertMvCorrect("cbc_mv_refresh_multi", expected)
    }

    it("keeps unclustered MV data unclustered after an ordinary full-refresh rewrite") {
      sql("CREATE TABLE cbc_refresh_plain (region STRING, amount INT) USING DELTA")
      sql("INSERT INTO cbc_refresh_plain VALUES ('east', 10), ('west', 20)")

      val viewBody = "SELECT region, amount, current_timestamp() AS observed_at FROM cbc_refresh_plain"
      val expected = "SELECT region, amount FROM cbc_refresh_plain"
      sql(s"CREATE MATERIALIZED VIEW cbc_mv_refresh_plain AS $viewBody")

      mvRefreshType("cbc_mv_refresh_plain") shouldBe RefreshTypeCode.FullRefresh
      val beforeId         = deltaMetadataId("cbc_mv_refresh_plain")
      val beforeSchemaJson = deltaSchemaJson("cbc_mv_refresh_plain")
      val beforeVersion    = mvDataVersion("cbc_mv_refresh_plain")
      deltaClusteringColumns("cbc_mv_refresh_plain") shouldBe empty

      clearQueryLog()
      refreshMv("cbc_mv_refresh_plain")

      DeltaCommitClassifier.classify(spark, mvDataLocation("cbc_mv_refresh_plain"), beforeVersion) shouldBe
        BatchVerdict.Replace
      deltaMetadataId("cbc_mv_refresh_plain") shouldBe beforeId
      deltaSchemaJson("cbc_mv_refresh_plain") shouldBe beforeSchemaJson
      deltaClusteringColumns("cbc_mv_refresh_plain") shouldBe empty
      assertDataFrameWriterReplaceWhereLogged()
      assertMvCorrect("cbc_mv_refresh_plain", expected)
    }

    it("replaces a nonempty clustered volatile MV with an empty result without losing layout") {
      sql("CREATE TABLE cbc_empty_src (entity_id BIGINT, day_key DATE, amount DOUBLE) USING DELTA")
      sql(
        "INSERT INTO cbc_empty_src VALUES " +
          "(1, DATE '2026-01-01', 10.0), (2, DATE '2026-01-02', 20.0)"
      )

      val viewBody =
        "SELECT entity_id, day_key, amount, current_timestamp() AS observed_at FROM cbc_empty_src"
      sql(s"CREATE MATERIALIZED VIEW cbc_mv_empty CLUSTER BY (entity_id, day_key) AS $viewBody")

      spark.table("cbc_mv_empty").count() shouldBe 2L
      mvRefreshType("cbc_mv_empty") shouldBe RefreshTypeCode.FullRefresh
      val beforeId         = deltaMetadataId("cbc_mv_empty")
      val beforeSchemaJson = deltaSchemaJson("cbc_mv_empty")
      val beforeVersion    = mvDataVersion("cbc_mv_empty")
      deltaClusteringColumns("cbc_mv_empty") shouldBe Seq("entity_id", "day_key")

      clearQueryLog()
      sql("DELETE FROM cbc_empty_src WHERE entity_id >= 0")
      refreshMv("cbc_mv_empty")

      spark.table("cbc_mv_empty").count() shouldBe 0L
      DeltaCommitClassifier.classify(spark, mvDataLocation("cbc_mv_empty"), beforeVersion) shouldBe
        BatchVerdict.Replace
      deltaMetadataId("cbc_mv_empty") shouldBe beforeId
      deltaSchemaJson("cbc_mv_empty") shouldBe beforeSchemaJson
      deltaClusteringColumns("cbc_mv_empty") shouldBe Seq("entity_id", "day_key")
      assertDataFrameWriterReplaceWhereLogged()
      assertMvCorrect("cbc_mv_empty", "SELECT entity_id, day_key, amount FROM cbc_empty_src")
    }
  }
}
