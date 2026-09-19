package org.openivm.spark.parity

import java.util.Locale

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.clustering.ClusteringMetadataDomain
import org.openivm.spark.common.{
  BatchVerdict,
  CdfWatermarkCatalog,
  DeltaCommitClassifier,
  FeatureGate,
  MvCatalog,
  RefreshProfileCatalog,
  RefreshSqlLogAsyncFlusher,
  RefreshTypeCode,
  RefreshSqlLogCatalog,
  StagingCatalog
}
import org.openivm.spark.parity.base.{CdfMode, IvmParitySpecBase}

class ClusterByRefreshCdfSpec extends IvmParitySpecBase("cluster-by-refresh-cdf") with CdfMode {

  override protected def extraSparkConf: Map[String, String] =
    Map(
      FeatureGate.QueryLogEnabledKey     -> "true",
      FeatureGate.ProfileRefreshKey      -> "true",
      FeatureGate.NoopFastExitEnabledKey -> "true"
    )

  private case class LayoutCase(suffix: String, description: String, clusterColumns: Seq[String])

  private val layouts = Seq(
    LayoutCase("one", "single-column", Seq("entity_id")),
    LayoutCase("multi", "multi-column", Seq("entity_id", "day_key")),
    LayoutCase("plain", "unclustered", Seq.empty)
  )

  private def mvMeta(name: String) = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    MvCatalog.lookup(spark, id).getOrElse(fail(s"MV $name not found in catalog"))
  }

  private def clusterClause(columns: Seq[String]): String =
    if (columns.isEmpty) ""
    else columns.mkString(" CLUSTER BY (", ", ", ")")

  private def deltaClusteringColumns(tableName: String): Seq[String] =
    ClusteringMetadataDomain
      .fromSnapshot(DeltaLog.forTable(spark, mvDataLocation(tableName)).update())
      .map(_.clusteringColumns.map(_.mkString(".")).map(_.toLowerCase(Locale.ROOT)))
      .getOrElse(Seq.empty)

  private def deltaMetadataId(tableName: String): String =
    DeltaLog.forTable(spark, mvDataLocation(tableName)).update().metadata.id

  private def deltaSchemaJson(tableName: String): String =
    DeltaLog.forTable(spark, mvDataLocation(tableName)).update().metadata.schema.json

  private def refreshLogText: String =
    queryLogRows.map(_.sqlText).mkString("\n")

  private def queryLogRows = {
    RefreshSqlLogAsyncFlusher.awaitQuiescence(30000L) shouldBe true
    RefreshSqlLogCatalog.scanAll(spark)
  }

  private def hasReplaceWhereRefreshWriter: Boolean =
    queryLogRows
      .exists(row =>
        row.category == "full_refresh_stmt" &&
          row.sqlText.contains("DataFrameWriter.format(\"delta\")") &&
          row.sqlText.contains(".option(\"replaceWhere\", \"true\")") &&
          !row.sqlText.contains("REPLACE WHERE true")
      )

  private def refreshProfileText: String =
    RefreshProfileCatalog.scanAll(spark).map(row => s"${row.stepName}:${row.detail}").mkString("\n")

  private def clearLogs(): Unit = {
    RefreshSqlLogAsyncFlusher.awaitQuiescence(30000L) shouldBe true
    RefreshSqlLogCatalog.removeAll(spark)
    RefreshProfileCatalog.removeAll(spark)
  }

  describe("CDF-mode full-recompute clustering preservation") {
    layouts.foreach { layout =>
      it(s"preserves ${layout.description} layout during full recompute after a non-intercepted source write") {
        exerciseFullRecomputeLayout(layout)
      }
    }

    it("reads source CDF rows for an incremental clustered refresh and preserves layout") {
      val src = "cbrc_inc_src"
      val mv  = "cbrc_inc_mv"
      sql(s"CREATE TABLE $src (entity_id BIGINT, day_key DATE, amount DOUBLE) USING DELTA")
      sql(
        s"INSERT INTO $src VALUES " +
          "(1, DATE '2026-01-01', 10.0), (2, DATE '2026-01-02', 20.0)"
      )

      val viewBody = s"SELECT entity_id, day_key, amount FROM $src"
      sql(s"CREATE MATERIALIZED VIEW $mv CLUSTER BY (entity_id, day_key) AS $viewBody")

      val meta                = mvMeta(mv)
      val source              = meta.sourceTables.head
      val beforeId            = deltaMetadataId(mv)
      val beforeSchemaJson    = deltaSchemaJson(mv)
      val beforeMvVersion     = mvDataVersion(mv)
      val beforeSourceVersion = DeltaCommitClassifier.latestVersion(spark, source)
      meta.refreshType shouldBe RefreshTypeCode.SimpleProjection
      deltaClusteringColumns(mv) shouldBe Seq("entity_id", "day_key")

      clearLogs()
      sql(s"INSERT INTO $src VALUES (3, DATE '2026-01-03', 30.0)")
      val sourceVersion = DeltaCommitClassifier.latestVersion(spark, source)

      DeltaCommitClassifier.classify(spark, source, beforeSourceVersion) shouldBe BatchVerdict.InsertOnly
      StagingCatalog.collectFor(spark, mv, Seq(source)) shouldBe empty

      refreshMv(mv)

      val logs = queryLogRows
      CdfWatermarkCatalog.get(spark, mv, source) shouldBe Some(sourceVersion)
      DeltaCommitClassifier.classify(spark, mvDataLocation(mv), beforeMvVersion) should not be BatchVerdict.Replace
      deltaMetadataId(mv) shouldBe beforeId
      deltaSchemaJson(mv) shouldBe beforeSchemaJson
      deltaClusteringColumns(mv) shouldBe Seq("entity_id", "day_key")
      logs.exists(row =>
        row.category == "register_source_delta" &&
          row.sqlText.contains("readChangeFeed") &&
          row.sqlText.contains(s"startingVersion=${beforeSourceVersion + 1L}") &&
          row.sqlText.contains(s"endingVersion=$sourceVersion")
      ) shouldBe true
      logs.exists(row => row.category == "rewritten_stmt" && row.stmtKind == "insert_into") shouldBe true
      logs.exists(_.category == "full_refresh_stmt") shouldBe false
      refreshProfileText should include("outcome=incremental_executed")
      refreshProfileText should include("pending_deltas=1")
      assertMvCorrect(mv, viewBody)
    }
  }

  private def exerciseFullRecomputeLayout(layout: LayoutCase): Unit = {
    val src = s"cbrc_${layout.suffix}_src"
    val mv  = s"cbrc_${layout.suffix}_mv"
    sql(s"CREATE TABLE $src (entity_id BIGINT, day_key DATE, amount DOUBLE) USING DELTA")
    sql(
      s"INSERT INTO $src VALUES " +
        "(1, DATE '2026-01-01', 10.0), (2, DATE '2026-01-02', 20.0)"
    )

    val viewBody =
      s"SELECT entity_id, day_key, amount, current_timestamp() AS observed_at FROM $src"
    sql(s"CREATE MATERIALIZED VIEW $mv${clusterClause(layout.clusterColumns)} AS $viewBody")

    val meta                = mvMeta(mv)
    val source              = meta.sourceTables.head
    val beforeId            = deltaMetadataId(mv)
    val beforeSchemaJson    = deltaSchemaJson(mv)
    val beforeMvVersion     = mvDataVersion(mv)
    val beforeSourceVersion = DeltaCommitClassifier.latestVersion(spark, source)
    meta.refreshType shouldBe RefreshTypeCode.FullRefresh
    deltaClusteringColumns(mv) shouldBe layout.clusterColumns

    clearLogs()
    sql(s"INSERT INTO $src VALUES (3, DATE '2026-01-03', 30.0)")
    val sourceVersion = DeltaCommitClassifier.latestVersion(spark, source)

    sourceVersion should be > beforeSourceVersion
    DeltaCommitClassifier.classify(spark, source, beforeSourceVersion) shouldBe BatchVerdict.InsertOnly
    StagingCatalog.collectFor(spark, mv, Seq(source)) shouldBe empty

    refreshMv(mv)

    CdfWatermarkCatalog.get(spark, mv, source) shouldBe Some(sourceVersion)
    DeltaCommitClassifier.classify(spark, mvDataLocation(mv), beforeMvVersion) shouldBe BatchVerdict.Replace
    deltaMetadataId(mv) shouldBe beforeId
    deltaSchemaJson(mv) shouldBe beforeSchemaJson
    deltaClusteringColumns(mv) shouldBe layout.clusterColumns
    hasReplaceWhereRefreshWriter shouldBe true
    refreshLogText should include("DataFrameWriter.format(\"delta\")")
    refreshLogText should include(".option(\"replaceWhere\", \"true\")")
    refreshLogText should not include "REPLACE WHERE true"
    refreshProfileText should include("outcome=full_refresh_executed")
    refreshProfileText should include("pending_deltas=1")
    assertMvCorrect(mv, s"SELECT entity_id, day_key, amount FROM $src")
  }
}
