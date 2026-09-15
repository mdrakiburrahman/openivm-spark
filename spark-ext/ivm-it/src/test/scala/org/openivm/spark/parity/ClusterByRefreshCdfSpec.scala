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

  private case class LayoutCase(suffix: String, clusterColumns: Seq[String])

  private val layouts = Seq(
    LayoutCase("one", Seq("entity_id")),
    LayoutCase("multi", Seq("entity_id", "day_key")),
    LayoutCase("plain", Seq.empty)
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
    RefreshSqlLogCatalog.scanAll(spark).map(_.sqlText).mkString("\n")

  private def hasReplaceWhereRefreshWriter: Boolean =
    RefreshSqlLogCatalog
      .scanAll(spark)
      .exists(row =>
        row.category == "full_refresh_stmt" &&
          row.sqlText.contains("DataFrameWriter.format(\"delta\")") &&
          row.sqlText.contains("REPLACE WHERE true")
      )

  private def refreshProfileText: String =
    RefreshProfileCatalog.scanAll(spark).map(row => s"${row.stepName}:${row.detail}").mkString("\n")

  describe("CDF full-refresh clustering preservation") {
    layouts.foreach { layout =>
      it(s"consumes CDF from a non-intercepted ${layout.suffix} source write and preserves layout") {
        exerciseLayout(layout)
      }
    }
  }

  private def exerciseLayout(layout: LayoutCase): Unit = {
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

    RefreshSqlLogCatalog.removeAll(spark)
    RefreshProfileCatalog.removeAll(spark)
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
    refreshLogText should include("REPLACE WHERE true")
    refreshProfileText should include("outcome=full_refresh_executed")
    refreshProfileText should include("pending_deltas=1")
    assertMvCorrect(mv, s"SELECT entity_id, day_key, amount FROM $src")
  }
}
