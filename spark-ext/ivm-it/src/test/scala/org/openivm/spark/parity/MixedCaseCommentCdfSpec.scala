package org.openivm.spark.parity

import org.openivm.spark.common.{FeatureGate, MvCatalog, RefreshProfileCatalog, RefreshTypeCode}
import org.openivm.spark.parity.base.{CdfMode, IvmParitySpecBase}

class MixedCaseCommentCdfSpec extends IvmParitySpecBase("mixed-case-comment") with CdfMode {
  override protected def extraSparkConf: Map[String, String] = Map(
    FeatureGate.ProfileRefreshKey                    -> "true",
    FeatureGate.WindowRunningIncrementalEnabledKey   -> "true",
    FeatureGate.GroupedWindowStateEnabledKey         -> "true",
    FeatureGate.CompileClassificationCacheEnabledKey -> "true"
  )

  it("preserves semicolon literals through initial population and incremental CDF refresh") {
    sql("CREATE TABLE mcc_literal_source (id INT, amount INT) USING DELTA")
    sql("INSERT INTO mcc_literal_source VALUES (1, 10)")
    val projection =
      """SELECT id, 'alpha; beta; gamma' AS coverage_scope,
        |'left;middle;right' AS compact_scope FROM mcc_literal_source""".stripMargin
    val grouped =
      """SELECT id, SUM(amount) AS total,
        |'retained_source_landing_months; no business-time filter; observed inputs only' AS coverage_scope
        |FROM mcc_literal_source GROUP BY id""".stripMargin
    sql(s"CREATE MATERIALIZED VIEW mcc_literal_projection AS $projection")
    sql(s"CREATE MATERIALIZED VIEW mcc_literal_group AS $grouped")
    Seq("mcc_literal_projection" -> projection, "mcc_literal_group" -> grouped).foreach { case (name, query) =>
      val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
      MvCatalog.lookup(spark, id).get.querySql shouldBe query
      assertMvCorrect(name, query)
    }
    val projectionId = spark.sessionState.sqlParser.parseTableIdentifier("mcc_literal_projection")
    val groupId      = spark.sessionState.sqlParser.parseTableIdentifier("mcc_literal_group")
    MvCatalog.lookup(spark, projectionId).get.refreshType shouldBe RefreshTypeCode.SimpleProjection
    MvCatalog.lookup(spark, groupId).get.refreshType shouldBe RefreshTypeCode.AggregateGroup

    sql("INSERT INTO mcc_literal_source VALUES (2, 20)")
    Seq("mcc_literal_projection" -> projection, "mcc_literal_group" -> grouped).foreach { case (name, query) =>
      RefreshProfileCatalog.removeAll(spark)
      refreshMv(name)
      val spans = RefreshProfileCatalog.scanAll(spark).filter(_.stepName == "command_span")
      spans should have size 1
      spans.head.detail should include("outcome=incremental_executed")
      assertMvCorrect(name, query)
    }
  }

  it("preserves literals after apostrophes in line and block comments on create and CDF refresh") {
    sql("CREATE TABLE mcc_source (id INT, flag BOOLEAN, reason STRING) USING DELTA")
    sql("INSERT INTO mcc_source VALUES (1, true, 'MiXeD'), (2, false, 'Needs Review')")

    val projection =
      """SELECT id, CASE WHEN flag THEN 'Not Ready' ELSE 'Ready' END AS state,
        |CASE WHEN reason = 'MiXeD' THEN 'ExactMatch' ELSE 'NoMatch' END AS reason_match,
        |'yyyyMM' AS format_mask
        |FROM mcc_source""".stripMargin
    val window =
      """SELECT id, CASE WHEN flag THEN 'Not Ready' ELSE 'Ready' END AS state,
        |ROW_NUMBER() OVER (PARTITION BY flag ORDER BY id) AS rn
        |FROM mcc_source""".stripMargin

    val lineSql  = projection.replaceFirst("SELECT", "SELECT\n-- synthetic's comment\n")
    val blockSql = window.replaceFirst("SELECT", "SELECT /* synthetic's comment */")
    sql(s"CREATE MATERIALIZED VIEW mcc_line AS $lineSql")
    sql(s"CREATE MATERIALIZED VIEW mcc_block AS $blockSql")
    val lineId    = spark.sessionState.sqlParser.parseTableIdentifier("mcc_line")
    val blockId   = spark.sessionState.sqlParser.parseTableIdentifier("mcc_block")
    val lineMeta  = MvCatalog.lookup(spark, lineId).get
    val blockMeta = MvCatalog.lookup(spark, blockId).get
    lineMeta.querySql should include("-- synthetic's comment")
    blockMeta.querySql should include("/* synthetic's comment */")
    lineMeta.refreshType shouldBe RefreshTypeCode.SimpleProjection
    blockMeta.refreshType shouldBe RefreshTypeCode.WindowPartition
    assertMvCorrect("mcc_line", projection)
    assertMvCorrect("mcc_block", window)

    sql("UPDATE mcc_source SET flag = false, reason = 'Needs Review' WHERE id = 1")
    sql("INSERT INTO mcc_source VALUES (3, true, 'MiXeD')")
    refreshMv("mcc_line")
    refreshMv("mcc_block")
    assertMvCorrect("mcc_line", projection)
    assertMvCorrect("mcc_block", window)
  }
}
