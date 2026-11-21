package org.openivm.spark.parity

import org.openivm.spark.common.{
  ChangeFeedMode,
  FeatureGate,
  MvCatalog,
  RefreshSqlLogCatalog,
  RefreshTypeCode,
  StagingCatalog,
  StagingDelta
}
import org.openivm.spark.parity.base.{CdfMode, InterceptMode, IvmParityMode, IvmParitySpecBase}

abstract class GroupCdfCompanionScenarios
    extends IvmParitySpecBase("group-cdf-companion") {
  self: IvmParityMode =>

  override protected def extraSparkConf: Map[String, String] =
    Map(FeatureGate.GroupCdfCompanionSkipEnabledKey -> "true", FeatureGate.QueryLogEnabledKey -> "true")

  it("preserves scoped groups and downstream propagation when a nullable composite group disappears") {
    sql("CREATE TABLE gcc_src(k1 STRING, k2 STRING, amount INT) USING DELTA")
    sql(
      "INSERT INTO gcc_src VALUES ('a','1',10), ('a','2',20), ('b','1',30), " +
        "(NULL,'1',40), (NULL,NULL,50)"
    )
    val expected = "SELECT k1, k2, SUM(DISTINCT amount) AS total FROM gcc_src GROUP BY k1, k2"
    sql(s"CREATE MATERIALIZED VIEW gcc_mv AS $expected")
    sql("CREATE MATERIALIZED VIEW gcc_child AS SELECT k1, k2, total FROM gcc_mv")
    val id = spark.sessionState.sqlParser.parseTableIdentifier("gcc_mv")
    MvCatalog.lookup(spark, id).get.refreshType shouldBe RefreshTypeCode.GroupRecompute
    RefreshSqlLogCatalog.ensureTables(spark)
    RefreshSqlLogCatalog.removeAll(spark)

    def changes(from: Long, to: Long) =
      spark.read
        .format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", from)
        .option("endingVersion", to)
        .load(mvDataLocation("gcc_mv"))

    val before = mvDataVersion("gcc_mv")
    sql("INSERT INTO gcc_src VALUES ('a','1',10), ('a','1',15), ('b','2',5)")
    refreshMv("gcc_mv")
    mvDataVersion("gcc_mv") shouldBe before + 2
    val statements = sql("SHOW OPENIVM QUERY LOG").collect().toSeq
      .filter(r => r.getString(1).split("\\.").last == "gcc_mv" && r.getString(6) == "rewritten_stmt")
      .map(_.getString(9))
    statements.exists(_.contains("WHEN MATCHED THEN DELETE")) shouldBe true
    statements.exists(_.contains("FROM openivm_new_gcc_mv")) shouldBe true
    val hasCompanion = statements.exists(_.contains("/_ivm/view_deltas/"))
    hasCompanion shouldBe (changeFeedMode == ChangeFeedMode.Intercept)
    if (changeFeedMode == ChangeFeedMode.Cdf) {
      val cdf = changes(before + 1, before + 2)
      cdf.filter("_change_type = 'delete'").count() shouldBe 1L
      cdf.filter("_change_type = 'insert'").count() shouldBe 2L
      cdf.filter("NOT (k1 <=> 'a' AND k2 <=> '1') AND NOT (k1 <=> 'b' AND k2 <=> '2')").count() shouldBe 0L
    } else {
      val childId = spark.sessionState.sqlParser.parseTableIdentifier("gcc_child")
      val childMeta = MvCatalog.lookup(spark, childId).get
      StagingCatalog.collectFor(spark, "gcc_child", childMeta.sourceTables)
        .exists(_.opType == StagingDelta.OpTypes.MvViewDelta) shouldBe true
    }
    assertMvCorrect("gcc_mv", expected)
    refreshMv("gcc_child")
    assertMvCorrect("gcc_child", expected)

    val beforeDelete = mvDataVersion("gcc_mv")
    sql("DELETE FROM gcc_src WHERE k1 IS NULL AND k2 IS NULL")
    refreshMv("gcc_mv")
    assertMvCorrect("gcc_mv", expected)
    if (changeFeedMode == ChangeFeedMode.Cdf) {
      val cdf = changes(beforeDelete + 1, mvDataVersion("gcc_mv"))
      cdf.count() shouldBe 1L
      cdf.filter("_change_type = 'delete' AND k1 IS NULL AND k2 IS NULL").count() shouldBe 1L
    }
    refreshMv("gcc_child")
    assertMvCorrect("gcc_child", expected)
  }
}

class GroupCdfCompanionCdfSpec extends GroupCdfCompanionScenarios with CdfMode
class GroupCdfCompanionInterceptSpec extends GroupCdfCompanionScenarios with InterceptMode
