package org.openivm.spark.parity

import org.openivm.spark.common.{FeatureGate, MvCatalog, RefreshTypeCode}
import org.openivm.spark.parity.base.{CdfMode, IvmParitySpecBase}

class GroupAtomicReplaceCdfSpec extends IvmParitySpecBase("group-atomic") with CdfMode {
  override protected def extraSparkConf: Map[String, String] =
    Map(FeatureGate.GroupAtomicReplaceEnabledKey -> "true")

  it("atomically replaces composite nullable groups and propagates CDF to a downstream MV") {
    sql("CREATE TABLE gar_src(k1 STRING, k2 STRING, amount INT) USING DELTA")
    sql(
      "INSERT INTO gar_src VALUES ('a','1',10), ('a','2',20), ('b','1',30), " +
        "(NULL,'1',40), (NULL,NULL,50)"
    )
    val expected = "SELECT k1, k2, SUM(DISTINCT amount) AS total FROM gar_src GROUP BY k1, k2"
    sql(s"CREATE MATERIALIZED VIEW gar_mv AS $expected")
    sql("CREATE MATERIALIZED VIEW gar_child AS SELECT k1, k2, total FROM gar_mv")
    val id = spark.sessionState.sqlParser.parseTableIdentifier("gar_mv")
    MvCatalog.lookup(spark, id).get.refreshType shouldBe RefreshTypeCode.GroupRecompute

    def changes(version: Long) =
      spark.read
        .format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", version)
        .option("endingVersion", version)
        .load(mvDataLocation("gar_mv"))

    val before = mvDataVersion("gar_mv")
    sql("INSERT INTO gar_src VALUES ('a','1',10), ('a','1',15), ('b','2',5)")
    refreshMv("gar_mv")
    mvDataVersion("gar_mv") shouldBe before + 1
    val first = changes(before + 1)
    first.filter("_change_type = 'delete'").count() shouldBe 1L
    first.filter("_change_type = 'insert'").count() shouldBe 2L
    first.filter("NOT (k1 <=> 'a' AND k2 <=> '1') AND NOT (k1 <=> 'b' AND k2 <=> '2')").count() shouldBe 0L
    assertMvCorrect("gar_mv", expected)
    refreshMv("gar_child")
    assertMvCorrect("gar_child", expected)

    val beforeDelete = mvDataVersion("gar_mv")
    sql("DELETE FROM gar_src WHERE k1 IS NULL AND k2 IS NULL")
    refreshMv("gar_mv")
    mvDataVersion("gar_mv") shouldBe beforeDelete + 1
    changes(beforeDelete + 1).filter("_change_type = 'delete' AND k1 IS NULL AND k2 IS NULL").count() shouldBe 1L
    changes(beforeDelete + 1).count() shouldBe 1L
    changes(beforeDelete + 1).filter("_change_type = 'insert'").count() shouldBe 0L
    assertMvCorrect("gar_mv", expected)
    refreshMv("gar_child")
    assertMvCorrect("gar_child", expected)
  }
}
