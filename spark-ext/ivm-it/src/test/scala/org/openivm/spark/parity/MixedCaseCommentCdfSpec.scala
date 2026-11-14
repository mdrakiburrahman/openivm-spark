package org.openivm.spark.parity

import org.openivm.spark.common.{MvCatalog, RefreshTypeCode}
import org.openivm.spark.parity.base.{CdfMode, IvmParitySpecBase}

class MixedCaseCommentCdfSpec extends IvmParitySpecBase("mixed-case-comment") with CdfMode {
  it("preserves literals after apostrophes in line and block comments on create and CDF refresh") {
    sql("CREATE TABLE mcc_source (id INT, flag BOOLEAN, reason STRING) USING DELTA")
    sql("INSERT INTO mcc_source VALUES (1, true, 'MiXeD'), (2, false, 'Needs Review')")

    val projection =
      """SELECT id, CASE WHEN flag THEN 'Not Ready' ELSE 'Ready' END AS state,
        |CASE WHEN reason = 'MiXeD' THEN 'ExactMatch'
        |     WHEN reason LIKE 'Needs%' THEN 'PatternMatch' ELSE 'NoMatch' END AS reason_match,
        |'yyyyMM' AS format_mask
        |FROM mcc_source""".stripMargin
    val window =
      """SELECT id, CASE WHEN flag THEN 'Not Ready' ELSE 'Ready' END AS state,
        |ROW_NUMBER() OVER (PARTITION BY flag ORDER BY id) AS rn
        |FROM mcc_source""".stripMargin

    sql(s"CREATE MATERIALIZED VIEW mcc_line AS\n-- synthetic's comment\n$projection")
    sql(s"CREATE MATERIALIZED VIEW mcc_block AS\n/* synthetic's comment */\n$window")
    val lineId  = spark.sessionState.sqlParser.parseTableIdentifier("mcc_line")
    val blockId = spark.sessionState.sqlParser.parseTableIdentifier("mcc_block")
    MvCatalog.lookup(spark, lineId).get.refreshType shouldBe RefreshTypeCode.SimpleProjection
    MvCatalog.lookup(spark, blockId).get.refreshType shouldBe RefreshTypeCode.WindowPartition
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
