package org.openivm.spark.parity

import org.openivm.spark.common.{MvCatalog, RefreshTypeCode}
import org.openivm.spark.parity.base.{CdfMode, IvmParitySpecBase}

class SparkExpressionCdfIntegrationSpec extends IvmParitySpecBase("expression-cdf") with CdfMode {
  describe("Spark expression bridge over CDF") {
    it("preserves incremental results for mixed changes") {
      assume(sys.env.get("OPENIVM_MARKER_INTEGRATION_TEST").contains("1"))
      spark.conf.set("spark.sql.legacy.sizeOfNull", "true")
      Seq(false, true).foreach { ansi =>
        spark.conf.set("spark.sql.ansi.enabled", ansi.toString)
        val source = s"expr_cdf_source_$ansi"
        val mv     = s"expr_cdf_mv_$ansi"
        val query = s"""SELECT id, txt RLIKE '(?i)ä' AS re, size(split(txt, '/')) AS n,
          |element_at(split(txt, '/'), -1) AS leaf,
          |sha2(to_json(named_struct('Odd Name', txt, 'nested', named_struct('Mixed', id)),
          |map('ignoreNullFields', 'false', 'timeZone', 'UTC')), 256) AS digest
          |FROM $source WHERE txt IS NULL OR CAST(txt AS STRING) RLIKE '(?i)ä' OR txt LIKE '%/%'""".stripMargin
        sql(s"CREATE TABLE $source (id INT, txt STRING) USING DELTA")
        sql(s"INSERT INTO $source VALUES (1, NULL), (2, 'Ä'), (3, 'a/b')")
        sql(s"CREATE MATERIALIZED VIEW $mv AS $query")
        val identifier = spark.sessionState.sqlParser.parseTableIdentifier(mv)
        MvCatalog.lookup(spark, identifier).get.refreshType shouldBe RefreshTypeCode.SimpleProjection
        assertMvCorrect(mv, query)
        sql(s"DELETE FROM $source WHERE id = 1")
        sql(s"UPDATE $source SET txt = 'x/y' WHERE id = 2")
        sql(s"INSERT INTO $source VALUES (4, 'ä')")
        refreshMv(mv)
        assertMvCorrect(mv, query)
      }
    }
  }
}
