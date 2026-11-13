package org.openivm.spark.parity

import org.openivm.spark.common.MvCatalog
import org.openivm.spark.parity.base.{InterceptMode, IvmParitySpecBase}

class CompileBridgeRlikeSpec extends IvmParitySpecBase("compile-bridge-rlike") with InterceptMode {
  describe("Spark RLIKE compiler normalization") {
    it("preserves nested substring matching through incremental refresh") {
      sql("CREATE TABLE cbrx_src (id INT, raw STRING) USING DELTA")
      sql("INSERT INTO cbrx_src VALUES (1, '2026-11-13T12:34:56.12345678'), (2, NULL)")
      val mvName = "cbrx_mv"
      val viewBody =
        """SELECT id, CASE WHEN SUBSTRING(REGEXP_EXTRACT(TRIM(CAST(raw AS STRING)),
          |  '[T ,][0-9]{2}[:.][0-9]{2}[:.][0-9]{2}[.]([0-9]+)', 1), 8) RLIKE '[1-9]'
          |  THEN 'hit' ELSE 'miss' END AS result FROM cbrx_src""".stripMargin
      sql(s"CREATE MATERIALIZED VIEW $mvName AS $viewBody")
      val id = spark.sessionState.sqlParser.parseTableIdentifier(mvName)
      MvCatalog
        .lookup(spark, id)
        .getOrElse(fail(s"Missing MV metadata for $mvName"))
        .refreshTypeName should not equal "FULL_REFRESH"
      assertMvCorrect(mvName, viewBody)
      sql("INSERT INTO cbrx_src VALUES (3, '2026-11-13T12:34:56.12345670'), (4, 'invalid')")
      refreshMv(mvName)
      MvCatalog
        .lookup(spark, id)
        .getOrElse(fail(s"Missing MV metadata for $mvName"))
        .refreshTypeName should not equal "FULL_REFRESH"
      assertMvCorrect(mvName, viewBody)
    }
  }
}
