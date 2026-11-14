package org.openivm.spark.parity

import org.openivm.spark.common.MvCatalog
import org.openivm.spark.parity.base.{InterceptMode, IvmParitySpecBase}

class CompileBridgeScalarSpec extends IvmParitySpecBase("compile-bridge-scalar") with InterceptMode {
  describe("Spark scalar expressions in a window view") {
    it("preserves boolean and timestamp semantics through initial load and refresh") {
      sql("CREATE TABLE cbs_src (id STRING, flag BOOLEAN, start_ts TIMESTAMP, end_ts TIMESTAMP) USING DELTA")
      sql("""INSERT INTO cbs_src VALUES
            |('a', true, TIMESTAMP'1970-01-01 00:00:00', TIMESTAMP'1970-01-01 00:00:01.100'),
            |('b', false, TIMESTAMP'1969-12-31 23:59:59.900', TIMESTAMP'1970-01-01 00:00:00.100'),
            |('c', NULL, NULL, NULL)""".stripMargin)
      val body = """WITH discovery AS (
                   |  SELECT id, flag, start_ts, end_ts,
                   |    ROW_NUMBER() OVER (PARTITION BY id ORDER BY start_ts DESC, end_ts DESC) AS rn
                   |  FROM cbs_src
                   |), latest AS (SELECT * FROM discovery WHERE rn = 1)
                   |SELECT id, CASE WHEN LOWER(flag) = 'true' THEN 1 ELSE 0 END AS f,
                   |  CASE WHEN UNIX_TIMESTAMP(start_ts) = 0 THEN CAST(NULL AS TIMESTAMP) ELSE start_ts END AS t,
                   |  CAST(COALESCE(GREATEST(TIMESTAMPDIFF(SECOND, start_ts, end_ts), 0), -1) AS INT) AS d
                   |FROM latest""".stripMargin
      sql(s"CREATE MATERIALIZED VIEW cbs_mv AS $body")
      val id = spark.sessionState.sqlParser.parseTableIdentifier("cbs_mv")
      MvCatalog.lookup(spark, id).getOrElse(fail("Missing MV metadata")).refreshTypeName should equal(
        "WINDOW_PARTITION"
      )
      assertMvCorrect("cbs_mv", body)
      sql("""INSERT INTO cbs_src VALUES
            |('a', false, TIMESTAMP'2024-01-01 00:00:00.900', TIMESTAMP'2024-01-01 00:00:01.100'),
            |('d', true, TIMESTAMP'2024-01-01 00:00:03', TIMESTAMP'2024-01-01 00:00:02')""".stripMargin)
      sql("DELETE FROM cbs_src WHERE id = 'b'")
      refreshMv("cbs_mv")
      MvCatalog.lookup(spark, id).getOrElse(fail("Missing MV metadata")).refreshTypeName should equal(
        "WINDOW_PARTITION"
      )
      assertMvCorrect("cbs_mv", body)
    }
  }
}
