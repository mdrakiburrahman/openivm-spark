package org.openivm.spark.parity

import org.openivm.spark.common.{FeatureGate, MvCatalog, RefreshProfileCatalog}
import org.openivm.spark.parity.base.{IvmParityMode, IvmParitySpecBase}

abstract class CompileBridgeScalarScenarios extends IvmParitySpecBase("compile-bridge-scalar") {
  self: IvmParityMode =>

  override protected def extraSparkConf: Map[String, String] = Map(FeatureGate.ProfileRefreshKey -> "true")

  it("recomputes multi-CTE grouped views with trim predicates and projections") {
    val src = s"cbs_${modeLabel}_trim_probe_src"
    val mv  = s"cbs_${modeLabel}_trim_probe_mv"
    sql(s"CREATE TABLE $src (id STRING, quantity INT) USING DELTA")
    sql(s"INSERT INTO $src VALUES (' a ', 5), (' a ', 6), ('b', 8), ('', 9), (NULL, 10)")
    val body = s"""WITH source_rows AS (
      | SELECT id, quantity, NULLIF(TRIM(CAST(id AS STRING)), '') AS normalized FROM $src
      |), candidates AS (
      | SELECT * FROM source_rows WHERE NULLIF(TRIM(id), '') IS NOT NULL
      |), base_rows AS (
      | SELECT id, MAX_BY(NAMED_STRUCT('quantity', quantity), quantity) AS winner FROM source_rows GROUP BY id
      |), link_rows AS (
      | SELECT id, MAX_BY(NAMED_STRUCT('normalized', normalized), quantity) AS winner FROM candidates GROUP BY id
      |)
      |SELECT b.id, b.winner.quantity AS amount, l.winner.normalized AS normalized
      |FROM base_rows b LEFT JOIN link_rows l ON b.id = l.id""".stripMargin
    sql(s"CREATE MATERIALIZED VIEW $mv AS $body")
    MvCatalog.lookup(spark, spark.sessionState.sqlParser.parseTableIdentifier(mv)).get.refreshTypeName should equal(
      "GROUP_RECOMPUTE"
    )
    assertMvCorrect(mv, body)
    sql(s"INSERT INTO $src VALUES (' a ', 7), ('e', 8)")
    sql(s"DELETE FROM $src WHERE id = 'b'")
    RefreshProfileCatalog.removeAll(spark)
    refreshMv(mv)
    RefreshProfileCatalog.scanAll(spark).map(_.detail).mkString("\n") should include("outcome=incremental_executed")
    assertMvCorrect(mv, body)
  }

  it("preserves infinity and trim predicates through incremental refresh") {
    val src       = s"cbs_${modeLabel}_nonfinite_src"
    val aggregate = s"cbs_${modeLabel}_nonfinite_mv"
    val grouped   = s"cbs_${modeLabel}_trim_mv"
    sql(s"CREATE TABLE $src (id STRING, quantity DOUBLE) USING DELTA")
    sql(s"""INSERT INTO $src VALUES
      |(' a ', 5D), (' a ', CAST('Infinity' AS DOUBLE)), ('b', CAST('-Infinity' AS DOUBLE)),
      |('', 9D), (NULL, 10D), ('c', CAST('NaN' AS DOUBLE)), ('d', NULL)""".stripMargin)
    val queries = Seq(
      aggregate -> s"""SELECT id, SUM(IF(quantity IS NOT NULL AND NOT ISNAN(quantity)
        |AND ABS(quantity) != CAST('Infinity' AS DOUBLE), quantity, NULL)) AS total
        |FROM $src GROUP BY id""".stripMargin,
      grouped -> s"""SELECT NULLIF(TRIM(CAST(id AS STRING)), '') AS key, COUNT(DISTINCT quantity) AS total
        |FROM $src WHERE NULLIF(TRIM(id), '') IS NOT NULL GROUP BY NULLIF(TRIM(CAST(id AS STRING)), '')""".stripMargin
    )
    queries.foreach { case (name, query) =>
      sql(s"CREATE MATERIALIZED VIEW $name AS $query")
      MvCatalog
        .lookup(spark, spark.sessionState.sqlParser.parseTableIdentifier(name))
        .get
        .refreshTypeName should not equal "FULL_REFRESH"
      assertMvCorrect(name, query)
    }
    sql(s"INSERT INTO $src VALUES (' a ', 7D), ('e', CAST('Infinity' AS DOUBLE)), ('f', -3D)")
    sql(s"DELETE FROM $src WHERE id = 'b'")
    queries.foreach { case (name, query) =>
      RefreshProfileCatalog.removeAll(spark)
      refreshMv(name)
      RefreshProfileCatalog.scanAll(spark).map(_.detail).mkString("\n") should include("outcome=incremental_executed")
      assertMvCorrect(name, query)
    }
  }

  describe("Spark scalar expressions in a window view") {
    it("preserves boolean and timestamp semantics through initial load and refresh") {
      Seq(
        "spark.openivm.refresh.windowRunningIncremental.enabled",
        "spark.openivm.refresh.groupedWindowState.enabled",
        "spark.openivm.compile.classificationCache.enabled"
      ).foreach(key => spark.conf.set(key, "true"))
      val src = s"cbs_${modeLabel}_src"
      val mv  = s"cbs_${modeLabel}_mv"
      sql(s"CREATE TABLE $src (id STRING, flag BOOLEAN, start_ts TIMESTAMP, end_ts TIMESTAMP) USING DELTA")
      sql(s"""INSERT INTO $src VALUES
            |('a', true, TIMESTAMP'1970-01-01 00:00:00', TIMESTAMP'1970-01-01 00:00:01.100'),
            |('b', false, TIMESTAMP'1969-12-31 23:59:59.900', TIMESTAMP'1970-01-01 00:00:00.100'),
            |('c', NULL, NULL, NULL)""".stripMargin)
      val body = s"""WITH discovery AS (
                   |  SELECT id, flag, start_ts, end_ts,
                   |    ROW_NUMBER() OVER (PARTITION BY id ORDER BY start_ts DESC, end_ts DESC) AS rn
                   |  FROM $src
                   |), latest AS (SELECT * FROM discovery WHERE rn = 1)
                   |SELECT id, CASE WHEN LOWER(flag) = 'true' THEN 1 ELSE 0 END AS f,
                   |  CASE WHEN UNIX_TIMESTAMP(start_ts) = 0 THEN CAST(NULL AS TIMESTAMP) ELSE start_ts END AS t,
                   |  CAST(COALESCE(GREATEST(TIMESTAMPDIFF(SECOND, start_ts, end_ts), 0), -1) AS INT) AS d
                   |FROM latest""".stripMargin
      sql(s"CREATE MATERIALIZED VIEW $mv AS $body")
      val id = spark.sessionState.sqlParser.parseTableIdentifier(mv)
      MvCatalog.lookup(spark, id).getOrElse(fail("Missing MV metadata")).refreshTypeName should equal(
        "WINDOW_PARTITION"
      )
      assertMvCorrect(mv, body)
      sql(s"""INSERT INTO $src VALUES
            |('a', false, TIMESTAMP'2024-01-01 00:00:00.900', TIMESTAMP'2024-01-01 00:00:01.100'),
            |('d', true, TIMESTAMP'2024-01-01 00:00:03', TIMESTAMP'2024-01-01 00:00:02')""".stripMargin)
      sql(s"DELETE FROM $src WHERE id = 'b'")
      refreshMv(mv)
      MvCatalog.lookup(spark, id).getOrElse(fail("Missing MV metadata")).refreshTypeName should equal(
        "WINDOW_PARTITION"
      )
      assertMvCorrect(mv, body)
    }

  }
}
