package org.openivm.spark.parity

import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.config.Configurator
import org.openivm.spark.common.{FeatureGate, MvCatalog, RefreshProfileCatalog, RefreshTypeCode}
import org.openivm.spark.parity.base.{CdfMode, InterceptMode, IvmParityMode, IvmParitySpecBase}

abstract class SparkDateGeneratorScenarios extends IvmParitySpecBase("date-generator") {
  self: IvmParityMode =>
  override protected def extraSparkConf: Map[String, String] = Map(
    FeatureGate.WindowRunningIncrementalEnabledKey   -> "true",
    FeatureGate.GroupedWindowStateEnabledKey         -> "true",
    FeatureGate.CompileClassificationCacheEnabledKey -> "true",
    FeatureGate.ProfileRefreshKey                    -> "true"
  )

  it("incrementally expands daily date sequences and preserves invalid bounds") {
    Seq("UTC", "America/Los_Angeles", "Pacific/Apia").zipWithIndex.foreach { case (zone, index) =>
      spark.conf.set("spark.sql.session.timeZone", zone)
      val source = s"date_gen_source_$index"
      val mv     = s"date_gen_mv_$index"
      sql(s"CREATE TABLE $source (id INT, start_date DATE, end_date DATE) USING DELTA")
      sql(s"""INSERT INTO $source VALUES
        |(1, DATE '2024-03-09', DATE '2024-03-12'),
        |(2, NULL, DATE '2024-03-11'),
        |(3, DATE '2024-11-02', DATE '2024-11-04'),
        |(6, DATE '2011-12-29', DATE '2011-12-31')""".stripMargin)
      val interval = if (index == 1) "INTERVAL 1 DAY" else "INTERVAL '1' DAY"
      val query = s"""SELECT t.id, explode(sequence(t.start_date, t.end_date, $interval)) AS d
        |FROM $source t""".stripMargin
      sql(s"CREATE MATERIALIZED VIEW $mv AS $query")
      val id = spark.sessionState.sqlParser.parseTableIdentifier(mv)
      MvCatalog.lookup(spark, id).get.refreshType shouldBe RefreshTypeCode.SimpleProjection
      assertMvCorrect(mv, query)
      sql(s"UPDATE $source SET end_date = DATE '2024-03-14' WHERE id = 1")
      sql(s"DELETE FROM $source WHERE id = 3")
      sql(s"INSERT INTO $source VALUES (4, DATE '2024-03-15', DATE '2024-03-15')")
      refreshMv(mv)
      assertMvCorrect(mv, query)
      sql(s"INSERT INTO $source VALUES (5, DATE '2024-03-20', DATE '2024-03-19')")
      val level = LogManager.getRootLogger.getLevel
      Configurator.setRootLevel(Level.FATAL)
      try {
        intercept[Exception] { sql(query).collect() }
        intercept[Exception] { refreshMv(mv) }
      } finally Configurator.setRootLevel(level)
    }
  }

  it("maintains a window above an expanded date CTE") {
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    sql("CREATE TABLE date_gen_window_source (id INT, start_date DATE, end_date DATE) USING DELTA")
    sql("INSERT INTO date_gen_window_source VALUES (1, DATE '2024-01-01', DATE '2024-01-03')")
    val query = """WITH expanded AS (
      | SELECT t.id, explode(sequence(t.start_date, t.end_date, INTERVAL 1 DAY)) AS d
      | FROM date_gen_window_source t
      |) SELECT id, d, row_number() OVER (PARTITION BY id ORDER BY d) AS rn FROM expanded""".stripMargin
    sql(s"CREATE MATERIALIZED VIEW date_gen_window_mv AS $query")
    val id = spark.sessionState.sqlParser.parseTableIdentifier("date_gen_window_mv")
    MvCatalog.lookup(spark, id).get.refreshType should not be RefreshTypeCode.FullRefresh
    assertMvCorrect("date_gen_window_mv", query)
    sql("UPDATE date_gen_window_source SET end_date = DATE '2024-01-04' WHERE id = 1")
    sql("INSERT INTO date_gen_window_source VALUES (2, DATE '2024-02-01', DATE '2024-02-02')")
    refreshMv("date_gen_window_mv")
    assertMvCorrect("date_gen_window_mv", query)
    sql("DELETE FROM date_gen_window_source WHERE id = 1")
    refreshMv("date_gen_window_mv")
    assertMvCorrect("date_gen_window_mv", query)
  }

  it("maintains daily dates following complex LEAD windows") {
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    sql("CREATE TABLE date_gen_lead (id INT, ts TIMESTAMP, deleted BOOLEAN, version INT, event_id INT) USING DELTA")
    sql("""INSERT INTO date_gen_lead VALUES
      |(1, TIMESTAMP '2024-01-01 10:00:00', false, 1, 1),
      |(1, TIMESTAMP '2024-01-03 10:00:00', NULL, 2, 2),
      |(1, TIMESTAMP '2024-01-04 10:00:00', true, 3, 3)""".stripMargin)
    val query = """WITH events AS (
      | SELECT id, ts, deleted,
      | LEAD(CAST(ts AS TIMESTAMP)) OVER (PARTITION BY id ORDER BY CAST(ts AS TIMESTAMP),
      |   COALESCE(deleted, FALSE), COALESCE(CAST(version AS STRING), ''),
      |   COALESCE(CAST(event_id AS STRING), '')) AS next_ts,
      | LEAD(COALESCE(deleted, FALSE), 2) OVER (PARTITION BY id ORDER BY CAST(ts AS TIMESTAMP),
      |   COALESCE(deleted, FALSE), COALESCE(CAST(version AS STRING), ''),
      |   COALESCE(CAST(event_id AS STRING), '')) AS next_deleted
      | FROM date_gen_lead
      |), bounds AS (
      | SELECT id, ts, next_deleted, CAST(ts AS DATE) AS start_date,
      |   CAST(COALESCE(next_ts, ts) AS DATE) AS end_date FROM events
      |) SELECT id, ts, next_deleted, explode(sequence(start_date, end_date, INTERVAL 1 DAY)) AS d FROM bounds""".stripMargin
    sql(s"CREATE MATERIALIZED VIEW date_gen_lead_mv AS $query")
    val id = spark.sessionState.sqlParser.parseTableIdentifier("date_gen_lead_mv")
    MvCatalog.lookup(spark, id).get.refreshType should not be RefreshTypeCode.FullRefresh
    assertMvCorrect("date_gen_lead_mv", query)
    sql("INSERT INTO date_gen_lead VALUES (1, TIMESTAMP '2024-01-05 10:00:00', false, 4, 4)")
    refreshMv("date_gen_lead_mv")
    assertMvCorrect("date_gen_lead_mv", query)
  }

  it("maintains two daily generators in separate CTEs") {
    sql("CREATE TABLE date_gen_left (id INT, start_date DATE, end_date DATE) USING DELTA")
    sql("CREATE TABLE date_gen_right (id INT, start_date DATE, end_date DATE) USING DELTA")
    sql("INSERT INTO date_gen_left VALUES (1, DATE '2024-01-01', DATE '2024-01-02')")
    sql("INSERT INTO date_gen_right VALUES (1, DATE '2024-02-01', DATE '2024-02-02')")
    val query = """WITH l AS (
      | SELECT id, explode(sequence(start_date, end_date, INTERVAL 1 DAY)) AS ld FROM date_gen_left
      |), r AS (
      | SELECT id, explode(sequence(start_date, end_date, INTERVAL 1 DAY)) AS rd FROM date_gen_right
      |) SELECT l.id, l.ld, r.rd FROM l JOIN r ON l.id = r.id""".stripMargin
    sql(s"CREATE MATERIALIZED VIEW date_gen_join_mv AS $query")
    val id = spark.sessionState.sqlParser.parseTableIdentifier("date_gen_join_mv")
    MvCatalog.lookup(spark, id).get.refreshType should not be RefreshTypeCode.FullRefresh
    assertMvCorrect("date_gen_join_mv", query)
    sql("UPDATE date_gen_left SET end_date = DATE '2024-01-03' WHERE id = 1")
    sql("INSERT INTO date_gen_right VALUES (1, DATE '2024-02-03', DATE '2024-02-03')")
    RefreshProfileCatalog.removeAll(spark)
    refreshMv("date_gen_join_mv")
    RefreshProfileCatalog.scanAll(spark).map(_.detail).mkString("\n") should include("outcome=incremental_executed")
    assertMvCorrect("date_gen_join_mv", query)
    sql("DELETE FROM date_gen_right WHERE start_date = DATE '2024-02-01'")
    RefreshProfileCatalog.removeAll(spark)
    refreshMv("date_gen_join_mv")
    RefreshProfileCatalog.scanAll(spark).map(_.detail).mkString("\n") should include("outcome=incremental_executed")
    assertMvCorrect("date_gen_join_mv", query)
  }
}

class SparkDateGeneratorCdfIntegrationSpec       extends SparkDateGeneratorScenarios with CdfMode
class SparkDateGeneratorInterceptIntegrationSpec extends SparkDateGeneratorScenarios with InterceptMode
