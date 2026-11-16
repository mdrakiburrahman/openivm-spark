package org.openivm.spark.parity

import org.openivm.spark.common.{FeatureGate, MvCatalog, RefreshProfileCatalog, RefreshTypeCode}
import org.openivm.spark.parity.base.{CdfMode, InterceptMode, IvmParityMode, IvmParitySpecBase}

abstract class DateFormatGroupRecomputeScenarios extends IvmParitySpecBase("date-format-group-recompute") {
  self: IvmParityMode =>

  override protected def extraSparkConf: Map[String, String] = Map(
    FeatureGate.ProfileRefreshKey                            -> "true",
    "spark.openivm.compile.classificationCache.enabled"      -> "true",
    "spark.openivm.refresh.windowRunningIncremental.enabled" -> "true",
    "spark.openivm.refresh.groupedWindowState.enabled"       -> "true",
    "spark.sql.session.timeZone"                             -> "UTC"
  )

  it("restores date expressions around projected struct ordering keys") {
    sql(
      "CREATE TABLE dfgr_events (id STRING, event_time TIMESTAMP, version INT, payload STRING, year_month STRING) USING DELTA"
    )
    sql("""INSERT INTO dfgr_events VALUES
      |('a', TIMESTAMP '2023-12-29 12:00:00', 1, 'older', '202312'),
      |('a', TIMESTAMP '2024-01-01 12:00:00', 2, 'newer', '202401'),
      |('b', TIMESTAMP '2023-12-28 12:00:00', 1, 'other', '202312'),
      |('c', NULL, 1, 'null', '202401')""".stripMargin)
    val queries = Seq(
      "dfgr_month_mv" -> ("""WITH source_rows AS (
        |SELECT id, event_time, version, payload, year_month FROM dfgr_events AS s
        |WHERE s.year_month >= DATE_FORMAT(DATE_SUB(DATE '2024-01-03', 6), 'yyyyMM')
        |AND s.event_time >= CAST(DATE_SUB(DATE '2024-01-03', 6) AS TIMESTAMP)
        |AND s.event_time < CAST(DATE_ADD(DATE '2024-01-03', 1) AS TIMESTAMP)),
        |source_candidates AS (SELECT *, NAMED_STRUCT('event_time', event_time, 'version', version)
        |AS selection_rank FROM source_rows),
        |base_candidates AS (SELECT id, event_time, payload, selection_rank FROM source_candidates),
        |base_rows AS (SELECT id, MAX_BY(NAMED_STRUCT('event_time', event_time, 'payload', payload),
        |selection_rank) AS winner FROM base_candidates GROUP BY id)
        |SELECT id, DATE_FORMAT(winner.event_time, 'yyyyMM') AS date_key, winner.payload AS payload FROM base_rows""".stripMargin),
      "dfgr_day_mv" -> ("""WITH source_rows AS (
        |SELECT id, event_time, version, payload, year_month FROM dfgr_events AS s
        |WHERE s.year_month >= DATE_FORMAT(DATE_SUB(DATE '2024-01-03', 6), 'yyyyMM')),
        |source_candidates AS (SELECT *, NAMED_STRUCT('event_time', event_time, 'version', version)
        |AS selection_rank FROM source_rows),
        |base_rows AS (SELECT id, MAX_BY(NAMED_STRUCT('event_time', event_time, 'payload', payload),
        |selection_rank) AS winner FROM source_candidates GROUP BY id)
        |SELECT id, DATE_FORMAT(winner.event_time, 'yyyyMMdd') AS date_key, winner.payload AS payload FROM base_rows""".stripMargin)
    )
    queries.foreach { case (name, query) =>
      sql(s"CREATE MATERIALIZED VIEW $name AS $query")
      val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
      MvCatalog.lookup(spark, id).get.refreshType should not be RefreshTypeCode.FullRefresh
      assertMvCorrect(name, query)
    }
    sql("UPDATE dfgr_events SET event_time = TIMESTAMP '2024-01-02 12:00:00' WHERE payload = 'older'")
    sql("DELETE FROM dfgr_events WHERE payload = 'newer'")
    sql("INSERT INTO dfgr_events VALUES ('b', TIMESTAMP '2024-01-03 12:00:00', 2, 'inserted', '202401')")
    queries.foreach { case (name, query) =>
      RefreshProfileCatalog.removeAll(spark)
      refreshMv(name)
      RefreshProfileCatalog.scanAll(spark).map(_.detail).mkString("\n") should include("outcome=incremental_executed")
      assertMvCorrect(name, query)
    }
  }

  it("maintains formatted dates in grouped recompute across mixed changes") {
    sql("CREATE TABLE dfgr_source (id INT, ts TIMESTAMP, amount INT) USING DELTA")
    sql("""INSERT INTO dfgr_source VALUES
      |(1, TIMESTAMP '2024-01-01 01:00:00', 10),
      |(2, TIMESTAMP '2024-01-01 02:00:00', 20),
      |(3, NULL, 5)""".stripMargin)
    val query = """SELECT date_format(ts, 'yyyy-MM-dd') AS day, sum(DISTINCT amount) AS total
      |FROM dfgr_source GROUP BY date_format(ts, 'yyyy-MM-dd')""".stripMargin
    sql(s"CREATE MATERIALIZED VIEW dfgr_mv AS $query")
    val id = spark.sessionState.sqlParser.parseTableIdentifier("dfgr_mv")
    MvCatalog.lookup(spark, id).get.refreshType shouldBe RefreshTypeCode.GroupRecompute
    assertMvCorrect("dfgr_mv", query)
    sql("UPDATE dfgr_source SET ts = TIMESTAMP '2024-02-01 00:00:00' WHERE id = 1")
    sql("DELETE FROM dfgr_source WHERE id = 2")
    sql("INSERT INTO dfgr_source VALUES (4, TIMESTAMP '2024-01-01 03:00:00', 15)")
    RefreshProfileCatalog.removeAll(spark)
    refreshMv("dfgr_mv")
    RefreshProfileCatalog.scanAll(spark).map(_.detail).mkString("\n") should include("outcome=incremental_executed")
    assertMvCorrect("dfgr_mv", query)
  }

  it("preserves Spark date formats for constant expressions") {
    sql("CREATE TABLE dfgr_const_source (id INT) USING DELTA")
    sql("INSERT INTO dfgr_const_source VALUES (1)")
    val query = "SELECT id, date_format(TIMESTAMP '2024-01-02 03:04:05', 'yyyy-MM-dd') AS day FROM dfgr_const_source"
    sql(s"CREATE MATERIALIZED VIEW dfgr_const_mv AS $query")
    val id = spark.sessionState.sqlParser.parseTableIdentifier("dfgr_const_mv")
    MvCatalog.lookup(spark, id).get.refreshType should not be RefreshTypeCode.FullRefresh
    assertMvCorrect("dfgr_const_mv", query)
    sql("INSERT INTO dfgr_const_source VALUES (2)")
    RefreshProfileCatalog.removeAll(spark)
    refreshMv("dfgr_const_mv")
    RefreshProfileCatalog.scanAll(spark).map(_.detail).mkString("\n") should include("outcome=incremental_executed")
    assertMvCorrect("dfgr_const_mv", query)
  }
}

class DateFormatGroupRecomputeCdfIntegrationSpec       extends DateFormatGroupRecomputeScenarios with CdfMode
class DateFormatGroupRecomputeInterceptIntegrationSpec extends DateFormatGroupRecomputeScenarios with InterceptMode
