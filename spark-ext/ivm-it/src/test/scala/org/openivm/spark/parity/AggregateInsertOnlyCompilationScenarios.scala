package org.openivm.spark.parity

import org.openivm.spark.common.RefreshSqlLogCatalog
import org.openivm.spark.parity.base.IvmParitySpecBase

/** Generic coverage for runtime selection of OpenIVM's insert-only aggregate
  * compile facts. Query-log assertions verify the executed SQL program in
  * addition to the usual full bag-equality checks.
  */
abstract class AggregateInsertOnlyCompilationScenarios extends IvmParitySpecBase("aggregate-insert-only-compilation") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  override protected def extraSparkConf: Map[String, String] =
    super.extraSparkConf + ("spark.openivm.queryLog.enabled" -> "true")

  private def rewrittenSql(view: String): Seq[String] =
    sql("SHOW OPENIVM QUERY LOG")
      .collect()
      .toSeq
      .filter(_.getString(1).split("\\.").last == view)
      .filter(_.getString(6) == "rewritten_stmt")
      .map(_.getString(9))

  describe("terminal insert-only grouped aggregates") {
    itCdf("executes one direct MERGE for SUM, COUNT, MIN, and MAX") {
      sql("CREATE TABLE aio_events (grp INT, value INT, event_ts TIMESTAMP) USING DELTA")
      sql(
        "INSERT INTO aio_events VALUES " +
          "(1, 10, TIMESTAMP'2026-01-01 10:00:00'), " +
          "(2, 20, TIMESTAMP'2026-01-01 11:00:00')"
      )
      val viewBody =
        "SELECT grp, SUM(value) AS total, COUNT(*) AS row_count, " +
          "MIN(event_ts) AS first_at, MAX(event_ts) AS last_at " +
          "FROM aio_events GROUP BY grp"
      sql(s"CREATE MATERIALIZED VIEW aio_rollup AS $viewBody")

      RefreshSqlLogCatalog.ensureTables(spark)
      RefreshSqlLogCatalog.removeAll(spark)
      sql(
        "INSERT INTO aio_events VALUES " +
          "(1, 7, TIMESTAMP'2025-12-31 09:00:00'), " +
          "(1, 8, TIMESTAMP'2026-01-03 12:00:00'), " +
          "(3, 30, TIMESTAMP'2026-01-02 08:00:00')"
      )
      refreshMv("aio_rollup")
      assertMvCorrect("aio_rollup", viewBody)

      val statements = rewrittenSql("aio_rollup")
      statements should have size 1
      statements.head.toUpperCase should include("MERGE INTO")
      statements.head.toUpperCase should not include "CREATE OR REPLACE TABLE"
      statements.head should include("__openivm_direct_delta")
    }

    itCdf("executes one direct MERGE for MIN/MAX with a derived projection") {
      sql("CREATE TABLE aio_status_events (grp INT, action STRING, event_ts TIMESTAMP) USING DELTA")
      sql("INSERT INTO aio_status_events VALUES (1, 'open', TIMESTAMP'2026-01-01 10:00:00')")
      val viewBody =
        "WITH grouped AS (" +
          "SELECT grp, MIN(CASE WHEN action = 'open' THEN event_ts END) AS opened_at, " +
          "MAX(CASE WHEN action = 'close' THEN event_ts END) AS closed_at " +
          "FROM aio_status_events GROUP BY grp) " +
          "SELECT *, CASE WHEN closed_at IS NULL THEN 'open' ELSE 'closed' END AS status FROM grouped"
      sql(s"CREATE MATERIALIZED VIEW aio_status_rollup AS $viewBody")

      RefreshSqlLogCatalog.ensureTables(spark)
      RefreshSqlLogCatalog.removeAll(spark)
      sql(
        "INSERT INTO aio_status_events VALUES " +
          "(1, 'close', TIMESTAMP'2026-01-02 10:00:00'), " +
          "(2, 'open', TIMESTAMP'2026-01-02 11:00:00')"
      )
      refreshMv("aio_status_rollup")
      assertMvCorrect("aio_status_rollup", viewBody)

      val statements = rewrittenSql("aio_status_rollup")
      statements should have size 1
      statements.head should include("__openivm_direct_delta")
    }

    itCdf("falls back to the general program for one mixed DML batch") {
      sql("CREATE TABLE aio_mixed_events (id INT, grp INT, value INT, event_ts TIMESTAMP) USING DELTA")
      sql(
        "INSERT INTO aio_mixed_events VALUES " +
          "(1, 1, 10, TIMESTAMP'2026-01-01 10:00:00'), " +
          "(2, 2, 20, TIMESTAMP'2026-01-01 11:00:00')"
      )
      val viewBody =
        "SELECT grp, SUM(value) AS total, COUNT(*) AS row_count, " +
          "MIN(event_ts) AS first_at, MAX(event_ts) AS last_at " +
          "FROM aio_mixed_events GROUP BY grp"
      sql(s"CREATE MATERIALIZED VIEW aio_mixed_rollup AS $viewBody")

      RefreshSqlLogCatalog.ensureTables(spark)
      RefreshSqlLogCatalog.removeAll(spark)
      sql("INSERT INTO aio_mixed_events VALUES (3, 3, 40, TIMESTAMP'2026-02-01 10:00:00')")
      sql("UPDATE aio_mixed_events SET value = 11 WHERE id = 1")
      sql("DELETE FROM aio_mixed_events WHERE id = 2")
      refreshMv("aio_mixed_rollup")

      assertMvCorrect("aio_mixed_rollup", viewBody)
      rewrittenSql("aio_mixed_rollup").size should be > 1
    }

    itCdf("does not apply the direct MERGE to an aggregate over a join") {
      sql("CREATE TABLE aio_join_facts (id INT, grp INT, value INT) USING DELTA")
      sql("CREATE TABLE aio_join_labels (id INT, label STRING) USING DELTA")
      sql("INSERT INTO aio_join_facts VALUES (1, 1, 10), (2, 2, 20)")
      val viewBody =
        "SELECT f.grp, SUM(f.value) AS total, COUNT(*) AS row_count " +
          "FROM aio_join_facts f LEFT JOIN aio_join_labels l ON f.id = l.id GROUP BY f.grp"
      sql(s"CREATE MATERIALIZED VIEW aio_join_rollup AS $viewBody")

      RefreshSqlLogCatalog.ensureTables(spark)
      RefreshSqlLogCatalog.removeAll(spark)
      sql("INSERT INTO aio_join_labels VALUES (1, 'one')")
      refreshMv("aio_join_rollup")
      assertMvCorrect("aio_join_rollup", viewBody)

      val statements = rewrittenSql("aio_join_rollup")
      statements.size should be > 1
      statements.mkString("\n") should not include "__openivm_direct_delta"
    }
  }

  describe("downstream identity") {
    itCdf("keeps cascade SQL for a real downstream consumer") {
      sql("CREATE TABLE aio_real_cascade_source (grp INT, value INT) USING DELTA")
      sql("INSERT INTO aio_real_cascade_source VALUES (1, 10)")
      val upstreamBody   = "SELECT grp, SUM(value) AS total FROM aio_real_cascade_source GROUP BY grp"
      val downstreamBody = "SELECT grp, total FROM aio_real_cascade_rollup"
      sql(s"CREATE MATERIALIZED VIEW aio_real_cascade_rollup AS $upstreamBody")
      sql(s"CREATE MATERIALIZED VIEW aio_rollup_consumer AS $downstreamBody")

      RefreshSqlLogCatalog.ensureTables(spark)
      RefreshSqlLogCatalog.removeAll(spark)
      sql("INSERT INTO aio_real_cascade_source VALUES (1, 2), (3, 30)")
      refreshMv("aio_real_cascade_rollup")
      refreshMv("aio_rollup_consumer")
      assertMvCorrect("aio_real_cascade_rollup", upstreamBody)
      assertMvCorrect("aio_rollup_consumer", downstreamBody)

      val cascadingSql = rewrittenSql("aio_real_cascade_rollup").mkString("\n").toUpperCase
      cascadingSql should include("CREATE OR REPLACE TABLE")
      cascadingSql should include("MERGE INTO")
      cascadingSql should not include "WHEN MATCHED THEN DELETE"
    }
  }
}
