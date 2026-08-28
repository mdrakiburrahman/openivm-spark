package org.openivm.spark.parity

import org.openivm.spark.common.MvCatalog
import org.openivm.spark.parity.base.{InterceptMode, IvmParitySpecBase}

/** Integration coverage for `SHOW REFRESH SQL FOR CREATE MATERIALIZED VIEW ...`
  * (#25).
  *
  * Verifies the dry run returns the incrementally-rewritten, Spark-executable
  * refresh program as a single plain-SQL row, that it never materialises the MV
  * (the user's "without doing any actual work" requirement), and that an upstream
  * MV that was only shown still resolves for a downstream SHOW in the same session.
  *
  * All table / MV names are prefixed `srs_` to avoid Delta-warehouse collisions
  * with sibling specs running in parallel forks.
  */
class ShowMaterializedViewRefreshSqlSpec extends IvmParitySpecBase("show-refresh-sql") with InterceptMode {

  private def refreshSql(sqlText: String): String = {
    val rows = spark.sql(sqlText).collect()
    rows.length shouldBe 1
    rows.head.getString(0)
  }

  private def mvExists(name: String): Boolean = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    MvCatalog.lookup(spark, id).isDefined
  }

  describe("SHOW REFRESH SQL FOR CREATE MATERIALIZED VIEW") {
    it("emits an incremental MERGE program for an AGGREGATE_GROUP view without materialising the MV") {
      sql("CREATE TABLE srs_sales (region STRING, amount INT) USING DELTA")
      sql("INSERT INTO srs_sales VALUES ('east', 10), ('west', 20)")

      val q = "SELECT region, SUM(amount) AS total FROM srs_sales GROUP BY region"
      val program =
        refreshSql(s"SHOW REFRESH SQL FOR CREATE MATERIALIZED VIEW srs_mv_agg AS $q")

      program.trim should not be empty
      program.toUpperCase should include("MERGE INTO")
      program should include("srs_mv_agg")
      program.trim should endWith(";")

      mvExists("srs_mv_agg") shouldBe false
    }

    it("emits an INSERT OVERWRITE program for a FULL_REFRESH (Top-K) view") {
      sql("CREATE TABLE srs_topk_src (region STRING, amount INT) USING DELTA")
      sql("INSERT INTO srs_topk_src VALUES ('east', 10), ('west', 20), ('north', 30)")

      val q = "SELECT region, amount FROM srs_topk_src ORDER BY amount DESC LIMIT 2"
      val program =
        refreshSql(s"SHOW REFRESH SQL FOR CREATE MATERIALIZED VIEW srs_mv_topk AS $q")

      program.toUpperCase should include("INSERT OVERWRITE")
      program should include("srs_mv_topk")
      mvExists("srs_mv_topk") shouldBe false
    }

    it("exposes a single refresh_sql STRING output column") {
      sql("CREATE TABLE srs_schema_src (id INT) USING DELTA")
      val df =
        spark.sql("SHOW REFRESH SQL FOR CREATE MATERIALIZED VIEW srs_mv_schema AS SELECT id FROM srs_schema_src")
      df.schema.fieldNames shouldBe Array("refresh_sql")
    }

    it("resolves an upstream MV that was only shown (cold DAG)") {
      sql("CREATE TABLE srs_cold_base (k INT, v INT) USING DELTA")
      sql("INSERT INTO srs_cold_base VALUES (1, 100), (2, 200)")

      refreshSql(
        "SHOW REFRESH SQL FOR CREATE MATERIALIZED VIEW srs_cold_up AS " +
          "SELECT k, SUM(v) AS s FROM srs_cold_base GROUP BY k"
      )
      mvExists("srs_cold_up") shouldBe false

      val down =
        refreshSql("SHOW REFRESH SQL FOR CREATE MATERIALIZED VIEW srs_cold_down AS SELECT k FROM srs_cold_up")
      down.trim should not be empty
      down should include("srs_cold_down")
      mvExists("srs_cold_down") shouldBe false
    }
  }
}
