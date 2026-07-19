package org.openivm.spark.parity

import java.io.File

import org.apache.spark.sql.{Encoders, Row}
import org.openivm.spark.common.{MvCatalog, RefreshTypeCode}
import org.openivm.spark.parity.base.{InterceptMode, IvmParitySpecBase}

/** Integration coverage for `EXPLAIN CREATE MATERIALIZED VIEW ... AS <query>` (#4).
  *
  * Verifies the dry-run eligibility verdict is emitted as a single JSON row, that
  * the verdict is byte-for-byte consistent with what a real CREATE classifies
  * (the anti-regression parity guard), that NO materialized view is actually
  * materialised, and that an upstream MV that was only EXPLAINed still resolves
  * for a downstream EXPLAIN in the same session (the dbt "cold DAG" case).
  *
  * All table / MV names are prefixed `expl_` to avoid Delta-warehouse collisions
  * with sibling specs running in parallel forks.
  */
class ExplainCreateMaterializedViewSpec extends IvmParitySpecBase("explain-create") with InterceptMode {

  /** Run an EXPLAIN statement and parse its single JSON row into a typed [[Row]]. */
  private def explain(sqlText: String): Row = {
    val rows = spark.sql(sqlText).collect()
    rows.length shouldBe 1
    val json = rows.head.getString(0)
    val ds   = spark.createDataset(Seq(json))(Encoders.STRING)
    spark.read.json(ds).head()
  }

  private def mvExists(name: String): Boolean = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    MvCatalog.lookup(spark, id).isDefined
  }

  private def mvRefreshType(name: String): Int = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    MvCatalog.lookup(spark, id).getOrElse(fail(s"MV $name not found in catalog")).refreshType
  }

  /** Physical Delta location an MV would occupy, so tests can assert it never got created. */
  private def mvLocationDir(name: String): File =
    new File(new File(warehouseDir, "_ivm/views"), name)

  describe("EXPLAIN CREATE MATERIALIZED VIEW") {
    it("emits an eligible AGGREGATE_GROUP verdict as JSON without materialising the MV") {
      sql("CREATE TABLE expl_sales (region STRING, amount INT) USING DELTA")
      sql("INSERT INTO expl_sales VALUES ('east', 10), ('west', 20), ('east', 30)")

      val q   = "SELECT region, SUM(amount) AS total FROM expl_sales GROUP BY region"
      val row = explain(s"EXPLAIN CREATE MATERIALIZED VIEW expl_mv_agg AS $q")

      row.getAs[String]("view") shouldBe "expl_mv_agg"
      row.getAs[Boolean]("eligible") shouldBe true
      row.getAs[Long]("refresh_type") shouldBe RefreshTypeCode.AggregateGroup.toLong
      row.getAs[String]("refresh_type_name") shouldBe "AGGREGATE_GROUP"
      row.getAs[String]("reason") shouldBe "kept"
      row.getAs[Seq[String]]("source_tables").exists(_.endsWith("expl_sales")) shouldBe true

      // No MV metadata, no Delta table on disk — the verdict is purely a dry run.
      mvExists("expl_mv_agg") shouldBe false
      mvLocationDir("expl_mv_agg").exists() shouldBe false
    }

    it("reports an ineligible FULL_REFRESH verdict with reason top_k for a Top-K view") {
      sql("CREATE TABLE expl_topk_src (region STRING, amount INT) USING DELTA")
      sql("INSERT INTO expl_topk_src VALUES ('east', 10), ('west', 20), ('north', 30)")

      val q   = "SELECT region, amount FROM expl_topk_src ORDER BY amount DESC LIMIT 2"
      val row = explain(s"EXPLAIN CREATE MATERIALIZED VIEW expl_mv_topk AS $q")

      row.getAs[Boolean]("eligible") shouldBe false
      row.getAs[Long]("refresh_type") shouldBe RefreshTypeCode.FullRefresh.toLong
      row.getAs[String]("refresh_type_name") shouldBe "FULL_REFRESH"
      row.getAs[String]("reason") shouldBe "top_k"
      row.getAs[Boolean]("emits_cascade_view_delta") shouldBe false
      mvExists("expl_mv_topk") shouldBe false
    }

    it("verdict matches the refresh type a real CREATE assigns (parity guard)") {
      sql("CREATE TABLE expl_parity_src (region STRING, amount INT) USING DELTA")
      sql("INSERT INTO expl_parity_src VALUES ('east', 10), ('west', 20)")

      // Two shapes: an AGGREGATE_GROUP and a SIMPLE_PROJECTION.
      val cases = Seq(
        "SELECT region, SUM(amount) AS total FROM expl_parity_src GROUP BY region",
        "SELECT region, amount FROM expl_parity_src WHERE amount > 5"
      )
      cases.zipWithIndex.foreach { case (q, i) =>
        val explained = explain(s"EXPLAIN CREATE MATERIALIZED VIEW expl_probe_$i AS $q")
        sql(s"CREATE MATERIALIZED VIEW expl_real_$i AS $q")
        withClue(s"case $i ($q): ") {
          explained.getAs[Long]("refresh_type").toInt shouldBe mvRefreshType(s"expl_real_$i")
          explained.getAs[String]("refresh_type_name") shouldBe {
            val id = spark.sessionState.sqlParser.parseTableIdentifier(s"expl_real_$i")
            MvCatalog.lookup(spark, id).get.refreshTypeName
          }
        }
      }
    }

    it("resolves an upstream MV that was only EXPLAINed (cold DAG)") {
      sql("CREATE TABLE expl_cold_base (k INT, v INT) USING DELTA")
      sql("INSERT INTO expl_cold_base VALUES (1, 100), (2, 200)")

      // Upstream is only explained — never materialised — yet its schema is
      // registered so the downstream EXPLAIN can resolve it as a source.
      explain(
        "EXPLAIN CREATE MATERIALIZED VIEW expl_cold_up AS " +
          "SELECT k, SUM(v) AS s FROM expl_cold_base GROUP BY k"
      )
      mvExists("expl_cold_up") shouldBe false

      val down = explain("EXPLAIN CREATE MATERIALIZED VIEW expl_cold_down AS SELECT k FROM expl_cold_up")
      down.getAs[String]("view") shouldBe "expl_cold_down"
      down.getAs[Seq[String]]("source_tables").exists(_.endsWith("expl_cold_up")) shouldBe true
      mvExists("expl_cold_down") shouldBe false
    }

    it("exposes a single explain STRING output column") {
      sql("CREATE TABLE expl_schema_src (id INT) USING DELTA")
      val df = spark.sql("EXPLAIN CREATE MATERIALIZED VIEW expl_mv_schema AS SELECT id FROM expl_schema_src")
      df.schema.fieldNames shouldBe Array("explain")
    }
  }
}
