package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.{MvCatalog, StagingCatalog}

import java.io.File

/** End-to-end parity coverage for `<db>.<table>`-qualified source references
  * in the user-supplied view body. This pattern is the norm in dbt / Hive
  * metastore workloads: every `ref()` and `source()` macro expands to a
  * qualified table name, so the view body that lands in `originalQueryText`
  * looks like `SELECT ... FROM tpcdi.staging_trade s JOIN tpcdi.date d ON …`
  * rather than the bare `FROM staging_trade s JOIN date d …` that the rest of
  * the parity suite uses.
  *
  * Without the fix in `OpenIvmCompiler.stripDbQualifiers` (sends short names
  * to DuckDB during compile) and the active-qualified-names map in
  * `SparkRefreshRewriter.rewriteMemoryMainPrefix` (expands `memory.main.<short>`
  * to `` `<db>`.`<table>` `` on the Spark side), CREATE MATERIALIZED VIEW
  * fails with `Catalog Error: Table … does not exist because schema "<db>"
  * does not exist`, and REFRESH MATERIALIZED VIEW fails with
  * `[DELTA_TABLE_NOT_FOUND] Delta table default.<short> doesn't exist`.
  */
abstract class QualifiedSourceScenarios extends IvmParitySpecBase("qualified-source") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-QualifiedSourceSpec")
      .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension,org.openivm.spark.OpenIvmSparkExtensions"
      )
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.openivm.enabled", "true")
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
    MvCatalog.ensureTables(spark)
    StagingCatalog.ensureTables(spark)
    // Create a sibling database for ALL tables in this spec — that's the
    // qualification we want to exercise. The MV itself can live anywhere.
    sql("CREATE DATABASE IF NOT EXISTS qualified_src_db")
  }

  override def afterAll(): Unit =
    try {
      if (spark != null) {
        sql("DROP DATABASE IF EXISTS qualified_src_db CASCADE")
        spark.stop()
      }
      deleteDir(new File(warehouseDir))
    } finally {
      super.afterAll()
    }

  describe("(1) Single-source SIMPLE_AGGREGATE — COUNT(*) on qualified table") {
    it("CREATE + REFRESH succeed when the source is referenced as <db>.<table>") {
      sql(
        "CREATE TABLE IF NOT EXISTS qualified_src_db.evt_qs1(id INT) USING DELTA"
      )
      sql("INSERT INTO qualified_src_db.evt_qs1 VALUES (1), (2), (3)")
      sql(
        "CREATE MATERIALIZED VIEW mv_qs1 AS " +
          "SELECT COUNT(*) AS c FROM qualified_src_db.evt_qs1"
      )
      assertMvCorrect("mv_qs1", "SELECT COUNT(*) AS c FROM qualified_src_db.evt_qs1")
      sql("INSERT INTO qualified_src_db.evt_qs1 VALUES (4), (5)")
      refreshMv("mv_qs1")
      assertMvCorrect("mv_qs1", "SELECT COUNT(*) AS c FROM qualified_src_db.evt_qs1")
    }
  }

  describe("(2) Single-source AGGREGATE_GROUP — GROUP BY on qualified table") {
    it("CREATE + REFRESH preserve groups across INSERT/DELETE on a <db>.<table>") {
      sql(
        "CREATE TABLE IF NOT EXISTS qualified_src_db.sales_qs2(region STRING, amount INT) USING DELTA"
      )
      sql("INSERT INTO qualified_src_db.sales_qs2 VALUES ('east', 10), ('west', 20)")
      sql(
        "CREATE MATERIALIZED VIEW mv_qs2 AS " +
          "SELECT region, SUM(amount) AS total FROM qualified_src_db.sales_qs2 GROUP BY region"
      )
      assertMvCorrect(
        "mv_qs2",
        "SELECT region, SUM(amount) AS total FROM qualified_src_db.sales_qs2 GROUP BY region"
      )
      sql("INSERT INTO qualified_src_db.sales_qs2 VALUES ('east', 5), ('north', 30)")
      sql("DELETE FROM qualified_src_db.sales_qs2 WHERE region = 'west'")
      refreshMv("mv_qs2")
      assertMvCorrect(
        "mv_qs2",
        "SELECT region, SUM(amount) AS total FROM qualified_src_db.sales_qs2 GROUP BY region"
      )
    }
  }

  describe("(3) Two-source JOIN — both tables referenced as <db>.<table>") {
    it("CREATE + REFRESH succeed for an inner JOIN across two qualified tables") {
      sql(
        "CREATE TABLE IF NOT EXISTS qualified_src_db.dept_qs3(dept_id INT, dept_name STRING) USING DELTA"
      )
      sql(
        "CREATE TABLE IF NOT EXISTS qualified_src_db.emp_qs3(emp_id INT, dept_id INT, name STRING) USING DELTA"
      )
      sql("INSERT INTO qualified_src_db.dept_qs3 VALUES (1, 'eng'), (2, 'sales')")
      sql(
        "INSERT INTO qualified_src_db.emp_qs3 VALUES (10, 1, 'Alice'), (11, 1, 'Bob'), (12, 2, 'Carol')"
      )
      sql(
        "CREATE MATERIALIZED VIEW mv_qs3 AS " +
          "SELECT e.emp_id, e.name, d.dept_name " +
          "FROM qualified_src_db.emp_qs3 e " +
          "JOIN qualified_src_db.dept_qs3 d ON e.dept_id = d.dept_id"
      )
      val expected =
        "SELECT e.emp_id, e.name, d.dept_name " +
          "FROM qualified_src_db.emp_qs3 e " +
          "JOIN qualified_src_db.dept_qs3 d ON e.dept_id = d.dept_id"
      assertMvCorrect("mv_qs3", expected)
      sql(
        "INSERT INTO qualified_src_db.emp_qs3 VALUES (13, 2, 'Dave'), (14, 1, 'Erin')"
      )
      refreshMv("mv_qs3")
      assertMvCorrect("mv_qs3", expected)
    }
  }
}
