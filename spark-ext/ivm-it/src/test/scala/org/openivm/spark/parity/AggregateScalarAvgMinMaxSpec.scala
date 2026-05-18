package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** Split-off from `AggregateSpec.scala` so that each chunk of ~10 tests runs
  * in its own forked JVM (see `spark-ext/project/Settings.scala`). All table
  * and MV names are prefixed with `aggsmm_` to guarantee that parallel
  * specs cannot collide on a Delta warehouse path.
  */
class AggregateScalarAvgMinMaxSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-aggregate-scalar-avg-min-max-spec-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-AggregateScalarAvgMinMaxSpec")
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
  }

  override def afterAll(): Unit =
    try {
      if (spark != null) spark.stop()
      deleteDir(new File(warehouseDir))
    } finally {
      super.afterAll()
    }

  // ── Helpers ────────────────────────────────────────────────────────────────

  private def deleteDir(f: File): Unit = {
    if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(deleteDir))
    f.delete()
    ()
  }

  /** Project away hidden `openivm_*` bookkeeping columns from the MV, then
    * perform a bidirectional EXCEPT ALL equivalence check against the
    * expected SQL expression. ARRAY columns must be passed in `arrayCols`
    * so they are JSON-serialised before the comparison.
    */
  private def assertMvCorrect(
      mvName: String,
      expectedSql: String,
      arrayCols: Set[String] = Set.empty
  ): Unit = {
    val expected: DataFrame = spark.sql(expectedSql)
    val userCols            = expected.columns.toSeq

    def project(df: DataFrame): DataFrame = {
      val exprs = userCols.map { c =>
        if (arrayCols.contains(c)) s"to_json(`$c`) AS `$c`"
        else s"`$c`"
      }
      df.selectExpr(exprs: _*)
    }

    val expectedProj = project(expected)
    val mv           = spark.table(mvName).select(userCols.head, userCols.tail: _*)
    val mvProj       = project(mv)

    withClue(s"$mvName EXCEPT ALL <expected> (MV has extra rows): ") {
      mvProj.exceptAll(expectedProj).count() shouldBe 0L
    }
    withClue(s"<expected> EXCEPT ALL $mvName (MV missing rows): ") {
      expectedProj.exceptAll(mvProj).count() shouldBe 0L
    }
  }

  private def refreshMv(name: String): Unit =
    spark.sql(s"REFRESH MATERIALIZED VIEW $name").collect()

  // openivm test/sql/aggregate.test §UNGROUPED (SCALAR) AGGREGATES
  describe("ungrouped scalar aggregate — aggsmm_scores(SUM(value), COUNT(label))") {
    it("scalar SUM + COUNT is maintained across the full DML matrix") {
      spark.sql("CREATE TABLE aggsmm_scores (id INT, value INT, label STRING) USING DELTA")
      spark.sql("INSERT INTO aggsmm_scores VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c')")
      val viewBody = "SELECT SUM(value) AS total, COUNT(label) AS cnt FROM aggsmm_scores"
      spark.sql(s"CREATE MATERIALIZED VIEW aggsmm_score_totals AS $viewBody")
      assertMvCorrect("aggsmm_score_totals", viewBody)

      spark.sql("INSERT INTO aggsmm_scores VALUES (4, 40, 'd')")
      refreshMv("aggsmm_score_totals")
      assertMvCorrect("aggsmm_score_totals", viewBody)

      spark.sql("DELETE FROM aggsmm_scores WHERE id = 1")
      refreshMv("aggsmm_score_totals")
      assertMvCorrect("aggsmm_score_totals", viewBody)

      // Mixed insert + delete in same cycle
      spark.sql("INSERT INTO aggsmm_scores VALUES (5, 50, 'e')")
      spark.sql("DELETE FROM aggsmm_scores WHERE id = 2")
      refreshMv("aggsmm_score_totals")
      assertMvCorrect("aggsmm_score_totals", viewBody)

      // No-op
      refreshMv("aggsmm_score_totals")
      assertMvCorrect("aggsmm_score_totals", viewBody)

      // Batch insert
      spark.sql("INSERT INTO aggsmm_scores VALUES (6, 10, 'f'), (7, 20, 'g'), (8, 30, 'h')")
      refreshMv("aggsmm_score_totals")
      assertMvCorrect("aggsmm_score_totals", viewBody)

      // Delete multiple rows at once
      spark.sql("DELETE FROM aggsmm_scores WHERE value <= 20")
      refreshMv("aggsmm_score_totals")
      assertMvCorrect("aggsmm_score_totals", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §MIN with GROUP BY (Test 1)
  describe("MIN with GROUP BY — aggsmm_t1") {
    it("MIN is recomputed correctly across INSERT and DELETE") {
      spark.sql("CREATE TABLE aggsmm_t1 (id INT, grp STRING, val INT) USING DELTA")
      spark.sql("INSERT INTO aggsmm_t1 VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 30), (4, 'b', 5)")
      val viewBody = "SELECT grp, MIN(val) AS min_val FROM aggsmm_t1 GROUP BY grp"
      spark.sql(s"CREATE MATERIALIZED VIEW aggsmm_mv_min AS $viewBody")

      spark.sql("INSERT INTO aggsmm_t1 VALUES (5, 'a', 3)")
      refreshMv("aggsmm_mv_min")
      assertMvCorrect("aggsmm_mv_min", viewBody)

      spark.sql("DELETE FROM aggsmm_t1 WHERE id = 5")
      refreshMv("aggsmm_mv_min")
      assertMvCorrect("aggsmm_mv_min", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §MAX with GROUP BY (Test 2)
  describe("MAX with GROUP BY — aggsmm_t2") {
    it("MAX is recomputed correctly across INSERT and DELETE") {
      spark.sql("CREATE TABLE aggsmm_t2 (id INT, grp STRING, val INT) USING DELTA")
      spark.sql("INSERT INTO aggsmm_t2 VALUES (1, 'x', 100), (2, 'x', 200), (3, 'y', 50), (4, 'y', 75)")
      val viewBody = "SELECT grp, MAX(val) AS max_val FROM aggsmm_t2 GROUP BY grp"
      spark.sql(s"CREATE MATERIALIZED VIEW aggsmm_mv_max AS $viewBody")

      spark.sql("INSERT INTO aggsmm_t2 VALUES (5, 'y', 300)")
      refreshMv("aggsmm_mv_max")
      assertMvCorrect("aggsmm_mv_max", viewBody)

      spark.sql("DELETE FROM aggsmm_t2 WHERE id = 5")
      refreshMv("aggsmm_mv_max")
      assertMvCorrect("aggsmm_mv_max", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §Mixed aggregates SUM + MIN + MAX (Test 3)
  describe("Mixed aggregates SUM + MIN + MAX — aggsmm_t3") {
    it("SUM/MIN/MAX in the same view all stay in sync") {
      spark.sql("CREATE TABLE aggsmm_t3 (id INT, grp STRING, a INT, b INT, c INT) USING DELTA")
      spark.sql(
        "INSERT INTO aggsmm_t3 VALUES (1, 'g1', 10, 100, 1), (2, 'g1', 20, 200, 2), (3, 'g2', 30, 300, 3)"
      )
      val viewBody =
        "SELECT grp, SUM(a) AS sum_a, MIN(b) AS min_b, MAX(c) AS max_c FROM aggsmm_t3 GROUP BY grp"
      spark.sql(s"CREATE MATERIALIZED VIEW aggsmm_mv_mixed AS $viewBody")

      spark.sql("INSERT INTO aggsmm_t3 VALUES (4, 'g1', 5, 50, 10)")
      refreshMv("aggsmm_mv_mixed")
      assertMvCorrect("aggsmm_mv_mixed", viewBody)

      spark.sql("DELETE FROM aggsmm_t3 WHERE id = 4")
      refreshMv("aggsmm_mv_mixed")
      assertMvCorrect("aggsmm_mv_mixed", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §Ungrouped MIN (Test 5)
  describe("Ungrouped MIN — aggsmm_t5") {
    it("scalar MIN tracks INSERT/DELETE correctly") {
      spark.sql("CREATE TABLE aggsmm_t5 (id INT, val INT) USING DELTA")
      spark.sql("INSERT INTO aggsmm_t5 VALUES (1, 100), (2, 50), (3, 200)")
      val viewBody = "SELECT MIN(val) AS min_val FROM aggsmm_t5"
      spark.sql(s"CREATE MATERIALIZED VIEW aggsmm_mv_simple_min AS $viewBody")

      spark.sql("INSERT INTO aggsmm_t5 VALUES (4, 10)")
      refreshMv("aggsmm_mv_simple_min")
      assertMvCorrect("aggsmm_mv_simple_min", viewBody)

      spark.sql("DELETE FROM aggsmm_t5 WHERE id = 4")
      refreshMv("aggsmm_mv_simple_min")
      assertMvCorrect("aggsmm_mv_simple_min", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §MIN/MAX with JOIN + GROUP BY (Test 6)
  describe("MIN/MAX with JOIN + GROUP BY — aggsmm_departments JOIN aggsmm_employees") {
    it("MIN+MAX over joined rows handle INSERTs to both sides correctly") {
      spark.sql("CREATE TABLE aggsmm_departments (dept_id INT, dept_name STRING) USING DELTA")
      spark.sql("INSERT INTO aggsmm_departments VALUES (1, 'engineering'), (2, 'aggsmm_sales')")
      spark.sql("CREATE TABLE aggsmm_employees (id INT, dept_id INT, salary INT) USING DELTA")
      spark.sql("INSERT INTO aggsmm_employees VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150), (4, 2, 300)")

      val viewBody =
        "SELECT d.dept_name, MIN(e.salary) AS min_salary, MAX(e.salary) AS max_salary " +
          "FROM aggsmm_departments d INNER JOIN aggsmm_employees e ON e.dept_id = d.dept_id " +
          "GROUP BY d.dept_name"
      spark.sql(s"CREATE MATERIALIZED VIEW aggsmm_mv_dept_salaries AS $viewBody")

      spark.sql("INSERT INTO aggsmm_employees VALUES (5, 1, 50)")
      refreshMv("aggsmm_mv_dept_salaries")
      assertMvCorrect("aggsmm_mv_dept_salaries", viewBody)

      spark.sql("DELETE FROM aggsmm_employees WHERE id = 5")
      refreshMv("aggsmm_mv_dept_salaries")
      assertMvCorrect("aggsmm_mv_dept_salaries", viewBody)

      spark.sql("INSERT INTO aggsmm_departments VALUES (3, 'marketing')")
      spark.sql("INSERT INTO aggsmm_employees VALUES (6, 3, 75)")
      refreshMv("aggsmm_mv_dept_salaries")
      assertMvCorrect("aggsmm_mv_dept_salaries", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §Insert-only MIN/MAX with NULL TIMESTAMP transitions
  describe("Insert-only MIN/MAX with NULL TIMESTAMP — aggsmm_agg_minmax_null_insert") {
    it("MIN/MAX skip NULL inputs and pick up the non-NULL value") {
      spark.sql(
        "CREATE TABLE aggsmm_agg_minmax_null_insert (grp STRING, placed TIMESTAMP, removed TIMESTAMP) USING DELTA"
      )
      spark.sql(
        "INSERT INTO aggsmm_agg_minmax_null_insert VALUES ('a', TIMESTAMP'2026-01-01 10:00:00', NULL)"
      )
      val viewBody =
        "SELECT grp, MIN(placed) AS placed_at, MAX(removed) AS removed_at " +
          "FROM aggsmm_agg_minmax_null_insert GROUP BY grp"
      spark.sql(s"CREATE MATERIALIZED VIEW aggsmm_mv_agg_minmax_null_insert AS $viewBody")

      spark.sql(
        "INSERT INTO aggsmm_agg_minmax_null_insert VALUES ('a', NULL, TIMESTAMP'2026-01-02 11:00:00')"
      )
      refreshMv("aggsmm_mv_agg_minmax_null_insert")
      assertMvCorrect("aggsmm_mv_agg_minmax_null_insert", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §AVG aggregate
  describe("AVG aggregate — aggsmm_avg_data") {
    it("AVG tracks INSERTs (existing + new groups) and DELETEs correctly") {
      spark.sql("CREATE TABLE aggsmm_avg_data (grp STRING, val DOUBLE) USING DELTA")
      spark.sql("INSERT INTO aggsmm_avg_data VALUES ('a', 10), ('a', 20), ('b', 30)")
      val viewBody = "SELECT grp, AVG(val) AS avg_val FROM aggsmm_avg_data GROUP BY grp"
      spark.sql(s"CREATE MATERIALIZED VIEW aggsmm_mv_avg AS $viewBody")
      assertMvCorrect("aggsmm_mv_avg", viewBody)

      spark.sql("INSERT INTO aggsmm_avg_data VALUES ('a', 40), ('c', 100)")
      refreshMv("aggsmm_mv_avg")
      assertMvCorrect("aggsmm_mv_avg", viewBody)

      spark.sql("DELETE FROM aggsmm_avg_data WHERE grp = 'a' AND val = 10")
      refreshMv("aggsmm_mv_avg")
      assertMvCorrect("aggsmm_mv_avg", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §Ungrouped AVG
  describe("Ungrouped AVG — aggsmm_avg_ungrouped") {
    it("scalar AVG is maintained across INSERT") {
      spark.sql("CREATE TABLE aggsmm_avg_ungrouped (val INT) USING DELTA")
      spark.sql("INSERT INTO aggsmm_avg_ungrouped VALUES (10), (20), (30)")
      val viewBody = "SELECT AVG(val) AS avg_val FROM aggsmm_avg_ungrouped"
      spark.sql(s"CREATE MATERIALIZED VIEW aggsmm_mv_avg_scalar AS $viewBody")
      assertMvCorrect("aggsmm_mv_avg_scalar", viewBody)

      spark.sql("INSERT INTO aggsmm_avg_ungrouped VALUES (40)")
      refreshMv("aggsmm_mv_avg_scalar")
      assertMvCorrect("aggsmm_mv_avg_scalar", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §AVG without explicit alias
  // TODO(P6f-AggregateSpec): `SELECT AVG(val)` without an alias produces a Spark
  // column named "avg(val)" that the openivm-spark refresh rewrite cannot resolve
  // (it expects the renamed `openivm_sum_avg_val` / `openivm_count_avg_val`
  // hidden columns to map back to the user alias). openivm fixes this via a
  // CREATE-time alias rewrite (parser.cpp:354-357 RewriteDerivedAggregates) which
  // is not yet propagated to the Spark side. Re-enable when the rewriter
  // canonicalises unaliased aggregate column names.
  describe("AVG without explicit alias — aggsmm_avg_noalias") {
    ignore("MV created from `SELECT AVG(val)` (no alias) survives INSERT and DELETE") {
      spark.sql("CREATE TABLE aggsmm_avg_noalias (val DECIMAL(10,2)) USING DELTA")
      spark.sql("INSERT INTO aggsmm_avg_noalias VALUES (10.0), (20.0), (30.0)")
      val viewBody = "SELECT AVG(val) FROM aggsmm_avg_noalias"
      spark.sql(s"CREATE MATERIALIZED VIEW aggsmm_mv_avg_noalias AS $viewBody")
      assertMvCorrect("aggsmm_mv_avg_noalias", viewBody)

      spark.sql("INSERT INTO aggsmm_avg_noalias VALUES (40.0)")
      refreshMv("aggsmm_mv_avg_noalias")
      assertMvCorrect("aggsmm_mv_avg_noalias", viewBody)

      spark.sql("DELETE FROM aggsmm_avg_noalias WHERE val = 10.0")
      refreshMv("aggsmm_mv_avg_noalias")
      assertMvCorrect("aggsmm_mv_avg_noalias", viewBody)
    }
  }

}
