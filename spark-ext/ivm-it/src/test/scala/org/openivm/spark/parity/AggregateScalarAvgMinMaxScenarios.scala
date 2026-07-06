package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** Split-off from `AggregateSpec.scala` so that each chunk of ~10 tests runs
  * in its own forked JVM (see `spark-ext/project/Settings.scala`). All table
  * and MV names are prefixed with `aggsmm_` to guarantee that parallel
  * specs cannot collide on a Delta warehouse path.
  */
abstract class AggregateScalarAvgMinMaxScenarios extends IvmParitySpecBase("aggregate-scalar-avg-min-max") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  // openivm test/sql/aggregate.test §UNGROUPED (SCALAR) AGGREGATES
  describe("ungrouped scalar aggregate — aggsmm_scores(SUM(value), COUNT(label))") {
    it("scalar SUM + COUNT is maintained across the full DML matrix") {
      sql("CREATE TABLE aggsmm_scores (id INT, value INT, label STRING) USING DELTA")
      sql("INSERT INTO aggsmm_scores VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c')")
      val viewBody = "SELECT SUM(value) AS total, COUNT(label) AS cnt FROM aggsmm_scores"
      sql(s"CREATE MATERIALIZED VIEW aggsmm_score_totals AS $viewBody")
      assertMvCorrect("aggsmm_score_totals", viewBody)

      sql("INSERT INTO aggsmm_scores VALUES (4, 40, 'd')")
      refreshMv("aggsmm_score_totals")
      assertMvCorrect("aggsmm_score_totals", viewBody)

      sql("DELETE FROM aggsmm_scores WHERE id = 1")
      refreshMv("aggsmm_score_totals")
      assertMvCorrect("aggsmm_score_totals", viewBody)

      // Mixed insert + delete in same cycle
      sql("INSERT INTO aggsmm_scores VALUES (5, 50, 'e')")
      sql("DELETE FROM aggsmm_scores WHERE id = 2")
      refreshMv("aggsmm_score_totals")
      assertMvCorrect("aggsmm_score_totals", viewBody)

      // No-op
      refreshMv("aggsmm_score_totals")
      assertMvCorrect("aggsmm_score_totals", viewBody)

      // Batch insert
      sql("INSERT INTO aggsmm_scores VALUES (6, 10, 'f'), (7, 20, 'g'), (8, 30, 'h')")
      refreshMv("aggsmm_score_totals")
      assertMvCorrect("aggsmm_score_totals", viewBody)

      // Delete multiple rows at once
      sql("DELETE FROM aggsmm_scores WHERE value <= 20")
      refreshMv("aggsmm_score_totals")
      assertMvCorrect("aggsmm_score_totals", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §MIN with GROUP BY (Test 1)
  describe("MIN with GROUP BY — aggsmm_t1") {
    it("MIN is recomputed correctly across INSERT and DELETE") {
      sql("CREATE TABLE aggsmm_t1 (id INT, grp STRING, val INT) USING DELTA")
      sql("INSERT INTO aggsmm_t1 VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 30), (4, 'b', 5)")
      val viewBody = "SELECT grp, MIN(val) AS min_val FROM aggsmm_t1 GROUP BY grp"
      sql(s"CREATE MATERIALIZED VIEW aggsmm_mv_min AS $viewBody")

      sql("INSERT INTO aggsmm_t1 VALUES (5, 'a', 3)")
      refreshMv("aggsmm_mv_min")
      assertMvCorrect("aggsmm_mv_min", viewBody)

      sql("DELETE FROM aggsmm_t1 WHERE id = 5")
      refreshMv("aggsmm_mv_min")
      assertMvCorrect("aggsmm_mv_min", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §MAX with GROUP BY (Test 2)
  describe("MAX with GROUP BY — aggsmm_t2") {
    it("MAX is recomputed correctly across INSERT and DELETE") {
      sql("CREATE TABLE aggsmm_t2 (id INT, grp STRING, val INT) USING DELTA")
      sql("INSERT INTO aggsmm_t2 VALUES (1, 'x', 100), (2, 'x', 200), (3, 'y', 50), (4, 'y', 75)")
      val viewBody = "SELECT grp, MAX(val) AS max_val FROM aggsmm_t2 GROUP BY grp"
      sql(s"CREATE MATERIALIZED VIEW aggsmm_mv_max AS $viewBody")

      sql("INSERT INTO aggsmm_t2 VALUES (5, 'y', 300)")
      refreshMv("aggsmm_mv_max")
      assertMvCorrect("aggsmm_mv_max", viewBody)

      sql("DELETE FROM aggsmm_t2 WHERE id = 5")
      refreshMv("aggsmm_mv_max")
      assertMvCorrect("aggsmm_mv_max", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §Mixed aggregates SUM + MIN + MAX (Test 3)
  describe("Mixed aggregates SUM + MIN + MAX — aggsmm_t3") {
    it("SUM/MIN/MAX in the same view all stay in sync") {
      sql("CREATE TABLE aggsmm_t3 (id INT, grp STRING, a INT, b INT, c INT) USING DELTA")
      sql(
        "INSERT INTO aggsmm_t3 VALUES (1, 'g1', 10, 100, 1), (2, 'g1', 20, 200, 2), (3, 'g2', 30, 300, 3)"
      )
      val viewBody =
        "SELECT grp, SUM(a) AS sum_a, MIN(b) AS min_b, MAX(c) AS max_c FROM aggsmm_t3 GROUP BY grp"
      sql(s"CREATE MATERIALIZED VIEW aggsmm_mv_mixed AS $viewBody")

      sql("INSERT INTO aggsmm_t3 VALUES (4, 'g1', 5, 50, 10)")
      refreshMv("aggsmm_mv_mixed")
      assertMvCorrect("aggsmm_mv_mixed", viewBody)

      sql("DELETE FROM aggsmm_t3 WHERE id = 4")
      refreshMv("aggsmm_mv_mixed")
      assertMvCorrect("aggsmm_mv_mixed", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §Ungrouped MIN (Test 5)
  describe("Ungrouped MIN — aggsmm_t5") {
    it("scalar MIN tracks INSERT/DELETE correctly") {
      sql("CREATE TABLE aggsmm_t5 (id INT, val INT) USING DELTA")
      sql("INSERT INTO aggsmm_t5 VALUES (1, 100), (2, 50), (3, 200)")
      val viewBody = "SELECT MIN(val) AS min_val FROM aggsmm_t5"
      sql(s"CREATE MATERIALIZED VIEW aggsmm_mv_simple_min AS $viewBody")

      sql("INSERT INTO aggsmm_t5 VALUES (4, 10)")
      refreshMv("aggsmm_mv_simple_min")
      assertMvCorrect("aggsmm_mv_simple_min", viewBody)

      sql("DELETE FROM aggsmm_t5 WHERE id = 4")
      refreshMv("aggsmm_mv_simple_min")
      assertMvCorrect("aggsmm_mv_simple_min", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §MIN/MAX with JOIN + GROUP BY (Test 6)
  describe("MIN/MAX with JOIN + GROUP BY — aggsmm_departments JOIN aggsmm_employees") {
    it("MIN+MAX over joined rows handle INSERTs to both sides correctly") {
      sql("CREATE TABLE aggsmm_departments (dept_id INT, dept_name STRING) USING DELTA")
      sql("INSERT INTO aggsmm_departments VALUES (1, 'engineering'), (2, 'aggsmm_sales')")
      sql("CREATE TABLE aggsmm_employees (id INT, dept_id INT, salary INT) USING DELTA")
      sql("INSERT INTO aggsmm_employees VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150), (4, 2, 300)")

      val viewBody =
        "SELECT d.dept_name, MIN(e.salary) AS min_salary, MAX(e.salary) AS max_salary " +
          "FROM aggsmm_departments d INNER JOIN aggsmm_employees e ON e.dept_id = d.dept_id " +
          "GROUP BY d.dept_name"
      sql(s"CREATE MATERIALIZED VIEW aggsmm_mv_dept_salaries AS $viewBody")

      sql("INSERT INTO aggsmm_employees VALUES (5, 1, 50)")
      refreshMv("aggsmm_mv_dept_salaries")
      assertMvCorrect("aggsmm_mv_dept_salaries", viewBody)

      sql("DELETE FROM aggsmm_employees WHERE id = 5")
      refreshMv("aggsmm_mv_dept_salaries")
      assertMvCorrect("aggsmm_mv_dept_salaries", viewBody)

      sql("INSERT INTO aggsmm_departments VALUES (3, 'marketing')")
      sql("INSERT INTO aggsmm_employees VALUES (6, 3, 75)")
      refreshMv("aggsmm_mv_dept_salaries")
      assertMvCorrect("aggsmm_mv_dept_salaries", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §Insert-only MIN/MAX with NULL TIMESTAMP transitions
  describe("Insert-only MIN/MAX with NULL TIMESTAMP — aggsmm_agg_minmax_null_insert") {
    it("MIN/MAX skip NULL inputs and pick up the non-NULL value") {
      sql(
        "CREATE TABLE aggsmm_agg_minmax_null_insert (grp STRING, placed TIMESTAMP, removed TIMESTAMP) USING DELTA"
      )
      sql(
        "INSERT INTO aggsmm_agg_minmax_null_insert VALUES ('a', TIMESTAMP'2026-01-01 10:00:00', NULL)"
      )
      val viewBody =
        "SELECT grp, MIN(placed) AS placed_at, MAX(removed) AS removed_at " +
          "FROM aggsmm_agg_minmax_null_insert GROUP BY grp"
      sql(s"CREATE MATERIALIZED VIEW aggsmm_mv_agg_minmax_null_insert AS $viewBody")

      sql(
        "INSERT INTO aggsmm_agg_minmax_null_insert VALUES ('a', NULL, TIMESTAMP'2026-01-02 11:00:00')"
      )
      refreshMv("aggsmm_mv_agg_minmax_null_insert")
      assertMvCorrect("aggsmm_mv_agg_minmax_null_insert", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §AVG aggregate
  describe("AVG aggregate — aggsmm_avg_data") {
    it("AVG tracks INSERTs (existing + new groups) and DELETEs correctly") {
      sql("CREATE TABLE aggsmm_avg_data (grp STRING, val DOUBLE) USING DELTA")
      sql("INSERT INTO aggsmm_avg_data VALUES ('a', 10), ('a', 20), ('b', 30)")
      val viewBody = "SELECT grp, AVG(val) AS avg_val FROM aggsmm_avg_data GROUP BY grp"
      sql(s"CREATE MATERIALIZED VIEW aggsmm_mv_avg AS $viewBody")
      assertMvCorrect("aggsmm_mv_avg", viewBody)

      sql("INSERT INTO aggsmm_avg_data VALUES ('a', 40), ('c', 100)")
      refreshMv("aggsmm_mv_avg")
      assertMvCorrect("aggsmm_mv_avg", viewBody)

      sql("DELETE FROM aggsmm_avg_data WHERE grp = 'a' AND val = 10")
      refreshMv("aggsmm_mv_avg")
      assertMvCorrect("aggsmm_mv_avg", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §Ungrouped AVG
  describe("Ungrouped AVG — aggsmm_avg_ungrouped") {
    it("scalar AVG is maintained across INSERT") {
      sql("CREATE TABLE aggsmm_avg_ungrouped (val INT) USING DELTA")
      sql("INSERT INTO aggsmm_avg_ungrouped VALUES (10), (20), (30)")
      val viewBody = "SELECT AVG(val) AS avg_val FROM aggsmm_avg_ungrouped"
      sql(s"CREATE MATERIALIZED VIEW aggsmm_mv_avg_scalar AS $viewBody")
      assertMvCorrect("aggsmm_mv_avg_scalar", viewBody)

      sql("INSERT INTO aggsmm_avg_ungrouped VALUES (40)")
      refreshMv("aggsmm_mv_avg_scalar")
      assertMvCorrect("aggsmm_mv_avg_scalar", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §AVG without explicit alias
  // TODO: `SELECT AVG(val)` without an alias produces a Spark
  // column named "avg(val)" that the openivm-spark refresh rewrite cannot resolve
  // (it expects the renamed `openivm_sum_avg_val` / `openivm_count_avg_val`
  // hidden columns to map back to the user alias). openivm fixes this via a
  // CREATE-time alias rewrite (parser.cpp:354-357 RewriteDerivedAggregates) which
  // is not yet propagated to the Spark side. Re-enable when the rewriter
  // canonicalises unaliased aggregate column names.
  describe("AVG without explicit alias — aggsmm_avg_noalias") {
    ignore("MV created from `SELECT AVG(val)` (no alias) survives INSERT and DELETE") {
      sql("CREATE TABLE aggsmm_avg_noalias (val DECIMAL(10,2)) USING DELTA")
      sql("INSERT INTO aggsmm_avg_noalias VALUES (10.0), (20.0), (30.0)")
      val viewBody = "SELECT AVG(val) FROM aggsmm_avg_noalias"
      sql(s"CREATE MATERIALIZED VIEW aggsmm_mv_avg_noalias AS $viewBody")
      assertMvCorrect("aggsmm_mv_avg_noalias", viewBody)

      sql("INSERT INTO aggsmm_avg_noalias VALUES (40.0)")
      refreshMv("aggsmm_mv_avg_noalias")
      assertMvCorrect("aggsmm_mv_avg_noalias", viewBody)

      sql("DELETE FROM aggsmm_avg_noalias WHERE val = 10.0")
      refreshMv("aggsmm_mv_avg_noalias")
      assertMvCorrect("aggsmm_mv_avg_noalias", viewBody)
    }
  }

}
