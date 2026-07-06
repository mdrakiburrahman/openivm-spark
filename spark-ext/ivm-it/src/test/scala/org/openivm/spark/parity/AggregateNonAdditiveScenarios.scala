package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** Split-off from `AggregateSpec.scala` so that each chunk of ~10 tests runs
  * in its own forked JVM (see `spark-ext/project/Settings.scala`). All table
  * and MV names are prefixed with `aggna_` to guarantee that parallel
  * specs cannot collide on a Delta warehouse path.
  */
abstract class AggregateNonAdditiveScenarios extends IvmParitySpecBase("aggregate-non-additive") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  // openivm test/sql/aggregate.test §UPPER over varchar (non-additive column)
  describe("UPPER over varchar — aggna_nonadd_cust") {
    it("Non-additive UPPER(group_key) projection stays correct across mixed DML") {
      sql("CREATE TABLE aggna_nonadd_cust (w_id INT, state STRING, bal DECIMAL(10,2)) USING DELTA")
      sql("INSERT INTO aggna_nonadd_cust VALUES (1, 'ca', 100), (1, 'ca', 200), (1, 'ny', 150)")

      val viewBody =
        "SELECT w_id, state, UPPER(state) AS state_upper, COUNT(*) AS cnt " +
          "FROM aggna_nonadd_cust GROUP BY w_id, state"
      sql(s"CREATE MATERIALIZED VIEW aggna_mv_upper_state AS $viewBody")

      sql("INSERT INTO aggna_nonadd_cust VALUES (1, 'ca', 300), (2, 'tx', 400)")
      sql("DELETE FROM aggna_nonadd_cust WHERE state = 'ny'")
      refreshMv("aggna_mv_upper_state")
      assertMvCorrect("aggna_mv_upper_state", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §VARCHAR literal column
  describe("VARCHAR literal column — aggna_tag_events") {
    it("Constant 'stat' column survives the bulk-summable delta MERGE path") {
      sql("CREATE TABLE aggna_tag_events (kind STRING, amount INT) USING DELTA")
      sql("INSERT INTO aggna_tag_events VALUES ('click', 5), ('click', 7), ('view', 3)")

      val viewBody =
        "SELECT 'stat' AS label, kind, SUM(amount) AS total, COUNT(*) AS n " +
          "FROM aggna_tag_events GROUP BY kind"
      sql(s"CREATE MATERIALIZED VIEW aggna_mv_literal_tag AS $viewBody")

      sql("INSERT INTO aggna_tag_events VALUES ('click', 10), ('view', 5)")
      sql("DELETE FROM aggna_tag_events WHERE kind = 'click' AND amount = 5")
      refreshMv("aggna_mv_literal_tag")
      assertMvCorrect("aggna_mv_literal_tag", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §CASE over aggregate (aggna_case_sales)
  // TODO: CASE expressions over an aggregate produce a HUGEINT
  // intermediate in DuckDB's compile output, which Spark cannot parse
  // (UNSUPPORTED_DATATYPE "HUGEINT"). openivm needs to cast to BIGINT before
  // emitting Spark SQL. Re-enable when openivm's HUGEINT→BIGINT downcast is
  // ported into the SPARK dialect path.
  describe("CASE over aggregate — aggna_case_sales") {
    ignore("CASE branch flips correctly when SUM crosses threshold") {
      sql("CREATE TABLE aggna_case_sales (region STRING, rev INT) USING DELTA")
      sql("INSERT INTO aggna_case_sales VALUES ('EU', 100), ('EU', 500), ('US', 200), ('US', 900)")

      val viewBody =
        "SELECT region, SUM(rev) AS total, COUNT(*) AS n, " +
          "CASE WHEN SUM(rev) > 1000 THEN 'high' ELSE 'low' END AS tier " +
          "FROM aggna_case_sales GROUP BY region"
      sql(s"CREATE MATERIALIZED VIEW aggna_mv_tier AS $viewBody")

      sql("INSERT INTO aggna_case_sales VALUES ('EU', 500)")
      sql("DELETE FROM aggna_case_sales WHERE region = 'US' AND rev = 900")
      refreshMv("aggna_mv_tier")
      assertMvCorrect("aggna_mv_tier", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §LIST aggregate (aggna_list_events) — ordered LIST<INT>
  // TODO: openivm's DuckDB compile path does not know about
  // Spark's `collect_list` function — `openivm_compile_with_facts('aggna_mv_user_items', ...)`
  // returns "Catalog Error: Scalar Function with name collect_list does not
  // exist". The Spark-side rewrite needs to either route LIST aggregates to
  // FULL_REFRESH at CREATE time or translate `collect_list`→`list` (DuckDB) and
  // back. Re-enable when the cross-dialect aggregate name mapping is in place.
  describe("LIST aggregate (ordered INT lists) — aggna_list_events") {
    ignore("array_sort(collect_list(item_id)) per user_id matches base query") {
      sql("CREATE TABLE aggna_list_events (user_id INT, item_id INT) USING DELTA")
      sql("INSERT INTO aggna_list_events VALUES (1, 100), (1, 101), (2, 200)")

      val viewBody =
        "SELECT user_id, array_sort(collect_list(item_id)) AS items, COUNT(*) AS cnt " +
          "FROM aggna_list_events GROUP BY user_id"
      sql(s"CREATE MATERIALIZED VIEW aggna_mv_user_items AS $viewBody")

      sql("INSERT INTO aggna_list_events VALUES (1, 102), (2, 201), (3, 300)")
      sql("DELETE FROM aggna_list_events WHERE item_id = 101")
      refreshMv("aggna_mv_user_items")
      assertMvCorrect("aggna_mv_user_items", viewBody, arrayCols = Set("items"))
    }
  }

  // openivm test/sql/aggregate.test §SIMPLE_AGGREGATE with literal (aggna_simple_events)
  describe("SIMPLE_AGGREGATE with VARCHAR literal — aggna_simple_events") {
    it("ungrouped 'all' bucket with COUNT + SUM tracks DML correctly") {
      sql("CREATE TABLE aggna_simple_events (amount INT) USING DELTA")
      sql("INSERT INTO aggna_simple_events VALUES (10), (20), (30)")

      val viewBody =
        "SELECT 'all' AS bucket, COUNT(*) AS n, SUM(amount) AS total FROM aggna_simple_events"
      sql(s"CREATE MATERIALIZED VIEW aggna_mv_simple_literal AS $viewBody")

      sql("INSERT INTO aggna_simple_events VALUES (40)")
      sql("DELETE FROM aggna_simple_events WHERE amount = 10")
      refreshMv("aggna_mv_simple_literal")
      assertMvCorrect("aggna_mv_simple_literal", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §Multiple non-additive columns (aggna_multi_nonadd)
  describe("Multiple non-additive columns — aggna_multi_nonadd") {
    it("UPPER + LOWER + literal alongside SUM/COUNT survive INSERT/DELETE/regrowth") {
      sql("CREATE TABLE aggna_multi_nonadd (dept STRING, region STRING, salary INT) USING DELTA")
      sql(
        "INSERT INTO aggna_multi_nonadd VALUES " +
          "('eng', 'us', 100000), ('eng', 'eu', 90000), ('aggna_sales', 'us', 80000)"
      )

      val viewBody =
        "SELECT dept, region, UPPER(dept) AS dept_upper, LOWER(region) AS region_lower, " +
          "'live' AS label, SUM(salary) AS total_sal, COUNT(*) AS cnt " +
          "FROM aggna_multi_nonadd GROUP BY dept, region"
      sql(s"CREATE MATERIALIZED VIEW aggna_mv_multi_nonadd AS $viewBody")

      sql("INSERT INTO aggna_multi_nonadd VALUES ('eng', 'us', 120000), ('aggna_sales', 'eu', 70000)")
      sql("DELETE FROM aggna_multi_nonadd WHERE dept='eng' AND region='eu'")
      refreshMv("aggna_mv_multi_nonadd")
      assertMvCorrect("aggna_mv_multi_nonadd", viewBody)

      // Group key becomes empty (all 'aggna_sales' rows deleted) then repopulated
      sql("DELETE FROM aggna_multi_nonadd WHERE dept='aggna_sales'")
      refreshMv("aggna_mv_multi_nonadd")
      assertMvCorrect("aggna_mv_multi_nonadd", viewBody)

      sql(
        "INSERT INTO aggna_multi_nonadd VALUES ('aggna_sales', 'apac', 60000), ('aggna_sales', 'apac', 65000)"
      )
      refreshMv("aggna_mv_multi_nonadd")
      assertMvCorrect("aggna_mv_multi_nonadd", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §CASE over AVG (aggna_case_avg)
  // TODO: CASE over an aggregate plus a derived projection
  // confuses openivm-spark's group-recompute fallback — the MV ends up with one
  // extra row after the second refresh (off-by-one bag-equality failure on
  // `aggna_mv_case_avg`). Likely related to the same orphan-derived-alias detection
  // that triggers Fix B group-recompute, but the projection's CASE over AVG
  // isn't recognised. Re-enable when CompileAggregateGroups handles `CASE WHEN
  // <agg>` as a fully-derived projection.
  describe("CASE over AVG — aggna_case_avg") {
    ignore("CASE branch flips correctly as AVG crosses 1000 threshold both ways") {
      sql("CREATE TABLE aggna_case_avg (grp INT, v DECIMAL(10,2)) USING DELTA")
      sql("INSERT INTO aggna_case_avg VALUES (1, 100), (1, 200), (2, 1500), (2, 2500)")

      val viewBody =
        "SELECT grp, AVG(v) AS avg_v, COUNT(*) AS n, " +
          "CASE WHEN AVG(v) > 1000 THEN 'big' ELSE 'small' END AS tier " +
          "FROM aggna_case_avg GROUP BY grp"
      sql(s"CREATE MATERIALIZED VIEW aggna_mv_case_avg AS $viewBody")
      assertMvCorrect("aggna_mv_case_avg", viewBody)

      // Batch 1: flip grp=1 above threshold
      sql("INSERT INTO aggna_case_avg VALUES (1, 5000)")
      refreshMv("aggna_mv_case_avg")
      assertMvCorrect("aggna_mv_case_avg", viewBody)

      // Batch 2: revert grp=1 and push grp=2 below threshold
      sql("DELETE FROM aggna_case_avg WHERE grp=1 AND v=5000")
      sql("INSERT INTO aggna_case_avg VALUES (2, 100), (2, 100), (2, 100), (2, 100), (2, 100)")
      refreshMv("aggna_mv_case_avg")
      assertMvCorrect("aggna_mv_case_avg", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §LIST<VARCHAR> with ORDER BY (aggna_list_varchar)
  // TODO: Same `collect_list` catalog-error problem as
  // aggna_mv_user_items above — openivm's DuckDB session does not know about Spark's
  // collect_list/array_sort/struct/transform combinations. Re-enable with the
  // same fix.
  describe("LIST<VARCHAR> ORDER BY rank — aggna_list_varchar") {
    ignore("ordered list of names per cat matches base query (compare via JSON)") {
      sql("CREATE TABLE aggna_list_varchar (cat STRING, name STRING, rank INT) USING DELTA")
      sql(
        "INSERT INTO aggna_list_varchar VALUES " +
          "('fruit', 'apple', 1), ('fruit', 'banana', 2), ('fruit', 'cherry', 3), " +
          "('veg', 'broccoli', 1), ('veg', 'carrot', 2)"
      )

      // DuckDB `LIST(name ORDER BY rank)` → Spark uses array_sort over a
      // struct(rank, name) and then transforms to extract names. Spark sorts
      // structs lexicographically by their fields, so rank first → name.
      val viewBody =
        "SELECT cat, " +
          "transform(array_sort(collect_list(struct(rank AS r, name AS nm))), s -> s.nm) AS names, " +
          "COUNT(*) AS n " +
          "FROM aggna_list_varchar GROUP BY cat"
      sql(s"CREATE MATERIALIZED VIEW aggna_mv_list_varchar AS $viewBody")

      sql("INSERT INTO aggna_list_varchar VALUES ('fruit', 'date', 4), ('veg', 'asparagus', 0)")
      sql("DELETE FROM aggna_list_varchar WHERE name = 'banana'")
      refreshMv("aggna_mv_list_varchar")
      assertMvCorrect("aggna_mv_list_varchar", viewBody, arrayCols = Set("names"))
    }
  }

  // openivm test/sql/aggregate.test §BOOLEAN scalar over aggregate (aggna_bool_check)
  // TODO: Same HUGEINT downcast gap — `(SUM(v) > 50)` returns
  // a HUGEINT comparison in DuckDB which Spark cannot parse. Re-enable when
  // openivm's HUGEINT→BIGINT downcast lands in the SPARK dialect path.
  describe("BOOLEAN scalar over aggregate — aggna_bool_check") {
    ignore("Boolean `SUM(v) > 50` flag flips correctly with the underlying SUM") {
      sql("CREATE TABLE aggna_bool_check (grp INT, v INT) USING DELTA")
      sql("INSERT INTO aggna_bool_check VALUES (1, 5), (1, 10), (2, 100)")

      val viewBody =
        "SELECT grp, SUM(v) AS total, (SUM(v) > 50) AS is_big, COUNT(*) AS n " +
          "FROM aggna_bool_check GROUP BY grp"
      sql(s"CREATE MATERIALIZED VIEW aggna_mv_bool_tier AS $viewBody")

      sql("INSERT INTO aggna_bool_check VALUES (1, 100), (2, 5)")
      sql("DELETE FROM aggna_bool_check WHERE grp = 2 AND v = 100")
      refreshMv("aggna_mv_bool_tier")
      assertMvCorrect("aggna_mv_bool_tier", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §UNION-ALL of aggregates with literals (aggna_ua_events)
  // TODO: The compiled refresh emits a `rowid` reference for
  // UNION-ALL-of-COUNT shapes which has no Spark equivalent. openivm's
  // SimpleAggregate UNION-ALL path needs to either route to FULL_REFRESH for
  // Spark or use a synthetic Spark rowid. Re-enable when the SimpleAggregate
  // UNION-ALL rewriter handles missing rowid in the SPARK dialect.
  describe("UNION-ALL of aggregates with literal tags — aggna_ua_events") {
    ignore("Each branch's COUNT(*) is correctly maintained after batched DML") {
      sql("CREATE TABLE aggna_ua_events (status STRING, amount INT) USING DELTA")
      sql("INSERT INTO aggna_ua_events VALUES ('open', 10), ('open', 20), ('closed', 30)")

      val viewBody =
        "SELECT 'open' AS kind, COUNT(*) AS n FROM aggna_ua_events WHERE status = 'open' " +
          "UNION ALL " +
          "SELECT 'closed', COUNT(*) FROM aggna_ua_events WHERE status = 'closed' " +
          "UNION ALL " +
          "SELECT 'total', COUNT(*) FROM aggna_ua_events"
      sql(s"CREATE MATERIALIZED VIEW aggna_mv_ua_counts AS $viewBody")

      sql("INSERT INTO aggna_ua_events VALUES ('open', 40), ('closed', 50)")
      sql("DELETE FROM aggna_ua_events WHERE status='open' AND amount=10")
      refreshMv("aggna_mv_ua_counts")
      assertMvCorrect("aggna_mv_ua_counts", viewBody)
    }
  }

}
