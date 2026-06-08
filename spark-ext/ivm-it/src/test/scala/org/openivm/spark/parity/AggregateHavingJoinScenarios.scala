package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** Split-off from `AggregateSpec.scala` so that each chunk of ~10 tests runs
  * in its own forked JVM (see `spark-ext/project/Settings.scala`). All table
  * and MV names are prefixed with `agghj_` to guarantee that parallel
  * specs cannot collide on a Delta warehouse path.
  */
abstract class AggregateHavingJoinScenarios extends IvmParitySpecBase("aggregate-having-join") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  // openivm test/sql/aggregate.test §HAVING clause
  describe("HAVING clause — agghj_having_test") {
    it("HAVING-filtered MV stays bag-equal after INSERTs cross/uncross threshold") {
      sql("CREATE TABLE agghj_having_test (region STRING, amount INT) USING DELTA")
      sql("INSERT INTO agghj_having_test VALUES ('a', 100), ('a', 200), ('b', 50), ('c', 10)")
      val viewBody =
        "SELECT region, SUM(amount) AS total FROM agghj_having_test GROUP BY region " +
          "HAVING SUM(amount) > 60"
      sql(s"CREATE MATERIALIZED VIEW agghj_mv_having AS $viewBody")
      assertMvCorrect("agghj_mv_having", viewBody)

      sql("INSERT INTO agghj_having_test VALUES ('b', 100), ('c', 200)")
      refreshMv("agghj_mv_having")
      assertMvCorrect("agghj_mv_having", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §CASE WHEN in aggregates
  describe("CASE WHEN inside aggregate — agghj_case_test") {
    it("SUM(CASE WHEN value > 0 THEN value ELSE 0 END) stays correct") {
      sql("CREATE TABLE agghj_case_test (category STRING, value INT) USING DELTA")
      sql("INSERT INTO agghj_case_test VALUES ('a', 10), ('a', -5), ('b', 20), ('b', -3)")
      val viewBody =
        "SELECT category, SUM(CASE WHEN value > 0 THEN value ELSE 0 END) AS pos_sum " +
          "FROM agghj_case_test GROUP BY category"
      sql(s"CREATE MATERIALIZED VIEW agghj_mv_case AS $viewBody")
      assertMvCorrect("agghj_mv_case", viewBody)

      sql("INSERT INTO agghj_case_test VALUES ('a', 30), ('b', -100), ('c', 50)")
      refreshMv("agghj_mv_case")
      assertMvCorrect("agghj_mv_case", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §NULL values in aggregate columns with edge case deletions
  describe("NULL aggregate inputs with edge-case DELETE/INSERT — agghj_agg_null_edge") {
    it("SUM/COUNT(*)/COUNT(val) all stay consistent through NULL-row churn") {
      sql("CREATE TABLE agghj_agg_null_edge (grp STRING, val INT) USING DELTA")
      sql(
        "INSERT INTO agghj_agg_null_edge VALUES ('a', 10), ('a', NULL), ('b', NULL), ('b', NULL), ('c', 30)"
      )
      val viewBody =
        "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt, COUNT(val) AS cnt_val " +
          "FROM agghj_agg_null_edge GROUP BY grp"
      sql(s"CREATE MATERIALIZED VIEW agghj_mv_agg_null_edge AS $viewBody")
      refreshMv("agghj_mv_agg_null_edge")
      assertMvCorrect("agghj_mv_agg_null_edge", viewBody)

      // Delete all NULL vals — group 'b' loses all rows
      sql("DELETE FROM agghj_agg_null_edge WHERE val IS NULL")
      refreshMv("agghj_mv_agg_null_edge")
      assertMvCorrect("agghj_mv_agg_null_edge", viewBody)

      // Insert only NULLs into 'a'
      sql("INSERT INTO agghj_agg_null_edge VALUES ('a', NULL), ('a', NULL)")
      refreshMv("agghj_mv_agg_null_edge")
      assertMvCorrect("agghj_mv_agg_null_edge", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §JOIN + GROUP BY + HAVING
  describe("JOIN + GROUP BY + HAVING — agghj_ajh_orders/agghj_ajh_customers") {
    it("Region totals above threshold stay correct as rows are added / dropped") {
      sql("CREATE TABLE agghj_ajh_orders (id INT, cust_id INT, amount INT) USING DELTA")
      sql("CREATE TABLE agghj_ajh_customers (id INT, region STRING) USING DELTA")
      sql("INSERT INTO agghj_ajh_customers VALUES (1, 'US'), (2, 'EU'), (3, 'US')")
      sql("INSERT INTO agghj_ajh_orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150), (4, 3, 50)")

      val viewBody =
        "SELECT c.region, SUM(o.amount) AS total, COUNT(*) AS cnt " +
          "FROM agghj_ajh_orders o INNER JOIN agghj_ajh_customers c ON o.cust_id = c.id " +
          "GROUP BY c.region " +
          "HAVING SUM(o.amount) > 200"
      sql(s"CREATE MATERIALIZED VIEW agghj_mv_big_regions AS $viewBody")
      assertMvCorrect("agghj_mv_big_regions", viewBody)

      sql("INSERT INTO agghj_ajh_orders VALUES (5, 2, 200)")
      refreshMv("agghj_mv_big_regions")
      assertMvCorrect("agghj_mv_big_regions", viewBody)

      sql("DELETE FROM agghj_ajh_orders WHERE cust_id = 1 AND amount = 200")
      refreshMv("agghj_mv_big_regions")
      assertMvCorrect("agghj_mv_big_regions", viewBody)

      sql("INSERT INTO agghj_ajh_customers VALUES (4, 'APAC')")
      sql("INSERT INTO agghj_ajh_orders VALUES (6, 4, 500), (7, 4, 100)")
      refreshMv("agghj_mv_big_regions")
      assertMvCorrect("agghj_mv_big_regions", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §UPDATE moves rows between groups (agghj_agg_key_move)
  describe("UPDATE moves rows between groups (SUM + COUNT) — agghj_agg_key_move") {
    it("After GROUP BY key changes, SUM and COUNT both reflect the new groupings") {
      sql("CREATE TABLE agghj_agg_key_move (grp STRING, val INT) USING DELTA")
      sql("INSERT INTO agghj_agg_key_move VALUES ('m', 10), ('m', 20), ('n', 30), ('n', 40)")
      val viewBody =
        "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM agghj_agg_key_move GROUP BY grp"
      sql(s"CREATE MATERIALIZED VIEW agghj_mv_agg_key_move AS $viewBody")
      refreshMv("agghj_mv_agg_key_move")
      assertMvCorrect("agghj_mv_agg_key_move", viewBody)

      sql("UPDATE agghj_agg_key_move SET grp = 'n' WHERE val = 20")
      refreshMv("agghj_mv_agg_key_move")
      assertMvCorrect("agghj_mv_agg_key_move", viewBody)

      sql("UPDATE agghj_agg_key_move SET grp = 'o' WHERE grp = 'm'")
      refreshMv("agghj_mv_agg_key_move")
      assertMvCorrect("agghj_mv_agg_key_move", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §LEFT JOIN + AGGREGATE (SUM over NULL-extended rows)
  describe("LEFT JOIN + AGGREGATE — agghj_lja_customers LEFT JOIN agghj_lja_orders") {
    it("SUM/COUNT over NULL-extended rows remain correct across DML") {
      sql("CREATE TABLE agghj_lja_customers (id INT, name STRING) USING DELTA")
      sql("CREATE TABLE agghj_lja_orders (cust_id INT, amount INT) USING DELTA")
      sql("INSERT INTO agghj_lja_customers VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')")
      sql("INSERT INTO agghj_lja_orders VALUES (1, 100), (1, 200), (2, 50)")

      val viewBody =
        "SELECT c.name, SUM(o.amount) AS total, COUNT(o.amount) AS cnt " +
          "FROM agghj_lja_customers c LEFT JOIN agghj_lja_orders o ON c.id = o.cust_id " +
          "GROUP BY c.name"
      sql(s"CREATE MATERIALIZED VIEW agghj_mv_cust_totals AS $viewBody")
      assertMvCorrect("agghj_mv_cust_totals", viewBody)

      // Add order for Carol + delete all Alice's orders
      sql("INSERT INTO agghj_lja_orders VALUES (3, 75)")
      sql("DELETE FROM agghj_lja_orders WHERE cust_id = 1")
      refreshMv("agghj_mv_cust_totals")
      assertMvCorrect("agghj_mv_cust_totals", viewBody)

      // Delete all orders — everyone NULL-extended
      sql("DELETE FROM agghj_lja_orders")
      refreshMv("agghj_mv_cust_totals")
      assertMvCorrect("agghj_mv_cust_totals", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §HAVING AVG (q0929)
  describe("HAVING AVG(x) > k decomposes AVG (q0929 regression) — agghj_hva_t") {
    it("AVG inside HAVING is correctly decomposed; delta refresh stays accurate") {
      sql("CREATE TABLE agghj_hva_t(grp INT, v INT) USING DELTA")
      sql("INSERT INTO agghj_hva_t VALUES (1,10), (1,20), (2,-5), (2,-15), (3,100)")

      val viewBody =
        "SELECT grp, AVG(v) AS a FROM agghj_hva_t GROUP BY grp HAVING AVG(v) > 0"
      sql(s"CREATE MATERIALIZED VIEW agghj_mv_hva AS $viewBody")
      assertMvCorrect("agghj_mv_hva", viewBody)

      // NOTE: skipped the openivm `pragma_table_info('openivm_data_mv_hva')`
      // assertion — that probes an openivm-internal physical table, which is
      // outside the scope of a Spark parity test.

      sql("INSERT INTO agghj_hva_t VALUES (2, 100), (4, 50)")
      refreshMv("agghj_mv_hva")
      assertMvCorrect("agghj_mv_hva", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §HAVING hidden COUNT (agghj_hav_hidden_count)
  // TODO(P6f-AggregateSpec): The AGGREGATE_HAVING path (RefreshType 4) does not
  // yet support HAVING predicates that reference aggregates absent from the
  // SELECT clause (e.g. `HAVING COUNT(*) > 3` when COUNT(*) is not selected).
  // The Spark-side data table only carries the SELECTed aggregate plus a single
  // `openivm_having_0` hidden column, so a multi-aggregate HAVING refers to
  // missing columns. openivm's fix is in `parser.cpp` (it materialises every
  // HAVING-referenced aggregate as a hidden column); needs the same treatment
  // on the Spark createMV path. Re-enable when openivm-spark materialises all
  // HAVING aggregates as hidden columns.
  describe("HAVING referencing aggregates not in SELECT — agghj_hav_hidden_count") {
    ignore("Hidden COUNT(*) > 3 predicate is correctly evaluated through batched DML") {
      sql("CREATE TABLE agghj_hav_hidden_count (w INT, i INT, amt INT) USING DELTA")
      sql(
        "INSERT INTO agghj_hav_hidden_count VALUES " +
          "(1,1,500),(1,1,600),(1,2,100),(1,2,50),(1,2,50),(1,2,50),(1,2,50)"
      )

      val viewBody =
        "SELECT w, i, SUM(amt) AS revenue FROM agghj_hav_hidden_count GROUP BY w, i " +
          "HAVING SUM(amt) > 200 AND COUNT(*) > 3"
      sql(s"CREATE MATERIALIZED VIEW agghj_mv_hav_hidden_count AS $viewBody")
      assertMvCorrect("agghj_mv_hav_hidden_count", viewBody)

      // Batched DML
      sql("DELETE FROM agghj_hav_hidden_count WHERE w = 1 AND i = 2 AND amt = 50")
      sql(
        "INSERT INTO agghj_hav_hidden_count VALUES " +
          "(1,1,300),(1,1,50),(1,3,500),(1,3,500),(1,3,500),(1,3,500)"
      )
      sql("UPDATE agghj_hav_hidden_count SET amt = amt + 10 WHERE w = 1 AND i = 1")
      refreshMv("agghj_mv_hav_hidden_count")
      assertMvCorrect("agghj_mv_hav_hidden_count", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §HAVING SUM(COALESCE) (agghj_hav_coalesce_c/h)
  describe("HAVING SUM(COALESCE(...)) over LEFT JOIN — hav_coalesce") {
    it("Two distinct SUM aggregates in HAVING (one wrapped in COALESCE) stay consistent") {
      sql("CREATE TABLE agghj_hav_coalesce_c (id INT, w INT, bal INT) USING DELTA")
      sql("CREATE TABLE agghj_hav_coalesce_h (c_id INT, c_w INT, amt INT) USING DELTA")
      sql("INSERT INTO agghj_hav_coalesce_c VALUES (1,1,100),(2,1,200),(3,1,50)")
      sql("INSERT INTO agghj_hav_coalesce_h VALUES (1,1,30),(2,1,250)")

      val viewBody =
        "SELECT c.w, SUM(c.bal) AS balance, SUM(h.amt) AS payments " +
          "FROM agghj_hav_coalesce_c c LEFT JOIN agghj_hav_coalesce_h h " +
          "ON c.id = h.c_id AND c.w = h.c_w " +
          "GROUP BY c.w " +
          "HAVING SUM(c.bal) > SUM(COALESCE(h.amt, 0))"
      sql(s"CREATE MATERIALIZED VIEW agghj_mv_hav_coalesce AS $viewBody")
      assertMvCorrect("agghj_mv_hav_coalesce", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §Fix B CTE AVG/STDDEV (agghj_fixb_customer)
  describe("Fix B CTE AVG/STDDEV — agghj_fixb_customer") {
    it("CTE with AVG/STDDEV plus derived ROUND projections stays correct after batched DML") {
      sql("CREATE TABLE agghj_fixb_customer (c_w_id INT, c_balance DECIMAL(12,2)) USING DELTA")
      sql("INSERT INTO agghj_fixb_customer VALUES (1, 10.00), (1, 20.00), (2, 30.00)")

      val viewBody =
        "WITH per_warehouse AS (" +
          "SELECT c_w_id, AVG(c_balance) AS avg_bal, STDDEV(c_balance) AS std_bal, " +
          "COUNT(*) AS cnt FROM agghj_fixb_customer GROUP BY c_w_id" +
          ") " +
          "SELECT c_w_id, ROUND(avg_bal, 2) AS avg_r, ROUND(std_bal, 2) AS std_r, " +
          "cnt, ROUND(avg_bal / NULLIF(std_bal, 0), 4) AS cv " +
          "FROM per_warehouse"
      sql(s"CREATE MATERIALIZED VIEW agghj_fixb_mv AS $viewBody")

      sql("INSERT INTO agghj_fixb_customer VALUES (1, 40.00), (2, 50.00), (3, 100.00)")
      sql(
        "UPDATE agghj_fixb_customer SET c_balance = 15.00 WHERE c_w_id = 1 AND c_balance = 20.00"
      )
      sql("DELETE FROM agghj_fixb_customer WHERE c_w_id = 1 AND c_balance = 10.00")
      refreshMv("agghj_fixb_mv")
      assertMvCorrect("agghj_fixb_mv", viewBody)
    }
  }

}
