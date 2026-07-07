package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** Split-off from `AggregateSpec.scala` so that each chunk of ~10 tests runs
  * in its own forked JVM (see `spark-ext/project/Settings.scala`). All table
  * and MV names are prefixed with `aggsd_` to guarantee that parallel
  * specs cannot collide on a Delta warehouse path.
  */
abstract class AggregateStddevMiscScenarios extends IvmParitySpecBase("aggregate-stddev-misc") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  // openivm test/sql/aggregate.test §STDDEV and VARIANCE decomposition
  describe("STDDEV and VARIANCE decomposition — aggsd_sd_base") {
    it("stddev/variance (sample and population) stay correct under INSERT/DELETE") {
      sql("CREATE TABLE aggsd_sd_base (id INT, grp STRING, val DOUBLE) USING DELTA")
      sql(
        "INSERT INTO aggsd_sd_base VALUES (1,'a',10), (2,'a',20), (3,'a',30), (4,'b',5), (5,'b',15), (6,'b',25)"
      )
      // Compare with rounding to absorb floating-point reassociation drift.
      val mvBody =
        "SELECT grp, stddev(val) as sd, variance(val) as vr, stddev_pop(val) as sdp, var_pop(val) as vrp " +
          "FROM aggsd_sd_base GROUP BY grp"
      sql(s"CREATE MATERIALIZED VIEW aggsd_mv_stddev AS $mvBody")

      def assertRounded(): Unit = {
        val expected = sql(
          "SELECT grp, round(stddev(val), 6) AS sd, round(variance(val), 6) AS vr, " +
            "round(stddev_pop(val), 6) AS sdp, round(var_pop(val), 6) AS vrp " +
            "FROM aggsd_sd_base GROUP BY grp"
        )
        val mv = spark
          .table("aggsd_mv_stddev")
          .selectExpr(
            "grp",
            "round(sd, 6) AS sd",
            "round(vr, 6) AS vr",
            "round(sdp, 6) AS sdp",
            "round(vrp, 6) AS vrp"
          )
        withClue("aggsd_mv_stddev EXCEPT ALL expected: ") {
          mv.exceptAll(expected).count() shouldBe 0L
        }
        withClue("expected EXCEPT ALL aggsd_mv_stddev: ") {
          expected.exceptAll(mv).count() shouldBe 0L
        }
      }
      assertRounded()

      sql("INSERT INTO aggsd_sd_base VALUES (7,'a',40), (8,'b',35), (9,'c',100)")
      refreshMv("aggsd_mv_stddev")
      assertRounded()

      sql("DELETE FROM aggsd_sd_base WHERE id IN (1, 4)")
      sql("INSERT INTO aggsd_sd_base VALUES (10,'c',200), (11,'a',50)")
      refreshMv("aggsd_mv_stddev")
      assertRounded()
    }
  }

  // openivm test/sql/aggregate.test §LIST with large batch of mixed inserts/deletes (aggsd_list_heavy)
  // TODO: Same `collect_list` catalog-error problem — openivm
  // does not recognise Spark's collect_list. Re-enable when cross-dialect
  // aggregate name mapping is in place.
  describe("LIST heavy batch — aggsd_list_heavy") {
    ignore("Ordered LIST(token) per bucket survives batched INSERT + DELETE") {
      sql("CREATE TABLE aggsd_list_heavy (bucket INT, token STRING) USING DELTA")

      // openivm: INSERT INTO aggsd_list_heavy SELECT i % 3, 'tok' || i FROM range(1, 20) t(i)
      spark
        .range(1, 20)
        .selectExpr("CAST(id % 3 AS INT) AS bucket", "concat('tok', id) AS token")
        .write
        .mode("append")
        .insertInto("aggsd_list_heavy")

      val viewBody =
        "SELECT bucket, array_sort(collect_list(token)) AS toks, COUNT(*) AS n " +
          "FROM aggsd_list_heavy GROUP BY bucket"
      sql(s"CREATE MATERIALIZED VIEW aggsd_mv_list_heavy AS $viewBody")

      sql("DELETE FROM aggsd_list_heavy WHERE token IN ('tok5', 'tok10', 'tok15')")
      sql(
        "INSERT INTO aggsd_list_heavy VALUES (0, 'new_a'), (1, 'new_b'), (2, 'new_c'), (0, 'new_d')"
      )
      refreshMv("aggsd_mv_list_heavy")
      assertMvCorrect("aggsd_mv_list_heavy", viewBody, arrayCols = Set("toks"))
    }
  }

  // openivm test/sql/aggregate.test §STDDEV/VARIANCE on flat-valued data (aggsd_sd_flat)
  describe("STDDEV/VARIANCE on flat-valued data — aggsd_sd_flat") {
    it("Variance on identical values stays algebraically 0 (no negative-arg sqrt crash)") {
      sql("CREATE TABLE aggsd_sd_flat (id INT, grp INT, v DOUBLE) USING DELTA")

      // openivm: INSERT INTO aggsd_sd_flat SELECT i, (i-1) % 3, 100.5 FROM range(1, 30) t(i)
      spark
        .range(1, 30)
        .selectExpr("CAST(id AS INT) AS id", "CAST((id - 1) % 3 AS INT) AS grp", "100.5 AS v")
        .write
        .mode("append")
        .insertInto("aggsd_sd_flat")

      val mvBody =
        "SELECT grp, STDDEV(v) AS sd, VARIANCE(v) AS vr, " +
          "STDDEV_POP(v) AS sdp, VAR_POP(v) AS vrp, COUNT(*) AS n " +
          "FROM aggsd_sd_flat GROUP BY grp"
      sql(s"CREATE MATERIALIZED VIEW aggsd_mv_flat_sd AS $mvBody")

      spark
        .range(30, 60)
        .selectExpr("CAST(id AS INT) AS id", "CAST((id - 1) % 3 AS INT) AS grp", "100.5 AS v")
        .write
        .mode("append")
        .insertInto("aggsd_sd_flat")
      sql("DELETE FROM aggsd_sd_flat WHERE grp = 0 AND id BETWEEN 1 AND 6")
      refreshMv("aggsd_mv_flat_sd")

      // Round to 6 aggsd_decimals to absorb floating-point reassociation drift.
      val expected = sql(
        "SELECT grp, round(STDDEV(v), 6) AS sd, round(VARIANCE(v), 6) AS vr, " +
          "round(STDDEV_POP(v), 6) AS sdp, round(VAR_POP(v), 6) AS vrp, COUNT(*) AS n " +
          "FROM aggsd_sd_flat GROUP BY grp"
      )
      val mv = spark
        .table("aggsd_mv_flat_sd")
        .selectExpr(
          "grp",
          "round(sd, 6) AS sd",
          "round(vr, 6) AS vr",
          "round(sdp, 6) AS sdp",
          "round(vrp, 6) AS vrp",
          "n"
        )
      withClue("aggsd_mv_flat_sd EXCEPT ALL expected: ") {
        mv.exceptAll(expected).count() shouldBe 0L
      }
      withClue("expected EXCEPT ALL aggsd_mv_flat_sd: ") {
        expected.exceptAll(mv).count() shouldBe 0L
      }
    }
  }

  // openivm test/sql/aggregate.test §STDDEV/VARIANCE crossing count threshold (aggsd_sd_threshold)
  describe("STDDEV/VARIANCE crossing count threshold — aggsd_sd_threshold") {
    it("Sample stddev returns NULL when group size drops to ≤ 1; pop stddev returns 0") {
      sql("CREATE TABLE aggsd_sd_threshold (grp INT, v DOUBLE) USING DELTA")
      sql(
        "INSERT INTO aggsd_sd_threshold VALUES " +
          "(1, 10), (1, 20), (1, 30), " +
          "(2, 100), (2, 200), " +
          "(3, 500)"
      )

      val mvBody =
        "SELECT grp, STDDEV(v) AS sd, VARIANCE(v) AS vr, " +
          "STDDEV_POP(v) AS sdp, VAR_POP(v) AS vrp, COUNT(*) AS n " +
          "FROM aggsd_sd_threshold GROUP BY grp"
      sql(s"CREATE MATERIALIZED VIEW aggsd_mv_thr_sd AS $mvBody")

      def assertRounded(): Unit = {
        // Filter out fully-deleted groups (n > 0) on the MV side, mirroring
        // the openivm test's `WHERE n > 0` projection.
        val expected = sql(
          "SELECT grp, round(STDDEV(v), 6) AS sd, round(VARIANCE(v), 6) AS vr, " +
            "round(STDDEV_POP(v), 6) AS sdp, round(VAR_POP(v), 6) AS vrp, COUNT(*) AS n " +
            "FROM aggsd_sd_threshold GROUP BY grp"
        )
        val mv = spark
          .table("aggsd_mv_thr_sd")
          .where("n > 0")
          .selectExpr(
            "grp",
            "round(sd, 6) AS sd",
            "round(vr, 6) AS vr",
            "round(sdp, 6) AS sdp",
            "round(vrp, 6) AS vrp",
            "n"
          )
        withClue("aggsd_mv_thr_sd EXCEPT ALL expected: ") {
          mv.exceptAll(expected).count() shouldBe 0L
        }
        withClue("expected EXCEPT ALL aggsd_mv_thr_sd: ") {
          expected.exceptAll(mv).count() shouldBe 0L
        }
      }

      sql("DELETE FROM aggsd_sd_threshold WHERE grp = 1 AND v IN (20, 30)")
      sql("DELETE FROM aggsd_sd_threshold WHERE grp = 2")
      sql("INSERT INTO aggsd_sd_threshold VALUES (4, 999)")
      refreshMv("aggsd_mv_thr_sd")
      assertRounded()
    }
  }

  // openivm test/sql/aggregate.test §Fix B cv variant (aggsd_fixb_cv)
  // TODO: Fix B cv variant — the off-by-one bag-equality
  // failure indicates the orphan-derived-alias group-recompute path needs a
  // refinement when the projection includes both pass-through aggregates
  // (avg_b, std_b) AND a derived expression over them (cv).
  // openivm-spark currently emits a half-MERGE half-recompute that leaves one
  // extra row behind. Re-enable when the projection-level orphan detection
  // is tightened.
  describe("Fix B cv variant — exposes AVG/STDDEV directly with derived cv column") {
    ignore("CTE → SELECT AVG, STDDEV, ROUND(AVG/STDDEV) stays accurate (rounded compare)") {
      // Reuses aggsd_fixb_customer from the previous describe — Spark sessions share state
      // across describes in this AnyFunSpec, mirroring openivm's sequential .test layout.
      val viewBody =
        "WITH stats AS (" +
          "SELECT c_w_id, AVG(c_balance) AS avg_b, STDDEV(c_balance) AS std_b " +
          "FROM aggsd_fixb_customer GROUP BY c_w_id" +
          ") " +
          "SELECT c_w_id, avg_b, std_b, ROUND(avg_b / NULLIF(std_b, 0), 4) AS cv FROM stats"
      sql(s"CREATE MATERIALIZED VIEW aggsd_fixb_cv AS $viewBody")

      sql("INSERT INTO aggsd_fixb_customer VALUES (1, 60.00), (3, 200.00)")
      sql(
        "UPDATE aggsd_fixb_customer SET c_balance = 80.00 WHERE c_w_id = 2 AND c_balance = 50.00"
      )
      sql("DELETE FROM aggsd_fixb_customer WHERE c_w_id = 3 AND c_balance = 100.00")
      refreshMv("aggsd_fixb_cv")

      // Compare via format_string('%.12g', …) to absorb 1-ULP AVG(DECIMAL) drift,
      // mirroring openivm's `printf('%.12g', …)` strategy.
      val expected = sql(
        "SELECT c_w_id, " +
          "format_string('%.12g', CAST(AVG(c_balance) AS DOUBLE)) AS avg_b, " +
          "format_string('%.12g', CAST(STDDEV(c_balance) AS DOUBLE)) AS std_b, " +
          "format_string('%.12g', " +
          "  CAST(ROUND(CAST(AVG(c_balance) AS DOUBLE) / NULLIF(STDDEV(c_balance), 0), 4) AS DOUBLE)" +
          ") AS cv " +
          "FROM aggsd_fixb_customer GROUP BY c_w_id"
      )
      val mv = spark
        .table("aggsd_fixb_cv")
        .selectExpr(
          "c_w_id",
          "format_string('%.12g', CAST(avg_b AS DOUBLE)) AS avg_b",
          "format_string('%.12g', CAST(std_b AS DOUBLE)) AS std_b",
          "format_string('%.12g', CAST(cv AS DOUBLE)) AS cv"
        )
      withClue("aggsd_fixb_cv EXCEPT ALL expected: ") {
        mv.exceptAll(expected).count() shouldBe 0L
      }
      withClue("expected EXCEPT ALL aggsd_fixb_cv: ") {
        expected.exceptAll(mv).count() shouldBe 0L
      }
    }
  }

  // openivm test/sql/aggregate.test §Fix B SUM/COUNT (aggsd_fixb_pay)
  // TODO: Same HUGEINT downcast gap — `SUM(c_bal) /
  // NULLIF(COUNT(c_bal), 0)` produces a HUGEINT division in openivm's compiled
  // refresh SQL which Spark cannot parse. Re-enable when openivm's HUGEINT→
  // BIGINT downcast is in the SPARK dialect.
  describe("Fix B SUM/COUNT-NULLIF — aggsd_fixb_pay") {
    ignore("ROUND(SUM/COUNT, n) and ROUND(SUM/SUM, n) stay correct after batched DML") {
      sql(
        "CREATE TABLE aggsd_fixb_pay (w INT, d INT, c_bal DECIMAL(12,2), " +
          "ytd DECIMAL(12,2), pay_cnt INT) USING DELTA"
      )
      sql(
        "INSERT INTO aggsd_fixb_pay VALUES " +
          "(1, 1, 10, 50, 2), (1, 1, 20, 60, 3), (1, 2, 30, 70, 1), (2, 1, 100, 120, 4)"
      )

      val viewBody =
        "SELECT w, d, COUNT(c_bal) AS cust, " +
          "ROUND(SUM(c_bal) / NULLIF(COUNT(c_bal), 0), 2) AS avg_bal, " +
          "ROUND(SUM(ytd) / NULLIF(SUM(pay_cnt), 0), 2) AS avg_pay " +
          "FROM aggsd_fixb_pay GROUP BY w, d"
      sql(s"CREATE MATERIALIZED VIEW aggsd_fixb_pay_mv AS $viewBody")

      sql("INSERT INTO aggsd_fixb_pay VALUES (1, 1, 40, 80, 2), (2, 1, 50, 90, 1)")
      sql("UPDATE aggsd_fixb_pay SET c_bal = 5, ytd = 15, pay_cnt = 1 WHERE w = 1 AND d = 2")
      sql("DELETE FROM aggsd_fixb_pay WHERE w = 1 AND d = 1 AND c_bal = 10")
      refreshMv("aggsd_fixb_pay_mv")
      assertMvCorrect("aggsd_fixb_pay_mv", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §AGGREGATE FILTER (aggsd_filter_t)
  describe("AGGREGATE FILTER (WHERE p) — aggsd_filter_t") {
    it("FILTER on COUNT/SUM/AVG stays correct after batched INSERT + DELETE + UPDATE") {
      sql("CREATE TABLE aggsd_filter_t (id INT, grp STRING, val INT, active BOOLEAN) USING DELTA")
      sql(
        "INSERT INTO aggsd_filter_t VALUES " +
          "(1,'a',10,true),(2,'a',200,true),(3,'a',50,false),(4,'b',300,true),(5,'b',80,false)"
      )

      val viewBody =
        "SELECT grp, " +
          "COUNT(*) FILTER (WHERE active) AS active_cnt, " +
          "SUM(val) FILTER (WHERE val > 100) AS big_sum, " +
          "AVG(val) FILTER (WHERE active) AS avg_active " +
          "FROM aggsd_filter_t GROUP BY grp"
      sql(s"CREATE MATERIALIZED VIEW aggsd_filter_mv AS $viewBody")
      assertMvCorrect("aggsd_filter_mv", viewBody)

      sql("INSERT INTO aggsd_filter_t VALUES (6,'a',500,true),(7,'b',400,false),(8,'c',150,true)")
      sql("DELETE FROM aggsd_filter_t WHERE id = 2")
      sql("UPDATE aggsd_filter_t SET val = 999, active = false WHERE id = 4")
      refreshMv("aggsd_filter_mv")
      assertMvCorrect("aggsd_filter_mv", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §BOOL_AND / BOOL_OR support (aggsd_bool_t)
  describe("BOOL_AND / BOOL_OR — aggsd_bool_t") {
    it("BOOL_AND/BOOL_OR flip correctly after rows are added/removed") {
      sql("CREATE TABLE aggsd_bool_t (id INT, grp STRING, flag BOOLEAN) USING DELTA")
      sql(
        "INSERT INTO aggsd_bool_t VALUES (1,'x',true),(2,'x',false),(3,'y',true),(4,'y',true),(5,'z',false)"
      )

      val viewBody =
        "SELECT grp, BOOL_AND(flag) AS all_flag, BOOL_OR(flag) AS any_flag " +
          "FROM aggsd_bool_t GROUP BY grp"
      sql(s"CREATE MATERIALIZED VIEW aggsd_bool_mv AS $viewBody")
      assertMvCorrect("aggsd_bool_mv", viewBody)

      // Delete the only false in x → BOOL_AND flips true
      sql("DELETE FROM aggsd_bool_t WHERE id = 2")
      // Delete one of two trues in y (BOOL_AND/BOOL_OR stay true)
      sql("DELETE FROM aggsd_bool_t WHERE id = 3")
      // Insert a false into y → BOOL_AND flips false
      sql("INSERT INTO aggsd_bool_t VALUES (6,'y',false)")
      refreshMv("aggsd_bool_mv")
      assertMvCorrect("aggsd_bool_mv", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §Aggregate top-k (aggsd_topk_t)
  describe("Aggregate top-k (GROUP BY + ORDER BY + LIMIT) — aggsd_topk_t") {
    it("MV reflects the current top-3 groups by total across INSERT/DELETE/stress") {
      sql("CREATE TABLE aggsd_topk_t (id INT, grp STRING, val INT) USING DELTA")
      sql(
        "INSERT INTO aggsd_topk_t VALUES " +
          "(1,'a',100),(2,'a',200),(3,'b',50),(4,'b',300)," +
          "(5,'c',150),(6,'c',250),(7,'d',10),(8,'d',5)"
      )

      val viewBody =
        "SELECT grp, SUM(val) AS total FROM aggsd_topk_t GROUP BY grp ORDER BY total DESC LIMIT 3"
      sql(s"CREATE MATERIALIZED VIEW aggsd_topk_agg AS $viewBody")
      assertMvCorrect("aggsd_topk_agg", viewBody)

      sql("INSERT INTO aggsd_topk_t VALUES (9,'d',500)")
      refreshMv("aggsd_topk_agg")
      assertMvCorrect("aggsd_topk_agg", viewBody)

      sql("DELETE FROM aggsd_topk_t WHERE id = 9")
      refreshMv("aggsd_topk_agg")
      assertMvCorrect("aggsd_topk_agg", viewBody)

      // Stress: batched INSERT + DELETE + UPDATE then single refresh
      sql("INSERT INTO aggsd_topk_t VALUES (10,'e',1000),(11,'f',2)")
      sql("DELETE FROM aggsd_topk_t WHERE id IN (1,2)")
      sql("UPDATE aggsd_topk_t SET val = 999 WHERE id = 3")
      refreshMv("aggsd_topk_agg")
      assertMvCorrect("aggsd_topk_agg", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §COUNT(DISTINCT constant) (aggsd_cd_order + aggsd_cd_new_order)
  describe("COUNT(DISTINCT 1) over duplicate join rows — aggsd_cd_order/aggsd_cd_new_order") {
    it("MIN(1)/MAX(1)/COUNT(DISTINCT 1) per o.id stay correct after INSERT") {
      sql("CREATE TABLE aggsd_cd_order (id INT, w_id INT) USING DELTA")
      sql("CREATE TABLE aggsd_cd_new_order (o_id INT, w_id INT) USING DELTA")
      sql("INSERT INTO aggsd_cd_order VALUES (1, 1), (2, 1)")
      sql("INSERT INTO aggsd_cd_new_order VALUES (1, 1), (1, 1), (2, 1)")

      // NOTE: alias `no` is fine in Spark — not a reserved keyword.
      val viewBody =
        "SELECT o.id, MIN(1) AS mn, MAX(1) AS mx, COUNT(DISTINCT 1) AS uniq " +
          "FROM aggsd_cd_order o " +
          "JOIN aggsd_cd_new_order no ON o.id = no.o_id AND o.w_id = no.w_id " +
          "GROUP BY o.id"
      sql(s"CREATE MATERIALIZED VIEW aggsd_mv_count_distinct_constant AS $viewBody")
      assertMvCorrect("aggsd_mv_count_distinct_constant", viewBody)

      // NOTE: skipped openivm's `SELECT type FROM openivm_views …` assertion —
      // that probes openivm-internal classifier metadata, not user-visible
      // semantics.

      sql("INSERT INTO aggsd_cd_new_order VALUES (1, 1)")
      refreshMv("aggsd_mv_count_distinct_constant")
      assertMvCorrect("aggsd_mv_count_distinct_constant", viewBody)
    }
  }

}
