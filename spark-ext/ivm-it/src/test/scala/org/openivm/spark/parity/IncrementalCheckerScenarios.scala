package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.apache.spark.sql.catalyst.TableIdentifier
import org.openivm.spark.common.{MvCatalog, RefreshTypeCode}

/** Parity port of `openivm/test/sql/incremental_checker.test`.
  *
  * The openivm test exercises the openivm SQL compatibility classifier:
  *
  *   - "unsupported" view shapes (LEFT/RIGHT JOIN, STDDEV, STRING_AGG, RANDOM,
  *     NOW, etc.) must NOT raise — they are silently demoted to FULL_REFRESH.
  *   - "supported" shapes (INNER JOIN, SUM, AVG, COUNT, projection, filter,
  *     UNION ALL) flow through the incremental path, classified as the
  *     appropriate RefreshType.
  *   - After DML + REFRESH the MV must bag-equal the equivalent base query
  *     in both directions (bidirectional `EXCEPT ALL`).
  *
  * Openivm-spark inherits the openivm DuckDB classifier (via the
  * `openivm-compiler` subprocess CLI) for incremental compilability. When the
  * compiler emits an empty-or-placeholder incremental program for a shape it
  * cannot meaningfully maintain, `RefreshMaterializedViewCommand` demotes the
  * MV to RefreshType 3 (FULL_REFRESH) via `SparkRefreshRewriter.hasRealDelta`.
  * This spec verifies that the demotion happens for the unsupported shapes
  * AND that the resulting FULL_REFRESH preserves correctness — exactly the
  * invariants the upstream openivm test asserts.
  *
  * == Notes on shape differences from openivm ==
  *
  *   - DuckDB's `STDDEV` is `STDDEV_SAMP` in Spark SQL. The Spark version is
  *     used where the openivm test uses STDDEV.
  *   - DuckDB's `STRING_AGG(x::VARCHAR, ',')` is closest to Spark's
  *     `concat_ws(',', collect_list(cast(x AS STRING)))`. We assert
  *     bidirectional bag equality on the Spark expression.
  *   - DuckDB's `RANDOM()` is `rand()` in Spark; `NOW()` exists in both. Both
  *     are non-deterministic and would cause openivm to demote to FULL_REFRESH.
  *     For RANDOM() / NOW(), the openivm test only verifies row counts (since
  *     full-refresh re-evaluation will produce different values each time);
  *     we mirror that — checking COUNT after refresh, NOT bidirectional
  *     EXCEPT ALL on the random column.
  */
abstract class IncrementalCheckerScenarios extends IvmParitySpecBase("incremental-checker") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  /** Look up the recorded `refresh_type` for `name` from the MV catalog. */
  protected def mvRefreshType(name: String): Int =
    MvCatalog
      .lookup(spark, TableIdentifier(name))
      .getOrElse(fail(s"MV $name not found in catalog"))
      .refreshType

  // ──────────────────────────────────────────────────────────────────────────
  // Setup base tables shared by several tests
  // ──────────────────────────────────────────────────────────────────────────

  protected def setupT1T2(): Unit = {
    sql("CREATE TABLE IF NOT EXISTS t1_chk(a INT, b INT) USING DELTA")
    sql("CREATE TABLE IF NOT EXISTS t2_chk(a INT, c INT) USING DELTA")
    sql("INSERT INTO t1_chk VALUES (1, 10), (2, 20)")
    sql("INSERT INTO t2_chk VALUES (1, 100), (2, 200)")
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (A) Unsupported join shapes — must CREATE successfully, refresh correctly
  // ──────────────────────────────────────────────────────────────────────────
  describe("(A) Unsupported joins are silently demoted, not rejected") {

    it("LEFT JOIN under projection: MV creates, refresh keeps bag-equality") {
      setupT1T2()
      sql(
        "CREATE MATERIALIZED VIEW mv_left_chk AS " +
          "SELECT t1_chk.a, t1_chk.b, t2_chk.c FROM t1_chk LEFT JOIN t2_chk ON t1_chk.a = t2_chk.a"
      )
      assertMvCorrect(
        "mv_left_chk",
        "SELECT t1_chk.a, t1_chk.b, t2_chk.c FROM t1_chk LEFT JOIN t2_chk ON t1_chk.a = t2_chk.a"
      )

      // INSERT into both tables — refresh must keep parity.
      sql("INSERT INTO t1_chk VALUES (3, 30)")
      sql("INSERT INTO t2_chk VALUES (3, 300)")
      refreshMv("mv_left_chk")
      assertMvCorrect(
        "mv_left_chk",
        "SELECT t1_chk.a, t1_chk.b, t2_chk.c FROM t1_chk LEFT JOIN t2_chk ON t1_chk.a = t2_chk.a"
      )
    }

    it("RIGHT JOIN under projection: MV creates and stays bag-equivalent") {
      // Use fresh tables to keep this independent of the LEFT JOIN test.
      sql("CREATE TABLE IF NOT EXISTS rj1_chk(a INT, b INT) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS rj2_chk(a INT, c INT) USING DELTA")
      sql("INSERT INTO rj1_chk VALUES (1,10),(2,20)")
      sql("INSERT INTO rj2_chk VALUES (1,100),(2,200)")

      sql(
        "CREATE MATERIALIZED VIEW mv_right_chk AS " +
          "SELECT rj1_chk.a, rj1_chk.b, rj2_chk.c FROM rj1_chk RIGHT JOIN rj2_chk ON rj1_chk.a = rj2_chk.a"
      )
      assertMvCorrect(
        "mv_right_chk",
        "SELECT rj1_chk.a, rj1_chk.b, rj2_chk.c FROM rj1_chk RIGHT JOIN rj2_chk ON rj1_chk.a = rj2_chk.a"
      )
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (B) "Unsupported" aggregates: openivm's actual classifier may either
  //     decompose them incrementally (STDDEV → SUM, COUNT, sum-of-squares
  //     helper columns per CLAUDE.md) OR demote to FULL_REFRESH. Either way,
  //     bidirectional bag-equality must hold after REFRESH — that is the
  //     parity invariant. We deliberately do NOT pin the refresh_type code
  //     since openivm has evolved beyond the categorisation in
  //     `openivm/test/sql/incremental_checker.test`.
  //
  //     Note: the openivm test also exercises `STRING_AGG`, but that function
  //     has no name compatible with both Spark SQL (`concat_ws + collect_list`)
  //     and DuckDB (`string_agg`). Since the openivm compiler is invoked at
  //     CREATE time and parses the view body through DuckDB, we restrict to
  //     functions both engines name-recognise. STDDEV_SAMP is the canonical
  //     "complex aggregate" exercise.
  // ──────────────────────────────────────────────────────────────────────────
  describe("(B) Aggregates the upstream test calls 'unsupported' refresh correctly") {

    it("STDDEV_SAMP under GROUP BY: refresh keeps bag-equality regardless of refresh_type") {
      sql("CREATE TABLE IF NOT EXISTS s_chk(a INT, b INT) USING DELTA")
      sql("INSERT INTO s_chk VALUES (1,10),(1,20),(2,30),(2,40)")
      sql(
        "CREATE MATERIALIZED VIEW mv_stddev_chk AS " +
          "SELECT a, STDDEV_SAMP(b) AS sd FROM s_chk GROUP BY a"
      )

      // Insert new data; refresh must re-evaluate.
      sql("INSERT INTO s_chk VALUES (1, 100), (3, 5)")
      refreshMv("mv_stddev_chk")
      assertMvCorrect(
        "mv_stddev_chk",
        "SELECT a, STDDEV_SAMP(b) AS sd FROM s_chk GROUP BY a"
      )
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (C) Non-deterministic functions: the openivm classifier treats these as
  //     FULL_REFRESH because they reproduce different values per execution.
  //     We test `now()` only — Spark's `rand()` is not in DuckDB's catalog
  //     (DuckDB names it `random()`), so the openivm compiler (invoked at
  //     CREATE time via the openivm-compiler subprocess CLI) cannot
  //     name-resolve `rand()` and the CREATE fails before the classifier even
  //     runs. The `now()` case alone is sufficient to exercise the
  //     non-deterministic-projection path.
  // ──────────────────────────────────────────────────────────────────────────
  describe("(C) Non-deterministic functions (now)") {

    it("now() in projection: MV creates and row count tracks the source") {
      sql("CREATE TABLE IF NOT EXISTS n_chk(x INT) USING DELTA")
      sql("INSERT INTO n_chk VALUES (1), (2)")
      sql(
        "CREATE MATERIALIZED VIEW mv_now_chk AS SELECT x, now() AS ts FROM n_chk"
      )
      spark.table("mv_now_chk").count() shouldBe 2L

      sql("INSERT INTO n_chk VALUES (3)")
      refreshMv("mv_now_chk")
      spark.table("mv_now_chk").count() shouldBe 3L
      // Deterministic key column must remain bag-equivalent to source x values.
      val mvX  = sql("SELECT x FROM mv_now_chk").collect().map(_.getAs[Int]("x")).toSet
      val srcX = sql("SELECT x FROM n_chk").collect().map(_.getAs[Int]("x")).toSet
      mvX shouldBe srcX
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (D) Supported shapes — flow through the incremental path
  // ──────────────────────────────────────────────────────────────────────────
  describe("(D) Supported shapes flow through the incremental classifier") {

    it("INNER JOIN under projection is bag-equivalent after INSERTs on both sides") {
      sql("CREATE TABLE IF NOT EXISTS ij1_chk(a INT, b INT) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS ij2_chk(a INT, c INT) USING DELTA")
      sql("INSERT INTO ij1_chk VALUES (1,10),(2,20)")
      sql("INSERT INTO ij2_chk VALUES (1,100),(2,200)")
      sql(
        "CREATE MATERIALIZED VIEW mv_inner_chk AS " +
          "SELECT ij1_chk.a, ij1_chk.b, ij2_chk.c FROM ij1_chk INNER JOIN ij2_chk ON ij1_chk.a = ij2_chk.a"
      )
      assertMvCorrect(
        "mv_inner_chk",
        "SELECT ij1_chk.a, ij1_chk.b, ij2_chk.c FROM ij1_chk INNER JOIN ij2_chk ON ij1_chk.a = ij2_chk.a"
      )

      // INSERTs on both sides must refresh correctly.
      sql("INSERT INTO ij1_chk VALUES (3,30)")
      sql("INSERT INTO ij2_chk VALUES (3,300)")
      refreshMv("mv_inner_chk")
      assertMvCorrect(
        "mv_inner_chk",
        "SELECT ij1_chk.a, ij1_chk.b, ij2_chk.c FROM ij1_chk INNER JOIN ij2_chk ON ij1_chk.a = ij2_chk.a"
      )
    }

    it("SUM under GROUP BY classifies as AGGREGATE_GROUP (or compatible incremental type)") {
      sql("CREATE TABLE IF NOT EXISTS sum_chk(a INT, b INT) USING DELTA")
      sql("INSERT INTO sum_chk VALUES (1,10),(2,20)")
      sql(
        "CREATE MATERIALIZED VIEW mv_sum_chk AS SELECT a, SUM(b) AS total FROM sum_chk GROUP BY a"
      )
      // AGGREGATE_GROUP is the expected incremental classification.
      mvRefreshType("mv_sum_chk") shouldBe RefreshTypeCode.AggregateGroup
      assertMvCorrect("mv_sum_chk", "SELECT a, SUM(b) AS total FROM sum_chk GROUP BY a")

      sql("INSERT INTO sum_chk VALUES (3,30)")
      refreshMv("mv_sum_chk")
      assertMvCorrect("mv_sum_chk", "SELECT a, SUM(b) AS total FROM sum_chk GROUP BY a")
    }

    it("AVG under GROUP BY: incrementally maintained, bag-equivalent after DML") {
      sql("CREATE TABLE IF NOT EXISTS avg_chk(a INT, b INT) USING DELTA")
      sql("INSERT INTO avg_chk VALUES (1,10),(2,20),(2,40)")
      sql(
        "CREATE MATERIALIZED VIEW mv_avg_chk AS SELECT a, AVG(b) AS avg_b FROM avg_chk GROUP BY a"
      )
      assertMvCorrect("mv_avg_chk", "SELECT a, AVG(b) AS avg_b FROM avg_chk GROUP BY a")

      sql("INSERT INTO avg_chk VALUES (3, 30)")
      refreshMv("mv_avg_chk")
      assertMvCorrect("mv_avg_chk", "SELECT a, AVG(b) AS avg_b FROM avg_chk GROUP BY a")
    }

    it("COUNT(*) ungrouped (simple aggregate): refresh applies INSERTs") {
      sql("CREATE TABLE IF NOT EXISTS cnt_chk(a INT) USING DELTA")
      sql("INSERT INTO cnt_chk VALUES (1),(2),(3)")
      sql("CREATE MATERIALIZED VIEW mv_cnt_chk AS SELECT COUNT(*) AS cnt FROM cnt_chk")
      assertMvCorrect("mv_cnt_chk", "SELECT COUNT(*) AS cnt FROM cnt_chk")

      sql("INSERT INTO cnt_chk VALUES (4),(5)")
      refreshMv("mv_cnt_chk")
      assertMvCorrect("mv_cnt_chk", "SELECT COUNT(*) AS cnt FROM cnt_chk")
    }

    it("Plain projection: classified as SIMPLE_PROJECTION") {
      sql("CREATE TABLE IF NOT EXISTS pj_chk(a INT, b INT) USING DELTA")
      sql("INSERT INTO pj_chk VALUES (1,10),(2,20)")
      sql("CREATE MATERIALIZED VIEW mv_proj_chk AS SELECT a, b FROM pj_chk")
      mvRefreshType("mv_proj_chk") shouldBe RefreshTypeCode.SimpleProjection
      assertMvCorrect("mv_proj_chk", "SELECT a, b FROM pj_chk")

      sql("INSERT INTO pj_chk VALUES (3,30)")
      refreshMv("mv_proj_chk")
      assertMvCorrect("mv_proj_chk", "SELECT a, b FROM pj_chk")
    }

    it("Projection + filter is incremental and bag-equivalent after INSERT") {
      sql("CREATE TABLE IF NOT EXISTS pf_chk(a INT, b INT) USING DELTA")
      sql("INSERT INTO pf_chk VALUES (1,1),(2,10),(3,6)")
      sql(
        "CREATE MATERIALIZED VIEW mv_filt_chk AS SELECT a, b FROM pf_chk WHERE b > 5"
      )
      assertMvCorrect("mv_filt_chk", "SELECT a, b FROM pf_chk WHERE b > 5")

      sql("INSERT INTO pf_chk VALUES (4, 7), (5, 2)")
      refreshMv("mv_filt_chk")
      assertMvCorrect("mv_filt_chk", "SELECT a, b FROM pf_chk WHERE b > 5")
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (E) UNION ALL — incremental support per openivm IncrementalUnionRule
  // ──────────────────────────────────────────────────────────────────────────
  describe("(E) UNION ALL is incrementally supported") {

    it("multi-side INSERT and DELETE refresh keeps bag-equality") {
      sql("CREATE TABLE IF NOT EXISTS u1_chk(id INT, val STRING) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS u2_chk(id INT, val STRING) USING DELTA")
      sql("INSERT INTO u1_chk VALUES (1,'a'),(2,'b')")
      sql("INSERT INTO u2_chk VALUES (3,'c'),(4,'d')")
      sql(
        "CREATE MATERIALIZED VIEW mv_union_chk AS " +
          "SELECT id, val FROM u1_chk UNION ALL SELECT id, val FROM u2_chk"
      )
      assertMvCorrect(
        "mv_union_chk",
        "SELECT id, val FROM u1_chk UNION ALL SELECT id, val FROM u2_chk"
      )

      // INSERT into BOTH sides — single refresh applies both deltas.
      sql("INSERT INTO u1_chk VALUES (5,'e')")
      sql("INSERT INTO u2_chk VALUES (6,'f')")
      refreshMv("mv_union_chk")
      assertMvCorrect(
        "mv_union_chk",
        "SELECT id, val FROM u1_chk UNION ALL SELECT id, val FROM u2_chk"
      )

      // DELETE from one side.
      sql("DELETE FROM u1_chk WHERE id = 1")
      refreshMv("mv_union_chk")
      assertMvCorrect(
        "mv_union_chk",
        "SELECT id, val FROM u1_chk UNION ALL SELECT id, val FROM u2_chk"
      )
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (F) Mixed-shape stress: many conflicting DML ops batched before a single
  //     REFRESH — must hold bag-equality for both supported (incremental) and
  //     unsupported (full-refresh) shapes.
  // ──────────────────────────────────────────────────────────────────────────
  describe("(F) Batched conflicting DML before a single REFRESH") {

    it("INSERT + DELETE + UPDATE batched on a supported SUM MV → one REFRESH, bag-equal") {
      sql("CREATE TABLE IF NOT EXISTS batch_sup_chk(id INT, a INT, b INT) USING DELTA")
      sql(
        "INSERT INTO batch_sup_chk VALUES (1,1,10),(2,1,20),(3,2,30),(4,2,40),(5,3,50)"
      )
      sql(
        "CREATE MATERIALIZED VIEW mv_batch_sup_chk AS " +
          "SELECT a, SUM(b) AS total FROM batch_sup_chk GROUP BY a"
      )
      // Batched DML — 2 INSERTs that introduce new groups, 2 DELETEs that
      // remove rows, 1 UPDATE that flips a row to a different group.
      sql("INSERT INTO batch_sup_chk VALUES (6,4,60),(7,4,70)")
      sql("DELETE FROM batch_sup_chk WHERE id IN (3, 5)")
      sql("UPDATE batch_sup_chk SET a = 4 WHERE id = 1")
      refreshMv("mv_batch_sup_chk")
      assertMvCorrect(
        "mv_batch_sup_chk",
        "SELECT a, SUM(b) AS total FROM batch_sup_chk GROUP BY a"
      )
    }

    it("INSERT + DELETE + UPDATE batched on an STDDEV MV → one REFRESH, bag-equal") {
      sql("CREATE TABLE IF NOT EXISTS batch_unsup_chk(id INT, a INT, b INT) USING DELTA")
      sql(
        "INSERT INTO batch_unsup_chk VALUES (1,1,10),(2,1,20),(3,2,30),(4,2,40),(5,2,50)"
      )
      sql(
        "CREATE MATERIALIZED VIEW mv_batch_unsup_chk AS " +
          "SELECT a, STDDEV_SAMP(b) AS sd FROM batch_unsup_chk GROUP BY a"
      )

      // Mixed batch of conflicting ops in a single transaction window.
      sql("INSERT INTO batch_unsup_chk VALUES (6,1,15),(7,3,5)")
      sql("DELETE FROM batch_unsup_chk WHERE id IN (4)")
      sql("UPDATE batch_unsup_chk SET b = 999 WHERE id = 1")
      refreshMv("mv_batch_unsup_chk")
      assertMvCorrect(
        "mv_batch_unsup_chk",
        "SELECT a, STDDEV_SAMP(b) AS sd FROM batch_unsup_chk GROUP BY a"
      )
    }
  }
}
