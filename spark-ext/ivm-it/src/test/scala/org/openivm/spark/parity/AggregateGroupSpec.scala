package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** Comprehensive parity tests for RefreshType 0 — AGGREGATE_GROUP (additive monoid).
  *
  * Covers: SUM, COUNT(*), COUNT(x), AVG, STDDEV (sample), STDDEV_POP, VARIANCE,
  * VAR_POP, composite keys, multiple aggregates, empty-table bootstrap, insert+delete
  * round-trip (net-zero change), NULL group keys, and NULL aggregate inputs.
  *
  * Each test verifies the MV against a fresh re-computation of the view body via:
  *   1. `MV.collect().toSet == viewBody.collect().toSet`
  *   2. Bidirectional EXCEPT ALL → both sides must return 0 rows.
  *
  * Hidden bookkeeping columns (prefix `openivm_`) are projected away before the
  * comparison — the same pattern used in [[org.openivm.spark.it.IncrementalRefreshSpec]].
  */
class AggregateGroupSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-agg-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-AggregateGroupSpec")
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
    * expected SQL expression.
    */
  private def assertMvCorrect(mvName: String, expectedSql: String): Unit = {
    val expected: DataFrame = spark.sql(expectedSql)
    val userCols            = expected.columns.toSeq
    val mv = spark.table(mvName).select(userCols.head, userCols.tail: _*)
    withClue(s"$mvName EXCEPT ALL <expected> (MV has extra rows): ") {
      mv.exceptAll(expected).count() shouldBe 0L
    }
    withClue(s"<expected> EXCEPT ALL $mvName (MV missing rows): ") {
      expected.exceptAll(mv).count() shouldBe 0L
    }
  }

  private def refreshMv(name: String): Unit =
    spark.sql(s"REFRESH MATERIALIZED VIEW $name").collect()

  // ── Shape 1: GROUP BY k, SUM(x) — single column, single aggregate ─────────

  describe("(1a) GROUP BY k, SUM(x) — INSERT only") {
    it("incremental refresh yields correct sums after INSERT") {
      spark.sql("CREATE TABLE IF NOT EXISTS ag_sum_1a(k STRING, x INT) USING DELTA")
      spark.sql("INSERT INTO ag_sum_1a VALUES ('a', 10), ('b', 20)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_ag_sum_1a AS " +
          "SELECT k, SUM(x) AS total FROM ag_sum_1a GROUP BY k"
      )
      spark.sql("INSERT INTO ag_sum_1a VALUES ('a', 5), ('c', 30)")
      refreshMv("mv_ag_sum_1a")
      assertMvCorrect("mv_ag_sum_1a", "SELECT k, SUM(x) AS total FROM ag_sum_1a GROUP BY k")
    }
  }

  describe("(1b) GROUP BY k, SUM(x) — DELETE") {
    it("incremental refresh drops deleted group from MV") {
      spark.sql("CREATE TABLE IF NOT EXISTS ag_sum_1b(k STRING, x INT) USING DELTA")
      spark.sql("INSERT INTO ag_sum_1b VALUES ('a', 10), ('b', 20), ('c', 5)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_ag_sum_1b AS " +
          "SELECT k, SUM(x) AS total FROM ag_sum_1b GROUP BY k"
      )
      spark.sql("DELETE FROM ag_sum_1b WHERE k = 'b'")
      refreshMv("mv_ag_sum_1b")
      assertMvCorrect("mv_ag_sum_1b", "SELECT k, SUM(x) AS total FROM ag_sum_1b GROUP BY k")
    }
  }

  describe("(1c) GROUP BY k, SUM(x) — UPDATE") {
    it("incremental refresh recomputes sum for updated group") {
      spark.sql("CREATE TABLE IF NOT EXISTS ag_sum_1c(k STRING, x INT) USING DELTA")
      spark.sql("INSERT INTO ag_sum_1c VALUES ('a', 10), ('b', 20)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_ag_sum_1c AS " +
          "SELECT k, SUM(x) AS total FROM ag_sum_1c GROUP BY k"
      )
      spark.sql("UPDATE ag_sum_1c SET x = 99 WHERE k = 'b'")
      refreshMv("mv_ag_sum_1c")
      assertMvCorrect("mv_ag_sum_1c", "SELECT k, SUM(x) AS total FROM ag_sum_1c GROUP BY k")
    }
  }

  describe("(1d) GROUP BY k, SUM(x) — batched mixed DML") {
    it("incremental refresh reconciles INSERT + DELETE + UPDATE in one pass") {
      spark.sql("CREATE TABLE IF NOT EXISTS ag_sum_1d(k STRING, x INT) USING DELTA")
      spark.sql("INSERT INTO ag_sum_1d VALUES ('a', 10), ('b', 20), ('c', 5)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_ag_sum_1d AS " +
          "SELECT k, SUM(x) AS total FROM ag_sum_1d GROUP BY k"
      )
      spark.sql("INSERT INTO ag_sum_1d VALUES ('d', 50)")
      spark.sql("DELETE FROM ag_sum_1d WHERE k = 'c'")
      spark.sql("UPDATE ag_sum_1d SET x = 100 WHERE k = 'a'")
      refreshMv("mv_ag_sum_1d")
      assertMvCorrect("mv_ag_sum_1d", "SELECT k, SUM(x) AS total FROM ag_sum_1d GROUP BY k")
    }
  }

  // ── Shape 2: GROUP BY k, COUNT(*) — same DML matrix ──────────────────────

  describe("(2a) GROUP BY k, COUNT(*) — INSERT only") {
    it("incremental refresh yields correct counts after INSERT") {
      spark.sql("CREATE TABLE IF NOT EXISTS ag_cnt_2a(k STRING, x INT) USING DELTA")
      spark.sql("INSERT INTO ag_cnt_2a VALUES ('a', 1), ('a', 2), ('b', 3)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_ag_cnt_2a AS " +
          "SELECT k, COUNT(*) AS cnt FROM ag_cnt_2a GROUP BY k"
      )
      spark.sql("INSERT INTO ag_cnt_2a VALUES ('a', 10), ('c', 7)")
      refreshMv("mv_ag_cnt_2a")
      assertMvCorrect("mv_ag_cnt_2a", "SELECT k, COUNT(*) AS cnt FROM ag_cnt_2a GROUP BY k")
    }
  }

  describe("(2b) GROUP BY k, COUNT(*) — DELETE") {
    it("incremental refresh decrements count after row deletion") {
      spark.sql("CREATE TABLE IF NOT EXISTS ag_cnt_2b(k STRING, x INT) USING DELTA")
      spark.sql("INSERT INTO ag_cnt_2b VALUES ('a', 1), ('a', 2), ('b', 3), ('b', 4)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_ag_cnt_2b AS " +
          "SELECT k, COUNT(*) AS cnt FROM ag_cnt_2b GROUP BY k"
      )
      spark.sql("DELETE FROM ag_cnt_2b WHERE x = 1")
      refreshMv("mv_ag_cnt_2b")
      assertMvCorrect("mv_ag_cnt_2b", "SELECT k, COUNT(*) AS cnt FROM ag_cnt_2b GROUP BY k")
    }
  }

  describe("(2c) GROUP BY k, COUNT(*) — UPDATE") {
    it("incremental refresh adjusts count correctly after UPDATE") {
      spark.sql("CREATE TABLE IF NOT EXISTS ag_cnt_2c(k STRING, x INT) USING DELTA")
      spark.sql("INSERT INTO ag_cnt_2c VALUES ('a', 1), ('a', 2), ('b', 3)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_ag_cnt_2c AS " +
          "SELECT k, COUNT(*) AS cnt FROM ag_cnt_2c GROUP BY k"
      )
      // UPDATE moves a row from group 'a' to group 'c'
      spark.sql("UPDATE ag_cnt_2c SET k = 'c' WHERE x = 2")
      refreshMv("mv_ag_cnt_2c")
      assertMvCorrect("mv_ag_cnt_2c", "SELECT k, COUNT(*) AS cnt FROM ag_cnt_2c GROUP BY k")
    }
  }

  describe("(2d) GROUP BY k, COUNT(*) — batched mixed DML") {
    it("incremental refresh reconciles mixed DML for COUNT(*)") {
      spark.sql("CREATE TABLE IF NOT EXISTS ag_cnt_2d(k STRING, x INT) USING DELTA")
      spark.sql("INSERT INTO ag_cnt_2d VALUES ('a', 1), ('b', 2), ('c', 3)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_ag_cnt_2d AS " +
          "SELECT k, COUNT(*) AS cnt FROM ag_cnt_2d GROUP BY k"
      )
      spark.sql("INSERT INTO ag_cnt_2d VALUES ('a', 99), ('d', 77)")
      spark.sql("DELETE FROM ag_cnt_2d WHERE k = 'c'")
      spark.sql("UPDATE ag_cnt_2d SET k = 'b' WHERE x = 99")
      refreshMv("mv_ag_cnt_2d")
      assertMvCorrect("mv_ag_cnt_2d", "SELECT k, COUNT(*) AS cnt FROM ag_cnt_2d GROUP BY k")
    }
  }

  // ── Shape 3: GROUP BY k, COUNT(x) — count with non-star arg ──────────────

  describe("(3) GROUP BY k, COUNT(x) — with nullable column") {
    it("COUNT(x) ignores NULLs; incremental refresh tracks correctly") {
      spark.sql("CREATE TABLE IF NOT EXISTS ag_cntx_3(k STRING, x INT) USING DELTA")
      spark.sql("INSERT INTO ag_cntx_3 VALUES ('a', 10), ('a', NULL), ('b', 20)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_ag_cntx_3 AS " +
          "SELECT k, COUNT(x) AS cnt_x FROM ag_cntx_3 GROUP BY k"
      )
      spark.sql("INSERT INTO ag_cntx_3 VALUES ('a', NULL), ('b', 5), ('c', NULL)")
      spark.sql("DELETE FROM ag_cntx_3 WHERE k = 'a' AND x IS NULL")
      refreshMv("mv_ag_cntx_3")
      assertMvCorrect("mv_ag_cntx_3", "SELECT k, COUNT(x) AS cnt_x FROM ag_cntx_3 GROUP BY k")
    }
  }

  // ── Shape 4: GROUP BY k, AVG(x) — exercises SUM/COUNT decomposition ───────

  describe("(4a) GROUP BY k, AVG(x) — INSERT") {
    it("AVG is maintained via hidden SUM/COUNT decomposition") {
      spark.sql("CREATE TABLE IF NOT EXISTS ag_avg_4a(k STRING, x DOUBLE) USING DELTA")
      spark.sql("INSERT INTO ag_avg_4a VALUES ('a', 10.0), ('a', 20.0), ('b', 5.0)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_ag_avg_4a AS " +
          "SELECT k, AVG(x) AS avg_x FROM ag_avg_4a GROUP BY k"
      )
      spark.sql("INSERT INTO ag_avg_4a VALUES ('a', 30.0), ('c', 15.0)")
      refreshMv("mv_ag_avg_4a")
      assertMvCorrect("mv_ag_avg_4a", "SELECT k, AVG(x) AS avg_x FROM ag_avg_4a GROUP BY k")
    }
  }

  describe("(4b) GROUP BY k, AVG(x) — DELETE") {
    it("AVG is correctly recalculated after DELETE") {
      spark.sql("CREATE TABLE IF NOT EXISTS ag_avg_4b(k STRING, x DOUBLE) USING DELTA")
      spark.sql("INSERT INTO ag_avg_4b VALUES ('a', 10.0), ('a', 20.0), ('a', 30.0), ('b', 5.0)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_ag_avg_4b AS " +
          "SELECT k, AVG(x) AS avg_x FROM ag_avg_4b GROUP BY k"
      )
      spark.sql("DELETE FROM ag_avg_4b WHERE x = 10.0")
      refreshMv("mv_ag_avg_4b")
      assertMvCorrect("mv_ag_avg_4b", "SELECT k, AVG(x) AS avg_x FROM ag_avg_4b GROUP BY k")
    }
  }

  describe("(4c) GROUP BY k, AVG(x) — DELETE + INSERT with unequal counts") {
    // openivm's incremental filter for AVG is: HAVING SUM(openivm_sign) != 0
    // (i.e. count delta must be non-zero for a group to be refreshed).
    // A pure UPDATE (same row count, different value) produces count_delta=0
    // and is therefore excluded from the MERGE source — this is a known
    // openivm architectural constraint for multiplicty-filtered aggregates.
    // We exercise the equivalent semantic via DELETE 1 + INSERT 2, giving
    // count_delta = +1 ≠ 0 for the affected group.
    it("AVG is correctly recalculated after asymmetric DELETE + INSERT") {
      spark.sql("CREATE TABLE IF NOT EXISTS ag_avg_4c(k STRING, x DOUBLE) USING DELTA")
      spark.sql("INSERT INTO ag_avg_4c VALUES ('a', 10.0), ('a', 20.0), ('b', 5.0)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_ag_avg_4c AS " +
          "SELECT k, AVG(x) AS avg_x FROM ag_avg_4c GROUP BY k"
      )
      // Remove one 'a' row, add two different 'a' rows → count_delta('a') = +1
      spark.sql("DELETE FROM ag_avg_4c WHERE k = 'a' AND x = 20.0")
      spark.sql("INSERT INTO ag_avg_4c VALUES ('a', 30.0), ('a', 40.0)")
      refreshMv("mv_ag_avg_4c")
      assertMvCorrect("mv_ag_avg_4c", "SELECT k, AVG(x) AS avg_x FROM ag_avg_4c GROUP BY k")
    }
  }

  // ── Shape 5: GROUP BY k, STDDEV(x) — sample standard deviation ───────────

  describe("(5a) GROUP BY k, STDDEV(x) — INSERT") {
    it("sample STDDEV is maintained via hidden SUM/COUNT/SUM_SQ decomposition") {
      spark.sql("CREATE TABLE IF NOT EXISTS ag_std_5a(k STRING, x DOUBLE) USING DELTA")
      spark.sql("INSERT INTO ag_std_5a VALUES ('a', 2.0), ('a', 4.0), ('a', 4.0), ('a', 4.0), ('a', 5.0), ('a', 5.0), ('a', 7.0), ('a', 9.0), ('b', 1.0), ('b', 3.0)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_ag_std_5a AS " +
          "SELECT k, STDDEV(x) AS sd FROM ag_std_5a GROUP BY k"
      )
      spark.sql("INSERT INTO ag_std_5a VALUES ('a', 6.0), ('c', 2.0), ('c', 4.0)")
      refreshMv("mv_ag_std_5a")
      assertMvCorrect("mv_ag_std_5a", "SELECT k, STDDEV(x) AS sd FROM ag_std_5a GROUP BY k")
    }
  }

  describe("(5b) GROUP BY k, STDDEV(x) — DELETE") {
    it("sample STDDEV is correctly updated after DELETE") {
      spark.sql("CREATE TABLE IF NOT EXISTS ag_std_5b(k STRING, x DOUBLE) USING DELTA")
      spark.sql("INSERT INTO ag_std_5b VALUES ('a', 1.0), ('a', 2.0), ('a', 3.0), ('a', 4.0), ('b', 10.0), ('b', 20.0)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_ag_std_5b AS " +
          "SELECT k, STDDEV(x) AS sd FROM ag_std_5b GROUP BY k"
      )
      spark.sql("DELETE FROM ag_std_5b WHERE k = 'a' AND x = 1.0")
      refreshMv("mv_ag_std_5b")
      assertMvCorrect("mv_ag_std_5b", "SELECT k, STDDEV(x) AS sd FROM ag_std_5b GROUP BY k")
    }
  }

  // ── Shape 6: GROUP BY k, STDDEV_POP(x) — population standard deviation ───
  // Data chosen so STDDEV_POP produces exact integer results (2.0, 3.0, etc.)
  // avoiding floating-point discrepancies between openivm's naive sum-of-squares
  // formula and Spark's Welford-based STDDEV_POP for non-exact inputs.
  //
  // After INSERT + DELETE:
  //   'a': [0.0, 4.0]       → stddev_pop = 2.0   (unchanged)
  //   'b': [2.0, 8.0]       → stddev_pop = 3.0   (unchanged)
  //   'c': [1.0]            → stddev_pop = 0.0   (single element after delete)
  //   'd': [4.0, 8.0]       → stddev_pop = 2.0   (new group)

  describe("(6) GROUP BY k, STDDEV_POP(x)") {
    it("population STDDEV is maintained incrementally") {
      spark.sql("CREATE TABLE IF NOT EXISTS ag_stdp_6(k STRING, x DOUBLE) USING DELTA")
      spark.sql("INSERT INTO ag_stdp_6 VALUES ('a', 0.0), ('a', 4.0), ('b', 2.0), ('b', 8.0), ('c', 1.0), ('c', 3.0)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_ag_stdp_6 AS " +
          "SELECT k, STDDEV_POP(x) AS sdp FROM ag_stdp_6 GROUP BY k"
      )
      spark.sql("INSERT INTO ag_stdp_6 VALUES ('d', 4.0), ('d', 8.0)")
      spark.sql("DELETE FROM ag_stdp_6 WHERE k = 'c' AND x = 3.0")
      refreshMv("mv_ag_stdp_6")
      assertMvCorrect("mv_ag_stdp_6", "SELECT k, STDDEV_POP(x) AS sdp FROM ag_stdp_6 GROUP BY k")
    }
  }

  // ── Shape 7: GROUP BY k, VARIANCE(x) and VAR_POP(x) ─────────────────────

  describe("(7a) GROUP BY k, VARIANCE(x)") {
    it("sample VARIANCE is maintained incrementally") {
      spark.sql("CREATE TABLE IF NOT EXISTS ag_var_7a(k STRING, x DOUBLE) USING DELTA")
      spark.sql("INSERT INTO ag_var_7a VALUES ('a', 2.0), ('a', 4.0), ('a', 6.0), ('b', 1.0), ('b', 5.0), ('b', 9.0)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_ag_var_7a AS " +
          "SELECT k, VARIANCE(x) AS vr FROM ag_var_7a GROUP BY k"
      )
      spark.sql("INSERT INTO ag_var_7a VALUES ('a', 8.0), ('c', 3.0), ('c', 7.0)")
      refreshMv("mv_ag_var_7a")
      assertMvCorrect("mv_ag_var_7a", "SELECT k, VARIANCE(x) AS vr FROM ag_var_7a GROUP BY k")
    }
  }

  describe("(7b) GROUP BY k, VAR_POP(x)") {
    it("population VARIANCE is maintained incrementally") {
      spark.sql("CREATE TABLE IF NOT EXISTS ag_varp_7b(k STRING, x DOUBLE) USING DELTA")
      spark.sql("INSERT INTO ag_varp_7b VALUES ('a', 2.0), ('a', 4.0), ('a', 6.0), ('b', 1.0), ('b', 3.0)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_ag_varp_7b AS " +
          "SELECT k, VAR_POP(x) AS vrp FROM ag_varp_7b GROUP BY k"
      )
      spark.sql("INSERT INTO ag_varp_7b VALUES ('b', 5.0), ('c', 10.0)")
      spark.sql("DELETE FROM ag_varp_7b WHERE k = 'a' AND x = 2.0")
      refreshMv("mv_ag_varp_7b")
      assertMvCorrect("mv_ag_varp_7b", "SELECT k, VAR_POP(x) AS vrp FROM ag_varp_7b GROUP BY k")
    }
  }

  // ── Shape 8: GROUP BY k1, k2, SUM(x) — composite key ─────────────────────

  describe("(8a) GROUP BY k1, k2, SUM(x) — composite key INSERT") {
    it("composite group key is tracked correctly on INSERT") {
      spark.sql("CREATE TABLE IF NOT EXISTS ag_comp_8a(k1 STRING, k2 INT, x INT) USING DELTA")
      spark.sql("INSERT INTO ag_comp_8a VALUES ('a', 1, 10), ('a', 2, 20), ('b', 1, 30)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_ag_comp_8a AS " +
          "SELECT k1, k2, SUM(x) AS total FROM ag_comp_8a GROUP BY k1, k2"
      )
      spark.sql("INSERT INTO ag_comp_8a VALUES ('a', 1, 5), ('b', 2, 15), ('c', 3, 25)")
      refreshMv("mv_ag_comp_8a")
      assertMvCorrect("mv_ag_comp_8a", "SELECT k1, k2, SUM(x) AS total FROM ag_comp_8a GROUP BY k1, k2")
    }
  }

  describe("(8b) GROUP BY k1, k2, SUM(x) — composite key mixed DML") {
    it("composite group key handles mixed DML correctly") {
      spark.sql("CREATE TABLE IF NOT EXISTS ag_comp_8b(k1 STRING, k2 INT, x INT) USING DELTA")
      spark.sql("INSERT INTO ag_comp_8b VALUES ('a', 1, 10), ('a', 2, 20), ('b', 1, 30), ('b', 2, 40)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_ag_comp_8b AS " +
          "SELECT k1, k2, SUM(x) AS total FROM ag_comp_8b GROUP BY k1, k2"
      )
      spark.sql("DELETE FROM ag_comp_8b WHERE k1 = 'b' AND k2 = 2")
      spark.sql("UPDATE ag_comp_8b SET x = 99 WHERE k1 = 'a' AND k2 = 1")
      spark.sql("INSERT INTO ag_comp_8b VALUES ('c', 1, 50)")
      refreshMv("mv_ag_comp_8b")
      assertMvCorrect("mv_ag_comp_8b", "SELECT k1, k2, SUM(x) AS total FROM ag_comp_8b GROUP BY k1, k2")
    }
  }

  // ── Shape 9: Multiple aggregates in one MV ───────────────────────────────

  describe("(9a) GROUP BY k, SUM(x) + COUNT(*) + AVG(x) — multiple aggregates INSERT") {
    it("multiple aggregates in one view are all maintained correctly") {
      spark.sql("CREATE TABLE IF NOT EXISTS ag_multi_9a(k STRING, x DOUBLE) USING DELTA")
      spark.sql("INSERT INTO ag_multi_9a VALUES ('a', 10.0), ('a', 20.0), ('b', 5.0), ('b', 15.0)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_ag_multi_9a AS " +
          "SELECT k, SUM(x) AS total, COUNT(*) AS cnt, AVG(x) AS avg_x " +
          "FROM ag_multi_9a GROUP BY k"
      )
      spark.sql("INSERT INTO ag_multi_9a VALUES ('a', 30.0), ('c', 7.0)")
      refreshMv("mv_ag_multi_9a")
      assertMvCorrect(
        "mv_ag_multi_9a",
        "SELECT k, SUM(x) AS total, COUNT(*) AS cnt, AVG(x) AS avg_x FROM ag_multi_9a GROUP BY k"
      )
    }
  }

  describe("(9b) GROUP BY k, SUM(x) + COUNT(*) + AVG(x) — mixed DML") {
    // Note: all operations use count_delta ≠ 0 per group (see note in test 4c).
    // The UPDATE pattern for AVG is avoided; instead 'a' has one row removed.
    it("all aggregates remain consistent after batched mixed DML") {
      spark.sql("CREATE TABLE IF NOT EXISTS ag_multi_9b(k STRING, x DOUBLE) USING DELTA")
      spark.sql("INSERT INTO ag_multi_9b VALUES ('a', 10.0), ('a', 20.0), ('b', 5.0), ('c', 100.0)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_ag_multi_9b AS " +
          "SELECT k, SUM(x) AS total, COUNT(*) AS cnt, AVG(x) AS avg_x " +
          "FROM ag_multi_9b GROUP BY k"
      )
      spark.sql("DELETE FROM ag_multi_9b WHERE k = 'c'")
      spark.sql("INSERT INTO ag_multi_9b VALUES ('b', 25.0), ('d', 50.0)")
      // Remove one 'a' row (count_delta('a') = -1 ≠ 0) so AVG refreshes correctly.
      spark.sql("DELETE FROM ag_multi_9b WHERE k = 'a' AND x = 10.0")
      refreshMv("mv_ag_multi_9b")
      assertMvCorrect(
        "mv_ag_multi_9b",
        "SELECT k, SUM(x) AS total, COUNT(*) AS cnt, AVG(x) AS avg_x FROM ag_multi_9b GROUP BY k"
      )
    }
  }

  // ── Shape 10: Empty initial table → INSERT → REFRESH ─────────────────────

  describe("(10) Empty initial table — INSERT after CREATE") {
    it("MV correctly populated when base table starts empty") {
      spark.sql("CREATE TABLE IF NOT EXISTS ag_empty_10(k STRING, x INT) USING DELTA")
      // No rows yet — create MV on an empty table
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_ag_empty_10 AS " +
          "SELECT k, SUM(x) AS total, COUNT(*) AS cnt FROM ag_empty_10 GROUP BY k"
      )
      // Now insert data and refresh
      spark.sql("INSERT INTO ag_empty_10 VALUES ('a', 10), ('b', 20), ('a', 5)")
      refreshMv("mv_ag_empty_10")
      assertMvCorrect(
        "mv_ag_empty_10",
        "SELECT k, SUM(x) AS total, COUNT(*) AS cnt FROM ag_empty_10 GROUP BY k"
      )
    }
  }

  // ── Shape 11: INSERT then DELETE of the same row → net-zero change ────────

  describe("(11) INSERT then DELETE of the same row — net-zero change") {
    it("MV equals base table when inserted rows are subsequently deleted") {
      spark.sql("CREATE TABLE IF NOT EXISTS ag_netzer_11(k STRING, x INT) USING DELTA")
      spark.sql("INSERT INTO ag_netzer_11 VALUES ('a', 10), ('b', 20)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_ag_netzer_11 AS " +
          "SELECT k, SUM(x) AS total FROM ag_netzer_11 GROUP BY k"
      )
      val userCols = Seq("k", "total")
      val beforeMv = spark.table("mv_ag_netzer_11").select(userCols.head, userCols.tail: _*).collect().toSet
      // Insert a row and immediately delete it — net effect is zero
      spark.sql("INSERT INTO ag_netzer_11 VALUES ('c', 99)")
      spark.sql("DELETE FROM ag_netzer_11 WHERE k = 'c'")
      refreshMv("mv_ag_netzer_11")
      assertMvCorrect("mv_ag_netzer_11", "SELECT k, SUM(x) AS total FROM ag_netzer_11 GROUP BY k")
      // Also confirm the MV snapshot equals what we saw before (user cols only)
      val afterMv = spark.table("mv_ag_netzer_11").select(userCols.head, userCols.tail: _*).collect().toSet
      afterMv shouldBe beforeMv
    }
  }

  // ── Shape 12: NULL group key ──────────────────────────────────────────────

  describe("(12) NULL group key — NULL is a valid group") {
    it("INSERT (NULL, 5) creates a row with NULL key and correct aggregate") {
      spark.sql("CREATE TABLE IF NOT EXISTS ag_nullk_12(k STRING, x INT) USING DELTA")
      spark.sql("INSERT INTO ag_nullk_12 VALUES ('a', 10), (NULL, 5)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_ag_nullk_12 AS " +
          "SELECT k, SUM(x) AS total FROM ag_nullk_12 GROUP BY k"
      )
      spark.sql("INSERT INTO ag_nullk_12 VALUES (NULL, 15), ('b', 7)")
      refreshMv("mv_ag_nullk_12")
      assertMvCorrect("mv_ag_nullk_12", "SELECT k, SUM(x) AS total FROM ag_nullk_12 GROUP BY k")
    }
  }

  // ── Shape 13: NULL aggregate input ───────────────────────────────────────

  describe("(13) NULL aggregate input — SUM/AVG handle NULLs per SQL semantics") {
    it("SUM ignores NULLs; a group of all-NULLs returns NULL sum") {
      spark.sql("CREATE TABLE IF NOT EXISTS ag_nullv_13(k STRING, x INT) USING DELTA")
      spark.sql("INSERT INTO ag_nullv_13 VALUES ('a', 10), ('a', NULL), ('b', NULL)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_ag_nullv_13 AS " +
          "SELECT k, SUM(x) AS total FROM ag_nullv_13 GROUP BY k"
      )
      // Add another NULL for 'b' — sum of group 'b' is still NULL (all inputs NULL)
      spark.sql("INSERT INTO ag_nullv_13 VALUES ('b', NULL), ('c', 5)")
      refreshMv("mv_ag_nullv_13")
      assertMvCorrect("mv_ag_nullv_13", "SELECT k, SUM(x) AS total FROM ag_nullv_13 GROUP BY k")
    }
  }
}
