package org.openivm.spark.parity

import java.io.File

import org.openivm.spark.common.{FeatureGate, MvCatalog, RefreshTypeCode, WindowStateCatalog}
import org.openivm.spark.parity.base.IvmParitySpecBase

/** o4 — running-window aux-state fast path (`windowRunningIncremental`, merge
  * `e1a6b06`).
  *
  * Exercises openivm's per-partition `openivm_aux_<view>` state program: a
  * running `SUM/COUNT/AVG(x) OVER (PARTITION BY k ORDER BY d)` MV whose refresh
  * reads the tiny persisted aux table (one row per partition) instead of
  * rescanning the full MV.  The suite runs under two gate configurations —
  * RocksDB aux state OFF (Delta-backed aux, Step A) and ON (RocksDB-backed aux,
  * Step B) — selected by [[rocksdbState]].
  *
  * Every case asserts bidirectional `EXCEPT ALL` against the live window query
  * (`assertMvCorrect`) and that the MV stays WINDOW_PARTITION (never demoted to
  * FULL_REFRESH).  Insert-only cases additionally assert the aux fast path
  * actually engaged so a silent fallback-to-full-recompute cannot pass.
  */
abstract class WindowRunningAuxStateScenarios extends IvmParitySpecBase("window-running-aux-state") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  /** Whether the RocksDB aux-state backing gate is enabled for this run. */
  protected def rocksdbState: Boolean = false

  override protected def extraSparkConf: Map[String, String] = {
    val base = Map(FeatureGate.WindowRunningIncrementalEnabledKey -> "true")
    if (rocksdbState) base + (FeatureGate.WindowRocksdbStateEnabledKey -> "true") else base
  }

  private def mvRefreshType(name: String): Int = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    MvCatalog.lookup(spark, id).getOrElse(fail(s"MV $name not found in catalog")).refreshType
  }

  /** Assert the running-window aux fast path engaged for `mvShort`.  Under the
    * Delta-backed configuration the aux program materialises the persistent
    * `_ivm/aux/<mvShort>` Delta table; under the RocksDB configuration it writes
    * per-partition rows into [[WindowStateCatalog]].  Either trace proves the
    * refresh took the aux fast path rather than a full-MV recompute. */
  private def assertAuxEngaged(mvShort: String): Unit =
    if (rocksdbState) {
      withClue(s"WindowStateCatalog state for $mvShort: ") {
        WindowStateCatalog.scanForView(spark, mvShort).nonEmpty shouldBe true
      }
    } else {
      val auxLog = new File(warehouseDir, s"_ivm/aux/$mvShort/_delta_log")
      withClue(s"aux Delta table $auxLog: ") {
        auxLog.exists() shouldBe true
      }
    }

  describe("running-window aux-state fast path") {
    it("extends a running SUM on a strict-suffix insert-only batch") {
      sql("CREATE TABLE wras_sum_src(d DATE, k STRING, x INT) USING DELTA")
      sql(
        "INSERT INTO wras_sum_src VALUES " +
          "(DATE '2024-01-01','A',10),(DATE '2024-01-02','A',20)," +
          "(DATE '2024-01-01','B',30)"
      )
      val q = "SELECT d, k, SUM(x) OVER (PARTITION BY k ORDER BY d) AS rs FROM wras_sum_src"
      sql(s"CREATE MATERIALIZED VIEW wras_sum_mv AS $q")
      assertMvCorrect("wras_sum_mv", q)

      sql(
        "INSERT INTO wras_sum_src VALUES " +
          "(DATE '2024-01-03','A',30),(DATE '2024-01-02','B',5),(DATE '2024-01-01','C',7)"
      )
      refreshMv("wras_sum_mv")
      assertMvCorrect("wras_sum_mv", q)
      mvRefreshType("wras_sum_mv") shouldBe RefreshTypeCode.WindowPartition
      assertAuxEngaged("wras_sum_mv")
    }

    it("extends a running COUNT on a strict-suffix insert-only batch") {
      sql("CREATE TABLE wras_cnt_src(d DATE, k STRING, x INT) USING DELTA")
      sql(
        "INSERT INTO wras_cnt_src VALUES " +
          "(DATE '2024-02-01','A',1),(DATE '2024-02-02','A',2)," +
          "(DATE '2024-02-01','B',3)"
      )
      val q = "SELECT d, k, COUNT(x) OVER (PARTITION BY k ORDER BY d) AS rc FROM wras_cnt_src"
      sql(s"CREATE MATERIALIZED VIEW wras_cnt_mv AS $q")
      assertMvCorrect("wras_cnt_mv", q)

      sql(
        "INSERT INTO wras_cnt_src VALUES " +
          "(DATE '2024-02-03','A',9),(DATE '2024-02-02','B',8),(DATE '2024-02-01','D',4)"
      )
      refreshMv("wras_cnt_mv")
      assertMvCorrect("wras_cnt_mv", q)
      mvRefreshType("wras_cnt_mv") shouldBe RefreshTypeCode.WindowPartition
      assertAuxEngaged("wras_cnt_mv")
    }

    ignore(
      "extends a running AVG (hidden prior-count) on a strict-suffix batch" +
        " /* TODO(openivm e1a6b06): aux seed references source column `x` absent" +
        " from openivm_data_<view> -> Binder Error; re-enable when upstream fixes" +
        " the AVG running-window aux compiler */"
    ) {
      sql("CREATE TABLE wras_avg_src(d DATE, k STRING, x INT) USING DELTA")
      sql("INSERT INTO wras_avg_src VALUES (DATE '2024-03-01','A',10),(DATE '2024-03-02','A',20)")
      val q = "SELECT d, k, AVG(x) OVER (PARTITION BY k ORDER BY d) AS ra FROM wras_avg_src"
      sql(s"CREATE MATERIALIZED VIEW wras_avg_mv AS $q")
      assertMvCorrect("wras_avg_mv", q)

      sql("INSERT INTO wras_avg_src VALUES (DATE '2024-03-03','A',30)")
      refreshMv("wras_avg_mv")
      assertMvCorrect("wras_avg_mv", q)
      mvRefreshType("wras_avg_mv") shouldBe RefreshTypeCode.WindowPartition
    }

    it("falls back to per-partition recompute for a backdated insert-only batch") {
      sql("CREATE TABLE wras_bd_src(d DATE, k STRING, x INT) USING DELTA")
      sql(
        "INSERT INTO wras_bd_src VALUES " +
          "(DATE '2024-04-01','A',10),(DATE '2024-04-05','A',20)"
      )
      val q = "SELECT d, k, SUM(x) OVER (PARTITION BY k ORDER BY d) AS rs FROM wras_bd_src"
      sql(s"CREATE MATERIALIZED VIEW wras_bd_mv AS $q")
      assertMvCorrect("wras_bd_mv", q)

      // 2024-04-03 lands BEFORE the existing 2024-04-05 max (backdated -> fallback);
      // 2024-04-09 is a clean suffix.  Both still multiplicity>0 (insert-only).
      sql(
        "INSERT INTO wras_bd_src VALUES " +
          "(DATE '2024-04-03','A',3),(DATE '2024-04-09','A',9)"
      )
      refreshMv("wras_bd_mv")
      assertMvCorrect("wras_bd_mv", q)
      mvRefreshType("wras_bd_mv") shouldBe RefreshTypeCode.WindowPartition
      assertAuxEngaged("wras_bd_mv")
    }

    it("stays correct when the max-order row of a partition is deleted") {
      sql("CREATE TABLE wras_del_src(d DATE, k STRING, x INT) USING DELTA")
      sql(
        "INSERT INTO wras_del_src VALUES " +
          "(DATE '2024-05-01','A',10),(DATE '2024-05-02','A',20),(DATE '2024-05-03','A',30)," +
          "(DATE '2024-05-01','B',5)"
      )
      val q = "SELECT d, k, SUM(x) OVER (PARTITION BY k ORDER BY d) AS rs FROM wras_del_src"
      sql(s"CREATE MATERIALIZED VIEW wras_del_mv AS $q")
      assertMvCorrect("wras_del_mv", q)

      // Deleting the current max-order row makes the batch non-insert-only, so
      // openivm compiles the classic WINDOW_PARTITION DELETE+recompute (no aux).
      sql("DELETE FROM wras_del_src WHERE d = DATE '2024-05-03' AND k = 'A'")
      refreshMv("wras_del_mv")
      assertMvCorrect("wras_del_mv", q)
      mvRefreshType("wras_del_mv") shouldBe RefreshTypeCode.WindowPartition
    }

    it("stays correct across a mixed INSERT + backdated + DELETE + UPDATE batch (stress)") {
      sql("CREATE TABLE wras_stress_src(d DATE, k STRING, x INT) USING DELTA")
      sql(
        "INSERT INTO wras_stress_src VALUES " +
          "(DATE '2024-06-01','A',10),(DATE '2024-06-02','A',20),(DATE '2024-06-03','A',30)," +
          "(DATE '2024-06-01','B',5),(DATE '2024-06-02','B',6)"
      )
      val q = "SELECT d, k, SUM(x) OVER (PARTITION BY k ORDER BY d) AS rs FROM wras_stress_src"
      sql(s"CREATE MATERIALIZED VIEW wras_stress_mv AS $q")
      assertMvCorrect("wras_stress_mv", q)

      // Batch mixes a clean suffix insert, a backdated insert, a delete, and an
      // update before ONE refresh -> classic WINDOW_PARTITION recompute; the aux
      // table (if present from a prior refresh) is invalidated to avoid stale
      // suffix state.
      sql("INSERT INTO wras_stress_src VALUES (DATE '2024-06-04','A',40),(DATE '2024-06-05','B',7)")
      sql("INSERT INTO wras_stress_src VALUES (DATE '2024-06-01','A',99)")
      sql("DELETE FROM wras_stress_src WHERE d = DATE '2024-06-02' AND k = 'A'")
      sql("UPDATE wras_stress_src SET x = 60 WHERE d = DATE '2024-06-03' AND k = 'A'")
      refreshMv("wras_stress_mv")
      assertMvCorrect("wras_stress_mv", q)
      mvRefreshType("wras_stress_mv") shouldBe RefreshTypeCode.WindowPartition
    }

    it("extends across two consecutive suffix-append refreshes (aux write-back reused)") {
      sql("CREATE TABLE wras_seq_src(d DATE, k STRING, x INT) USING DELTA")
      sql("INSERT INTO wras_seq_src VALUES (DATE '2024-07-01','A',10),(DATE '2024-07-01','B',5)")
      val q = "SELECT d, k, SUM(x) OVER (PARTITION BY k ORDER BY d) AS rs FROM wras_seq_src"
      sql(s"CREATE MATERIALIZED VIEW wras_seq_mv AS $q")

      sql("INSERT INTO wras_seq_src VALUES (DATE '2024-07-02','A',20)")
      refreshMv("wras_seq_mv")
      assertMvCorrect("wras_seq_mv", q)
      assertAuxEngaged("wras_seq_mv")

      sql("INSERT INTO wras_seq_src VALUES (DATE '2024-07-03','A',30),(DATE '2024-07-02','B',7)")
      refreshMv("wras_seq_mv")
      assertMvCorrect("wras_seq_mv", q)
      mvRefreshType("wras_seq_mv") shouldBe RefreshTypeCode.WindowPartition
      assertAuxEngaged("wras_seq_mv")
    }
  }
}
