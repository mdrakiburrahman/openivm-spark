package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.apache.spark.sql.Row
import org.openivm.spark.common.RefreshSqlLogCatalog
import org.openivm.spark.testkit.HotPathBudget

import scala.concurrent.duration._

/** End-to-end coverage of `RefreshSqlLog` instrumentation in
  * `MaterializedViewCommands` + the `SHOW OPENIVM QUERY LOG` SQL
  * statement. All table/MV names are prefixed `qlog_` to avoid Delta
  * warehouse path collisions when this spec runs in parallel with other
  * parity specs (per `Settings.parallelForkSettings` / repo convention).
  */
abstract class QueryLogScenarios extends IvmParitySpecBase("query-log") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  override protected def extraSparkConf: Map[String, String] =
    Map("spark.openivm.queryLog.enabled" -> "true")

  override def beforeAll(): Unit = {
    super.beforeAll()
    RefreshSqlLogCatalog.ensureTables(spark)
  }

  protected def clearLog(): Unit = RefreshSqlLogCatalog.removeAll(spark)

  protected def showLog(): Seq[Row] =
    sql("SHOW OPENIVM QUERY LOG").collect().toSeq

  /** Column indices in the SHOW OPENIVM QUERY LOG result, mirroring
    * `ShowQueryLogCommand.output`. Kept as constants so future schema
    * changes only need updating in one place.
    */
  protected val ColRefreshId  = 0
  protected val ColViewName   = 1
  protected val ColStmtOrder  = 3
  protected val ColAttempt    = 4
  protected val ColMode       = 5
  protected val ColCategory   = 6
  protected val ColStmtKind   = 7
  protected val ColDurationMs = 8
  protected val ColSqlText    = 9

  protected def categoriesByRefresh(rows: Seq[Row]): Map[String, Seq[String]] =
    rows.groupBy(_.getString(ColRefreshId)).map { case (rid, rs) =>
      rid -> rs.sortBy(_.getInt(ColStmtOrder)).map(_.getString(ColCategory))
    }

  describe("RefreshSqlLog — CREATE path") {
    it("emits path CTAS and named registration rows in stable order") {
      clearLog()
      sql("CREATE TABLE qlog_t1 (k INT, v INT) USING DELTA")
      sql("INSERT INTO qlog_t1 VALUES (1, 10), (2, 20)")
      sql(
        "CREATE MATERIALIZED VIEW qlog_mv1 AS SELECT k, SUM(v) AS s FROM qlog_t1 GROUP BY k"
      )

      val rows  = showLog()
      val byRid = categoriesByRefresh(rows)

      byRid.keys.exists(_.contains("_create_mv_")) shouldBe true
      val createRid = byRid.keys.find(_.contains("_create_mv_")).get
      val cats      = byRid(createRid).toSet
      cats should contain("original_query")
      cats should contain("initial_load_ctas")
      cats should contain("catalog_registration")

      val createRows = rows.filter(_.getString(ColRefreshId) == createRid)
      createRows.foreach(_.getString(ColMode) shouldBe "create")

      val origRow = createRows.find(_.getString(ColCategory) == "original_query").get
      origRow.getInt(ColStmtOrder) shouldBe -1
      origRow.getLong(ColDurationMs) shouldBe -1L
      origRow.getString(ColSqlText) should include("SUM(v)")

      val initRow = createRows.find(_.getString(ColCategory) == "initial_load_ctas").get
      initRow.getInt(ColStmtOrder) shouldBe 0
      initRow.getString(ColSqlText) should include("CREATE TABLE delta.`")

      val registrationRow = createRows.find(_.getString(ColCategory) == "catalog_registration").get
      registrationRow.getInt(ColStmtOrder) shouldBe 1
      registrationRow.getString(ColSqlText) should include("CREATE TABLE IF NOT EXISTS")
      registrationRow.getString(ColSqlText) should include("USING DELTA LOCATION")
      registrationRow.getString(ColSqlText) should not include " AS SELECT "
    }

    it("logs an optional backing user view after catalog registration") {
      clearLog()
      sql("CREATE TABLE qlog_t1_backing (k INT, v INT) USING DELTA")
      sql("INSERT INTO qlog_t1_backing VALUES (1, 10), (1, 20), (2, 5)")
      sql(
        "CREATE MATERIALIZED VIEW qlog_mv1_backing AS " +
          "SELECT k, SUM(v) AS s FROM qlog_t1_backing GROUP BY k HAVING SUM(v) > 10"
      )

      val rows      = showLog()
      val createRid = categoriesByRefresh(rows).keys.find(_.contains("_create_mv_")).get
      val createRows = rows
        .filter(_.getString(ColRefreshId) == createRid)
        .sortBy(_.getInt(ColStmtOrder))

      createRows.map(_.getString(ColCategory)) should contain inOrderOnly (
        "original_query",
        "initial_load_ctas",
        "catalog_registration",
        "backing_user_view"
      )
      createRows
        .find(_.getString(ColCategory) == "backing_user_view")
        .get
        .getInt(ColStmtOrder) shouldBe 2
    }
  }

  describe("RefreshSqlLog — REFRESH path") {
    it("emits register_source_delta + rewritten_stmt + drop_cleanup with mode=refresh") {
      clearLog()
      sql("CREATE TABLE qlog_t2 (k INT, v INT) USING DELTA")
      sql("INSERT INTO qlog_t2 VALUES (1, 10)")
      sql(
        "CREATE MATERIALIZED VIEW qlog_mv2 AS SELECT k, SUM(v) AS s FROM qlog_t2 GROUP BY k"
      )
      sql("INSERT INTO qlog_t2 VALUES (2, 20), (1, 30)")
      clearLog() // discard CREATE rows so we only assert on REFRESH

      sql("REFRESH MATERIALIZED VIEW qlog_mv2").collect()

      val rows  = showLog()
      val byRid = categoriesByRefresh(rows)

      byRid.keys.exists(rid => !rid.contains("_create_mv_")) shouldBe true
      val refreshRid = byRid.keys.find(rid => !rid.contains("_create_mv_")).get
      val cats       = byRid(refreshRid).toSet

      cats should contain("original_query")
      cats should contain("register_source_delta")
      cats should contain("rewritten_stmt")
      cats should contain("drop_cleanup")

      val refreshRows = rows.filter(_.getString(ColRefreshId) == refreshRid)
      refreshRows.foreach(_.getString(ColMode) shouldBe "refresh")

      // Every non-original_query row should have a non-empty SQL text.
      refreshRows
        .filter(_.getString(ColCategory) != "original_query")
        .foreach(r => r.getString(ColSqlText).trim should not be empty)
    }
  }

  describe("RefreshSqlLog — stmt_order monotonicity") {
    it("stmt_order is monotonic per refresh after the leading original_query row") {
      clearLog()
      sql("CREATE TABLE qlog_t3 (k INT, v INT) USING DELTA")
      sql("INSERT INTO qlog_t3 VALUES (1, 10)")
      sql(
        "CREATE MATERIALIZED VIEW qlog_mv3 AS SELECT k, SUM(v) AS s FROM qlog_t3 GROUP BY k"
      )
      sql("INSERT INTO qlog_t3 VALUES (2, 20)")
      sql("REFRESH MATERIALIZED VIEW qlog_mv3").collect()

      val rows = showLog()
      rows.size should be > 0

      // Within each refresh, stmt_order is monotonically increasing.
      // original_query uses -1 by convention, so skip it.
      rows.groupBy(_.getString(ColRefreshId)).foreach { case (_, rs) =>
        val orders =
          rs.filter(_.getString(ColCategory) != "original_query").map(_.getInt(ColStmtOrder))
        orders.sliding(2).foreach {
          case Seq(a, b) => a should be < b
          case _         => () // single-element or empty
        }
      }
    }
  }

  describe("RefreshSqlLog — query log gate OFF") {
    it("SHOW OPENIVM QUERY LOG returns 0 rows after CREATE/REFRESH when gate is off") {
      restartSpark(Map("spark.openivm.queryLog.enabled" -> "false"))
      RefreshSqlLogCatalog.ensureTables(spark)
      clearLog()

      sql("CREATE TABLE qlog_t4 (k INT, v INT) USING DELTA")
      sql("INSERT INTO qlog_t4 VALUES (1, 10)")
      sql(
        "CREATE MATERIALIZED VIEW qlog_mv4 AS SELECT k, SUM(v) AS s FROM qlog_t4 GROUP BY k"
      )
      sql("INSERT INTO qlog_t4 VALUES (2, 20)")
      sql("REFRESH MATERIALIZED VIEW qlog_mv4").collect()

      val rows = showLog()
      rows shouldBe empty
    }
  }

  describe("RefreshSqlLog — hot-path budget") {

    /** How much slower than a bare buffered append `record()` may be.  Also
      * the relief factor: a JVM whose reference workload measures 10x its
      * idle cost gets 10x the budget.  `record()` measures ~3.5x idle and
      * ~5.7x under a 34-suite parallel load (the metrics update degrades
      * faster than a plain append under cache pressure), so the ratio itself
      * is not a stable enough quantity to assert on directly — it is used
      * only to scale the budget.
      */
    val RecordRatioBudget = 8.0

    /** The original absolute gate, retained as a floor so this assertion never
      * becomes stricter than it used to be on an idle box.  It only relaxes
      * above this when the calibration proves the JVM itself is slower right
      * now (full-suite runs oversubscribe the machine 4x).
      */
    val RecordFloor = 50.millis

    it("10_000 record(...) calls stay within a small constant factor of a bare buffered append") {
      restartSpark(Map("spark.openivm.queryLog.enabled" -> "true"))
      org.openivm.spark.common.RefreshSqlLogCatalog.ensureTables(spark)

      val log = org.openivm.spark.commands.RefreshSqlLog.start(
        spark,
        "qlog_bench_view_1234567890",
        "qlog_bench_view",
        org.openivm.spark.commands.RefreshSqlLog.ModeRefresh
      )
      log.isActive shouldBe true

      // Don't flush — we don't want the RocksDB write in the budget. The
      // buffer is cleared by GC when this method exits.
      val measured = HotPathBudget.measure("RefreshSqlLog.record", iterations = 10000) { iterations =>
        var i = 0
        while (i < iterations) {
          log.record("rewritten_stmt", i, 0, "merge", "SELECT 1", 0L)
          i += 1
        }
      }
      info(measured.describe(RecordRatioBudget, RecordFloor))

      // The budget is the original constant, relaxed only by a slowdown that
      // the reference workload measured in this same JVM, in this same
      // instant. It is never tightened below the constant, and a hot path
      // that starts doing IO / serialization / locking blows past it at any
      // load level.
      withClue(measured.describe(RecordRatioBudget, RecordFloor) + ": ") {
        measured.subjectNanos should be <= measured.budgetNanos(RecordRatioBudget, RecordFloor)
      }
    }
  }
}
