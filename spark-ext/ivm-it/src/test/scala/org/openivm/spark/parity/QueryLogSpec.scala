package org.openivm.spark.parity

import org.apache.spark.sql.{Row, SparkSession}
import org.openivm.spark.common.{MvCatalog, RefreshSqlLogCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** End-to-end coverage of `RefreshSqlLog` instrumentation in
  * `MaterializedViewCommands` + the `SHOW OPENIVM QUERY LOG` SQL
  * statement. All table/MV names are prefixed `qlog_` to avoid Delta
  * warehouse path collisions when this spec runs in parallel with other
  * parity specs (per `Settings.parallelForkSettings` / repo convention).
  */
class QueryLogSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-query-log-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  private def buildSpark(qlogEnabled: Boolean): SparkSession =
    SparkSession
      .builder()
      .master("local[1]")
      .appName(s"openivm-spark-QueryLogSpec-${qlogEnabled}")
      .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension,org.openivm.spark.OpenIvmSparkExtensions"
      )
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.openivm.enabled", "true")
      .config("spark.openivm.queryLog.enabled", qlogEnabled.toString)
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = buildSpark(qlogEnabled = true)
    MvCatalog.ensureTables(spark)
    StagingCatalog.ensureTables(spark)
    RefreshSqlLogCatalog.ensureTables(spark)
  }

  override def afterAll(): Unit =
    try {
      if (spark != null) spark.stop()
      deleteDir(new File(warehouseDir))
    } finally {
      super.afterAll()
    }

  private def deleteDir(f: File): Unit = {
    if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(deleteDir))
    f.delete()
    ()
  }

  private def clearLog(): Unit = RefreshSqlLogCatalog.removeAll(spark)

  private def showLog(): Seq[Row] =
    spark.sql("SHOW OPENIVM QUERY LOG").collect().toSeq

  /** Column indices in the SHOW OPENIVM QUERY LOG result, mirroring
    * `ShowQueryLogCommand.output`. Kept as constants so future schema
    * changes only need updating in one place.
    */
  private val ColRefreshId  = 0
  private val ColViewName   = 1
  private val ColStmtOrder  = 3
  private val ColAttempt    = 4
  private val ColMode       = 5
  private val ColCategory   = 6
  private val ColStmtKind   = 7
  private val ColDurationMs = 8
  private val ColSqlText    = 9

  private def categoriesByRefresh(rows: Seq[Row]): Map[String, Seq[String]] =
    rows.groupBy(_.getString(ColRefreshId)).map { case (rid, rs) =>
      rid -> rs.sortBy(_.getInt(ColStmtOrder)).map(_.getString(ColCategory))
    }

  describe("RefreshSqlLog — CREATE path") {
    it("emits original_query and initial_load_ctas rows with mode=create") {
      clearLog()
      spark.sql("CREATE TABLE qlog_t1 (k INT, v INT) USING DELTA")
      spark.sql("INSERT INTO qlog_t1 VALUES (1, 10), (2, 20)")
      spark.sql(
        "CREATE MATERIALIZED VIEW qlog_mv1 AS SELECT k, SUM(v) AS s FROM qlog_t1 GROUP BY k"
      )

      val rows  = showLog()
      val byRid = categoriesByRefresh(rows)

      byRid.keys.exists(_.contains("_create_mv_")) shouldBe true
      val createRid = byRid.keys.find(_.contains("_create_mv_")).get
      val cats      = byRid(createRid).toSet
      cats should contain("original_query")
      cats should contain("initial_load_ctas")

      val createRows = rows.filter(_.getString(ColRefreshId) == createRid)
      createRows.foreach(_.getString(ColMode) shouldBe "create")

      val origRow = createRows.find(_.getString(ColCategory) == "original_query").get
      origRow.getInt(ColStmtOrder) shouldBe -1
      origRow.getLong(ColDurationMs) shouldBe -1L
      origRow.getString(ColSqlText) should include("SUM(v)")

      val initRow = createRows.find(_.getString(ColCategory) == "initial_load_ctas").get
      initRow.getInt(ColStmtOrder) shouldBe 0
      initRow.getString(ColSqlText) should include("CREATE TABLE")
    }
  }

  describe("RefreshSqlLog — REFRESH path") {
    it("emits register_source_delta + rewritten_stmt + drop_cleanup with mode=refresh") {
      clearLog()
      spark.sql("CREATE TABLE qlog_t2 (k INT, v INT) USING DELTA")
      spark.sql("INSERT INTO qlog_t2 VALUES (1, 10)")
      spark.sql(
        "CREATE MATERIALIZED VIEW qlog_mv2 AS SELECT k, SUM(v) AS s FROM qlog_t2 GROUP BY k"
      )
      spark.sql("INSERT INTO qlog_t2 VALUES (2, 20), (1, 30)")
      clearLog() // discard CREATE rows so we only assert on REFRESH

      spark.sql("REFRESH MATERIALIZED VIEW qlog_mv2").collect()

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
      spark.sql("CREATE TABLE qlog_t3 (k INT, v INT) USING DELTA")
      spark.sql("INSERT INTO qlog_t3 VALUES (1, 10)")
      spark.sql(
        "CREATE MATERIALIZED VIEW qlog_mv3 AS SELECT k, SUM(v) AS s FROM qlog_t3 GROUP BY k"
      )
      spark.sql("INSERT INTO qlog_t3 VALUES (2, 20)")
      spark.sql("REFRESH MATERIALIZED VIEW qlog_mv3").collect()

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
      spark.stop()
      spark = buildSpark(qlogEnabled = false)
      MvCatalog.ensureTables(spark)
      StagingCatalog.ensureTables(spark)
      RefreshSqlLogCatalog.ensureTables(spark)
      clearLog()

      spark.sql("CREATE TABLE qlog_t4 (k INT, v INT) USING DELTA")
      spark.sql("INSERT INTO qlog_t4 VALUES (1, 10)")
      spark.sql(
        "CREATE MATERIALIZED VIEW qlog_mv4 AS SELECT k, SUM(v) AS s FROM qlog_t4 GROUP BY k"
      )
      spark.sql("INSERT INTO qlog_t4 VALUES (2, 20)")
      spark.sql("REFRESH MATERIALIZED VIEW qlog_mv4").collect()

      val rows = showLog()
      rows shouldBe empty
    }
  }

  describe("RefreshSqlLog — hot-path budget") {
    it("10_000 record(...) calls complete in under 50 ms (≤ 5 µs each)") {
      // Re-enable the gate so record() actually allocates.
      spark.stop()
      spark = buildSpark(qlogEnabled = true)
      MvCatalog.ensureTables(spark)
      StagingCatalog.ensureTables(spark)
      org.openivm.spark.common.RefreshSqlLogCatalog.ensureTables(spark)

      val log = org.openivm.spark.commands.RefreshSqlLog.start(
        spark,
        "qlog_bench_view_1234567890",
        "qlog_bench_view",
        org.openivm.spark.commands.RefreshSqlLog.ModeRefresh
      )
      log.isActive shouldBe true

      // Warm up the JIT.
      var i = 0
      while (i < 1000) {
        log.record("rewritten_stmt", i, 0, "merge", "SELECT 1", 0L)
        i += 1
      }

      val t0 = System.nanoTime()
      i = 0
      while (i < 10000) {
        log.record("rewritten_stmt", i, 0, "merge", "SELECT 1", 0L)
        i += 1
      }
      val elapsedMs = (System.nanoTime() - t0) / 1000000L
      // Don't flush — we don't want the RocksDB write in the budget. The
      // buffer is cleared by GC when this method exits.
      withClue(s"record() 10k iterations took ${elapsedMs}ms (budget=50ms): ") {
        elapsedMs should be < 50L
      }
    }
  }
}
