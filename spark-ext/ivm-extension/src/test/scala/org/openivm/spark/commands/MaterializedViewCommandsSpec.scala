package org.openivm.spark.commands

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.openivm.spark.common.{MvCatalog, MvMetadata, RefreshTypeCode, StagingCatalog, StagingDelta}
import org.openivm.spark.analyzer.IvmDmlInterceptorRule
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.sql.Timestamp
import java.util.UUID

/**
 * End-to-end integration tests for the three materialized-view DDL commands.
 *
 * Uses a real local SparkSession with the Delta and OpenIvm extensions loaded.
 * The OpenIVM compiler subprocess must be reachable at the paths set by
 * OPENIVM_EXTENSION_PATH / OPENIVM_CLI_PATH (propagated via build.sbt envVars).
 */
class MaterializedViewCommandsSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-cmds-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-CommandsSpec")
      .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension,org.openivm.spark.OpenIvmSparkExtensions"
      )
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.openivm.enabled", "true")
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config("spark.ui.enabled", "false")
      // Suppress noisy Delta / Spark log lines in test output
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()

    // Ensure catalog tables exist once for the whole suite
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

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /** Fully delete a directory tree (best-effort). */
  private def deleteDir(f: File): Unit = {
    if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(deleteDir))
    f.delete()
    ()
  }

  /**
   * Create a staging Delta table that holds `rows` and register it in
   * StagingCatalog.  Returns the staging path so tests can track it.
   *
   * This simulates what the P4.dml DML interceptor would do automatically.
   * The bypass flag prevents the DML interceptor from re-wrapping these
   * administrative writes (staging path is not a tracked source table).
   */
  private def simulateDmlStaging(
      baseTable: String,
      stagingSubPath: String,
      rows: Seq[(String, Int)]
  ): String = {
    val stagingPath = s"$warehouseDir/_ivm/staging/$stagingSubPath"
    IvmDmlInterceptorRule.bypass.set(true)
    try {
      spark.sql(
        s"""CREATE TABLE IF NOT EXISTS delta.`$stagingPath` USING DELTA LOCATION '$stagingPath'
            AS SELECT col1 AS region, col2 AS amount
            FROM VALUES ${rows.map { case (r, a) => s"('$r', $a)" }.mkString(", ")}
            AS t(col1, col2)
         """
      )
      StagingCatalog.record(
        spark,
        StagingDelta(
          baseTable = baseTable,
          opType = "INSERT",
          stagingPath = stagingPath,
          txnTs = new Timestamp(System.currentTimeMillis()),
          consumedBy = Seq.empty
        )
      )
    } finally {
      IvmDmlInterceptorRule.bypass.set(false)
    }
    stagingPath
  }

  // ---------------------------------------------------------------------------
  // Test 1 — CREATE happy path
  // ---------------------------------------------------------------------------
  describe("(1) CREATE MATERIALIZED VIEW — happy path") {
    it("creates the MV table, loads initial data, and registers catalog entry") {
      spark.sql("CREATE TABLE IF NOT EXISTS sales_t1(region STRING, amount INT) USING DELTA")
      spark.sql("INSERT INTO sales_t1 VALUES ('east', 100), ('west', 200)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_t1 AS " +
          "SELECT region, SUM(amount) AS total FROM sales_t1 GROUP BY region"
      )

      // The MV table must exist in the Spark catalog
      spark.catalog.tableExists("mv_t1") shouldBe true

      // Initial load: 2 distinct regions → 2 rows
      spark.table("mv_t1").count() shouldBe 2L

      // Catalog metadata must be present
      MvCatalog.lookup(spark, TableIdentifier("mv_t1")) should not be empty
    }
  }

  // ---------------------------------------------------------------------------
  // Test 2 — CREATE … IF NOT EXISTS on an existing MV (no-op)
  // ---------------------------------------------------------------------------
  describe("(2) CREATE MATERIALIZED VIEW IF NOT EXISTS — already exists → no-op") {
    it("silently does nothing when the MV already exists") {
      spark.sql("CREATE TABLE IF NOT EXISTS sales_t2(region STRING, amount INT) USING DELTA")
      spark.sql("INSERT INTO sales_t2 VALUES ('north', 50)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_t2 AS SELECT region, SUM(amount) AS total FROM sales_t2 GROUP BY region"
      )
      // Second CREATE with IF NOT EXISTS must not throw
      noException should be thrownBy {
        spark.sql(
          "CREATE MATERIALIZED VIEW IF NOT EXISTS mv_t2 AS SELECT region, SUM(amount) AS total FROM sales_t2 GROUP BY region"
        )
      }
      // Row count unchanged
      spark.table("mv_t2").count() shouldBe 1L
    }
  }

  // ---------------------------------------------------------------------------
  // Test 3 — CREATE without IF NOT EXISTS on an existing MV → AnalysisException
  // ---------------------------------------------------------------------------
  describe("(3) CREATE MATERIALIZED VIEW — duplicate without IF NOT EXISTS → error") {
    it("throws AnalysisException when the MV already exists") {
      spark.sql("CREATE TABLE IF NOT EXISTS sales_t3(region STRING, amount INT) USING DELTA")
      spark.sql("INSERT INTO sales_t3 VALUES ('south', 75)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_t3 AS SELECT region, SUM(amount) AS total FROM sales_t3 GROUP BY region"
      )
      an[AnalysisException] should be thrownBy {
        spark.sql(
          "CREATE MATERIALIZED VIEW mv_t3 AS SELECT region, SUM(amount) AS total FROM sales_t3 GROUP BY region"
        )
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Test 4 — REFRESH MATERIALIZED VIEW reflects new data
  // ---------------------------------------------------------------------------
  describe("(4) REFRESH MATERIALIZED VIEW — new row appears after staging + refresh") {
    it("MV gains the new aggregate row after REFRESH; EXCEPT ALL check passes both ways") {
      spark.sql("CREATE TABLE IF NOT EXISTS sales_t4(region STRING, amount INT) USING DELTA")
      spark.sql("INSERT INTO sales_t4 VALUES ('east', 100), ('west', 200)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_t4 AS SELECT region, SUM(amount) AS total FROM sales_t4 GROUP BY region"
      )
      spark.table("mv_t4").count() shouldBe 2L

      // Insert a new row into the base table — the DML interceptor records
      // a staging Delta entry automatically.
      spark.sql("INSERT INTO sales_t4 VALUES ('north', 300)")

      // Refresh
      spark.sql("REFRESH MATERIALIZED VIEW mv_t4")

      // The MV must now reflect all current data in sales_t4.
      // Project the user-visible columns (the openivm-emitted initial load
      // includes a hidden openivm_count_star bookkeeping column).
      val expected = spark.sql(
        "SELECT region, SUM(amount) AS total FROM sales_t4 GROUP BY region"
      )
      val mv = spark.table("mv_t4").select("region", "total")

      // Cross-check: EXCEPT ALL in both directions must be empty
      mv.exceptAll(expected).count() shouldBe 0L
      expected.exceptAll(mv).count() shouldBe 0L
    }
  }

  // ---------------------------------------------------------------------------
  // Test 5 — REFRESH with no pending delta → no-op
  // ---------------------------------------------------------------------------
  describe("(5) REFRESH MATERIALIZED VIEW — no pending delta → no-op") {
    it("returns without error and without changing the MV when no staging rows exist") {
      spark.sql("CREATE TABLE IF NOT EXISTS sales_t5(region STRING, amount INT) USING DELTA")
      spark.sql("INSERT INTO sales_t5 VALUES ('center', 500)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_t5 AS SELECT region, SUM(amount) AS total FROM sales_t5 GROUP BY region"
      )
      val countBefore = spark.table("mv_t5").count()
      // No staging records → REFRESH must be a no-op
      noException should be thrownBy { spark.sql("REFRESH MATERIALIZED VIEW mv_t5") }
      spark.table("mv_t5").count() shouldBe countBefore
    }
  }

  // ---------------------------------------------------------------------------
  // Test 6 — REFRESH after schema change → AnalysisException
  // ---------------------------------------------------------------------------
  describe("(6) REFRESH MATERIALIZED VIEW — source schema changed → error") {
    it("throws AnalysisException containing 'schema' when source table gains a column") {
      spark.sql("CREATE TABLE IF NOT EXISTS sales_t6(region STRING, amount INT) USING DELTA")
      spark.sql("INSERT INTO sales_t6 VALUES ('a', 1)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_t6 AS SELECT region, SUM(amount) AS total FROM sales_t6 GROUP BY region"
      )

      // Add a column to the source table after CREATE MV — fingerprint should mismatch
      spark.sql("ALTER TABLE sales_t6 ADD COLUMN extra INT")

      // Put a dummy staging record so the early-exit guard doesn't fire
      simulateDmlStaging("default.sales_t6", "sales_t6/txn_schema", Seq("a" -> 1))

      val ex = intercept[AnalysisException] {
        spark.sql("REFRESH MATERIALIZED VIEW mv_t6")
      }
      ex.getMessage should include("schema")
    }
  }

  // ---------------------------------------------------------------------------
  // Test 7 — DROP MATERIALIZED VIEW
  // ---------------------------------------------------------------------------
  describe("(7) DROP MATERIALIZED VIEW") {
    it("removes the table, the catalog entry, and the storage directory") {
      spark.sql("CREATE TABLE IF NOT EXISTS sales_t7(region STRING, amount INT) USING DELTA")
      spark.sql("INSERT INTO sales_t7 VALUES ('x', 10)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_t7 AS SELECT region, SUM(amount) AS total FROM sales_t7 GROUP BY region"
      )
      val meta = MvCatalog.lookup(spark, TableIdentifier("mv_t7")).get
      val loc  = new java.io.File(meta.location)

      spark.sql("DROP MATERIALIZED VIEW mv_t7")

      // Spark table must be gone
      spark.catalog.tableExists("mv_t7") shouldBe false
      // Catalog entry must be gone
      MvCatalog.lookup(spark, TableIdentifier("mv_t7")) shouldBe empty
      // Physical directory must be gone
      loc.exists() shouldBe false
    }
  }

  // ---------------------------------------------------------------------------
  // Test 8 — DROP IF EXISTS on a non-existent MV → no error
  // ---------------------------------------------------------------------------
  describe("(8) DROP MATERIALIZED VIEW IF EXISTS — non-existent → no-op") {
    it("silently does nothing for a view that was never created") {
      noException should be thrownBy {
        spark.sql("DROP MATERIALIZED VIEW IF EXISTS mv_does_not_exist_8")
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Test 9 — DROP without IF EXISTS on a non-existent MV → AnalysisException
  // ---------------------------------------------------------------------------
  describe("(9) DROP MATERIALIZED VIEW — non-existent without IF EXISTS → error") {
    it("throws AnalysisException when the view does not exist") {
      an[AnalysisException] should be thrownBy {
        spark.sql("DROP MATERIALIZED VIEW mv_does_not_exist_9")
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Test 10 — End-to-end conflicting-DML stress
  // ---------------------------------------------------------------------------
  describe("(10) End-to-end conflicting-DML stress") {
    it("INSERT + DELETE + UPDATE on the same keys; single REFRESH; MV == ground-truth SELECT (EXCEPT ALL both ways)") {
      spark.sql(
        "CREATE TABLE IF NOT EXISTS sales_t10(region STRING, amount INT) USING DELTA"
      )
      // Seed data
      spark.sql("INSERT INTO sales_t10 VALUES ('east', 100), ('west', 200), ('north', 50)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_t10 AS " +
          "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM sales_t10 GROUP BY region"
      )
      spark.table("mv_t10").count() shouldBe 3L

      // Conflicting DML — the DML interceptor records DELETE / INSERT /
      // UPDATE_BEFORE+UPDATE_AFTER staging entries automatically.
      spark.sql("DELETE FROM sales_t10 WHERE region = 'north'")
      spark.sql("INSERT INTO sales_t10 VALUES ('east', 50), ('south', 300)")
      spark.sql("UPDATE sales_t10 SET amount = 250 WHERE region = 'west'")

      // Single REFRESH replays all pending deltas.
      spark.sql("REFRESH MATERIALIZED VIEW mv_t10")

      val groundTruth = spark.sql(
        "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM sales_t10 GROUP BY region"
      )
      // Project user-visible columns; the openivm-emitted initial load may
      // add a hidden openivm_count_star bookkeeping column.
      val mv = spark.table("mv_t10").select("region", "total", "cnt")

      // Cross-check with EXCEPT ALL in both directions
      mv.exceptAll(groundTruth).count() shouldBe 0L
      groundTruth.exceptAll(mv).count() shouldBe 0L
    }
  }

  // ---------------------------------------------------------------------------
  // Test 11 — downstream demotion uses persisted cascade capability
  // ---------------------------------------------------------------------------
  describe("(11) MV-over-MV demotion uses the persisted cascade capability") {
    it("demotes downstreams and synthesizes OVERWRITE triggers when an upstream opts out of cascade view-delta") {
      spark.sql("CREATE TABLE IF NOT EXISTS sales_t11(region STRING, amount INT) USING DELTA")
      spark.sql("INSERT INTO sales_t11 VALUES ('east', 100), ('west', 200)")
      spark.sql("CREATE MATERIALIZED VIEW mv_t11_up AS SELECT region, amount FROM sales_t11")

      val upstreamMeta = MvCatalog.lookup(spark, TableIdentifier("mv_t11_up")).get
      MvCatalog.upsert(
        spark,
        upstreamMeta.copy(
          properties = upstreamMeta.properties ++ MvMetadata.cascadeViewDeltaProperties(false)
        )
      )

      spark.sql(
        "CREATE MATERIALIZED VIEW mv_t11_down AS " +
          "SELECT region, COUNT(*) AS cnt FROM mv_t11_up GROUP BY region"
      )

      val downstreamMeta = MvCatalog.lookup(spark, TableIdentifier("mv_t11_down")).get
      downstreamMeta.refreshType shouldBe RefreshTypeCode.FullRefresh
      downstreamMeta.refreshTypeName shouldBe "FULL_REFRESH"

      spark.sql("INSERT INTO sales_t11 VALUES ('east', 50), ('north', 300)")
      spark.sql("REFRESH MATERIALIZED VIEW mv_t11_up")

      val pending = StagingCatalog.collectFor(
        spark,
        MvCommandHelper.metaName(downstreamMeta.name),
        downstreamMeta.sourceTables,
        downstreamMeta.sourceWatermarks
      )
      pending should not be empty
      pending.map(_.opType).toSet shouldBe Set(StagingDelta.OpTypes.Overwrite)

      noException should be thrownBy {
        spark.sql("REFRESH MATERIALIZED VIEW mv_t11_down")
      }

      val expected = spark.sql(
        "SELECT region, COUNT(*) AS cnt FROM mv_t11_up GROUP BY region"
      )
      val mv = spark.table("mv_t11_down").select("region", "cnt")
      mv.exceptAll(expected).count() shouldBe 0L
      expected.exceptAll(mv).count() shouldBe 0L
    }
  }

  // ---------------------------------------------------------------------------
  // Test 12 — WINDOW_PARTITION demotes when initial-load SQL changes semantics
  // ---------------------------------------------------------------------------
  describe("(12) WINDOW_PARTITION demotes when initial-load SQL is not bag-equal") {
    it("falls back to FULL_REFRESH for ignoreNulls windows whose translated initial load drops semantics") {
      spark.sql(
        "CREATE TABLE IF NOT EXISTS sales_t12_window(id INT, customer_id INT, effective_ts TIMESTAMP, status STRING) USING DELTA"
      )
      spark.sql(
        "INSERT INTO sales_t12_window VALUES " +
          "(1, 10, TIMESTAMP'2024-01-01 09:00:00', 'bronze'), " +
          "(2, 10, TIMESTAMP'2024-01-02 09:00:00', NULL), " +
          "(3, 20, TIMESTAMP'2024-01-01 12:00:00', 'starter')"
      )

      val viewBody =
        "SELECT id, customer_id, effective_ts, " +
          "last_value(status, true) OVER (PARTITION BY customer_id ORDER BY effective_ts) AS carried_status " +
          "FROM sales_t12_window"
      spark.sql(s"CREATE MATERIALIZED VIEW mv_t12_window AS $viewBody")

      val meta = MvCatalog.lookup(spark, TableIdentifier("mv_t12_window")).get
      meta.refreshType shouldBe RefreshTypeCode.FullRefresh
      meta.refreshTypeName shouldBe "FULL_REFRESH"
      meta.properties.contains(MvMetadata.CompiledSqlKey) shouldBe false

      val expectedCreate = spark.sql(viewBody)
      val mvCreate       = spark.table("mv_t12_window").select("id", "customer_id", "effective_ts", "carried_status")
      mvCreate.exceptAll(expectedCreate).count() shouldBe 0L
      expectedCreate.exceptAll(mvCreate).count() shouldBe 0L

      spark.sql("INSERT INTO sales_t12_window VALUES (4, 10, TIMESTAMP'2024-01-04 09:00:00', NULL)")
      spark.sql("REFRESH MATERIALIZED VIEW mv_t12_window")

      val expectedRefresh = spark.sql(viewBody)
      val mvRefresh       = spark.table("mv_t12_window").select("id", "customer_id", "effective_ts", "carried_status")
      mvRefresh.exceptAll(expectedRefresh).count() shouldBe 0L
      expectedRefresh.exceptAll(mvRefresh).count() shouldBe 0L
    }
  }

  // ---------------------------------------------------------------------------
  // Test 13 — SIMPLE_PROJECTION demotes when refresh SQL has no data apply step
  // ---------------------------------------------------------------------------
  describe("(13) SIMPLE_PROJECTION demotes when rewrite emits no data apply statement") {
    it("falls back to FULL_REFRESH for a VALUES-join projection whose incremental SQL only emits view-delta rows") {
      spark.sql(
        "CREATE TABLE IF NOT EXISTS sales_t13_value_customers(id INT, credit STRING, last_name STRING) USING DELTA"
      )
      spark.sql(
        "INSERT INTO sales_t13_value_customers VALUES (1, 'GC', 'Able'), (2, 'BC', 'Baker'), (3, 'XX', 'Cross')"
      )

      val viewBody =
        "SELECT c.id, c.last_name, v.priority " +
          "FROM sales_t13_value_customers c " +
          "JOIN (VALUES ('GC', 1), ('BC', 2)) AS v(credit, priority) ON c.credit = v.credit"
      spark.sql(s"CREATE MATERIALIZED VIEW mv_t13_value_join AS $viewBody")

      val meta = MvCatalog.lookup(spark, TableIdentifier("mv_t13_value_join")).get
      meta.refreshType shouldBe RefreshTypeCode.FullRefresh
      meta.refreshTypeName shouldBe "FULL_REFRESH"
      meta.properties.contains(MvMetadata.CompiledSqlKey) shouldBe false

      val expectedCreate = spark.sql(viewBody)
      val mvCreate       = spark.table("mv_t13_value_join").select("id", "last_name", "priority")
      mvCreate.exceptAll(expectedCreate).count() shouldBe 0L
      expectedCreate.exceptAll(mvCreate).count() shouldBe 0L

      spark.sql("UPDATE sales_t13_value_customers SET credit = 'GC' WHERE id = 3")
      spark.sql("REFRESH MATERIALIZED VIEW mv_t13_value_join")

      val expectedRefresh = spark.sql(viewBody)
      val mvRefresh       = spark.table("mv_t13_value_join").select("id", "last_name", "priority")
      mvRefresh.exceptAll(expectedRefresh).count() shouldBe 0L
      expectedRefresh.exceptAll(mvRefresh).count() shouldBe 0L
    }
  }

  // ---------------------------------------------------------------------------
  // Test helper: REFRESH on a non-existent MV → AnalysisException
  // ---------------------------------------------------------------------------
  describe("REFRESH MATERIALIZED VIEW — non-existent → error") {
    it("throws AnalysisException when the view has not been created") {
      an[AnalysisException] should be thrownBy {
        spark.sql("REFRESH MATERIALIZED VIEW mv_ghost_11")
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Delta version is advanced after CREATE
  // ---------------------------------------------------------------------------
  describe("MvCatalog.advance is called after CREATE") {
    it("lastVersion in MvMetadata is >= 0 after CREATE") {
      spark.sql("CREATE TABLE IF NOT EXISTS sales_t12(region STRING, amount INT) USING DELTA")
      spark.sql("INSERT INTO sales_t12 VALUES ('z', 1)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_t12 AS SELECT region, SUM(amount) AS total FROM sales_t12 GROUP BY region"
      )
      val meta = MvCatalog.lookup(spark, TableIdentifier("mv_t12")).get
      meta.lastVersion should be >= 0L
    }
  }
}
