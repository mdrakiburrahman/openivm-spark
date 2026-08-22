package org.openivm.spark.commands

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.Property
import org.apache.logging.log4j.core.layout.PatternLayout
import org.apache.logging.log4j.core.{LogEvent, Logger}
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.openivm.spark.common.{
  BatchVerdict,
  ChangeWatermark,
  MvCatalog,
  MvMetadata,
  RefreshTypeCode,
  StagingCatalog,
  StagingChangeBatch,
  StagingDelta
}
import org.openivm.spark.analyzer.IvmDmlInterceptorRule
import org.openivm.spark.compiler.CompiledRefresh
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

/**
 * End-to-end integration tests for the three materialized-view DDL commands.
 *
 * Uses a real local SparkSession with the Delta and OpenIvm extensions loaded.
 * The OpenIVM compiler subprocess must be reachable at the paths set by
 * OPENIVM_EXTENSION_PATH / OPENIVM_CLI_PATH (propagated via build.sbt envVars).
 */
class MaterializedViewCommandsSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val Json = new ObjectMapper()

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
      .master("local[4]")
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
      if (spark != null) {
        spark.stop()
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
      }
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

  private def withPool[A](parallelism: Int)(body: ExecutionContext => A): A = {
    val pool = Executors.newFixedThreadPool(parallelism)
    try {
      body(ExecutionContext.fromExecutorService(pool))
    } finally {
      pool.shutdown()
      pool.awaitTermination(30, TimeUnit.SECONDS)
    }
  }

  private def awaitResult[A](future: Future[A], timeout: FiniteDuration = 600.seconds): A =
    Await.result(future, timeout)

  private final class BufferingAppender(name: String)
      extends AbstractAppender(
        name,
        null,
        PatternLayout.createDefaultLayout(),
        false,
        Property.EMPTY_ARRAY
      ) {
    private val buffer = scala.collection.mutable.ArrayBuffer.empty[String]

    override def append(event: LogEvent): Unit =
      buffer.synchronized {
        buffer += event.getMessage.getFormattedMessage
      }

    def messages: Seq[String] = buffer.synchronized(buffer.toVector)
  }

  private def withLogCapture[A](body: BufferingAppender => A): A = {
    val appender = new BufferingAppender(s"commands-span-${UUID.randomUUID()}")
    val root     = LogManager.getRootLogger.asInstanceOf[Logger]
    appender.start()
    root.addAppender(appender)
    try body(appender)
    finally {
      root.removeAppender(appender)
      appender.stop()
    }
  }

  private def executionSpanPayloads(messages: Seq[String], materializedView: String): Seq[JsonNode] =
    messages
      .collect {
        case line if line.startsWith("OPENIVM_EXECUTION_SPAN ") =>
          Json.readTree(line.stripPrefix("OPENIVM_EXECUTION_SPAN "))
      }
      .filter(_.path("materialized_view").asText() == materializedView)

  private def withSparkLocalProperties[A](entries: (String, String)*)(body: => A): A = {
    val previous = entries.map { case (key, _) => key -> spark.sparkContext.getLocalProperty(key) }
    entries.foreach { case (key, value) => spark.sparkContext.setLocalProperty(key, value) }
    try body
    finally
      previous.foreach { case (key, value) =>
        spark.sparkContext.setLocalProperty(key, value)
      }
  }

  private def assertBagEqual(tableName: String, expectedSql: String): Unit = {
    val expected = spark.sql(expectedSql)
    val cols     = expected.columns.toSeq
    val actual   = spark.table(tableName).select(cols.head, cols.tail: _*)
    val wanted   = expected.select(cols.head, cols.tail: _*)
    withClue(s"$tableName EXCEPT ALL <expected>: ") {
      actual.exceptAll(wanted).count() shouldBe 0L
    }
    withClue(s"<expected> EXCEPT ALL $tableName: ") {
      wanted.exceptAll(actual).count() shouldBe 0L
    }
  }

  private def assertSqlBagEqual(actualSql: String, expectedSql: String): Unit = {
    val actual   = spark.sql(actualSql)
    val expected = spark.sql(expectedSql)
    withClue(s"$actualSql EXCEPT ALL $expectedSql: ") {
      actual.exceptAll(expected).count() shouldBe 0L
    }
    withClue(s"$expectedSql EXCEPT ALL $actualSql: ") {
      expected.exceptAll(actual).count() shouldBe 0L
    }
  }

  private def realDeltaSql(viewLogicalName: String): String =
    s"""INSERT INTO openivm_delta_$viewLogicalName
       |SELECT 1 AS id, CAST(1 AS INTEGER) AS openivm_multiplicity
       |""".stripMargin

  /**
   * Create a staging Delta table that holds `rows` and register it in
   * StagingCatalog.  Returns the staging path so tests can track it.
   *
   * This simulates what the DML interceptor would do automatically.
   * The bypass flag prevents the DML interceptor from re-wrapping these
   * administrative writes (staging path is not a tracked source table).
   */
  private def simulateDmlStaging(
      baseTable: String,
      stagingSubPath: String,
      rows: Seq[(String, Int)]
  ): String = {
    val stagingPath    = s"$warehouseDir/_ivm/staging/$stagingSubPath"
    val previousBypass = IvmDmlInterceptorRule.bypass.get()
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
      IvmDmlInterceptorRule.bypass.set(previousBypass)
    }
    stagingPath
  }

  // ---------------------------------------------------------------------------
  // Test 1 — CREATE happy path
  // ---------------------------------------------------------------------------
  describe("replacement-batch detection") {
    val timestamp = new Timestamp(0L)

    def stagingBatch(opType: String): StagingChangeBatch = {
      val delta = StagingDelta(
        baseTable = "default.source",
        opType = opType,
        stagingPath = "/tmp/openivm-test-delta",
        txnTs = timestamp,
        consumedBy = Seq.empty
      )
      StagingChangeBatch(
        baseTable = delta.baseTable,
        deltas = Seq(delta),
        endWatermark = ChangeWatermark.TxnTs(timestamp)
      )
    }

    it("recognizes replacement CDF verdicts and explicit staging overwrites") {
      MvCommandHelper.hasReplacementBatch(Seq.empty, Seq(BatchVerdict.Replace)) shouldBe true
      MvCommandHelper.hasReplacementBatch(
        Seq(stagingBatch(StagingDelta.OpTypes.Overwrite)),
        Seq.empty
      ) shouldBe true
    }

    it("keeps signed cascade view deltas eligible for incremental fast paths") {
      MvCommandHelper.hasReplacementBatch(
        Seq(stagingBatch(StagingDelta.OpTypes.MvViewDelta)),
        Seq.empty
      ) shouldBe false
    }

    it("skips the SIMPLE_PROJECTION apply probe for pathologically large compiled SQL") {
      val compiled = CompiledRefresh(
        refreshType = RefreshTypeCode.SimpleProjection,
        refreshTypeName = "SIMPLE_PROJECTION",
        sql = "x" * (MvCommandHelper.SimpleProjectionProbeMaxSqlChars + 1),
        initialLoadSql = ""
      )

      MvCommandHelper.computeSimpleProjectionHasDataApply(
        spark = null,
        compiled = compiled,
        name = TableIdentifier("mv_large_probe"),
        location = "target/mv_large_probe",
        qualSchemas = Map.empty,
        shortToQual = Map.empty
      ) shouldBe true
    }
  }

  describe("effective refresh classification") {
    it("keeps incremental classifications when the compiled delta is real") {
      val viewShortName = "mv_incremental_kept"
      val compiled = CompiledRefresh(
        refreshType = RefreshTypeCode.SimpleProjection,
        refreshTypeName = "SIMPLE_PROJECTION",
        sql = realDeltaSql(viewShortName),
        initialLoadSql = ""
      )

      val classification = MvCommandHelper.classifyEffectiveRefreshType(
        compiled = compiled,
        viewShortName = viewShortName,
        topKViewSpec = MvCommandHelper.TopKViewSpec(detected = false, suffixSql = None),
        simpleProjectionHasDataApply = true,
        nonCascadeUpstreamReason = None,
        rawHavingPred = None,
        aggregateHavingDataColumns = None
      )

      classification.compileRefreshTypeName shouldBe "SIMPLE_PROJECTION"
      classification.refreshType shouldBe RefreshTypeCode.SimpleProjection
      classification.refreshTypeName shouldBe "SIMPLE_PROJECTION"
      classification.reason shouldBe "kept"
      classification.isDemotionToFullRefresh shouldBe false
    }

    it("surfaces the non_cascade_upstream detail in the final classification reason") {
      val upstream = MvMetadata(
        name = TableIdentifier("upstream_mv", Some("default")),
        querySql = "SELECT 1",
        refreshType = RefreshTypeCode.SimpleProjection,
        refreshTypeName = "SIMPLE_PROJECTION",
        lastVersion = 0L,
        sourceTables = Seq.empty,
        sourceSchemaFingerprint = "fp-upstream",
        location = "target/upstream_mv",
        createdAt = new Timestamp(0L),
        properties = Map(MvMetadata.EmitsCascadeViewDeltaKey -> "false")
      )
      val reason =
        MvCommandHelper.computeNonCascadeUpstreamReason(Map("default.upstream_mv" -> upstream))

      val classification = MvCommandHelper.classifyEffectiveRefreshType(
        compiled = CompiledRefresh(
          refreshType = RefreshTypeCode.SimpleProjection,
          refreshTypeName = "SIMPLE_PROJECTION",
          sql = realDeltaSql("mv_non_cascade"),
          initialLoadSql = ""
        ),
        viewShortName = "mv_non_cascade",
        topKViewSpec = MvCommandHelper.TopKViewSpec(detected = false, suffixSql = None),
        simpleProjectionHasDataApply = true,
        nonCascadeUpstreamReason = reason,
        rawHavingPred = None,
        aggregateHavingDataColumns = None
      )

      reason shouldBe Some("non_cascade:default.upstream_mv")
      classification.refreshType shouldBe RefreshTypeCode.FullRefresh
      classification.reason shouldBe "non_cascade_upstream:non_cascade:default.upstream_mv"
    }

    it("keeps compile_failed authoritative over the generic full-refresh fallback") {
      val compiledFallback = CompiledRefresh(
        refreshType = RefreshTypeCode.FullRefresh,
        refreshTypeName = "FULL_REFRESH",
        sql = "",
        initialLoadSql = ""
      )

      val generic = MvCommandHelper.classifyEffectiveRefreshType(
        compiled = compiledFallback,
        viewShortName = "mv_compile_failed",
        topKViewSpec = MvCommandHelper.TopKViewSpec(detected = false, suffixSql = None),
        simpleProjectionHasDataApply = true,
        nonCascadeUpstreamReason = None,
        rawHavingPred = None,
        aggregateHavingDataColumns = None
      )
      val authoritative = MvCommandHelper.classifyEffectiveRefreshType(
        compiled = compiledFallback,
        viewShortName = "mv_compile_failed",
        topKViewSpec = MvCommandHelper.TopKViewSpec(detected = false, suffixSql = None),
        simpleProjectionHasDataApply = true,
        nonCascadeUpstreamReason = None,
        rawHavingPred = None,
        aggregateHavingDataColumns = None,
        authoritativeClassification = Some(
          MvCommandHelper.authoritativeFullRefreshClassification(
            compileRefreshTypeName = "COMPILE_FAILED",
            refreshReason = "compile_failed"
          )
        )
      )

      generic.reason shouldBe "no_real_delta"
      authoritative.compileRefreshTypeName shouldBe "COMPILE_FAILED"
      authoritative.refreshTypeName shouldBe "FULL_REFRESH"
      authoritative.reason shouldBe "compile_failed"
      authoritative.isDemotionToFullRefresh shouldBe true
    }
  }

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
      // No staging records → REFRESH must be a no-op
      noException should be thrownBy { spark.sql("REFRESH MATERIALIZED VIEW mv_t5") }
      assertBagEqual(
        "mv_t5",
        "SELECT region, SUM(amount) AS total FROM sales_t5 GROUP BY region"
      )
    }
  }

  describe("(5a) REFRESH execution spans reconstruct persisted classification on refresh-only runs") {
    it("reads compile/effective type and reason from metadata without recompiling") {
      spark.sql("CREATE TABLE IF NOT EXISTS sales_t5a(region STRING, amount INT) USING DELTA")
      spark.sql("INSERT INTO sales_t5a VALUES ('east', 100), ('west', 200)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_t5a_span AS " +
          "SELECT region, SUM(amount) AS total FROM sales_t5a GROUP BY region"
      )

      val meta = MvCatalog.lookup(spark, TableIdentifier("mv_t5a_span")).get
      meta.properties.contains(MvMetadata.CompileRefreshTypeKey) shouldBe true
      meta.properties.contains(MvMetadata.RefreshReasonKey) shouldBe true

      val payloads = withLogCapture { appender =>
        withSparkLocalProperties(
          "openivm.request_id" -> "req-refresh-only",
          "openivm.node_id"    -> "model.refresh.only"
        ) {
          spark.sql("REFRESH MATERIALIZED VIEW mv_t5a_span").collect()
        }
        executionSpanPayloads(appender.messages, "mv_t5a_span")
      }

      payloads should have size 1
      val payload = payloads.head
      payload.path("operation").asText() shouldBe "refresh"
      payload.path("request_id").asText() shouldBe "req-refresh-only"
      payload.path("dbt_node_id").asText() shouldBe "model.refresh.only"
      payload.path("compile_refresh_type").asText() shouldBe meta.properties(MvMetadata.CompileRefreshTypeKey)
      payload.path("effective_refresh_type").asText() shouldBe meta.refreshTypeName
      payload.path("refresh_reason").asText() shouldBe meta.properties(MvMetadata.RefreshReasonKey)
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
  // Test 12 — WINDOW_PARTITION preserves ignore-null forward-fill semantics
  // ---------------------------------------------------------------------------
  describe("(12) WINDOW_PARTITION preserves ignore-null forward-fill semantics") {
    it("stays incremental for ignoreNulls windows whose translated initial load is bag-equal") {
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
      meta.refreshType shouldBe RefreshTypeCode.WindowPartition
      meta.refreshTypeName shouldBe "WINDOW_PARTITION"
      meta.properties.contains(MvMetadata.CompiledInitialLoadSqlKey) shouldBe true

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
  // Test 13 — SIMPLE_PROJECTION remains incremental with openivm_net data apply step
  // ---------------------------------------------------------------------------
  describe("(13) SIMPLE_PROJECTION stays incremental when rewrite emits openivm_net data apply") {
    it("uses SIMPLE_PROJECTION for a VALUES-join projection whose current OpenIVM SQL emits data apply rows") {
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
      meta.refreshType shouldBe RefreshTypeCode.SimpleProjection
      meta.refreshTypeName shouldBe "SIMPLE_PROJECTION"
      meta.properties.contains(MvMetadata.CompiledInitialLoadSqlKey) shouldBe true

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
  // Test 14 — unrelated CREATE and REFRESH must overlap
  // ---------------------------------------------------------------------------
  describe("(14) Unrelated CREATE and REFRESH overlap") {
    it("CREATE on one MV does not take a global lock that blocks REFRESH on another MV") {
      spark.sql("CREATE TABLE IF NOT EXISTS sales_t14_create(region STRING, amount INT) USING DELTA")
      spark.sql("CREATE TABLE IF NOT EXISTS sales_t14_refresh(region STRING, amount INT) USING DELTA")
      spark.sql("INSERT INTO sales_t14_create VALUES ('east', 10), ('west', 20), ('north', 30)")
      spark.sql("INSERT INTO sales_t14_refresh VALUES ('east', 5), ('west', 7)")

      val createSql  = "SELECT region, SUM(amount) AS total FROM sales_t14_create GROUP BY region"
      val refreshSql = "SELECT region, SUM(amount) AS total FROM sales_t14_refresh GROUP BY region"
      spark.sql(s"CREATE MATERIALIZED VIEW mv_t14_refresh AS $refreshSql")
      spark.sql("INSERT INTO sales_t14_refresh VALUES ('north', 11)")

      val createEntered = new CountDownLatch(1)
      val releaseCreate = new CountDownLatch(1)

      CommandConcurrencyInjection.withBeforeCreateBody({
        createEntered.countDown()
        releaseCreate.await(30, TimeUnit.SECONDS) shouldBe true
      }) {
        withPool(2) { implicit ec =>
          val createFuture =
            Future { spark.sql(s"CREATE MATERIALIZED VIEW mv_t14_create AS $createSql").collect() }
          createEntered.await(30, TimeUnit.SECONDS) shouldBe true
          val refreshFuture =
            Future { spark.sql("REFRESH MATERIALIZED VIEW mv_t14_refresh").collect() }
          awaitResult(refreshFuture, 300.seconds)
          createFuture.isCompleted shouldBe false
          releaseCreate.countDown()
          awaitResult(createFuture, 600.seconds)
        }
      }

      assertBagEqual("mv_t14_refresh", refreshSql)
      assertBagEqual("mv_t14_create", createSql)
    }
  }

  describe("(14a) Same-MV CREATE execution spans") {
    it("emit one primary span per CREATE and capture queued same-MV lock waits") {
      spark.sql("CREATE TABLE IF NOT EXISTS sales_t14a_create(region STRING, amount INT) USING DELTA")
      spark.sql("INSERT INTO sales_t14a_create VALUES ('east', 10), ('west', 20)")

      val createSql     = "SELECT region, SUM(amount) AS total FROM sales_t14a_create GROUP BY region"
      val createEntered = new CountDownLatch(1)
      val releaseCreate = new CountDownLatch(1)

      val payloads = withLogCapture { appender =>
        CommandConcurrencyInjection.withBeforeCreateBody({
          createEntered.countDown()
          releaseCreate.await(30, TimeUnit.SECONDS) shouldBe true
        }) {
          withPool(2) { implicit ec =>
            val create1 = Future {
              withSparkLocalProperties(
                "openivm.request_id" -> "req-create-1",
                "openivm.node_id"    -> "model.create.one"
              ) {
                spark.sql(s"CREATE MATERIALIZED VIEW IF NOT EXISTS mv_t14a_create_span AS $createSql").collect()
              }
            }
            createEntered.await(30, TimeUnit.SECONDS) shouldBe true
            val create2 = Future {
              withSparkLocalProperties(
                "openivm.request_id" -> "req-create-2",
                "openivm.node_id"    -> "model.create.two"
              ) {
                spark.sql(s"CREATE MATERIALIZED VIEW IF NOT EXISTS mv_t14a_create_span AS $createSql").collect()
              }
            }
            Thread.sleep(150L)
            create2.isCompleted shouldBe false
            releaseCreate.countDown()
            awaitResult(create1, 600.seconds)
            awaitResult(create2, 600.seconds)
          }
        }
        executionSpanPayloads(appender.messages, "mv_t14a_create_span")
      }

      val byRequest = payloads.groupBy(_.path("request_id").asText())
      byRequest.keySet shouldBe Set("req-create-1", "req-create-2")
      byRequest.values.foreach(_.size shouldBe 1)
      byRequest("req-create-1").head.path("dbt_node_id").asText() shouldBe "model.create.one"
      val waitingCreate = byRequest("req-create-2").head
      waitingCreate.path("dbt_node_id").asText() shouldBe "model.create.two"
      waitingCreate.path("same_mv_lock_wait_ms").asLong() should be > 0L

      assertBagEqual("mv_t14a_create_span", createSql)
    }
  }

  // ---------------------------------------------------------------------------
  // Test 15 — same-MV refreshes serialize and the queued one sees newer deltas
  // ---------------------------------------------------------------------------
  describe("(15) Same-MV REFRESH requests serialize and re-read queued deltas") {
    it("queues by fully-qualified MV name so the waiting refresh sees a later batch exactly once") {
      spark.sql("CREATE TABLE IF NOT EXISTS sales_t15(region STRING, amount INT) USING DELTA")
      spark.sql("INSERT INTO sales_t15 VALUES ('seed', 1)")

      val refreshSql = "SELECT region, SUM(amount) AS total FROM sales_t15 GROUP BY region"
      spark.sql(s"CREATE MATERIALIZED VIEW mv_t15_serial AS $refreshSql")

      spark.sql(
        "INSERT INTO sales_t15 " +
          "SELECT CASE " +
          "  WHEN id % 4 = 0 THEN 'east' " +
          "  WHEN id % 4 = 1 THEN 'west' " +
          "  WHEN id % 4 = 2 THEN 'north' " +
          "  ELSE 'south' END AS region, " +
          "CAST(id AS INT) AS amount FROM range(250000)"
      )

      val payloads = withLogCapture { appender =>
        withPool(2) { implicit ec =>
          val refresh1 = Future {
            withSparkLocalProperties(
              "openivm.request_id" -> "req-refresh-1",
              "openivm.node_id"    -> "model.refresh.one"
            ) {
              spark.sql("REFRESH MATERIALIZED VIEW mv_t15_serial").collect()
            }
          }
          Thread.sleep(150L)
          val refresh2 = Future {
            withSparkLocalProperties(
              "openivm.request_id" -> "req-refresh-2",
              "openivm.node_id"    -> "model.refresh.two"
            ) {
              spark.sql("REFRESH MATERIALIZED VIEW mv_t15_serial").collect()
            }
          }
          Thread.sleep(150L)
          spark.sql(
            "INSERT INTO sales_t15 " +
              "SELECT CASE " +
              "  WHEN id % 4 = 0 THEN 'east' " +
              "  WHEN id % 4 = 1 THEN 'west' " +
              "  WHEN id % 4 = 2 THEN 'north' " +
              "  ELSE 'south' END AS region, " +
              "CAST(id + 250000 AS INT) AS amount FROM range(150000)"
          )

          awaitResult(refresh1, 600.seconds)
          refresh2.isCompleted shouldBe false
          awaitResult(refresh2, 600.seconds)
        }
        executionSpanPayloads(appender.messages, "mv_t15_serial")
      }

      val byRequest = payloads.groupBy(_.path("request_id").asText())
      byRequest.keySet shouldBe Set("req-refresh-1", "req-refresh-2")
      byRequest.values.foreach(_.size shouldBe 1)
      byRequest("req-refresh-1").head.path("dbt_node_id").asText() shouldBe "model.refresh.one"
      val waitingRefresh = byRequest("req-refresh-2").head
      waitingRefresh.path("dbt_node_id").asText() shouldBe "model.refresh.two"
      waitingRefresh.path("same_mv_lock_wait_ms").asLong() should be > 0L

      assertBagEqual("mv_t15_serial", refreshSql)
    }
  }

  // ---------------------------------------------------------------------------
  // Test 16 — DROP beside an unrelated REFRESH must not share a global lock
  // ---------------------------------------------------------------------------
  describe("(16) DROP beside an unrelated REFRESH") {
    it("drops one MV while another refresh is in flight without cross-MV blocking") {
      spark.sql("CREATE TABLE IF NOT EXISTS sales_t16_drop(region STRING, amount INT) USING DELTA")
      spark.sql("CREATE TABLE IF NOT EXISTS sales_t16_keep(region STRING, amount INT) USING DELTA")
      spark.sql("INSERT INTO sales_t16_drop VALUES ('east', 3), ('west', 4)")
      spark.sql("INSERT INTO sales_t16_keep VALUES ('east', 8), ('west', 9)")

      val keepSql = "SELECT region, SUM(amount) AS total FROM sales_t16_keep GROUP BY region"
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_t16_drop AS SELECT region, SUM(amount) AS total FROM sales_t16_drop GROUP BY region"
      )
      spark.sql(s"CREATE MATERIALIZED VIEW mv_t16_keep AS $keepSql")
      spark.sql(
        "INSERT INTO sales_t16_keep " +
          "SELECT CASE " +
          "  WHEN id % 4 = 0 THEN 'east' " +
          "  WHEN id % 4 = 1 THEN 'west' " +
          "  WHEN id % 4 = 2 THEN 'north' " +
          "  ELSE 'south' END AS region, " +
          "CAST(id AS INT) AS amount FROM range(250000)"
      )

      withPool(2) { implicit ec =>
        val refreshFuture = Future { spark.sql("REFRESH MATERIALIZED VIEW mv_t16_keep").collect() }
        Thread.sleep(150L)
        val dropFuture = Future { spark.sql("DROP MATERIALIZED VIEW mv_t16_drop").collect() }
        awaitResult(dropFuture, 300.seconds)
        refreshFuture.isCompleted shouldBe false
        awaitResult(refreshFuture, 600.seconds)
      }

      spark.catalog.tableExists("mv_t16_drop") shouldBe false
      MvCatalog.lookup(spark, TableIdentifier("mv_t16_drop")) shouldBe empty
      assertBagEqual("mv_t16_keep", keepSql)
    }
  }

  // ---------------------------------------------------------------------------
  // Test 17 — failed REFRESH can retry without replaying a successful batch
  // ---------------------------------------------------------------------------
  describe("(17) REFRESH failure + retry does not double-apply staged changes") {
    it("retries a compensated WINDOW_PARTITION refresh from a fresh command state") {
      spark.sql("CREATE TABLE IF NOT EXISTS sales_t18_window(id BIGINT, seq INT, payload BIGINT) USING DELTA")
      spark.sql(
        "INSERT INTO sales_t18_window " +
          "SELECT id, 1 AS seq, id AS payload FROM range(1001)"
      )

      val viewSql =
        "SELECT id, seq, payload, " +
          "LAG(payload) OVER (PARTITION BY id ORDER BY seq) AS previous_payload, " +
          "ROW_NUMBER() OVER (PARTITION BY id ORDER BY seq) AS row_num " +
          "FROM sales_t18_window"
      val downstreamSql = "SELECT id, seq, payload FROM mv_t18_window"

      spark.sql(s"CREATE MATERIALIZED VIEW mv_t18_window AS $viewSql")
      spark.sql(s"CREATE MATERIALIZED VIEW mv_t18_downstream AS $downstreamSql")
      spark.sql(
        "CREATE TABLE mv_t18_before_failure USING DELTA AS " +
          "SELECT id, seq, payload, previous_payload, row_num FROM mv_t18_window"
      )
      val meta = MvCatalog.lookup(spark, TableIdentifier("mv_t18_window")).get
      meta.refreshType shouldBe RefreshTypeCode.WindowPartition
      meta.refreshTypeName shouldBe "WINDOW_PARTITION"

      assertBagEqual("mv_t18_window", viewSql)
      assertBagEqual("mv_t18_downstream", downstreamSql)

      spark.sql("UPDATE sales_t18_window SET payload = payload + 100000 WHERE seq = 1")
      spark.sql(
        "INSERT INTO sales_t18_window " +
          "SELECT id, 2 AS seq, id + 200000 AS payload FROM range(1001)"
      )
      spark.sql("DELETE FROM sales_t18_window WHERE seq = 1 AND id % 11 = 0")

      RefreshFailureInjection.failNextWindowCascadeInsert(spark)
      intercept[RuntimeException] {
        spark.sql("REFRESH MATERIALIZED VIEW mv_t18_window")
      }

      assertSqlBagEqual(
        "SELECT id, seq, payload, previous_payload, row_num FROM mv_t18_window",
        "SELECT id, seq, payload, previous_payload, row_num FROM mv_t18_before_failure"
      )

      spark.sql("REFRESH MATERIALIZED VIEW mv_t18_window")
      spark.sql("REFRESH MATERIALIZED VIEW mv_t18_downstream")

      assertBagEqual("mv_t18_window", viewSql)
      assertBagEqual("mv_t18_downstream", downstreamSql)
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
