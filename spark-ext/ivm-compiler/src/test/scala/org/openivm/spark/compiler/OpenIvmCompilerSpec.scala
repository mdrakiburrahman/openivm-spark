package org.openivm.spark.compiler

import java.nio.channels.FileChannel
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.time.LocalDate
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.openivm.spark.common.{ForeignKeyRelation, WorkloadFacts}
import org.openivm.spark.telemetry.metrics.OpenIvmMetrics
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.io.Source

/** Integration tests for [[OpenIvmCompiler]].
  *
  * All tests that exercise `compile()` require the OpenIVM DuckDB extension to
  * be present at the path given by the `OPENIVM_EXTENSION_PATH` environment
  * variable (or the default `/opt/openivm/openivm.duckdb_extension`), which is
  * installed inside the `spark-ext` Docker image.
  */
class OpenIvmCompilerSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  // ── Shared compiler instance ────────────────────────────────────────────────

  private val extensionPath: String =
    sys.env.getOrElse("OPENIVM_EXTENSION_PATH", "/opt/openivm/openivm.duckdb_extension")

  // Shared compiler for all tests that need a live DuckDB session.
  // Created lazily so that the "bad path" boot test can run independently.
  private var sharedCompiler: OpenIvmCompiler = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sharedCompiler = OpenIvmCompiler.build(extensionPath)
  }

  override def afterAll(): Unit = {
    if (sharedCompiler != null) sharedCompiler.close()
    super.afterAll()
  }

  // ── Helpers ─────────────────────────────────────────────────────────────────

  private val salesSchema: StructType =
    StructType.fromDDL("region STRING, amount INT")

  private val tSchema: StructType =
    StructType.fromDDL("id INT, value INT")

  /** A deterministic verified physical Delta path for a pinned short source,
    * mirroring the resolved binding's `DeltaLog.dataPath` the command layer
    * supplies via `CompileRequest.sourceSnapshotPinnedPaths`. Every pinned
    * initial-load read must bind to this path, not the logical/friendly name.
    */
  private def pinPath(short: String): String =
    s"abfss://ws@onelake.dfs.fabric.microsoft.com/lh/Tables/$short"

  private val cliTestRequest = CompileRequest(
    viewName = "mv_cli_liveness",
    viewSql = "SELECT id FROM cli_source",
    sources = Map("cli_source" -> tSchema)
  )

  private def withFakeCli(body: String)(test: (OpenIvmCompiler, Path, ExecutionContext) => Unit): Unit = {
    val dir = Files.createTempDirectory(Files.createDirectories(Paths.get("target")), "compiler-cli-").toAbsolutePath
    val wrapper = dir.resolve("duckdb")
    val pidFile = dir.resolve("pids")
    Files.write(
      wrapper,
      s"""#!/usr/bin/env bash
         |set -euo pipefail
         |fixture_dir='${dir.toString.replace("'", "'\\''")}'
         |printf '%s\n' "$$$$" > "$$fixture_dir/pids"
         |$body
         |""".stripMargin.getBytes("UTF-8")
    )
    wrapper.toFile.setExecutable(true) shouldBe true
    val compiler = OpenIvmCompiler.build(extensionPath, cliPath = wrapper.toString)
    val pool     = Executors.newSingleThreadExecutor()
    try test(compiler, dir, ExecutionContext.fromExecutorService(pool))
    finally {
      val processes =
        if (Files.exists(pidFile)) {
          Files
            .readAllLines(pidFile)
            .asScala
            .filter(_.nonEmpty)
            .flatMap { pid =>
              val process = ProcessHandle.of(pid.toLong)
              if (process.isPresent) Some(process.get()) else None
            }
            .toVector
        } else Vector.empty
      processes.reverse.foreach(_.destroyForcibly())
      pool.shutdownNow()
      pool.awaitTermination(10, TimeUnit.SECONDS) shouldBe true
      processes.filter(_.isAlive).foreach(_.onExit().get(10, TimeUnit.SECONDS))
      compiler.close()
      val files = Files.list(dir)
      try files.forEach(path => Files.deleteIfExists(path))
      finally files.close()
      Files.deleteIfExists(dir)
    }
  }

  private def awaitCli[A](limit: FiniteDuration)(body: => A)(implicit ec: ExecutionContext): A = {
    @volatile var worker: Thread = null
    val result = Future {
      worker = Thread.currentThread()
      body
    }
    val returned =
      try {
        Await.ready(result, limit)
        true
      } catch {
        case _: scala.concurrent.TimeoutException => false
      }
    val stack = Option(worker).map(_.getStackTrace.mkString("\n")).getOrElse("worker not started")
    withClue(s"Compiler did not return within $limit. Worker stack:\n$stack\n") {
      returned shouldBe true
    }
    Await.result(result, Duration.Zero)
  }

  private def assertCliStopped(dir: Path): Unit =
    Files.readAllLines(dir.resolve("pids")).asScala.foreach { pid =>
      val process = ProcessHandle.of(pid.toLong)
      if (process.isPresent) {
        process.get().onExit().get(5, TimeUnit.SECONDS)
        process.get().isAlive shouldBe false
      }
    }

  // ── Test 1: Boot ─────────────────────────────────────────────────────────────

  "OpenIvmCompiler.build" should "succeed when the extension exists" in {
    sharedCompiler should not be null
  }

  it should "throw IllegalArgumentException when the extension path does not exist" in {
    val ex = the[IllegalArgumentException] thrownBy
      OpenIvmCompiler.build(extensionPath = "/nonexistent/path/openivm.duckdb_extension")
    ex.getMessage should include("/nonexistent/path/openivm.duckdb_extension")
  }

  it should "throw NotImplementedError for ChildProcess isolation" in {
    a[NotImplementedError] should be thrownBy
      OpenIvmCompiler.build(extensionPath, isolation = OpenIvmCompiler.ChildProcess)
  }

  // ── Test 2: Aggregate compile ─────────────────────────────────────────────

  "compile" should "classify a GROUP BY aggregate view as AGGREGATE_GROUP (type 0)" in {
    val req = CompileRequest(
      viewName = "mv_sales_agg",
      viewSql = "SELECT region, SUM(amount) AS total FROM sales GROUP BY region",
      sources = Map("sales" -> salesSchema)
    )
    val result = sharedCompiler.compile(req)
    result.refreshType shouldBe 0
    result.refreshTypeName shouldBe "AGGREGATE_GROUP"
    result.sql should not be empty
  }

  // ── Test 3: Simple projection compile ────────────────────────────────────

  it should "classify a filter-projection view as SIMPLE_PROJECTION (type 2)" in {
    val req = CompileRequest(
      viewName = "mv_t_proj",
      viewSql = "SELECT id FROM t WHERE id > 0",
      sources = Map("t" -> tSchema)
    )
    val result = sharedCompiler.compile(req)
    result.refreshType shouldBe 2
    result.refreshTypeName shouldBe "SIMPLE_PROJECTION"
    result.sql should not be empty
  }

  it should "preserve native and Spark add_months parity in the compiled initial load" in {
    val cases = Seq(
      ("2015-02-28", 1),
      ("2016-02-29", 1),
      ("2015-04-30", 1),
      ("2015-09-30", -1),
      ("2015-01-31", 1)
    )
    cases.zipWithIndex.foreach { case ((date, months), index) =>
      val expected = LocalDate
        .ofEpochDay(DateTimeUtils.dateAddMonths(LocalDate.parse(date).toEpochDay.toInt, months))
        .toString
      val result = sharedCompiler.compile(
        CompileRequest(
          viewName = s"mv_month_end_$index",
          viewSql = s"SELECT id, add_months(DATE '$date', $months) AS shifted FROM month_src",
          sources = Map("month_src" -> tSchema)
        )
      )
      result.refreshTypeName shouldBe "SIMPLE_PROJECTION"
      val script =
        s"""LOAD '${extensionPath.replace("'", "''")}';
           |CREATE TABLE month_src(id INTEGER, value INTEGER);
           |INSERT INTO month_src VALUES (1, 0);
           |SELECT shifted FROM (${result.initialLoadSql}) actual;
           |""".stripMargin
      val builder = new ProcessBuilder(
        sharedCompiler.cliPath,
        ":memory:",
        "-unsigned",
        "-csv",
        "-noheader",
        "-c",
        script
      ).redirectErrorStream(true)
      sys.env.get("OPENIVM_CLI_LD_LIBRARY_PATH").filter(_.trim.nonEmpty).foreach { extra =>
        val inherited = Option(builder.environment().get("LD_LIBRARY_PATH")).filter(_.nonEmpty).toSeq
        builder.environment().put("LD_LIBRARY_PATH", (Seq(extra) ++ inherited).mkString(java.io.File.pathSeparator))
      }
      val process = builder.start()
      try {
        process.waitFor(30, TimeUnit.SECONDS) shouldBe true
        val output = Source.fromInputStream(process.getInputStream)
        val actual =
          try output.mkString.trim
          finally output.close()
        withClue(actual) {
          process.exitValue() shouldBe 0
          actual shouldBe expected
        }
      } finally process.destroyForcibly()
    }
  }

  it should "emit signed cascade-delta SQL for WINDOW_PARTITION recomputes" in {
    val salesWpSchema = StructType.fromDDL("id INT, region STRING, amount INT")
    val req = CompileRequest(
      viewName = "mv_wp_cascade",
      viewSql =
        "SELECT id, region, amount, ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount, id) AS rn FROM sales_wp",
      sources = Map("sales_wp" -> salesWpSchema)
    )
    val result = sharedCompiler.compile(req)
    import org.openivm.spark.common.SparkRefreshRewriter
    result.refreshType shouldBe 5
    result.refreshTypeName shouldBe "WINDOW_PARTITION"
    result.sql should include("CREATE OR REPLACE TEMP TABLE openivm_old_mv_wp_cascade")
    result.sql should include("CREATE OR REPLACE TEMP TABLE openivm_new_mv_wp_cascade")
    result.sql.toUpperCase should include("INSERT INTO OPENIVM_DELTA_MV_WP_CASCADE")
    SparkRefreshRewriter.hasRealDelta(result.sql, "mv_wp_cascade") shouldBe true
  }

  it should "emit signed cascade-delta SQL for GROUP_RECOMPUTE recomputes" in {
    val salesGrSchema = StructType.fromDDL("id INT, region STRING, amount INT")
    val req = CompileRequest(
      viewName = "mv_gr_cascade",
      viewSql = "SELECT region, SUM(DISTINCT amount) AS total_distinct FROM sales_gr GROUP BY region",
      sources = Map("sales_gr" -> salesGrSchema)
    )
    val result = sharedCompiler.compile(req)
    import org.openivm.spark.common.SparkRefreshRewriter
    result.refreshType shouldBe 6
    result.refreshTypeName shouldBe "GROUP_RECOMPUTE"
    result.sql should include("CREATE OR REPLACE TEMP TABLE openivm_old_mv_gr_cascade")
    result.sql should include("CREATE OR REPLACE TEMP TABLE openivm_new_mv_gr_cascade")
    result.sql.toUpperCase should include("INSERT INTO OPENIVM_DELTA_MV_GR_CASCADE")
    SparkRefreshRewriter.hasRealDelta(result.sql, "mv_gr_cascade") shouldBe true
  }

  // ── Test 4: SELECT DISTINCT compile ──────────────────────────────────────
  //
  // OpenIVM internally represents top-level SELECT DISTINCT as a GROUP BY
  // (via DuckDB's logical planner), so it is classified as AGGREGATE_GROUP (0)
  // rather than DISTINCT_INCREMENTAL (8).  DISTINCT_INCREMENTAL requires
  // inner-DISTINCT-under-AGG (openivm_distinct_aux_state=true) which is outside
  // the default connection settings for the compiler bridge.

  it should "compile a SELECT DISTINCT view without error and return a non-empty SQL" in {
    val req = CompileRequest(
      viewName = "mv_sales_distinct",
      viewSql = "SELECT DISTINCT region FROM sales",
      sources = Map("sales" -> salesSchema)
    )
    val result = sharedCompiler.compile(req)
    result.refreshType should be >= 0
    result.sql should not be empty
  }

  // ── Test 4b: real-delta detection for multi-source JOIN ────────────────────
  //
  // The current compiler emits a real signed delta even for this two-source
  // CTE JOIN shape. Document that bridge behavior here; higher layers may still
  // choose a safer effective refresh type based on create-time checks.
  it should "emit a real delta for a two-source CTE JOIN" in {
    val usersSchema    = StructType.fromDDL("id INT, name STRING, age INT")
    val activitySchema = StructType.fromDDL("id INT, last_seen_days_ago INT")
    val req = CompileRequest(
      viewName = "mv_c3_probe",
      viewSql = """WITH adults AS (SELECT id, name FROM users_c3 WHERE age >= 18),
          |     active  AS (SELECT id FROM activity_c3 WHERE last_seen_days_ago <= 7)
          |SELECT u.id, u.name FROM adults u JOIN active a ON u.id = a.id""".stripMargin,
      sources = Map("users_c3" -> usersSchema, "activity_c3" -> activitySchema)
    )
    val result = sharedCompiler.compile(req)
    import org.openivm.spark.common.SparkRefreshRewriter
    SparkRefreshRewriter.hasRealDelta(result.sql, "mv_c3_probe") shouldBe true
  }

  // ── Test 4c: CTE-fed DISTINCT compiled SQL inspection ────────────────────
  it should "compile a CTE-fed DISTINCT view and produce expected SQL structure" in {
    val salesSchema2 = StructType.fromDDL("region STRING, amount INT")
    val req = CompileRequest(
      viewName = "mv_c6_probe",
      viewSql = """WITH t AS (SELECT region FROM sales_c6)
          |SELECT DISTINCT region FROM t""".stripMargin,
      sources = Map("sales_c6" -> salesSchema2)
    )
    val result = sharedCompiler.compile(req)
    result.refreshType should be >= 0
    result.sql should not be empty
  }

  // ── Test 4d: plain COUNT(*) GROUP BY compiled SQL (ag_cnt_2c comparison) ─────
  it should "compile a plain COUNT GROUP BY and show its SQL structure" in {
    val tSchema = StructType.fromDDL("k STRING, x INT")
    val req = CompileRequest(
      viewName = "mv_cnt_probe",
      viewSql = "SELECT k, COUNT(*) AS cnt FROM ag_cnt_probe GROUP BY k",
      sources = Map("ag_cnt_probe" -> tSchema)
    )
    val result = sharedCompiler.compile(req)
    result.refreshType should be >= 0
    result.sql should not be empty
  }

  // ── Test 5: SPARK dialect identifier quoting ──────────────────────────────
  //
  // With `target_dialect="spark"` set in the CompileFacts JSON payload,
  // OpenIVM compiles SIMPLE_PROJECTION via the lpts pipeline, which uses
  // fully-qualified `catalog.schema.table` identifiers in the generated SQL.
  // Current output backtick-quotes each identifier segment, so the delta-scan
  // CTE should reference the staged source as `` `memory`.`main`.`...` ``.

  it should "produce fully-qualified backtick-quoted memory.main table references in SPARK dialect for SIMPLE_PROJECTION" in {
    val req = CompileRequest(
      viewName = "mv_sales_proj",
      viewSql = "SELECT region FROM sales WHERE amount > 0",
      sources = Map("sales" -> salesSchema)
    )
    val result = sharedCompiler.compile(req)
    result.refreshType shouldBe 2
    result.refreshTypeName shouldBe "SIMPLE_PROJECTION"
    result.sql should include("`memory`.`main`.`openivm_delta_sales`")
  }

  // ── Test 6: Type mapping ──────────────────────────────────────────────────

  it should "register a table with a wide Spark schema without error" in {
    val wideSchema = StructType(
      Seq(
        StructField("byte_col", ByteType),
        StructField("short_col", ShortType),
        StructField("int_col", IntegerType),
        StructField("long_col", LongType),
        StructField("float_col", FloatType),
        StructField("double_col", DoubleType),
        StructField("bool_col", BooleanType),
        StructField("str_col", StringType),
        StructField("date_col", DateType),
        StructField("ts_col", TimestampType),
        StructField("bin_col", BinaryType),
        StructField("dec_col", DecimalType(10, 2)),
        StructField("arr_col", ArrayType(StringType))
      )
    )
    // If any type conversion fails, compile() throws — the view body is minimal.
    val req = CompileRequest(
      viewName = "mv_wide_proj",
      viewSql = "SELECT int_col FROM wide_src WHERE int_col > 0",
      sources = Map("wide_src" -> wideSchema)
    )
    val result = sharedCompiler.compile(req)
    result.sql should not be empty
  }

  // ── Test 6b: Reserved-word source column ──────────────────────────────────

  it should "compile when a source table has a DuckDB reserved-word column name" in {
    // `collation` is a DuckDB reserved word; unquoted it fails CREATE TABLE parsing.
    val schema = StructType(
      Seq(
        StructField("id", IntegerType),
        StructField("collation", StringType),
        StructField("region", StringType)
      )
    )
    val req = CompileRequest(
      viewName = "mv_reserved_col",
      viewSql = "SELECT id, region FROM reserved_src WHERE id > 0",
      sources = Map("reserved_src" -> schema)
    )
    // Before the fix this threw a DuckDB "syntax error at or near collation".
    val result = sharedCompiler.compile(req)
    result.sql should not be empty
    result.refreshTypeName shouldBe "SIMPLE_PROJECTION"
  }

  // ── Test 7: Unsupported type ──────────────────────────────────────────────

  it should "throw NotImplementedError for a schema containing a UserDefinedType" in {
    // Minimal concrete UserDefinedType subclass used only in tests.
    val udt = new UserDefinedType[String] {
      override def sqlType: DataType               = StringType
      override def serialize(obj: String): Any     = obj
      override def deserialize(datum: Any): String = datum.toString
      override def userClass: Class[String]        = classOf[String]
      override def typeName: String                = "test_udt"
    }
    val schema = StructType(Seq(StructField("x", udt)))
    val req = CompileRequest(
      viewName = "mv_udt",
      viewSql = "SELECT x FROM src_udt",
      sources = Map("src_udt" -> schema)
    )
    val ex = the[NotImplementedError] thrownBy sharedCompiler.compile(req)
    ex.getMessage should include("Unsupported Spark DataType")
  }

  // ── Test 7b: DB-qualified source references in viewSql (Hive/dbt path) ────
  //
  // The unit test below pokes the private[compiler] stripDbQualifiers helper
  // directly; the integration check exercises an end-to-end compile with a
  // qualified table reference in the view body.

  it should "compile a view that references a tracked source by its qualified <db>.<table> name" in {
    val req = CompileRequest(
      viewName = "mv_qual_count",
      viewSql = "SELECT COUNT(*) AS c FROM tpcdi.sales",
      sources = Map("sales" -> salesSchema),
      sourceQualifiedNames = Map("sales" -> "tpcdi.sales")
    )
    val result = sharedCompiler.compile(req)
    result.refreshType should be >= 0
    result.sql should not be empty
  }

  it should "compile a view that joins two sources via qualified <db>.<table> names" in {
    val depts = StructType.fromDDL("dept_id INT, dept_name STRING")
    val emps  = StructType.fromDDL("emp_id INT, dept_id INT, name STRING")
    val req = CompileRequest(
      viewName = "mv_qual_join",
      viewSql = "SELECT e.emp_id, d.dept_name FROM tpcdi.employees e JOIN tpcdi.departments d ON e.dept_id = d.dept_id",
      sources = Map("employees" -> emps, "departments" -> depts),
      sourceQualifiedNames = Map("employees" -> "tpcdi.employees", "departments" -> "tpcdi.departments")
    )
    val result = sharedCompiler.compile(req)
    result.refreshType should be >= 0
    result.sql should not be empty
  }

  // ── Test 7c: Snapshot-pinned sources (Spark/Delta time travel) ────────────
  //
  // DuckDB cannot represent a Delta snapshot, so a `VERSION AS OF` pin used to
  // abort the compile (`Parser Error: syntax error at or near "as"`), demoting
  // the whole view to COMPILE_FAILED -> FULL_REFRESH. The pin is a storage
  // concern only: it is split out of the compile-bridge copy of the body and
  // re-applied to every Spark-side source read.

  it should "classify a view whose source is pinned with VERSION AS OF instead of failing to compile" in {
    val req = CompileRequest(
      viewName = "mv_pinned_agg",
      viewSql = "SELECT region, SUM(amount) AS total FROM sales VERSION AS OF 366 GROUP BY region",
      sources = Map("sales" -> salesSchema),
      sourceSnapshotPinnedPaths = Map("sales" -> pinPath("sales"))
    )
    val result = sharedCompiler.compile(req)
    result.refreshTypeName shouldBe "AGGREGATE_GROUP"
    result.sql should not be empty
  }

  it should "classify a view pinned with TIMESTAMP AS OF" in {
    val req = CompileRequest(
      viewName = "mv_pinned_ts",
      viewSql = "SELECT region, SUM(amount) AS total FROM sales TIMESTAMP AS OF '2024-01-01' GROUP BY region",
      sources = Map("sales" -> salesSchema),
      sourceSnapshotPinnedPaths = Map("sales" -> pinPath("sales"))
    )
    val result = sharedCompiler.compile(req)
    result.refreshTypeName shouldBe "AGGREGATE_GROUP"
  }

  it should "classify a view that pins a qualified source" in {
    val req = CompileRequest(
      viewName = "mv_pinned_qual",
      viewSql = "SELECT region, SUM(amount) AS total FROM tpcdi.sales VERSION AS OF 366 GROUP BY region",
      sources = Map("sales" -> salesSchema),
      sourceQualifiedNames = Map("sales" -> "tpcdi.sales"),
      sourceSnapshotPinnedPaths = Map("sales" -> pinPath("sales"))
    )
    val result = sharedCompiler.compile(req)
    result.refreshTypeName shouldBe "AGGREGATE_GROUP"
  }

  it should "classify a join whose sources are pinned to different versions" in {
    val depts = StructType.fromDDL("dept_id INT, dept_name STRING")
    val emps  = StructType.fromDDL("emp_id INT, dept_id INT, name STRING")
    val req = CompileRequest(
      viewName = "mv_pinned_join",
      viewSql = "SELECT e.emp_id, d.dept_name FROM employees VERSION AS OF 2 e " +
        "JOIN departments VERSION AS OF 5 d ON e.dept_id = d.dept_id",
      sources = Map("employees" -> emps, "departments" -> depts),
      sourceSnapshotPinnedPaths = Map("employees" -> pinPath("employees"), "departments" -> pinPath("departments"))
    )
    val result = sharedCompiler.compile(req)
    result.refreshType should be >= 0
    result.sql should not be empty
  }

  it should "re-apply the snapshot pin to the Spark-side initial-load SQL" in {
    val req = CompileRequest(
      viewName = "mv_pinned_initial_load",
      viewSql = "SELECT region, SUM(amount) AS total FROM tpcdi.sales VERSION AS OF 366 GROUP BY region",
      sources = Map("sales" -> salesSchema),
      sourceQualifiedNames = Map("sales" -> "tpcdi.sales"),
      sourceSnapshotPinnedPaths = Map("sales" -> pinPath("sales"))
    )
    val result = sharedCompiler.compile(req)
    result.initialLoadSql should not be empty
    result.initialLoadSql should include(s"delta.`${pinPath("sales")}` VERSION AS OF 366")
    result.initialLoadSql should not include "memory.main."
    result.initialLoadSql should not include "tpcdi.sales VERSION AS OF"
  }

  it should "leave the initial-load SQL unpinned for an unpinned view" in {
    val req = CompileRequest(
      viewName = "mv_unpinned_initial_load",
      viewSql = "SELECT region, SUM(amount) AS total FROM tpcdi.sales GROUP BY region",
      sources = Map("sales" -> salesSchema),
      sourceQualifiedNames = Map("sales" -> "tpcdi.sales")
    )
    val result = sharedCompiler.compile(req)
    result.initialLoadSql should not be empty
    result.initialLoadSql.toUpperCase should not include "VERSION AS OF"
  }

  // ── Test 7d: Aliased (dbt-shaped) pinned relations ────────────────────────
  //
  // dbt renders every `ref()` as a backtick-qualified relation with an alias,
  // and Spark puts the temporal clause BETWEEN the relation and its alias
  // (`identifierReference temporalClause? tableAlias`). Splitting the clause
  // out has to keep the alias attached to the relation, otherwise the SQL that
  // reaches DuckDB is either invalid or silently refers to the wrong relation.
  // These compile against a live DuckDB, so an invalid de-pinned body shows up
  // as a COMPILE_FAILED classification rather than a string mismatch.

  it should "classify a dbt-shaped view whose pinned relation carries a bare alias" in {
    val req = CompileRequest(
      viewName = "mv_pinned_alias_bare",
      viewSql = "SELECT p.region, SUM(p.amount) AS total FROM tpcdi.sales VERSION AS OF 366 p GROUP BY p.region",
      sources = Map("sales" -> salesSchema),
      sourceQualifiedNames = Map("sales" -> "tpcdi.sales"),
      sourceSnapshotPinnedPaths = Map("sales" -> pinPath("sales"))
    )
    val result = sharedCompiler.compile(req)
    result.refreshTypeName shouldBe "AGGREGATE_GROUP"
    result.sql should not be empty
  }

  it should "classify a dbt-shaped view whose pinned relation carries an AS alias" in {
    val req = CompileRequest(
      viewName = "mv_pinned_alias_as",
      viewSql = "SELECT p.region, SUM(p.amount) AS total FROM tpcdi.sales VERSION AS OF 366 AS p GROUP BY p.region",
      sources = Map("sales" -> salesSchema),
      sourceQualifiedNames = Map("sales" -> "tpcdi.sales"),
      sourceSnapshotPinnedPaths = Map("sales" -> pinPath("sales"))
    )
    val result = sharedCompiler.compile(req)
    result.refreshTypeName shouldBe "AGGREGATE_GROUP"
  }

  it should "classify a dbt-shaped CTE model whose pinned relation carries an alias" in {
    val req = CompileRequest(
      viewName = "mv_pinned_alias_cte",
      viewSql = "WITH source AS (SELECT p.region, p.amount FROM tpcdi.sales VERSION AS OF 366 AS p) " +
        "SELECT region, SUM(amount) AS total FROM source GROUP BY region",
      sources = Map("sales" -> salesSchema),
      sourceQualifiedNames = Map("sales" -> "tpcdi.sales"),
      sourceSnapshotPinnedPaths = Map("sales" -> pinPath("sales"))
    )
    val result = sharedCompiler.compile(req)
    result.refreshTypeName shouldBe "AGGREGATE_GROUP"
  }

  it should "classify an aliased join whose pinned side is dbt-shaped" in {
    val depts = StructType.fromDDL("dept_id INT, dept_name STRING")
    val emps  = StructType.fromDDL("emp_id INT, dept_id INT, name STRING")
    val req = CompileRequest(
      viewName = "mv_pinned_alias_join",
      viewSql = "SELECT e.emp_id, d.dept_name FROM tpcdi.employees AS e " +
        "JOIN tpcdi.departments VERSION AS OF 5 AS d ON e.dept_id = d.dept_id",
      sources = Map("employees" -> emps, "departments" -> depts),
      sourceQualifiedNames = Map("employees" -> "tpcdi.employees", "departments" -> "tpcdi.departments"),
      sourceSnapshotPinnedPaths = Map("departments" -> pinPath("departments"))
    )
    val result = sharedCompiler.compile(req)
    result.refreshType should be >= 0
    result.sql should not be empty
  }

  it should "re-apply the snapshot pin before the alias in the Spark-side initial-load SQL" in {
    val req = CompileRequest(
      viewName = "mv_pinned_alias_initial_load",
      viewSql = "SELECT p.region, SUM(p.amount) AS total FROM tpcdi.sales VERSION AS OF 366 AS p GROUP BY p.region",
      sources = Map("sales" -> salesSchema),
      sourceQualifiedNames = Map("sales" -> "tpcdi.sales"),
      sourceSnapshotPinnedPaths = Map("sales" -> pinPath("sales"))
    )
    val result = sharedCompiler.compile(req)
    result.initialLoadSql should not be empty
    result.initialLoadSql should include(s"delta.`${pinPath("sales")}` VERSION AS OF 366")
    result.initialLoadSql should not include "memory.main."
    result.initialLoadSql should not include "tpcdi.sales VERSION AS OF"
    // The pin must never trail the alias — that order parses in neither dialect.
    """(?i)\bAS\s+p\s+VERSION\s+AS\s+OF""".r.findFirstIn(result.initialLoadSql) shouldBe None
  }

  // ── Test 7e: Pin shapes OpenIVM refuses itself ────────────────────────────
  //
  // OpenIVM re-applies a pin per SOURCE, so it cannot maintain a source read at
  // two versions, or pinned in one place and live in another. Those bodies used
  // to be stopped by DuckDB's parser choking on `VERSION AS OF`; an LPTS
  // front-end that accepts Spark's `temporalClause` would let them through with
  // no pin registered, silently maintaining a frozen relation from live rows.
  // The bridge refuses them itself, so the FULL_REFRESH fallback (which
  // re-executes the pinned body verbatim) does not depend on a downstream parser.

  it should "refuse a view that reads one source at two different versions" in {
    val req = CompileRequest(
      viewName = "mv_pinned_two_versions",
      viewSql = "SELECT a.region, b.region AS region_then FROM tpcdi.sales VERSION AS OF 2 a " +
        "JOIN tpcdi.sales VERSION AS OF 5 b ON a.region = b.region",
      sources = Map("sales" -> salesSchema),
      sourceQualifiedNames = Map("sales" -> "tpcdi.sales")
    )
    val thrown = the[OpenIvmCompileException] thrownBy sharedCompiler.compile(req)
    thrown.getMessage should include("mv_pinned_two_versions")
    thrown.getMessage should include("two different versions")
  }

  it should "refuse a view that reads a source both pinned and live" in {
    val req = CompileRequest(
      viewName = "mv_pinned_and_live",
      viewSql = "SELECT a.region FROM tpcdi.sales VERSION AS OF 2 a JOIN tpcdi.sales b ON a.region = b.region",
      sources = Map("sales" -> salesSchema),
      sourceQualifiedNames = Map("sales" -> "tpcdi.sales")
    )
    the[OpenIvmCompileException] thrownBy sharedCompiler.compile(req)
  }

  it should "still compile a source pinned twice at the SAME version" in {
    val req = CompileRequest(
      viewName = "mv_pinned_same_version",
      viewSql = "SELECT a.region FROM tpcdi.sales VERSION AS OF 2 a " +
        "JOIN tpcdi.sales VERSION AS OF 2 b ON a.region = b.region",
      sources = Map("sales" -> salesSchema),
      sourceQualifiedNames = Map("sales" -> "tpcdi.sales"),
      sourceSnapshotPinnedPaths = Map("sales" -> pinPath("sales"))
    )
    val result = sharedCompiler.compile(req)
    result.refreshType should be >= 0
  }

  // ── Test 7f: Prefix-colliding source names + gauge hygiene ────────────────
  //
  // openivm emits `memory.main.<source>` for every relation it reads and the
  // bridge rewrites those back to Spark identifiers, re-attaching the user's
  // pin. That rewrite has to be identifier-bounded: with sources `customer`
  // and `customer_address`, a per-name `String.replace` either injects the
  // shorter name's pin into the middle of the longer identifier or eats its
  // prefix so the longer name keeps NO pin and silently loads live rows.

  private val customerSchema: StructType        = StructType.fromDDL("id INT, name STRING")
  private val customerAddressSchema: StructType = StructType.fromDDL("id INT, city STRING")

  /** Runs the initial-load rewrite over a synthetic openivm compile output, so
    * the assertion is about the rewrite and not about which plan openivm picks.
    */
  private def initialLoadSqlFor(req: CompileRequest, compiledQueries: String): String = {
    val root = Files.createDirectories(java.nio.file.Paths.get("target", "openivm-initial-load"))
    val dir  = Files.createTempDirectory(root, s"${req.viewName}-")
    val file = dir.resolve(s"openivm_compiled_queries_${req.viewName}.sql")
    try {
      Files.write(file, compiledQueries.getBytes("UTF-8"))
      sharedCompiler.parseInitialLoadSql(dir, req)
    } finally {
      Files.deleteIfExists(file)
      Files.deleteIfExists(dir)
    }
  }

  private val prefixCollisionRequest: CompileRequest = CompileRequest(
    viewName = "mv_prefix_collision",
    viewSql = "SELECT c.name, a.city FROM db.customer VERSION AS OF 3 AS c " +
      "JOIN db.customer_address AS a ON a.id = c.id",
    sources = Map("customer" -> customerSchema, "customer_address" -> customerAddressSchema),
    sourceQualifiedNames = Map("customer" -> "db.customer", "customer_address" -> "db.customer_address"),
    sourceSnapshotPinnedPaths =
      Map("customer" -> pinPath("customer"), "customer_address" -> pinPath("customer_address"))
  )

  private val prefixCollisionQueries: String =
    "CREATE TABLE openivm_data_mv_prefix_collision AS SELECT c.name, a.city " +
      "FROM memory.main.customer AS c JOIN memory.main.customer_address AS a ON a.id = c.id;"

  it should "not bleed a shorter source's pin into a longer source name in the initial load" in {
    val sql = initialLoadSqlFor(prefixCollisionRequest, prefixCollisionQueries)
    sql should include(s"delta.`${pinPath("customer")}` VERSION AS OF 3 AS c")
    sql should include("db.customer_address AS a")
    sql should not include "memory.main."
    sql should not include "3_address"
    """(?i)customer_address\s+VERSION""".r.findFirstIn(sql) shouldBe None
  }

  it should "keep the LONGER source's pin when only it is pinned" in {
    val req = prefixCollisionRequest.copy(
      viewName = "mv_prefix_collision_long",
      viewSql = "SELECT c.name, a.city FROM db.customer AS c " +
        "JOIN db.customer_address VERSION AS OF 7 AS a ON a.id = c.id"
    )
    val sql = initialLoadSqlFor(
      req,
      prefixCollisionQueries.replace("mv_prefix_collision", "mv_prefix_collision_long")
    )
    sql should include(s"delta.`${pinPath("customer_address")}` VERSION AS OF 7 AS a")
    // The unpinned shorter source must stay live (friendly name, no pin).
    sql should include("db.customer AS c")
    """(?i)db\.customer\s+VERSION""".r.findFirstIn(sql) shouldBe None
  }

  it should "re-attach both pins when a prefix pair is pinned to different versions" in {
    val req = prefixCollisionRequest.copy(
      viewName = "mv_prefix_collision_both",
      viewSql = "SELECT c.name, a.city FROM db.customer VERSION AS OF 3 AS c " +
        "JOIN db.customer_address VERSION AS OF 7 AS a ON a.id = c.id"
    )
    val sql = initialLoadSqlFor(
      req,
      prefixCollisionQueries.replace("mv_prefix_collision", "mv_prefix_collision_both")
    )
    sql should include(s"delta.`${pinPath("customer")}` VERSION AS OF 3 AS c")
    sql should include(s"delta.`${pinPath("customer_address")}` VERSION AS OF 7 AS a")
  }

  it should "compile a view whose sources have colliding name prefixes" in {
    val result = sharedCompiler.compile(prefixCollisionRequest)
    result.refreshType should be >= 0
    result.sql should not be empty
    result.sql.toUpperCase should not include "VERSION AS OF"
  }

  it should "refuse a pin that does not resolve to exactly one tracked source" in {
    val req = CompileRequest(
      viewName = "mv_pin_unresolved",
      viewSql = "SELECT region, SUM(amount) AS total FROM other_db.sales VERSION AS OF 2 GROUP BY region",
      sources = Map("sales" -> salesSchema),
      sourceQualifiedNames = Map("sales" -> "tpcdi.sales")
    )
    val thrown = the[OpenIvmCompileException] thrownBy sharedCompiler.compile(req)
    thrown.getMessage should include("other_db.sales")
    thrown.getMessage should include("does not resolve")
  }

  it should "refuse a pin whose value moves with wall-clock time" in {
    val req = CompileRequest(
      viewName = "mv_pin_moving",
      viewSql = "SELECT region, SUM(amount) AS total FROM tpcdi.sales TIMESTAMP AS OF current_timestamp() " +
        "GROUP BY region",
      sources = Map("sales" -> salesSchema),
      sourceQualifiedNames = Map("sales" -> "tpcdi.sales")
    )
    the[OpenIvmCompileException] thrownBy sharedCompiler.compile(req)
  }

  it should "not leak the inflight compiler gauge when it refuses a pin" in {
    val req = CompileRequest(
      viewName = "mv_pin_gauge",
      viewSql = "SELECT a.region FROM tpcdi.sales VERSION AS OF 2 a " +
        "JOIN tpcdi.sales VERSION AS OF 5 b ON a.region = b.region",
      sources = Map("sales" -> salesSchema),
      sourceQualifiedNames = Map("sales" -> "tpcdi.sales")
    )
    val baseline = OpenIvmMetrics.CompilerInflight.get()
    (1 to 3).foreach(_ => the[OpenIvmCompileException] thrownBy sharedCompiler.compile(req))
    // The decrement lives in `compile`'s `finally`; a refusal thrown after the
    // increment would strand the gauge one higher per refused view.
    OpenIvmMetrics.CompilerInflight.get() shouldBe baseline
  }

  it should "emit RELY FK declarations for qualified compile facts when opted in" in {
    val req = CompileRequest(
      viewName = "mv_relyfk_script",
      viewSql = "SELECT e.emp_id, d.dept_name FROM tpcdi.employees e JOIN tpcdi.departments d ON e.dept_id = d.dept_id",
      sources = Map(
        "employees"   -> StructType.fromDDL("emp_id INT, dept_id INT"),
        "departments" -> StructType.fromDDL("dept_id INT")
      ),
      sourceQualifiedNames = Map("employees" -> "tpcdi.employees", "departments" -> "tpcdi.departments"),
      facts = WorkloadFacts(
        declareRelyFk = true,
        fkRelations = Seq(ForeignKeyRelation("tpcdi.employees", Seq("dept_id"), "tpcdi.departments", Seq("dept_id")))
      )
    )

    sharedCompiler.declareRelyFkStatements(req) should contain only
      """PRAGMA openivm_declare_rely_fk('employees','["dept_id"]','departments','["dept_id"]');"""
  }

  it should "terminate a stalled DuckDB CLI within the compilation deadline" in {
    withFakeCli(
      """cat > "$fixture_dir/input.sql"
        |printf '%s\n' '{"refresh_type":2,"refresh_type_name":"SIMPLE_PROJECTION","sql":"SELECT 1;"}'
        |sleep 300 &
        |printf '%s\n' "$!" >> "$fixture_dir/pids"
        |wait
        |""".stripMargin
    ) { (compiler, dir, context) =>
      implicit val ec: ExecutionContext = context
      val baseline                      = OpenIvmMetrics.CompilerInflight.get()
      val ex = the[OpenIvmCompileException] thrownBy awaitCli(135.seconds) {
        compiler.compile(cliTestRequest)
      }
      ex.getMessage shouldBe "DuckDB CLI timed out after 120 seconds"
      OpenIvmMetrics.CompilerInflight.get() shouldBe baseline
      assertCliStopped(dir)
      val input = new String(Files.readAllBytes(dir.resolve("input.sql")), "UTF-8")
      val scratchPath = input.linesIterator
        .find(_.startsWith("SET openivm_files_path="))
        .get
        .stripPrefix("SET openivm_files_path='")
        .stripSuffix("';")
        .replace("''", "'")
      Files.exists(Paths.get(scratchPath)) shouldBe false
    }
  }

  it should "contain inherited output writers before fixture teardown" in {
    withFakeCli(
      """cat > /dev/null
        |(
        |  trap '' PIPE
        |  while [[ ! -e "$fixture_dir/write-after-return" ]]; do sleep 0.05; done
        |  while :; do
        |    stdout_writable=0
        |    stderr_writable=0
        |    if printf '%s\n' 'orphan stdout'; then stdout_writable=1; fi
        |    if printf '%s\n' 'orphan stderr' >&2; then stderr_writable=1; fi
        |    if [[ "$stdout_writable" -eq 0 && "$stderr_writable" -eq 0 ]]; then
        |      : > "$fixture_dir/writes-rejected"
        |      exit 0
        |    fi
        |    : > "$fixture_dir/wrote-after-return"
        |    sleep 0.05
        |  done
        |) &
        |printf '%s\n' "$!" >> "$fixture_dir/pids"
        |printf '%s\n' '{"refresh_type":2,"refresh_type_name":"SIMPLE_PROJECTION","sql":"SELECT 1;"}'
        |sleep 0.5
        |""".stripMargin
    ) { (compiler, dir, context) =>
      implicit val ec: ExecutionContext = context
      val outcome =
        try
          Right(awaitCli(15.seconds) {
            compiler.compile(cliTestRequest)
          })
        catch { case e: OpenIvmCompileException => Left(e) }
      val writer = ProcessHandle.of(Files.readAllLines(dir.resolve("pids")).get(1).toLong).get()
      Files.write(dir.resolve("write-after-return"), Array.emptyByteArray)
      writer.onExit().get(5, TimeUnit.SECONDS)
      withClue("The inherited writer could still append output after compilation returned") {
        Files.exists(dir.resolve("wrote-after-return")) shouldBe false
      }
      Files.exists(dir.resolve("writes-rejected")) shouldBe true
      outcome match {
        case Right(result) =>
          result.refreshType shouldBe 2
          result.sql shouldBe "SELECT 1;"
        case Left(failure) =>
          failure.getMessage shouldBe "DuckDB CLI output streams remained open after process exit"
      }
    }
  }

  it should "collect DuckDB CLI output larger than pipe buffers without losing result rows" in {
    withFakeCli(
      """cat > /dev/null
        |for ((i=0; i<4096; i++)); do
        |  printf '%s\n' 'synthetic stdout diagnostic padding'
        |  printf '%s\n' 'synthetic stderr diagnostic padding' >&2
        |done
        |printf '%s\n' '{"refresh_type":2,"refresh_type_name":"SIMPLE_PROJECTION","sql":"SELECT 1;"}'
        |printf '%s\n' '{"refresh_type":2,"refresh_type_name":"SIMPLE_PROJECTION","sql":"SELECT 2;"}'
        |""".stripMargin
    ) { (compiler, dir, context) =>
      implicit val ec: ExecutionContext = context
      val result = awaitCli(15.seconds) {
        compiler.compile(cliTestRequest)
      }
      result.refreshType shouldBe 2
      result.sql shouldBe "SELECT 1;\nSELECT 2;"
      assertCliStopped(dir)
    }
  }

  it should "preserve DuckDB CLI error diagnostics and clean up after a nonzero exit" in {
    withFakeCli(
      """cat > /dev/null
        |printf '%s\n' '{"MATERIALIZED VIEW CREATION":"true"}'
        |printf 'synthetic CLI failure\r\nsecond line\r\n' >&2
        |exit 7
        |""".stripMargin
    ) { (compiler, dir, context) =>
      implicit val ec: ExecutionContext = context
      val ex = the[OpenIvmCompileException] thrownBy awaitCli(15.seconds) {
        compiler.compile(cliTestRequest)
      }
      ex.getMessage should include("failed during openivm_compile_with_facts")
      ex.getMessage should include("CLI stderr:\nsynthetic CLI failure\nsecond line")
      assertCliStopped(dir)
    }
  }

  it should "report a DuckDB CLI launch failure through the bounded I/O helper" in {
    withFakeCli("") { (compiler, dir, context) =>
      implicit val ec: ExecutionContext = context
      dir.resolve("duckdb").toFile.setExecutable(false) shouldBe true
      val ex = the[OpenIvmCompileException] thrownBy awaitCli(15.seconds) {
        compiler.compile(cliTestRequest)
      }
      ex.getMessage should include("DuckDB CLI I/O helper failed")
      ex.getMessage should include("Cannot run program")
      ex.getMessage should include(compiler.cliPath)
    }
  }

  it should "terminate a native root launched after the initial cancellation snapshot" in {
    val dir       = Files.createTempDirectory(Files.createDirectories(Paths.get("target")), "compiler-startup-")
    val lifecycle = new OpenIvmCliLifecycle(dir)
    lifecycle.initialize()
    lifecycle.beginLaunch() shouldBe true
    val helper                        = new ProcessBuilder("sleep", "300").start()
    val nativeProcess                 = new AtomicReference[Process]()
    val published                     = new CountDownLatch(1)
    val events                        = new ConcurrentLinkedQueue[String]()
    val pool                          = Executors.newSingleThreadExecutor()
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(pool)
    val launcher = Future {
      val deadline = 5.seconds.fromNow
      while (!lifecycle.cancellationRequested() && deadline.hasTimeLeft()) Thread.sleep(10)
      lifecycle.cancellationRequested() shouldBe true
      val child = new ProcessBuilder("sleep", "300").start()
      nativeProcess.set(child)
      events.add("native launched")
      lifecycle.acknowledgeNative(child.toHandle())
      published.countDown()
    }
    val racingHelper = new Process {
      override def getOutputStream                                 = helper.getOutputStream
      override def getInputStream                                  = helper.getInputStream
      override def getErrorStream                                  = helper.getErrorStream
      override def waitFor(): Int                                  = helper.waitFor()
      override def waitFor(timeout: Long, unit: TimeUnit): Boolean = helper.waitFor(timeout, unit)
      override def exitValue(): Int                                = helper.exitValue()
      override def destroy(): Unit                                 = helper.destroy()
      override def isAlive: Boolean                                = helper.isAlive
      override def toHandle(): ProcessHandle                       = helper.toHandle()
      override def descendants(): java.util.stream.Stream[ProcessHandle] = {
        events.add("empty descendant snapshot")
        java.util.stream.Stream.empty[ProcessHandle]()
      }
      override def destroyForcibly(): Process = {
        lifecycle.requestCancellation(System.nanoTime() + 5.seconds.toNanos)
        published.await(5, TimeUnit.SECONDS) shouldBe true
        helper.destroyForcibly()
        events.add("helper killed")
        this
      }
    }
    try {
      sharedCompiler.terminateCliProcess(racingHelper, lifecycle, System.nanoTime() + 10.seconds.toNanos)
      Await.result(launcher, 5.seconds)
      withClue(events.iterator().asScala.mkString(" -> ") + ": ") {
        nativeProcess.get().isAlive shouldBe false
      }
      helper.isAlive shouldBe false
    } finally {
      Option(nativeProcess.get()).foreach(_.destroyForcibly())
      helper.destroyForcibly()
      pool.shutdownNow()
      pool.awaitTermination(5, TimeUnit.SECONDS) shouldBe true
      val files = Files.walk(dir)
      try files.sorted(java.util.Comparator.reverseOrder()).forEach(path => Files.deleteIfExists(path))
      finally files.close()
    }
  }

  it should "cancel a real worker during native startup before observing its PID" in {
    withFakeCli("exec sleep 300") { (compiler, dir, context) =>
      val controlDir = Files.createDirectory(dir.resolve("compile"))
      val lifecycle  = new OpenIvmCliLifecycle(controlDir)
      val worker     = new AtomicReference[Thread]()
      val result = Future {
        worker.set(Thread.currentThread())
        try {
          compiler.runCli("SELECT 1;\n", controlDir)
          None
        } catch {
          case e: InterruptedException => Some(e)
        }
      }(context)
      try {
        val deadline = 5.seconds.fromNow
        while (!lifecycle.launchCommitted() && deadline.hasTimeLeft()) Thread.sleep(1)
        lifecycle.launchCommitted() shouldBe true
        worker.get().interrupt()
        Await.result(result, 10.seconds).isDefined shouldBe true
        val identity = lifecycle.nativeIdentity()
        identity.isPresent shouldBe true
        identity.get().start.isPresent shouldBe true
        val native = ProcessHandle.of(identity.get().pid)
        if (native.isPresent) {
          val birth = native.get().info().startInstant()
          if (birth == identity.get().start) {
            native.get().onExit().get(5, TimeUnit.SECONDS)
            native.get().isAlive shouldBe false
          } else if (!birth.isPresent) native.get().isAlive shouldBe false
        }
      } finally {
        val files = Files.walk(controlDir)
        try files.sorted(java.util.Comparator.reverseOrder()).forEach(path => Files.deleteIfExists(path))
        finally files.close()
      }
    }
  }

  it should "prevent native startup when cancellation wins the launch gate" in {
    val dir       = Files.createTempDirectory(Files.createDirectories(Paths.get("target")), "compiler-startup-cancel-")
    val lifecycle = new OpenIvmCliLifecycle(dir)
    lifecycle.initialize()
    val helper = new ProcessBuilder("sleep", "300").start()
    try {
      sharedCompiler.terminateCliProcess(helper, lifecycle, System.nanoTime() + 5.seconds.toNanos)
      lifecycle.beginLaunch() shouldBe false
      lifecycle.launchCommitted() shouldBe false
      helper.isAlive shouldBe false
    } finally {
      helper.destroyForcibly()
      val files = Files.walk(dir)
      try files.sorted(java.util.Comparator.reverseOrder()).forEach(path => Files.deleteIfExists(path))
      finally files.close()
    }
  }

  it should "report incomplete startup cleanup without force-killing an unacknowledged launcher" in {
    val dir       = Files.createTempDirectory(Files.createDirectories(Paths.get("target")), "compiler-startup-pending-")
    val lifecycle = new OpenIvmCliLifecycle(dir)
    lifecycle.initialize()
    lifecycle.beginLaunch() shouldBe true
    val helper = new ProcessBuilder("sleep", "300").start()
    try {
      val ex = the[OpenIvmCliCleanupException] thrownBy
        sharedCompiler.terminateCliProcess(helper, lifecycle, System.nanoTime() + 250.millis.toNanos)
      ex.getMessage should include("cleanup incomplete")
      ex.getMessage should include("acknowledgement was not received")
      lifecycle.cancellationRequested() shouldBe true
      helper.isAlive shouldBe true
      Files.exists(dir.resolve("cli-launch.lock")) shouldBe true
    } finally {
      helper.destroyForcibly()
      val files = Files.walk(dir)
      try files.sorted(java.util.Comparator.reverseOrder()).forEach(path => Files.deleteIfExists(path))
      finally files.close()
    }
  }

  it should "retain irreversible cancellation when the launch gate cannot be acquired" in {
    withFakeCli("printf '%s\\n' 'unexpected native capture'") { (compiler, dir, _) =>
      val controlDir = Files.createDirectory(dir.resolve("compile"))
      val lifecycle  = new OpenIvmCliLifecycle(controlDir)
      lifecycle.initialize()
      val lockPath     = controlDir.resolve("cli-launch.lock")
      val cancelled    = controlDir.resolve("cli-cancelled")
      val directoryKey = Files.getAttribute(controlDir, "basic:fileKey")
      val lockKey      = Files.getAttribute(lockPath, "basic:fileKey")
      directoryKey should not be null
      lockKey should not be null
      val channel         = FileChannel.open(lockPath, StandardOpenOption.WRITE)
      val gate            = channel.lock()
      var helper: Process = null
      try {
        val workerClass = classOf[OpenIvmCliWorker]
        val script      = controlDir.resolve("cli.sql")
        Files.write(script, "SELECT 1;\n".getBytes("UTF-8"))
        helper = new ProcessBuilder(
          Paths.get(System.getProperty("java.home"), "bin", "java").toString,
          "-Xmx64m",
          "-cp",
          Paths.get(workerClass.getProtectionDomain.getCodeSource.getLocation.toURI).toString,
          workerClass.getName,
          compiler.cliPath,
          script.toString,
          controlDir.resolve("cli.stdout").toString,
          controlDir.resolve("cli.stderr").toString,
          controlDir.resolve("cli-worker.error").toString,
          ""
        ).redirectOutput(ProcessBuilder.Redirect.DISCARD)
          .redirectError(ProcessBuilder.Redirect.DISCARD)
          .start()
        val ex = the[OpenIvmCliCleanupException] thrownBy
          compiler.terminateCliProcess(helper, lifecycle, System.nanoTime() + 250.millis.toNanos)
        ex.getMessage should include("cleanup incomplete")
        ex.getMessage should include("startup coordination failed")
        withClue("Cancellation must remain observable after gate acquisition times out: ") {
          lifecycle.cancellationRequested() shouldBe true
        }
        helper.isAlive shouldBe true
        gate.isValid shouldBe true
        Files.getAttribute(controlDir, "basic:fileKey") shouldBe directoryKey
        Files.getAttribute(lockPath, "basic:fileKey") shouldBe lockKey
        val cancellationKey = Files.getAttribute(cancelled, "basic:fileKey")
        the[OpenIvmCliCleanupException] thrownBy
          compiler.terminateCliProcess(helper, lifecycle, System.nanoTime() + 50.millis.toNanos)
        Files.getAttribute(cancelled, "basic:fileKey") shouldBe cancellationKey

        gate.release()
        helper.waitFor(5, TimeUnit.SECONDS) shouldBe true
        helper.exitValue() shouldBe OpenIvmCliWorker.PROCESS_TIMEOUT
        lifecycle.launchCommitted() shouldBe false
        lifecycle.nativeIdentity().isPresent shouldBe false
        Seq(dir.resolve("pids"), controlDir.resolve("cli.stdout"), controlDir.resolve("cli.stderr"))
          .foreach(path => Files.exists(path) shouldBe false)
        (1 to 2).foreach(_ => lifecycle.requestCancellation(System.nanoTime() + 5.seconds.toNanos))
        Files.getAttribute(cancelled, "basic:fileKey") shouldBe cancellationKey
        Files.getAttribute(controlDir, "basic:fileKey") shouldBe directoryKey
        Files.getAttribute(lockPath, "basic:fileKey") shouldBe lockKey
      } finally {
        try {
          if (helper != null) {
            helper.destroyForcibly()
            helper.waitFor(5, TimeUnit.SECONDS) shouldBe true
          }
        } finally {
          if (gate.isValid) gate.release()
          channel.close()
          val files = Files.walk(controlDir)
          try files.sorted(java.util.Comparator.reverseOrder()).forEach(path => Files.deleteIfExists(path))
          finally files.close()
        }
      }
    }
  }

  it should "not signal a PID whose birth time differs from the acknowledged native root" in {
    val dir = Files.createTempDirectory(Files.createDirectories(Paths.get("target")), "compiler-startup-identity-")
    val lifecycle = new OpenIvmCliLifecycle(dir)
    lifecycle.initialize()
    lifecycle.beginLaunch() shouldBe true
    val helper = new ProcessBuilder("sleep", "300").start()
    val decoy  = new ProcessBuilder("sleep", "300").start()
    try {
      Files.write(dir.resolve("cli-native.identity"), s"${decoy.pid()}\n1970-01-01T00:00:00Z\n".getBytes("UTF-8"))
      Files.createFile(dir.resolve("cli-native.ready"))
      sharedCompiler.terminateCliProcess(helper, lifecycle, System.nanoTime() + 5.seconds.toNanos)
      decoy.isAlive shouldBe true
      helper.isAlive shouldBe false
    } finally {
      decoy.destroyForcibly()
      helper.destroyForcibly()
      val files = Files.walk(dir)
      try files.sorted(java.util.Comparator.reverseOrder()).forEach(path => Files.deleteIfExists(path))
      finally files.close()
    }
  }

  it should "interrupt a DuckDB CLI that never reads a large input script without leaking its process" in {
    withFakeCli("exec sleep 300") { (compiler, dir, context) =>
      val worker      = new AtomicReference[Thread]()
      val interrupted = new AtomicBoolean(false)
      val baseline    = OpenIvmMetrics.CompilerInflight.get()
      val result = Future {
        worker.set(Thread.currentThread())
        try
          compiler.compile(cliTestRequest.copy(viewSql = cliTestRequest.viewSql + " /*" + ("padding " * 32768) + "*/"))
        finally interrupted.set(Thread.currentThread().isInterrupted)
      }(context)
      val startedDeadline = 5.seconds.fromNow
      while (!Files.exists(dir.resolve("pids")) && startedDeadline.hasTimeLeft()) Thread.sleep(10)
      Files.exists(dir.resolve("pids")) shouldBe true
      worker.get().interrupt()
      val ex = the[OpenIvmCompileException] thrownBy Await.result(result, 10.seconds)
      ex.getCause shouldBe a[InterruptedException]
      interrupted.get() shouldBe true
      OpenIvmMetrics.CompilerInflight.get() shouldBe baseline
      assertCliStopped(dir)
    }
  }

  // ── Test 8: Thread safety ─────────────────────────────────────────────────

  it should "overlap 8 independent DuckDB subprocesses and compile without errors" in {
    val pool                          = Executors.newFixedThreadPool(8)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(pool)
    val barrierDir                    = Files.createTempDirectory("openivm_compile_overlap_")
    val wrapper                       = barrierDir.resolve("duckdb-barrier")
    val parentLibraryPath             = Option(System.getenv("LD_LIBRARY_PATH"))
    val nativeLibraryPath = (Seq(barrierDir.toString) ++
      sys.env.get("OPENIVM_CLI_LD_LIBRARY_PATH").filter(_.trim.nonEmpty))
      .mkString(java.io.File.pathSeparator)
    val subprocessLibraryPath = (Seq(nativeLibraryPath) ++ parentLibraryPath.filter(_.nonEmpty))
      .mkString(java.io.File.pathSeparator)
    def shellQuote(value: String): String = "'" + value.replace("'", "'\\''") + "'"
    Files.write(
      wrapper,
      s"""#!/usr/bin/env bash
         |set -euo pipefail
         |[[ "$${LD_LIBRARY_PATH:-}" == ${shellQuote(subprocessLibraryPath)} ]]
         |touch ${shellQuote(barrierDir.toString)}/started-$$$$
         |for attempt in {1..1200}; do
         |  peers=(${shellQuote(barrierDir.toString)}/started-*)
         |  if [[ $${#peers[@]} -eq 8 ]]; then
         |    exec ${shellQuote(sharedCompiler.cliPath)} "$$@"
         |  fi
         |  sleep 0.05
         |done
         |echo 'Eight DuckDB subprocesses did not overlap' >&2
         |exit 1
         |""".stripMargin.getBytes("UTF-8")
    )
    wrapper.toFile.setExecutable(true) shouldBe true
    val concurrentCompiler = OpenIvmCompiler.build(
      extensionPath,
      cliPath = wrapper.toString,
      nativeLibraryPath = Some(nativeLibraryPath)
    )
    try {
      val req = CompileRequest(
        viewName = "mv_concurrent_agg",
        viewSql = "SELECT region, SUM(amount) AS total FROM sales GROUP BY region",
        sources = Map("sales" -> salesSchema)
      )

      // No process can execute DuckDB until all eight have entered the CLI
      // boundary. A global compiler mutex fails the barrier instead of passing
      // merely because all calls eventually finish.
      val latch = new CountDownLatch(1)
      val futures = (1 to 8).map { _ =>
        Future {
          latch.await()
          concurrentCompiler.compile(req)
        }
      }
      latch.countDown()

      val results = Await.result(Future.sequence(futures), 120.seconds)
      results should have size 8
      results.foreach { r =>
        r.refreshType shouldBe 0
        r.refreshTypeName shouldBe "AGGREGATE_GROUP"
        r.sql should not be empty
      }
      Option(System.getenv("LD_LIBRARY_PATH")) shouldBe parentLibraryPath
    } finally {
      pool.shutdownNow()
      pool.awaitTermination(120, TimeUnit.SECONDS) shouldBe true
      concurrentCompiler.close()
      val files = Files.list(barrierDir)
      try files.forEach(path => Files.deleteIfExists(path))
      finally files.close()
      Files.deleteIfExists(barrierDir)
    }
  }

  // ── Test 9: Post-close behaviour ──────────────────────────────────────────

  it should "throw IllegalStateException when compile is called after close()" in {
    val disposableCompiler = OpenIvmCompiler.build(extensionPath)
    disposableCompiler.close()
    val req = CompileRequest(
      viewName = "mv_after_close",
      viewSql = "SELECT region FROM sales",
      sources = Map("sales" -> salesSchema)
    )
    a[IllegalStateException] should be thrownBy disposableCompiler.compile(req)
  }

  // ── Test 10: Spark→DuckDB syntax normalization ────────────────────────────

  "normalizeSparkSqlForDuckdb" should "translate LEFT SEMI/ANTI JOIN to bare SEMI/ANTI JOIN" in {
    val in = "SELECT g.* FROM gods g LEFT SEMI JOIN payments p ON g.uid = p.from_uid"
    sharedCompiler.normalizeSparkSqlForDuckdb(in) shouldBe
      "SELECT g.* FROM gods g SEMI JOIN payments p ON g.uid = p.from_uid"

    val in2 = "SELECT g.* FROM gods g LEFT ANTI JOIN payments p ON g.uid = p.from_uid"
    sharedCompiler.normalizeSparkSqlForDuckdb(in2) shouldBe
      "SELECT g.* FROM gods g ANTI JOIN payments p ON g.uid = p.from_uid"
  }

  it should "be case-insensitive and whitespace-tolerant" in {
    val in  = "SELECT * FROM a   left   semi   join b ON a.k = b.k"
    val out = sharedCompiler.normalizeSparkSqlForDuckdb(in)
    out should not include "left"
    out.toLowerCase should include("semi join")
    // Round-trip safe: applying the normalizer again is a no-op.
    sharedCompiler.normalizeSparkSqlForDuckdb(out) shouldBe out
  }

  it should "be a no-op for SQL without LEFT SEMI/ANTI JOIN clauses" in {
    val in = "SELECT region, SUM(amount) FROM sales GROUP BY region"
    sharedCompiler.normalizeSparkSqlForDuckdb(in) shouldBe in
  }

  it should "translate Spark backslash-escaped string literals to DuckDB's doubled-quote convention first" in {
    // '\\' (Spark: one backslash value) and '\'' (Spark: one quote value) must
    // both be DuckDB-parseable before any other pass scans the SQL for quotes.
    val in = "SELECT REPLACE(REPLACE(txt, '\\\\', '_'), '\\'', '_') AS cleaned FROM t"
    sharedCompiler.normalizeSparkSqlForDuckdb(in) shouldBe
      "SELECT REPLACE(REPLACE(txt, '\\', '_'), '''', '_') AS cleaned FROM t"
  }

  // ── Test 11: stripDbQualifiers (Hive/dbt qualified-name handling) ─────────

  "stripDbQualifiers" should "be a no-op when the qualified map is empty" in {
    val sql = "SELECT * FROM sales"
    sharedCompiler.stripDbQualifiers(sql, Map.empty) shouldBe sql
  }

  it should "be a no-op when no qualified name actually contains a dot" in {
    val sql = "SELECT * FROM sales"
    // short == qualified — nothing to strip.
    sharedCompiler.stripDbQualifiers(sql, Map("sales" -> "sales")) shouldBe sql
  }

  it should "rewrite tpcdi.sales to sales" in {
    val sql = "SELECT region FROM tpcdi.sales WHERE amount > 0"
    sharedCompiler.stripDbQualifiers(sql, Map("sales" -> "tpcdi.sales")) shouldBe
      "SELECT region FROM sales WHERE amount > 0"
  }

  it should "rewrite multiple distinct qualified sources in a JOIN" in {
    val sql =
      "SELECT e.id FROM tpcdi.employees e JOIN tpcdi.departments d ON e.dept = d.id"
    sharedCompiler.stripDbQualifiers(
      sql,
      Map("employees" -> "tpcdi.employees", "departments" -> "tpcdi.departments")
    ) shouldBe
      "SELECT e.id FROM employees e JOIN departments d ON e.dept = d.id"
  }

  it should "not touch column qualifications like alias.col" in {
    val sql = "SELECT t.region FROM tpcdi.sales t"
    sharedCompiler.stripDbQualifiers(sql, Map("sales" -> "tpcdi.sales")) shouldBe
      "SELECT t.region FROM sales t"
  }

  it should "be case-insensitive on the qualified name" in {
    val sql = "SELECT * FROM TPCDI.SALES"
    sharedCompiler.stripDbQualifiers(sql, Map("sales" -> "tpcdi.sales")) shouldBe
      "SELECT * FROM sales"
  }

  it should "prefer the longest qualified name when both 2-part and 3-part are tracked" in {
    val sql =
      "SELECT * FROM spark_catalog.tpcdi.sales s JOIN tpcdi.audit a ON s.id = a.id"
    sharedCompiler.stripDbQualifiers(
      sql,
      Map("sales" -> "spark_catalog.tpcdi.sales", "audit" -> "tpcdi.audit")
    ) shouldBe "SELECT * FROM sales s JOIN audit a ON s.id = a.id"
  }

  // ── Test 12: stripSparkBacktickIdentifiers (dbt backtick-quoted sources) ──

  "stripSparkBacktickIdentifiers" should "unquote a simple backtick-quoted db qualifier" in {
    val sql = "SELECT region FROM `arc_sql_db_bi`.machine_infrastructure_dim"
    sharedCompiler.stripSparkBacktickIdentifiers(sql) shouldBe
      "SELECT region FROM arc_sql_db_bi.machine_infrastructure_dim"
  }

  it should "unquote every backtick-quoted identifier segment" in {
    val sql = "SELECT d.id FROM `lakehouse_openivm`.arc_sql_server_pit_28d_mat_view d"
    sharedCompiler.stripSparkBacktickIdentifiers(sql) shouldBe
      "SELECT d.id FROM lakehouse_openivm.arc_sql_server_pit_28d_mat_view d"
  }

  it should "double-quote a non-simple identifier and escape internal quotes" in {
    val sql = "SELECT `weird col` FROM t"
    sharedCompiler.stripSparkBacktickIdentifiers(sql) shouldBe
      "SELECT \"weird col\" FROM t"
  }

  it should "honor `` as an escaped backtick inside an identifier" in {
    val sql = "SELECT `a``b` FROM t"
    sharedCompiler.stripSparkBacktickIdentifiers(sql) shouldBe
      "SELECT \"a`b\" FROM t"
  }

  it should "leave backticks inside single-quoted string literals untouched" in {
    val sql = "SELECT region FROM t WHERE note = 'use `backticks` here'"
    sharedCompiler.stripSparkBacktickIdentifiers(sql) shouldBe sql
  }

  it should "correctly skip a Spark backslash-escaped quote inside a literal without desyncing a later backtick identifier" in {
    // The Spark literal `'it\'s here'` contains a backslash-escaped quote.
    // A scanner unaware of Spark's backslash-escape convention would treat
    // the escaped quote as the literal's close, desyncing everything after
    // it -- including a genuine backtick-quoted identifier later in the SQL.
    val sql = "SELECT region FROM t WHERE note = 'it\\'s here' AND x = `lakehouse_openivm`.foo"
    sharedCompiler.stripSparkBacktickIdentifiers(sql) shouldBe
      "SELECT region FROM t WHERE note = 'it\\'s here' AND x = lakehouse_openivm.foo"
  }

  it should "not let an apostrophe in a -- line comment desync backtick stripping" in {
    // A `--` comment with an apostrophe (e.g. possessive "consumer's") must not
    // be treated as an open string literal, or real backtick-quoted identifiers
    // after it are skipped (the MV-over-MV COMPILE_FAILED regression).
    val sql =
      "-- consumer's note\nSELECT region FROM `lakehouse_openivm`.foo_mat_view"
    sharedCompiler.stripSparkBacktickIdentifiers(sql) shouldBe
      "-- consumer's note\nSELECT region FROM lakehouse_openivm.foo_mat_view"
  }

  it should "not let an apostrophe in a /* */ block comment desync backtick stripping" in {
    val sql = "/* it's fine */ SELECT a FROM `db`.t"
    sharedCompiler.stripSparkBacktickIdentifiers(sql) shouldBe
      "/* it's fine */ SELECT a FROM db.t"
  }

  it should "be a no-op for SQL with no backticks" in {
    val sql = "SELECT region FROM sales WHERE amount > 0"
    sharedCompiler.stripSparkBacktickIdentifiers(sql) shouldBe sql
  }

  it should "compose with stripDbQualifiers to reduce a backtick-quoted source to its short name" in {
    // Reproduces the dbt-server benchmark corpus pattern that silently demoted
    // every MV to COMPILE_FAILED -> FULL_REFRESH before the backtick fix.
    val raw      = "SELECT region FROM `arc_sql_db_bi`.machine_infrastructure_dim WHERE is_row_effective = TRUE"
    val unticked = sharedCompiler.stripSparkBacktickIdentifiers(raw)
    sharedCompiler.stripDbQualifiers(
      unticked,
      Map("machine_infrastructure_dim" -> "arc_sql_db_bi.machine_infrastructure_dim")
    ) shouldBe "SELECT region FROM machine_infrastructure_dim WHERE is_row_effective = TRUE"
  }

  // ── Test 13: compile-failure diagnostics ──────────────────────────────────

  "classifyCompileFailureStage" should
    "classify as CompileWithFacts when CREATE succeeded (stdout carries the MV-creation marker)" in {
      val stdout = """{"MATERIALIZED VIEW CREATION":"true"}"""
      OpenIvmCompiler.classifyCompileFailureStage(stdout) shouldBe OpenIvmCompiler.CompileFailureStage.CompileWithFacts
    }

  it should "classify as CreateOrBind when CREATE itself never succeeded (no marker in stdout)" in {
    OpenIvmCompiler.classifyCompileFailureStage("") shouldBe OpenIvmCompiler.CompileFailureStage.CreateOrBind
    OpenIvmCompiler.classifyCompileFailureStage(
      """{"some_other_row":"true"}"""
    ) shouldBe OpenIvmCompiler.CompileFailureStage.CreateOrBind
  }

  "compile" should "surface the classified failure stage in the exception message for a CREATE/bind failure" in {
    val req = CompileRequest(
      viewName = "mv_diag_bad_fn",
      viewSql = "SELECT totally_bogus_fn_xyz(id) AS d FROM t",
      sources = Map("t" -> tSchema)
    )
    val ex = the[OpenIvmCompileException] thrownBy sharedCompiler.compile(req)
    ex.getMessage should include("CREATE MATERIALIZED VIEW (bind)")
  }

  it should "persist a diagnostic bundle (original/normalized SQL, facts, stdout/stderr, stage) when a bundle dir is configured" in {
    val bundleRoot   = Files.createTempDirectory("openivm-failure-bundle-test")
    val diagCompiler = OpenIvmCompiler.build(extensionPath, failureBundleDir = Some(bundleRoot.toString))
    try {
      val req = CompileRequest(
        viewName = "mv_diag_bundle",
        viewSql = "SELECT totally_bogus_fn_xyz(id) AS d FROM t",
        sources = Map("t" -> tSchema)
      )
      a[OpenIvmCompileException] should be thrownBy diagCompiler.compile(req)

      val bundleDirs = bundleRoot.toFile.listFiles(_.isDirectory)
      bundleDirs should not be null
      bundleDirs should have length 1
      val dir = bundleDirs.head.toPath

      def read(name: String): String = new String(Files.readAllBytes(dir.resolve(name)), "UTF-8")

      dir.getFileName.toString should startWith("mv_diag_bundle-")
      read("original.sql") should include("totally_bogus_fn_xyz")
      read("normalized.sql") should not be empty
      read("facts.json") should include("\"target_dialect\"")
      read("stderr.log") should include("totally_bogus_fn_xyz")
      read("stage.txt").trim shouldBe "CREATE MATERIALIZED VIEW (bind)"
    } finally {
      diagCompiler.close()
    }
  }

  // ── parseInitialLoadSql: path-bound pinned reads (TOCTOU wiring) ───────────

  "parseInitialLoadSql" should "path-bind a pinned initial-load source read to its verified Delta path" in {
    val tmpDir = Files.createTempDirectory("openivm-initload-pin")
    try {
      val viewName = "mv_ll_pin"
      val path     = "abfss://ws@onelake.dfs.fabric.microsoft.com/lh/Tables/arm_collection_dim"
      Files.write(
        tmpDir.resolve(s"openivm_compiled_queries_$viewName.sql"),
        s"create table openivm_data_$viewName as SELECT arm_collection_id FROM memory.main.arm_collection_dim;"
          .getBytes("UTF-8")
      )
      val req = CompileRequest(
        viewName = viewName,
        viewSql = "SELECT arm_collection_id FROM `arc_sql_db_bi`.`arm_collection_dim` VERSION AS OF 46094",
        sources = Map("arm_collection_dim" -> StructType.fromDDL("arm_collection_id INT")),
        sourceQualifiedNames = Map("arm_collection_dim" -> "arc_sql_db_bi.arm_collection_dim"),
        sourceSnapshotPinnedPaths = Map("arm_collection_dim" -> path)
      )
      val out = sharedCompiler.parseInitialLoadSql(tmpDir, req)
      out should include(s"delta.`$path` VERSION AS OF 46094")
      out should not include "memory.main"
      out should not include "arc_sql_db_bi"
    } finally {
      Files.walk(tmpDir).sorted(java.util.Comparator.reverseOrder()).forEach(p => Files.deleteIfExists(p))
    }
  }

  it should "hard-fail when a pinned initial-load source has no verified Delta path" in {
    val tmpDir = Files.createTempDirectory("openivm-initload-nopath")
    try {
      val viewName = "mv_ll_nopath"
      Files.write(
        tmpDir.resolve(s"openivm_compiled_queries_$viewName.sql"),
        s"create table openivm_data_$viewName as SELECT arm_collection_id FROM memory.main.arm_collection_dim;"
          .getBytes("UTF-8")
      )
      val req = CompileRequest(
        viewName = viewName,
        viewSql = "SELECT arm_collection_id FROM `arc_sql_db_bi`.`arm_collection_dim` VERSION AS OF 46094",
        sources = Map("arm_collection_dim" -> StructType.fromDDL("arm_collection_id INT")),
        sourceQualifiedNames = Map("arm_collection_dim" -> "arc_sql_db_bi.arm_collection_dim")
      )
      the[org.openivm.spark.common.PinnedSourcePathMissingException] thrownBy
        sharedCompiler.parseInitialLoadSql(tmpDir, req)
    } finally {
      Files.walk(tmpDir).sorted(java.util.Comparator.reverseOrder()).forEach(p => Files.deleteIfExists(p))
    }
  }

}
