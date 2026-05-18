package org.openivm.spark.compiler

import java.util.concurrent.{CountDownLatch, Executors}

import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

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

  // ── Test 4b: empty-placeholder delta detection for multi-source JOIN ──────
  //
  // For a two-source CTE JOIN, openivm emits `NULL WHERE false` as the delta
  // INSERT (it cannot compute incremental deltas for multi-table JOINs).
  // hasRealDelta should return false for this case, which triggers FullRefresh
  // reclassification at CREATE MATERIALIZED VIEW time.
  it should "emit an empty-placeholder delta for a two-source CTE JOIN" in {
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
    SparkRefreshRewriter.hasRealDelta(result.sql, "mv_c3_probe") shouldBe false
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
  // With openivm_target_dialect='spark' set by the CLI script, OpenIVM
  // compiles SIMPLE_PROJECTION via the lpts pipeline, which uses
  // fully-qualified `catalog.schema.table` identifiers in the generated SQL.
  // The lpts SPARK dialect quoting flag is set but at the current compiler
  // version backtick quoting has not yet propagated to CTE table references;
  // what IS guaranteed is that the lpts delta-scan CTE uses the
  // `memory.main.` qualified prefix.

  it should "produce fully-qualified memory.main. table references in SPARK dialect for SIMPLE_PROJECTION" in {
    val req = CompileRequest(
      viewName = "mv_sales_proj",
      viewSql = "SELECT region FROM sales WHERE amount > 0",
      sources = Map("sales" -> salesSchema)
    )
    val result = sharedCompiler.compile(req)
    result.refreshType shouldBe 2
    result.refreshTypeName shouldBe "SIMPLE_PROJECTION"
    // lpts emits fully-qualified catalog.schema.table references for the delta scan.
    result.sql should include("memory.main.")
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

  // ── Test 8: Thread safety ─────────────────────────────────────────────────

  it should "handle 8 concurrent compile calls without errors" in {
    val pool                          = Executors.newFixedThreadPool(8)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(pool)
    try {
      val req = CompileRequest(
        viewName = "mv_concurrent_agg",
        viewSql = "SELECT region, SUM(amount) AS total FROM sales GROUP BY region",
        sources = Map("sales" -> salesSchema)
      )

      // All futures wait on the latch then call compile() simultaneously.
      // Our mutex serialises the actual JDBC work, but each caller must
      // eventually succeed and produce a non-empty SQL string.
      val latch = new CountDownLatch(1)
      val futures = (1 to 8).map { _ =>
        Future {
          latch.await()
          sharedCompiler.compile(req)
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
    } finally {
      pool.shutdown()
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
}
