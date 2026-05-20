package org.openivm.spark.compiler

import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Validates the Spark-only function shims registered in
  * [[OpenIvmCompiler.sparkFunctionShimsPrologue]] + their LptsSparkDialect
  * back-translations. Each shim must satisfy three properties:
  *
  *  1. `OpenIvmCompiler.compile` succeeds on an MV body that calls the
  *     function — i.e. DuckDB's binder accepts the macro and openivm
  *     classifies the resulting plan.
  *  2. The emitted refresh SQL (after LptsSparkDialect.translate) contains
  *     a Spark-executable expression — either the original Spark function
  *     name preserved (when openivm preserves the macro name) OR a back-
  *     translated form (when openivm inlines the macro body — see
  *     `rewriteSparkFunctionInlinings`).
  *  3. The classification is not FULL_REFRESH (the shim unblocks
  *     incrementalization).
  */
class OpenIvmCompilerShimSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val extensionPath: String =
    sys.env.getOrElse("OPENIVM_EXTENSION_PATH", "/opt/openivm/openivm.duckdb_extension")

  private var compiler: OpenIvmCompiler = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    compiler = OpenIvmCompiler.build(extensionPath)
  }

  override def afterAll(): Unit = {
    if (compiler != null) compiler.close()
    super.afterAll()
  }

  // Helper: compile a view body that uses the shim, returns CompiledRefresh.
  private def compileBody(
      viewName: String,
      sources: Map[String, StructType],
      body: String
  ): CompiledRefresh =
    compiler.compile(CompileRequest(viewName = viewName, viewSql = body, sources = sources))

  // ── regexp_like / date-function / last_value shims ───────────────────────────

  private val textSchema: StructType    = StructType.fromDDL("id INT, txt STRING")
  private val rawDateSchema: StructType = StructType.fromDDL("id INT, raw STRING")
  private val tsSchema: StructType      = StructType.fromDDL("id INT, ts TIMESTAMP")
  private val windowSchema: StructType  = StructType.fromDDL("id INT, grp INT, ts TIMESTAMP, name STRING")

  "shim regexp_like(s, p)" should "compile and emit a non-FULL_REFRESH classification" in {
    val r = compileBody(
      viewName = "shim_re_v1",
      sources = Map("textsrc" -> textSchema),
      body = "SELECT id, txt FROM textsrc WHERE regexp_like(txt, '^[A-Z]+$')"
    )
    // Filter-projection over a single base table → SIMPLE_PROJECTION (or equivalent
    // non-FULL_REFRESH classification). A FULL_REFRESH here would mean DuckDB
    // rejected the call.
    r.refreshTypeName should not equal "FULL_REFRESH"
  }

  it should "emit a Spark-executable form (regexp_like) after LptsSparkDialect back-translation" in {
    val r = compileBody(
      viewName = "shim_re_v2",
      sources = Map("textsrc" -> textSchema),
      body = "SELECT id, txt FROM textsrc WHERE regexp_like(txt, '^[A-Z]+$')"
    )
    // openivm's LPTS serializer INLINES the macro body, so the raw emitted
    // SQL contains `regexp_matches` (the DuckDB function). The
    // LptsSparkDialect post-pass rewrites it back to Spark's `regexp_like`.
    val translated = LptsSparkDialect.translate(r.sql)
    translated should include("regexp_like")
    translated should not include "regexp_matches"
  }

  "shim to_date(s)" should "compile, stay incremental, and translate back to Spark's 1-arg to_date" in {
    val r = compileBody(
      viewName = "shim_to_date_v0",
      sources = Map("datesrc" -> rawDateSchema),
      body = "SELECT id, to_date(raw) AS parsed_date FROM datesrc"
    )
    r.refreshTypeName should not equal "FULL_REFRESH"

    val translated = LptsSparkDialect.translate(r.sql)
    translated should include("to_date(")
    translated should not include "strptime"
    translated should not include "%Y-%m-%d"
  }

  "shim to_date(s, fmt)" should "compile, stay incremental, and translate back to Spark to_date" in {
    val r = compileBody(
      viewName = "shim_to_date_v1",
      sources = Map("datesrc" -> rawDateSchema),
      body = "SELECT id, to_date(raw, 'yyyyMMdd') AS parsed_date FROM datesrc"
    )
    r.refreshTypeName should not equal "FULL_REFRESH"

    val translated = LptsSparkDialect.translate(r.sql)
    translated should include("to_date(")
    translated should not include "strptime"
  }

  "shim to_timestamp(s)" should "compile, stay incremental, and translate back to Spark's 1-arg to_timestamp" in {
    val r = compileBody(
      viewName = "shim_to_timestamp_v0",
      sources = Map("timesrc" -> rawDateSchema),
      body = "SELECT id, to_timestamp(raw) AS parsed_ts FROM timesrc"
    )
    r.refreshTypeName should not equal "FULL_REFRESH"

    val translated = LptsSparkDialect.translate(r.sql)
    translated should include("to_timestamp(")
    translated should not include "strptime"
    translated should not include "%Y-%m-%d %H:%M:%S"
  }

  "shim to_timestamp(s, fmt)" should "compile, stay incremental, and translate back to Spark to_timestamp" in {
    val r = compileBody(
      viewName = "shim_to_timestamp_v1",
      sources = Map("timesrc" -> rawDateSchema),
      body = "SELECT id, to_timestamp(raw, 'yyyy-MM-dd HH:mm:ss') AS parsed_ts FROM timesrc"
    )
    r.refreshTypeName should not equal "FULL_REFRESH"

    val translated = LptsSparkDialect.translate(r.sql)
    translated should include("to_timestamp(")
    translated should not include "strptime"
  }

  "shim date_format(d, fmt)" should "compile, stay incremental, and translate back to Spark date_format" in {
    val r = compileBody(
      viewName = "shim_date_format_v1",
      sources = Map("timesrc" -> tsSchema),
      body = "SELECT id, date_format(ts, 'yyyyMMdd') AS formatted_ts FROM timesrc"
    )
    r.refreshTypeName should not equal "FULL_REFRESH"

    val translated = LptsSparkDialect.translate(r.sql)
    translated should include("date_format(")
    translated should not include "strftime"
  }

  "shim last_value(expr, ignoreNulls)" should "compile, stay incremental, and emit Spark's 1-arg last_value at refresh time" in {
    val r = compileBody(
      viewName = "shim_last_value_v1",
      sources = Map("winsrc" -> windowSchema),
      body =
        "SELECT id, grp, ts, last_value(name, true) OVER (PARTITION BY grp ORDER BY ts) AS carried_name FROM winsrc"
    )
    r.refreshTypeName should not equal "FULL_REFRESH"

    val translated = LptsSparkDialect.translate(r.sql)
    translated should not include "__sparkfn_last_value"
    translated should include("last_value(")
  }

  // ── sparkFunctionShimsPrologue contract ──────────────────────────────────────

  "sparkFunctionShimsPrologue" should "register the regexp_like and __sparkfn_* shim macros" in {
    val p = OpenIvmCompiler.sparkFunctionShimsPrologue
    p should include("CREATE OR REPLACE MACRO regexp_like")
    p should include("CREATE OR REPLACE MACRO __sparkfn_to_date_1arg(s) AS CAST(strptime(s, '%Y-%m-%d') AS DATE);")
    p should include("CREATE OR REPLACE MACRO __sparkfn_to_date(s, fmt) AS CAST(strptime(s, fmt) AS DATE);")
    p should include("CREATE OR REPLACE MACRO __sparkfn_to_timestamp_1arg(s) AS strptime(s, '%Y-%m-%d %H:%M:%S');")
    p should include("CREATE OR REPLACE MACRO __sparkfn_to_timestamp(s, fmt) AS strptime(s, fmt);")
    p should include("CREATE OR REPLACE MACRO __sparkfn_date_format(d, fmt) AS strftime(d, fmt);")
    p should include("CREATE OR REPLACE MACRO __sparkfn_last_value(expr, ignore_nulls) AS last(expr);")
  }
}
