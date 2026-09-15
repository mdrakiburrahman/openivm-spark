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
  *     incrementalization) — EXCEPT `current_date()` / `current_timestamp()`,
  *     which are inherently non-deterministic per invocation, so a
  *     FULL_REFRESH classification for those two is expected and acceptable;
  *     the only requirement for them is that compile succeeds (no binder
  *     error) and the emitted SQL round-trips back to Spark's own spelling.
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
  private val epochSchema: StructType   = StructType.fromDDL("id INT, epoch BIGINT")
  private val windowSchema: StructType  = StructType.fromDDL("id INT, grp INT, ts TIMESTAMP, name STRING")
  private val intervalSchema: StructType =
    StructType.fromDDL("dim_date_billing_usage_event_key STRING, dim_time_billing_usage_event_key INT")
  private val billingJsonSchema: StructType =
    StructType.fromDDL("usageuploadtime TIMESTAMP, azureresourceid STRING, additionalinfo STRING")

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

  it should "compile to_date(<timestamp_col>) without binder error" in {
    val r = compileBody(
      viewName = "shim_to_date_v0_ts",
      sources = Map("timesrc" -> tsSchema),
      body = "SELECT id, to_date(ts) AS parsed_date FROM timesrc"
    )
    r.refreshTypeName should not equal "FULL_REFRESH"

    val translated = LptsSparkDialect.translate(r.sql)
    translated should include("to_date(")
    translated should not include "CASE WHEN"
    translated should not include "IS NOT NULL"
  }

  "shim to_date(s, fmt)" should "compile, stay incremental, and translate to a Spark-executable DATE cast" in {
    val r = compileBody(
      viewName = "shim_to_date_v1",
      sources = Map("datesrc" -> rawDateSchema),
      body = "SELECT id, to_date(raw, 'yyyyMMdd') AS parsed_date FROM datesrc"
    )
    r.refreshTypeName should not equal "FULL_REFRESH"

    val translated = LptsSparkDialect.translate(r.sql)
    translated should include("to_timestamp(")
    translated.toUpperCase should include(" AS DATE)")
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

  it should "compile to_timestamp(<bigint_col>) without binder error" in {
    val r = compileBody(
      viewName = "shim_to_timestamp_v0_epoch",
      sources = Map("epochsrc" -> epochSchema),
      body = "SELECT id, to_timestamp(epoch) AS parsed_ts FROM epochsrc"
    )
    r.refreshTypeName should not equal "FULL_REFRESH"

    val translated = LptsSparkDialect.translate(r.sql)
    translated should include("to_timestamp(")
    translated should not include "CASE WHEN"
    translated should not include "IS NOT NULL"
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

  "shim make_interval(...)" should
    "compile the status/billing canary shape, stay incremental, and restore Spark make_interval" in {
      val r = compileBody(
        viewName = "shim_make_interval_v1",
        sources = Map("intervalsrc" -> intervalSchema),
        body = "SELECT to_timestamp(dim_date_billing_usage_event_key, 'yyyyMMdd') + " +
          "make_interval(0, 0, 0, 0, 0, 0, cast(dim_time_billing_usage_event_key AS int)) AS event_time " +
          "FROM intervalsrc"
      )
      r.refreshTypeName should not equal "FULL_REFRESH"

      r.initialLoadSql should include("make_interval(0, 0, 0, 0, 0, 0, dim_time_billing_usage_event_key)")
      r.initialLoadSql should not include SparkFunctionShimSql.MakeIntervalMarker

      val translated = LptsSparkDialect.translate(r.sql)
      translated should include("make_interval(0, 0, 0, 0, 0, 0,")
      translated should not include SparkFunctionShimSql.MakeIntervalMarker
      translated should not include "__sparkfn_make_interval"
    }

  "shim get_json_object(json, path)" should
    "compile the silver-billing canary shape, stay incremental, and restore Spark JSON extraction" in {
      val r = compileBody(
        viewName = "shim_get_json_object_v1",
        sources = Map("billingsrc" -> billingJsonSchema),
        body = """SELECT
            |  usageuploadtime AS usage_upload_time,
            |  LOWER(TRIM(azureresourceid)) AS azureresourceid,
            |  COALESCE(
            |    LOWER(GET_JSON_OBJECT(additionalinfo, '$.ArcMachineResourceUri')),
            |    LOWER('NOT APPLICABLE')
            |  ) AS container_resource_id,
            |  TRY_CAST(GET_JSON_OBJECT(additionalinfo, '$.NumberOfCores') AS INT) AS billing_reported_cores
            |FROM billingsrc""".stripMargin
      )
      r.refreshTypeName should not equal "FULL_REFRESH"

      r.initialLoadSql should include("get_json_object(additionalinfo, '$.ArcMachineResourceUri')")
      r.initialLoadSql should include("get_json_object(additionalinfo, '$.NumberOfCores')")
      r.initialLoadSql should not include SparkFunctionShimSql.GetJsonObjectMarker

      val translated = LptsSparkDialect.translate(r.sql)
      translated should include("get_json_object(")
      translated should include("$.ArcMachineResourceUri")
      translated should include("$.NumberOfCores")
      translated should not include SparkFunctionShimSql.GetJsonObjectMarker
      translated should not include "__sparkfn_get_json_object"
    }

  "shim last_value(expr, ignoreNulls)" should "compile, stay incremental, and emit Spark's ignore-null last_value at refresh time" in {
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
    translated should include(", true)")
  }

  "shim first_value(expr, ignoreNulls)" should "compile, stay incremental, and emit Spark's ignore-null first_value at refresh time" in {
    val r = compileBody(
      viewName = "shim_first_value_v1",
      sources = Map("winsrc" -> windowSchema),
      body =
        "SELECT id, grp, ts, first_value(name, true) OVER (PARTITION BY grp ORDER BY ts) AS carried_name FROM winsrc"
    )
    r.refreshTypeName should not equal "FULL_REFRESH"

    val translated = LptsSparkDialect.translate(r.sql)
    translated should not include "__sparkfn_first_value"
    translated should include("first_value(")
    translated should include(", true)")
  }

  "shim current_date()" should "compile without a binder error and translate back to Spark's current_date()" in {
    val r = compileBody(
      viewName = "shim_current_date_v1",
      sources = Map("datesrc" -> rawDateSchema),
      body = "SELECT id, current_date() AS today FROM datesrc"
    )

    val translated = LptsSparkDialect.translate(r.sql)
    translated should include("current_date()")
    translated should not include "get_current_timestamp"
  }

  "shim current_timestamp()" should "compile without a binder error and translate back to Spark's current_timestamp()" in {
    val r = compileBody(
      viewName = "shim_current_timestamp_v1",
      sources = Map("datesrc" -> rawDateSchema),
      body = "SELECT id, current_timestamp() AS now_ts FROM datesrc"
    )

    val translated = LptsSparkDialect.translate(r.sql)
    translated should include("current_timestamp()")
    translated should not include "get_current_timestamp"
  }

  "shim Spark backslash-escaped string literals (normalize_os_name fragment)" should
    "compile, stay incremental, and preserve Spark-safe literal escaping through the refresh SQL" in {
      val r = compileBody(
        viewName = "shim_os_name_v1",
        sources = Map("textsrc" -> textSchema),
        body = """SELECT id,
            |  TRIM('_' FROM
            |    REPLACE(REPLACE(REPLACE(REPLACE(
            |      LOWER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(txt,
            |        ' ', '_'), '-', '_'), '.', '_'), '/', '_'), '\\', '_'), '(', '_'), ')', '_'), ',', '_'), ':', '_'), '\'', '_'))
            |    , '____', '_'), '___', '_'), '__', '_'), '__', '_')
            |  ) AS cleaned_txt
            |FROM textsrc""".stripMargin
      )
      // A nested scalar REPLACE/TRIM projection over a single base table stays
      // incremental -- unlike current_date()/current_timestamp(), there is
      // nothing non-deterministic here.
      r.refreshTypeName should not equal "FULL_REFRESH"

      // The initial-load SQL must rewrite DuckDB's serialized positional
      // 2-arg `trim(<expr>, '_')` call into Spark's unambiguous ANSI
      // `TRIM('_' FROM <expr>)` form. Left as a plain positional call, Spark
      // resolves the two arguments in the OPPOSITE order from DuckDB (trim
      // chars first, source string second), silently trimming every
      // character that appears in `<expr>` off the two-character string
      // `'_'` -- erasing every row to an empty string with no parse error.
      r.initialLoadSql should include("TRIM('_' FROM")
      r.initialLoadSql should not include "trim(replace"
      r.initialLoadSql should not include "`trim`("

      // The initial-load SQL is translated exactly once by the compiler and
      // once more by the extension when it materializes the CREATE; both
      // the trim-argument-order fix and the literal-escaping fix must be
      // stable under that double application.
      LptsSparkDialect.translate(r.initialLoadSql) shouldBe r.initialLoadSql

      // The refresh SQL must be safe for SPARK to re-parse: a literal
      // backslash and a literal quote must be lifted out of string-literal
      // syntax into unambiguous concat(chr(...)) expressions, since neither
      // Spark's backslash-escape pairing nor DuckDB's native doubled-quote
      // escaping survive this translation being applied twice (once when
      // the compiler first parses the initial-load SQL, again when the
      // extension materializes it).
      val translated = LptsSparkDialect.translate(r.sql)
      translated should include("concat(chr(92))")
      translated should include("concat(chr(39))")
      translated should not include "'\\'"
      translated should not include "''''"

      // The refresh SQL's own `trim(<expr>, '_')` call must also be swapped
      // to the ANSI form, and stable under a second translate application.
      translated should include("TRIM('_' FROM")
      translated should not include "`trim`("
      LptsSparkDialect.translate(translated) shouldBe translated
    }

  // ── sparkFunctionShimsPrologue contract ──────────────────────────────────────

  "sparkFunctionShimsPrologue" should "register the regexp_like and __sparkfn_* shim macros" in {
    val p = OpenIvmCompiler.sparkFunctionShimsPrologue
    p should include("CREATE OR REPLACE MACRO regexp_like")
    p should include(
      "CREATE OR REPLACE MACRO __sparkfn_to_date_1arg(s) AS CAST(CASE WHEN s IS NOT NULL THEN NULL WHEN s IS NULL THEN NULL END AS DATE);"
    )
    p should include("CREATE OR REPLACE MACRO __sparkfn_to_date(s, fmt) AS CAST(strptime(s, fmt) AS DATE);")
    p should include(
      "CREATE OR REPLACE MACRO __sparkfn_to_timestamp_1arg(s) AS CAST(CASE WHEN s IS NOT NULL THEN NULL WHEN s IS NULL THEN NULL END AS TIMESTAMP);"
    )
    p should include("CREATE OR REPLACE MACRO __sparkfn_to_timestamp(s, fmt) AS strptime(s, fmt);")
    p should include("CREATE OR REPLACE MACRO __sparkfn_date_format(d, fmt) AS strftime(d, fmt);")
    p should include("CREATE OR REPLACE MACRO __sparkfn_make_interval")
    p should include(SparkFunctionShimSql.MakeIntervalMarker)
    p should include("CREATE OR REPLACE MACRO __sparkfn_get_json_object")
    p should include(SparkFunctionShimSql.GetJsonObjectMarker)
    p should include("CREATE OR REPLACE MACRO __sparkfn_last_value(expr, ignore_nulls) AS last(expr);")
    p should include("CREATE OR REPLACE MACRO __sparkfn_first_value(expr, ignore_nulls) AS first(expr);")
    p should include(
      "CREATE OR REPLACE MACRO __sparkfn_current_timestamp() AS CAST(get_current_timestamp() AS TIMESTAMP);"
    )
    p should include(
      "CREATE OR REPLACE MACRO __sparkfn_current_date() AS CAST(CAST(get_current_timestamp() AS TIMESTAMP) AS DATE);"
    )
  }
}
