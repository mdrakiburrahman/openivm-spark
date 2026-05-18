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

  // ── regexp_like ──────────────────────────────────────────────────────────────

  private val textSchema: StructType = StructType.fromDDL("id INT, txt STRING")

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

  // ── sparkFunctionShimsPrologue contract ──────────────────────────────────────

  "sparkFunctionShimsPrologue" should "register the regexp_like shim macro" in {
    val p = OpenIvmCompiler.sparkFunctionShimsPrologue
    p should include("CREATE OR REPLACE MACRO regexp_like")
  }
}
