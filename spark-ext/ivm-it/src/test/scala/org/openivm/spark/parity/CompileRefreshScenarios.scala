package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.apache.spark.sql.types._
import org.openivm.spark.compiler.{CompileRequest, OpenIvmCompiler}

/** Parity port of `openivm/test/sql/compile_refresh.test`.
  *
  * The openivm test exercises `openivm_compile_with_facts('mv_region', '{}')`,
  * which returns the compiled refresh SQL WITHOUT executing it — purely an
  * emission test. The Spark-side analogue is
  * [[org.openivm.spark.compiler.OpenIvmCompiler#compile]], which embeds an
  * in-process DuckDB session loaded with the OpenIVM extension and returns a
  * [[org.openivm.spark.compiler.CompiledRefresh]] containing the RefreshType
  * ordinal, name, and the generated SQL.
  *
  * The openivm-spark compile path is, by design, the "compile only" path: it never
  * mutates any Spark catalog or executes any DML. Execution lives in
  * [[org.openivm.spark.commands.RefreshMaterializedViewCommand]] (covered by
  * other parity specs).
  *
  * All tests require the OpenIVM DuckDB extension at the path given by
  * `OPENIVM_EXTENSION_PATH` (default `/opt/openivm/openivm.duckdb_extension`),
  * which is installed inside the `spark-ext` Docker image.
  *
  * == Mapping of openivm scenarios → Spark assertions ==
  *
  *   - openivm Test 1 (`openivm_compile_with_facts` returns refresh_type /
  *     refresh_type_name / sql for an AGGREGATE_GROUP view) → `compile`
  *     returns `refreshType = 0`, `refreshTypeName = "AGGREGATE_GROUP"`, and
  *     SQL containing `openivm_delta_mv_region` (case-insensitive).
  *   - openivm Test 2 (`openivm_compile_with_facts` does not mutate the MV) →
  *     `compile` is a pure function; verified here by calling `compile` twice
  *     with the same request and asserting structural equality of both
  *     results.
  *   - openivm Test 3 (subsequent `PRAGMA refresh` still works after
  *     `openivm_compile_with_facts` — i.e. the per-call `compile_only` fact
  *     did not leak as global state) → in openivm-spark the compile path and
  *     the executor are different code paths, so there is no flag to leak;
  *     we exercise the compiler twice in a row to assert it remains usable
  *     for downstream `compile` calls.
  *   - openivm Test 4 (`target_dialect="spark"` in the CompileFacts JSON) →
  *     `OpenIvmCompiler` always emits SPARK-dialect SQL; verified for a
  *     SIMPLE_PROJECTION view by asserting the generated SQL contains the
  *     fully-qualified `memory.main.` prefix that the lpts SPARK pipeline
  *     emits.
  *   - openivm Test 5 (AGGREGATE_GROUP compile returns non-empty SQL) →
  *     duplicate of Test 1 with a deliberately different shape, retained for
  *     1:1 parity.
  *   - openivm Test 6 (`openivm_compile_with_facts('does_not_exist', '{}')`
  *     fails with "materialized view 'does_not_exist' not found") →
  *     `OpenIvmCompiler.compile` takes a self-contained [[CompileRequest]]
  *     (it doesn't look up persistent views), so there is no "view not
  *     found" code path to exercise. The analogous failure mode is a request
  *     that references a source table not listed in `sources`; verified here
  *     by asserting `compile` raises when the view SQL names an unregistered
  *     table.
  */
abstract class CompileRefreshScenarios extends IvmParitySpecBase("compile-refresh") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  protected val extensionPath: String =
    sys.env.getOrElse("OPENIVM_EXTENSION_PATH", "/opt/openivm/openivm.duckdb_extension")

  private var sharedCompiler: OpenIvmCompiler = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sharedCompiler = OpenIvmCompiler.build(extensionPath)
  }

  override def afterAll(): Unit = {
    try {
      if (sharedCompiler != null) sharedCompiler.close()
    } finally {
      super.afterAll()
    }
  }

  // ── Schemas matching the openivm test setup ────────────────────────────────
  //   CREATE TABLE sales (region VARCHAR, amount INT);

  protected val salesSchema: StructType =
    StructType.fromDDL("region STRING, amount INT")

  // The materialized view body under test:
  //   SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM sales GROUP BY region
  protected val mvRegionSql: String =
    "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM sales GROUP BY region"

  protected def mvRegionRequest: CompileRequest =
    CompileRequest(
      viewName = "mv_region",
      viewSql = mvRegionSql,
      sources = Map("sales" -> salesSchema)
    )

  /** Replace `'YYYY-MM-DD HH:MM:SS.ffffff'` datetime literals — which openivm
    * embeds via `now()` at compile time to bound the delta-scan window, in either
    * `'…'::TIMESTAMP` or `CAST('…' AS TIMESTAMP)` form — with a stable placeholder,
    * so two compile invocations on the same request can be compared structurally.
    */
  protected val TimestampLiteral =
    """'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)?'""".r

  protected def stripTimestamps(sql: String): String =
    TimestampLiteral.replaceAllIn(sql, "'<TS>'")

  // ── Tests ──────────────────────────────────────────────────────────────────

  describe("CompileRefreshSpec — parity with openivm/test/sql/compile_refresh.test") {

    // openivm Test 1
    it("compiles an AGGREGATE_GROUP MV and emits an INSERT into openivm_delta_<mv>") {
      val result = sharedCompiler.compile(mvRegionRequest)
      result.refreshType shouldBe 0
      result.refreshTypeName shouldBe "AGGREGATE_GROUP"
      result.sql should not be empty
      result.sql.toLowerCase should include("openivm_delta_mv_region")
    }

    // openivm Test 2 — `openivm_compile_with_facts` did NOT mutate the MV.
    //
    // OpenIvmCompiler.compile is pure with respect to external state: it
    // produces SQL for the caller (Spark) to execute and never touches the
    // Spark catalog. The generated SQL does, however, embed a wall-clock
    // `now()` timestamp into each `openivm_timestamp > '…'::TIMESTAMP`
    // delta-window predicate, so two invocations are only equal after
    // normalising those literals. We compare refresh type, name, and the
    // timestamp-normalised SQL string.
    it("is a pure function — compiling the same request twice yields equal results modulo now() literals") {
      val first  = sharedCompiler.compile(mvRegionRequest)
      val second = sharedCompiler.compile(mvRegionRequest)
      second.refreshType shouldBe first.refreshType
      second.refreshTypeName shouldBe first.refreshTypeName
      stripTimestamps(second.sql) shouldBe stripTimestamps(first.sql)
      stripTimestamps(second.initialLoadSql) shouldBe stripTimestamps(first.initialLoadSql)
    }

    // openivm Test 3 — PRAGMA refresh still works after a
    // `openivm_compile_with_facts` call (i.e. the per-call `compile_only`
    // CompileFacts flag did not leak into global state).
    //
    // openivm-spark splits compile from execute across two different code
    // paths (OpenIvmCompiler vs RefreshMaterializedViewCommand), so there is
    // no global flag that the compile path could leak into a subsequent
    // refresh.  We verify the compiler instance itself remains usable: an
    // AGGREGATE_GROUP compile followed by a SIMPLE_PROJECTION compile both
    // succeed without interference.
    it("compile path is decoupled from execute — repeated compiles remain valid") {
      val agg = sharedCompiler.compile(mvRegionRequest)
      agg.refreshType shouldBe 0
      agg.sql should not be empty

      val proj = sharedCompiler.compile(
        CompileRequest(
          viewName = "mv_region_proj",
          viewSql = "SELECT region FROM sales WHERE amount > 0",
          sources = Map("sales" -> salesSchema)
        )
      )
      proj.refreshType shouldBe 2
      proj.refreshTypeName shouldBe "SIMPLE_PROJECTION"
      proj.sql should not be empty
    }

    // openivm Test 4 — `target_dialect="spark"` in the CompileFacts JSON is
    // accepted and the SPARK compile path runs cleanly.
    //
    // OpenIvmCompiler always sets the SPARK dialect on its embedded DuckDB
    // session, so we verify by compiling a SIMPLE_PROJECTION view (which
    // exercises the lpts SPARK pipeline) and asserting the generated SQL
    // contains the fully-qualified quoted `memory`.`main`.`...` table reference
    // that the SPARK dialect emits.
    it("emits SPARK-dialect SQL with fully-qualified quoted memory.main references for SIMPLE_PROJECTION") {
      val result = sharedCompiler.compile(
        CompileRequest(
          viewName = "mv_sales_proj",
          viewSql = "SELECT region FROM sales WHERE amount > 0",
          sources = Map("sales" -> salesSchema)
        )
      )
      result.refreshType shouldBe 2
      result.refreshTypeName shouldBe "SIMPLE_PROJECTION"
      result.sql should include("`memory`.`main`.")
    }

    // openivm Test 5 — AGGREGATE_GROUP shape, compile returns non-empty SQL.
    //
    // openivm Test 5 in the source file re-runs `openivm_compile_with_facts`
    // after toggling `target_dialect`; the SPARK pipeline must still produce
    // a non-empty plan for the AGGREGATE_GROUP shape.
    it("compiles an AGGREGATE_GROUP view in SPARK dialect and returns non-empty SQL") {
      val result = sharedCompiler.compile(mvRegionRequest)
      result.refreshType shouldBe 0
      result.refreshTypeName shouldBe "AGGREGATE_GROUP"
      result.sql.trim should not be empty
    }

    // openivm Test 6 — `openivm_compile_with_facts('does_not_exist', '{}')`
    // fails with "materialized view 'does_not_exist' not found".
    //
    // OpenIvmCompiler.compile operates on a self-contained CompileRequest and
    // never looks up persistent views, so there is no analogous "view not
    // found" code path. The closest equivalent failure mode is a CompileRequest
    // whose view SQL references a source table that is not declared in
    // `sources` — DuckDB then fails to bind the table and `compile` raises.
    it("fails cleanly when the view SQL references a source table that is not registered") {
      val ex = the[Exception] thrownBy sharedCompiler.compile(
        CompileRequest(
          viewName = "mv_missing_source",
          viewSql = "SELECT region FROM does_not_exist",
          sources = Map.empty
        )
      )
      ex.getMessage.toLowerCase should include("does_not_exist".toLowerCase)
    }
  }
}
