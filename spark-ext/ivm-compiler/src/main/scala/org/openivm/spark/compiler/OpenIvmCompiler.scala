package org.openivm.spark.compiler

import java.io.{BufferedReader, File, InputStreamReader}
import java.nio.file.{Files, Path}
import java.util.Comparator
import java.util.concurrent.{Callable, Executors, TimeUnit}

import org.apache.spark.sql.types._

/** Output of `PRAGMA compile_refresh`. */
final case class CompiledRefresh(
    refreshType: Int,        // RefreshType enum ordinal (0 = AGGREGATE_GROUP, 2 = SIMPLE_PROJECTION, …)
    refreshTypeName: String, // e.g. "AGGREGATE_GROUP"
    sql: String,             // full refresh SQL program in the target dialect
    initialLoadSql: String   // SELECT query for initial MV load including hidden columns; empty if unavailable
)

/** Sources: Spark table name → resolved StructType.  Used by the bridge to
  * register matching empty DuckDB tables so OpenIVM can plan/classify.
  *
  * @param sourceQualifiedNames Optional map from short table name (matching
  *   keys of [[sources]]) to the fully-qualified Spark table name as it should
  *   appear in the rewritten initial-load SQL. Used to translate
  *   `memory.main.<short>` references emitted by openivm into the correct
  *   Spark identifier. Empty by default — callers that don't need rewriting
  *   can leave it unset.
  */
final case class CompileRequest(
    viewName: String,
    viewSql: String,
    sources: Map[String, StructType],
    sourceQualifiedNames: Map[String, String] = Map.empty
)

/** Wraps DuckDB CLI errors surfaced by the OpenIVM compiler bridge.
  * The DuckDB error text is preserved verbatim in `getMessage`.
  */
final class OpenIvmCompileException(message: String, cause: Throwable) extends RuntimeException(message, cause)

/** DuckDB CLI bridge that loads the OpenIVM extension and translates a Spark
  * materialized-view definition into a refresh-SQL program via
  * `PRAGMA compile_refresh`.
  *
  * Each [[compile]] call spawns an independent `duckdb :memory: -jsonlines`
  * subprocess, so multiple threads can compile concurrently without any shared
  * mutable state.
  *
  * Create via [[OpenIvmCompiler.build]]; release via [[close]].
  */
class OpenIvmCompiler private (
    val extensionPath: String,
    val cliPath: String,
    val isolation: OpenIvmCompiler.Isolation
) extends AutoCloseable {

  @volatile private var closed: Boolean = false

  /** Translates `req.viewSql` into a [[CompiledRefresh]] by registering empty
    * source tables, creating a temporary materialized view, invoking
    * `PRAGMA compile_refresh`, and tearing down the ephemeral DuckDB process.
    *
    * Each call spawns a fresh `duckdb :memory:` subprocess, so calls from
    * concurrent threads proceed in parallel without contention.
    *
    * @throws OpenIvmCompileException on any DuckDB CLI or OpenIVM error.
    * @throws NotImplementedError     if a source schema column has an unsupported Spark type.
    * @throws IllegalStateException   if called after [[close]].
    */
  def compile(req: CompileRequest): CompiledRefresh = {
    if (closed) throw new IllegalStateException("OpenIvmCompiler has been closed")

    // Compute CREATE TABLE DDLs *before* any try-catch so that a
    // NotImplementedError from an unsupported type propagates directly to the
    // caller without being caught or wrapped by the CLI error handler below.
    val tableDdls: Seq[(String, String)] = req.sources.toSeq.map { case (name, schema) =>
      val cols = schema.fields.map(f => s"${f.name} ${sparkToDuckdbType(f.dataType)}").mkString(", ")
      name -> s"CREATE TABLE $name ($cols)"
    }

    val tmpDir = Files.createTempDirectory("openivm_compiler_")
    try {
      val script           = buildScript(req, tableDdls, tmpDir)
      val (stdout, stderr) = runCli(script)
      val partial          = parseCompileResult(stdout, req.viewName, stderr)
      val initLoad         = parseInitialLoadSql(tmpDir, req)
      partial.copy(initialLoadSql = initLoad)
    } catch {
      case e: OpenIvmCompileException => throw e
      case e: IllegalStateException   => throw e
      case e: Exception =>
        throw new OpenIvmCompileException(
          s"DuckDB CLI error during compile of '${req.viewName}': ${e.getMessage}",
          e
        )
    } finally {
      deleteDirRecursively(tmpDir)
    }
  }

  /** Marks the compiler as closed.  Subsequent calls to [[compile]] throw
    * [[IllegalStateException]].  Idempotent.
    */
  def close(): Unit = { closed = true }

  /** Maps a Spark [[DataType]] to its DuckDB SQL type string.
    *
    * Covers all primitive types, DECIMAL(p,s), arrays, structs, maps, and
    * BINARY.  Nested types are handled recursively.
    *
    * @throws NotImplementedError for [[UserDefinedType]] or any unrecognised type.
    */
  private[compiler] def sparkToDuckdbType(t: DataType): String = t match {
    case _: UserDefinedType[_] =>
      throw new NotImplementedError(
        s"Unsupported Spark DataType for DuckDB DDL registration: ${t.typeName}. " +
          "Only primitive types, DECIMAL, ARRAY, STRUCT, and MAP are supported."
      )
    case ByteType      => "TINYINT"
    case ShortType     => "SMALLINT"
    case IntegerType   => "INTEGER"
    case LongType      => "BIGINT"
    case FloatType     => "FLOAT"
    case DoubleType    => "DOUBLE"
    case BooleanType   => "BOOLEAN"
    case StringType    => "VARCHAR"
    case DateType      => "DATE"
    case TimestampType => "TIMESTAMP"
    case BinaryType    => "BLOB"
    case d: DecimalType =>
      s"DECIMAL(${d.precision}, ${d.scale})"
    case a: ArrayType =>
      s"${sparkToDuckdbType(a.elementType)}[]"
    case s: StructType =>
      val fields = s.fields.map(f => s"${f.name} ${sparkToDuckdbType(f.dataType)}").mkString(", ")
      s"STRUCT($fields)"
    case m: MapType =>
      s"MAP(${sparkToDuckdbType(m.keyType)}, ${sparkToDuckdbType(m.valueType)})"
    case other =>
      throw new NotImplementedError(
        s"Unsupported Spark DataType for DuckDB DDL registration: ${other.typeName}"
      )
  }

  private def buildScript(
      req: CompileRequest,
      tableDdls: Seq[(String, String)],
      tmpDir: Path
  ): String = {
    val sb = new StringBuilder
    sb ++= s"LOAD '${escapeSql(extensionPath)}';\n"
    // Force grouped MIN/MAX to always emit the affected-groups DELETE+recompute SQL
    // (refresh_compiler.cpp:372-387). Without this, openivm reads its empty
    // compile-time delta tables and picks the insert-only MERGE fast path
    // (refresh_delta_fast_paths.cpp:80-126), which silently produces wrong
    // values once a delete or update of the current min/max arrives at refresh
    // time. The fast path can be re-enabled later by analysing the staged delta
    // contents before invoking the compiler.
    sb ++= "SET openivm_minmax_incremental=false;\n"
    sb ++= s"SET openivm_files_path='${escapeSql(tmpDir.toAbsolutePath.toString)}';\n"
    sb ++= s"DROP VIEW IF EXISTS ${req.viewName};\n"
    for ((tableName, _) <- tableDdls) sb ++= s"DROP TABLE IF EXISTS $tableName;\n"
    for ((_, ddl)       <- tableDdls) sb ++= s"$ddl;\n"
    // ── Spark-only function shims ──
    //
    // compile_only=true in the CompileFacts payload means DuckDB only parses +
    // binds the MV body; it does not execute the functions. openivm's LPTS
    // serializer INLINES the macro body into the emitted refresh SQL, so
    // collision-prone and arity-mismatched Spark built-ins use a pre-pass
    // (`renameSparkFunctionShimCalls`) to rename the user call to
    // `__sparkfn_*` before DuckDB sees it, and a post-pass
    // (`LptsSparkDialect.rewriteSparkFunctionInlinings`) to recover Spark's
    // original spelling before refresh-time execution.
    //
    // Each macro is type-correct (returns a value of the type Spark would
    // return); DuckDB only needs the binder to succeed during compile.
    sb ++= OpenIvmCompiler.sparkFunctionShimsPrologue
    sb ++= s"CREATE OR REPLACE MATERIALIZED VIEW ${req.viewName} AS ${normalizeSparkSqlForDuckdb(stripDbQualifiers(req.viewSql, req.sourceQualifiedNames))};\n"
    // openivm_compile_with_facts replaces the three deleted PRAGMAs
    // (openivm_target_dialect / openivm_compile_only / openivm_force_view_delta_cascade)
    // plus their consolidated `emit_cascade_delta_for_recompute` driver with a
    // single CompileFacts JSON payload. The function is non-mutating: every
    // refresh statement is returned in `sql` rows (one per top-level
    // statement) without touching aux state.
    //
    // - target_dialect="spark":         emit Spark/Delta SQL (was openivm_target_dialect).
    // - compile_only=true:              preserve inclusion-exclusion terms for empty
    //                                   compile-time deltas + skip aux-state mutation.
    // - force_view_delta_cascade=true:  always emit openivm_delta_<view> cascade rows
    //                                   for AGGREGATE_GROUP / AGGREGATE_HAVING /
    //                                   WINDOW_PARTITION / GROUP_RECOMPUTE so depth-2
    //                                   MV-over-MV chains never fall back to FULL_REFRESH.
    sb ++= s"SELECT * FROM openivm_compile_with_facts('${escapeSql(req.viewName)}', '${escapeSql(OpenIvmCompiler.SparkCompileFactsJson)}');\n"
    sb.toString
  }

  /** Rewrites occurrences of a tracked qualified source name (`<db>.<table>`)
    * to its short form (`<table>`) in the supplied SQL.
    *
    * Spark stores `originalQueryText` verbatim as the user wrote it; when a
    * dbt-style workload references tables as `<db>.<table>` the resulting
    * SQL flows into the DuckDB compiler subprocess, which only has the short
    * tables registered (`CREATE TABLE <table> (...)`). Without this rewrite,
    * `PRAGMA compile_refresh` fails with `Catalog Error: Table with name
    * "<db>.<table>" does not exist because schema "<db>" does not exist.`
    *
    * Implementation: word-boundary regex, longest qualified name first so a
    * 3-part `catalog.db.table` substring doesn't get half-rewritten. The
    * `memory.main.<short>` references that openivm emits in the compiled
    * output are unaffected — they go through the inverse rewrite in
    * `parseInitialLoadSql` and `SparkRefreshRewriter`.
    *
    * @param sql              caller-supplied view SELECT body
    * @param shortToQualified `req.sourceQualifiedNames` (short → qualified)
    * @return                 SQL with qualified names replaced by their short
    *                         counterparts. Unchanged if the map is empty or
    *                         no qualified names contain a dot.
    */
  private[compiler] def stripDbQualifiers(
      sql: String,
      shortToQualified: Map[String, String]
  ): String = {
    val pairs = shortToQualified.toSeq.collect {
      case (short, qual) if qual.contains(".") && qual != short => (qual, short)
    }
    // Longest first to ensure `catalog.db.table` is matched before `db.table`.
    val sorted = pairs.sortBy(-_._1.length)
    sorted.foldLeft(sql) { case (acc, (qual, short)) =>
      val pattern = "(?i)\\b" + java.util.regex.Pattern.quote(qual) + "\\b"
      acc.replaceAll(pattern, java.util.regex.Matcher.quoteReplacement(short))
    }
  }

  /** Spark-specific pre-normalizations applied before the MV body is handed to
    * DuckDB's parser/binder.
    *
    * Currently translated:
    *   - Spark 1-arg / 2-arg `to_date` / `to_timestamp` and 2-arg
    *     `date_format` → collision-free `__sparkfn_*` names so DuckDB binds
    *     our shim macro instead of its own incompatible built-in overloads /
    *     arities.
    *   - Spark literal-boolean `last_value(expr, true|false)` → DuckDB's
    *     native window spelling (`last_value(expr IGNORE NULLS)` or
    *     `last_value(expr)`) so null-handling semantics survive planning.
    *   - `LEFT SEMI JOIN` → `SEMI JOIN` (DuckDB does not accept the LEFT prefix)
    *   - `LEFT ANTI JOIN` → `ANTI JOIN`
    *
    * The original Spark SQL is still stored in `MvMetadata.querySql` and used
    * for FULL_REFRESH `INSERT OVERWRITE`; this normalization only affects the
    * compile-bridge copy sent to DuckDB.
    */
  private[compiler] def normalizeSparkSqlForDuckdb(sql: String): String = {
    val leftSemi   = """(?i)\bLEFT\s+SEMI\s+JOIN\b""".r
    val leftAnti   = """(?i)\bLEFT\s+ANTI\s+JOIN\b""".r
    val renamedFns = OpenIvmCompiler.renameSparkFunctionShimCalls(sql)
    leftAnti.replaceAllIn(
      leftSemi.replaceAllIn(renamedFns, "SEMI JOIN"),
      "ANTI JOIN"
    )
  }

  /** Escapes single-quote characters for embedding a value inside SQL single quotes. */
  private def escapeSql(s: String): String = s.replace("'", "''")

  /** Spawns `duckdb :memory: -jsonlines`, pipes `script` on stdin, and returns
    * (stdout, stderr) after the process exits.  Stdout and stderr are read in
    * parallel threads to avoid pipe-buffer deadlocks.
    */
  private def runCli(script: String): (String, String) = {
    val pb      = new ProcessBuilder(cliPath, ":memory:", "-jsonlines")
    val process = pb.start()

    val executor = Executors.newFixedThreadPool(2)
    val stdoutF = executor.submit(new Callable[String] {
      def call(): String = {
        val r = new BufferedReader(new InputStreamReader(process.getInputStream, "UTF-8"))
        try Iterator.continually(r.readLine()).takeWhile(_ != null).mkString("\n")
        finally r.close()
      }
    })
    val stderrF = executor.submit(new Callable[String] {
      def call(): String = {
        val r = new BufferedReader(new InputStreamReader(process.getErrorStream, "UTF-8"))
        try Iterator.continually(r.readLine()).takeWhile(_ != null).mkString("\n")
        finally r.close()
      }
    })

    val stdin = process.getOutputStream
    stdin.write(script.getBytes("UTF-8"))
    stdin.close()

    process.waitFor(120, TimeUnit.SECONDS)
    executor.shutdown()
    executor.awaitTermination(10, TimeUnit.SECONDS)
    (stdoutF.get(), stderrF.get())
  }

  private def parseCompileResult(stdout: String, viewName: String, stderr: String): CompiledRefresh = {
    // openivm_compile_with_facts is a table function that returns ONE ROW
    // PER TOP-LEVEL STATEMENT in the refresh program. Each row has the
    // same `refresh_type` / `refresh_type_name`; the `sql` payload is one
    // statement (already terminated with `;`). Concatenate all rows in
    // `stmt_order` to recover the full refresh SQL program that the
    // refresh-time loop expects.
    val rows = stdout.linesIterator.collect {
      case line if line.contains("\"refresh_type\"") => parseRefreshLine(line)
    }.toVector
    if (rows.isEmpty) {
      val hint = if (stderr.nonEmpty) s"\nCLI stderr:\n$stderr" else ""
      throw new OpenIvmCompileException(
        s"openivm_compile_with_facts('$viewName', ...) produced no result$hint",
        null
      )
    }
    val head = rows.head
    val sql  = rows.iterator.map(_.sql).mkString("\n")
    head.copy(sql = sql)
  }

  /** Reads `<tmpDir>/openivm_compiled_queries_<viewName>.sql` and extracts the
    * `create table openivm_data_<viewName> as <query>;` body, which represents
    * the initial-load query (including any hidden `openivm_count_star` column).
    *
    * Returns the empty string if the file is missing or no matching CTAS is
    * found.  The extracted SQL has `memory.main.<short>` references rewritten
    * to the qualified Spark table name supplied via `req.sourceQualifiedNames`,
    * any leftover `memory.main.` prefix stripped, and is run through
    * [[LptsSparkDialect.translate]] to handle DuckDB-isms (e.g. `count_star()`,
    * `::TIMESTAMP` casts).
    */
  private[compiler] def parseInitialLoadSql(tmpDir: Path, req: CompileRequest): String = {
    import java.util.regex.Pattern
    val file = tmpDir.resolve(s"openivm_compiled_queries_${req.viewName}.sql")
    if (!Files.exists(file)) return ""
    val content = new String(Files.readAllBytes(file), "UTF-8")

    val pat = Pattern.compile(
      s"(?is)create\\s+table\\s+openivm_data_${Pattern.quote(req.viewName)}\\s+as\\s+((?:WITH\\b|SELECT\\b).+?);",
      Pattern.DOTALL
    )
    val m = pat.matcher(content)
    if (!m.find()) return ""

    var sql = m.group(1).trim

    // openivm uses `rowid` (a DuckDB-internal hidden column) in the initial-load
    // plan for COUNT(*) aggregates. That column does not exist in Spark/Delta
    // tables, so fall back to the original user-supplied view body instead.
    if ("""(?i)\browid\b""".r.findFirstIn(sql).isDefined) return ""

    for ((short, qual) <- req.sourceQualifiedNames) {
      sql = sql.replace(s"memory.main.$short", qual)
    }
    sql = sql.replaceAll("memory\\.main\\.", "")
    LptsSparkDialect.translate(sql)
  }

  /** Parses a single `-jsonlines` row of the form
    * `{"refresh_type":N,"refresh_type_name":"...","sql":"..."}`.
    */
  private def parseRefreshLine(line: String): CompiledRefresh = {
    val rtMatch = """"refresh_type"\s*:\s*(\d+)""".r
      .findFirstMatchIn(line)
      .getOrElse(
        throw new OpenIvmCompileException(s"Cannot extract refresh_type from JSON line: $line", null)
      )
    val refreshType = rtMatch.group(1).toInt

    val rtnKeyIdx            = line.indexOf("\"refresh_type_name\"")
    val rtnValIdx            = nextStringStart(line, rtnKeyIdx + "\"refresh_type_name\"".length)
    val (refreshTypeName, _) = decodeJsonString(line, rtnValIdx)

    val sqlKeyIdx = line.lastIndexOf("\"sql\"")
    val sqlValIdx = nextStringStart(line, sqlKeyIdx + "\"sql\"".length)
    val (sql, _)  = decodeJsonString(line, sqlValIdx)

    CompiledRefresh(refreshType, refreshTypeName, sql, initialLoadSql = "")
  }

  /** Returns the index of the `"` that starts a JSON string value at or after `from`. */
  private def nextStringStart(s: String, from: Int): Int = {
    var i = from
    while (i < s.length && s(i) != '"') i += 1
    i
  }

  /** Decodes a JSON-encoded string starting at `s(start)` (which must be `"`).
    * Returns `(decoded, indexAfterClosingQuote)`.
    */
  private def decodeJsonString(s: String, start: Int): (String, Int) = {
    val sb = new StringBuilder
    var i  = start + 1
    while (i < s.length && s(i) != '"') {
      if (s(i) == '\\') {
        i += 1
        s(i) match {
          case '"'  => sb += '"'
          case '\\' => sb += '\\'
          case '/'  => sb += '/'
          case 'n'  => sb += '\n'
          case 'r'  => sb += '\r'
          case 't'  => sb += '\t'
          case 'b'  => sb += '\b'
          case 'f'  => sb += '\f'
          case 'u' =>
            sb += Integer.parseInt(s.substring(i + 1, i + 5), 16).toChar
            i += 4
          case c => sb += c
        }
      } else {
        sb += s(i)
      }
      i += 1
    }
    (sb.toString, i + 1)
  }

  private def deleteDirRecursively(dir: Path): Unit = {
    val stream = Files.walk(dir)
    try stream.sorted(Comparator.reverseOrder[Path]()).forEach(p => Files.delete(p))
    finally stream.close()
  }
}

object OpenIvmCompiler {

  /** JSON payload threaded into `openivm_compile_with_facts(view, facts)` for
    * every spark-ext compile call. Mirrors the three deleted PRAGMA flags
    * (`openivm_target_dialect`, `openivm_compile_only`, `openivm_force_view_delta_cascade`)
    * plus the consolidated `emit_cascade_delta_for_recompute` driver:
    *
    *   - `target_dialect="spark"`        — emit Spark/Delta SQL.
    *   - `compile_only=true`             — preserve inclusion-exclusion terms
    *     when delta tables are empty and skip aux-state mutation (so the
    *     ephemeral DuckDB :memory: process never runs `CREATE OR REPLACE
    *     TABLE openivm_*_aux_<view>`).
    *   - `force_view_delta_cascade=true` — always emit signed `openivm_delta_<view>`
    *     companion rows for AGGREGATE_GROUP / AGGREGATE_HAVING /
    *     WINDOW_PARTITION / GROUP_RECOMPUTE so downstream MVs at depth 2 see
    *     a real cascade delta and never fall back to FULL_REFRESH.
    *
    * Embedded into SQL via [[buildScript]] inside single quotes; only
    * single-quote escape is required and is handled by `escapeSql`.
    */
  private[compiler] val SparkCompileFactsJson: String =
    """{"target_dialect":"spark","compile_only":true,"force_view_delta_cascade":true}"""

  /** Isolation strategy for the underlying DuckDB runtime. */
  sealed trait Isolation

  /** CLI subprocess: each compile spawns a fresh `duckdb :memory:` process.
    * Default and only supported mode for this implementation.
    */
  case object InProcess extends Isolation

  /** Out-of-process pool: reserved for future work; currently unimplemented. */
  case object ChildProcess extends Isolation

  /** Constructs an [[OpenIvmCompiler]] backed by a CLI subprocess pool.
    *
    * @param extensionPath  Absolute path to the compiled OpenIVM DuckDB extension
    *                       binary (`.duckdb_extension`).  Defaults to the
    *                       `OPENIVM_EXTENSION_PATH` environment variable or
    *                       `/opt/openivm/openivm.duckdb_extension`.
    * @param cliPath        Absolute path to the DuckDB CLI binary that is ABI-
    *                       compatible with `extensionPath`.  Defaults to the
    *                       `OPENIVM_CLI_PATH` environment variable, or the
    *                       `duckdb` executable co-located with `extensionPath`.
    * @param isolation      `InProcess` (default) or `ChildProcess` (not yet implemented).
    * @throws IllegalArgumentException if `extensionPath` or `cliPath` does not exist on disk.
    * @throws NotImplementedError      if `isolation == ChildProcess`.
    */
  def build(
      extensionPath: String = sys.env.getOrElse(
        "OPENIVM_EXTENSION_PATH",
        "/opt/openivm/openivm.duckdb_extension"
      ),
      cliPath: String = sys.env.getOrElse("OPENIVM_CLI_PATH", defaultCliPath()),
      isolation: Isolation = InProcess
  ): OpenIvmCompiler = isolation match {
    case ChildProcess =>
      throw new NotImplementedError(
        "ChildProcess isolation is reserved for future work and is not yet implemented"
      )
    case InProcess =>
      if (!new File(extensionPath).exists())
        throw new IllegalArgumentException(
          s"OpenIVM extension binary not found at: $extensionPath"
        )
      if (!new File(cliPath).exists())
        throw new IllegalArgumentException(
          s"OpenIVM DuckDB CLI not found at: $cliPath"
        )
      new OpenIvmCompiler(extensionPath, cliPath, isolation)
  }

  private def defaultCliPath(): String = {
    val extPath = sys.env.getOrElse("OPENIVM_EXTENSION_PATH", "/opt/openivm/openivm.duckdb_extension")
    Option(new File(extPath).getParentFile)
      .map(dir => new File(dir, "duckdb").getAbsolutePath)
      .getOrElse("/opt/openivm/duckdb")
  }

  private[compiler] def renameSparkFunctionShimCalls(sql: String): String =
    SparkFunctionShimSql.renameSparkFunctionShimCalls(sql)

  /** Prologue of `CREATE OR REPLACE MACRO` statements that register
    * type-correct stubs for Spark-only functions which DuckDB's binder would
    * otherwise reject. The set is driven by the dbt-server corpus (TPC-DI
    * bronze/silver/gold models) — extend as new gaps surface.
    *
    * Contract: each macro must return a value of the type the equivalent
    * Spark function returns. openivm's LPTS serializer INLINES the macro body
    * into the emitted refresh SQL, NOT the original macro name, so the body
    * must preserve enough structure for `LptsSparkDialect.translate` to
    * recover Spark's original spelling at refresh time.
    *
    * Collision-prone or arity-incompatible Spark built-ins (1-arg / 2-arg
    * `to_date`, 1-arg / 2-arg `to_timestamp`, 2-arg `date_format`) are renamed
    * to `__sparkfn_*` by [[renameSparkFunctionShimCalls]] before the SQL reaches
    * DuckDB. The 1-arg date/time spellings use dedicated `*_1arg` macro names
    * because DuckDB macros do not overload by arity. The macros below define
    * the corresponding DuckDB-side bodies that openivm will inline.
    * `LptsSparkDialect.rewriteSparkFunctionInlinings` reverses the date/time
    * inlinings back to Spark's original functions in the emitted refresh SQL.
    *
    * Literal-boolean `last_value(expr, true|false)` calls are normalized to
    * DuckDB's native window syntax before compile. `__sparkfn_last_value`
    * remains as a compatibility fallback for non-literal second arguments; it
    * still uses DuckDB's `last(expr)` stand-in and therefore cannot preserve
    * dynamic ignore-null flags.
    */
  private[compiler] val sparkFunctionShimsPrologue: String = {
    val macros = Seq(
      // regexp_like(string, pattern) → BOOLEAN
      // Spark-only function; DuckDB has `regexp_matches` (same semantics).
      // No name collision — direct macro registration works. The inlined
      // `regexp_matches` call is back-translated to `regexp_like` in
      // `LptsSparkDialect.rewriteSparkFunctionInlinings`.
      "CREATE OR REPLACE MACRO regexp_like(s, p) AS regexp_matches(s, p);",
      // 1-arg / 2-arg Spark spellings that collide with DuckDB built-ins are
      // pre-renamed to `__sparkfn_*` before compile. openivm inlines the body,
      // and LptsSparkDialect rewrites the inlined `CASE` / `strptime` /
      // `strftime` forms back to Spark's original function spelling at refresh
      // time. DuckDB macros do not overload by arity, so the 1-arg shims use
      // dedicated `*_1arg` names. Those 1-arg bodies use only `IS NULL` /
      // `IS NOT NULL` predicates so DuckDB binds them for any argument type
      // while still referencing the original expression for post-pass recovery.
      "CREATE OR REPLACE MACRO __sparkfn_to_date_1arg(s) AS CAST(CASE WHEN s IS NOT NULL THEN NULL WHEN s IS NULL THEN NULL END AS DATE);",
      "CREATE OR REPLACE MACRO __sparkfn_to_date(s, fmt) AS CAST(strptime(s, fmt) AS DATE);",
      "CREATE OR REPLACE MACRO __sparkfn_to_timestamp_1arg(s) AS CAST(CASE WHEN s IS NOT NULL THEN NULL WHEN s IS NULL THEN NULL END AS TIMESTAMP);",
      "CREATE OR REPLACE MACRO __sparkfn_to_timestamp(s, fmt) AS strptime(s, fmt);",
      "CREATE OR REPLACE MACRO __sparkfn_date_format(d, fmt) AS strftime(d, fmt);",
      // Fallback for non-literal Spark `last_value(expr, ignoreNulls)` calls.
      // Literal boolean flags are handled in the pre-pass with DuckDB's native
      // `IGNORE NULLS` modifier; dynamic flags keep the legacy compile-time
      // stand-in and cannot preserve ignore-null semantics.
      "CREATE OR REPLACE MACRO __sparkfn_last_value(expr, ignore_nulls) AS last(expr);"
    )
    macros.mkString("", "\n", "\n")
  }
}
