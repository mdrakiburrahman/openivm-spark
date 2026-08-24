package org.openivm.spark.compiler

import java.io.{BufferedReader, File, InputStreamReader}
import java.nio.file.{Files, Path, Paths}
import java.util.{Comparator, Locale}
import java.util.concurrent.{Callable, Executors, TimeUnit}

import org.apache.spark.sql.types._
import org.openivm.spark.common.{ForeignKeyRelation, WorkloadFacts}
import org.openivm.spark.telemetry.metrics.OpenIvmMetrics

/** Output of `openivm_compile_with_facts(view_name, facts_json)`. */
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
    sourceQualifiedNames: Map[String, String] = Map.empty,
    facts: WorkloadFacts = WorkloadFacts()
)

/** Wraps DuckDB CLI errors surfaced by the OpenIVM compiler bridge.
  * The DuckDB error text is preserved verbatim in `getMessage`.
  */
final class OpenIvmCompileException(message: String, cause: Throwable) extends RuntimeException(message, cause)

/** DuckDB CLI bridge that loads the OpenIVM extension and translates a Spark
  * materialized-view definition into a refresh-SQL program via the
  * `openivm_compile_with_facts(view_name, facts_json)` table function.
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
    val isolation: OpenIvmCompiler.Isolation,
    private val failureBundleDir: Option[String]
) extends AutoCloseable {

  @volatile private var closed: Boolean = false

  /** Translates `req.viewSql` into a [[CompiledRefresh]] by registering empty
    * source tables, creating a temporary materialized view, invoking
    * `openivm_compile_with_facts`, and tearing down the ephemeral DuckDB
    * process.
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
    OpenIvmMetrics.increment("compiler.compile.count")
    OpenIvmMetrics.CompilerInflight.incrementAndGet()
    val compileStarted = System.nanoTime()

    // Compute CREATE TABLE DDLs *before* any try-catch so that a
    // NotImplementedError from an unsupported type propagates directly to the
    // caller without being caught or wrapped by the CLI error handler below.
    val tableDdls: Seq[(String, String)] = req.sources.toSeq.map { case (name, schema) =>
      val cols = schema.fields.map(f => s"${quoteDuckdbIdent(f.name)} ${sparkToDuckdbType(f.dataType)}").mkString(", ")
      name -> s"CREATE TABLE $name ($cols)"
    }
    // Spark/Delta snapshot pins (`… VERSION AS OF <v>`) are a storage concern
    // DuckDB cannot parse and the bridge's row-less compile tables cannot
    // model, so the pin is split out of the COMPILE COPY only. The pin is
    // re-applied to every openivm-emitted source read (initial load here,
    // refresh program in `SparkRefreshRewriter`), and `MvMetadata.querySql`
    // keeps the user's pinned SQL verbatim.
    val depinnedViewSql = SparkTimeTravelSql.stripSnapshotPins(req.viewSql)
    val normalizedViewSql =
      normalizeSparkSqlForDuckdb(
        stripDbQualifiers(stripSparkBacktickIdentifiers(depinnedViewSql), req.sourceQualifiedNames)
      )

    val tmpDir = Files.createTempDirectory("openivm_compiler_")
    // Captured for diagnostics (failure-bundle persistence below) — updated
    // right after `runCli` returns, so a failure bundle written from either
    // catch branch reflects the actual CLI output for this attempt even
    // though `stdout`/`stderr` themselves are scoped to the `try` block.
    var lastStdout = ""
    var lastStderr = ""
    try {
      val script = buildScript(req, tableDdls, tmpDir, normalizedViewSql)
      val (stdout, stderr) = OpenIvmMetrics.time("compiler.duckdb_subprocess") {
        runCli(script)
      }
      lastStdout = stdout
      lastStderr = stderr
      val partial  = parseCompileResult(stdout, req.viewName, stderr)
      val initLoad = parseInitialLoadSql(tmpDir, req)
      partial.copy(initialLoadSql = initLoad)
    } catch {
      case e: OpenIvmCompileException =>
        persistFailureBundleIfConfigured(req, normalizedViewSql, lastStdout, lastStderr)
        throw e
      case e: IllegalStateException => throw e
      case e: Exception =>
        persistFailureBundleIfConfigured(req, normalizedViewSql, lastStdout, lastStderr)
        throw new OpenIvmCompileException(
          s"DuckDB CLI error during compile of '${req.viewName}': ${e.getMessage}",
          e
        )
    } finally {
      OpenIvmMetrics.updateTimer("compiler.compile", System.nanoTime() - compileStarted)
      OpenIvmMetrics.CompilerInflight.decrementAndGet()
      deleteDirRecursively(tmpDir)
    }
  }

  /** Marks the compiler as closed.  Subsequent calls to [[compile]] throw
    * [[IllegalStateException]].  Idempotent.
    */
  def close(): Unit = { closed = true }

  /** Quotes an identifier for DuckDB DDL using double quotes, escaping any
    * embedded double quote by doubling it.  Source columns may be named with
    * DuckDB reserved words (e.g. `collation`), which fail to parse unquoted in
    * a `CREATE TABLE` column list.
    */
  private[compiler] def quoteDuckdbIdent(name: String): String =
    "\"" + name.replace("\"", "\"\"") + "\""

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
      val fields = s.fields.map(f => s"${quoteDuckdbIdent(f.name)} ${sparkToDuckdbType(f.dataType)}").mkString(", ")
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
      tmpDir: Path,
      normalizedViewSql: String
  ): String = {
    val sb = new StringBuilder
    sb ++= s"LOAD '${escapeSql(extensionPath)}';\n"
    // Force grouped MIN/MAX to always emit the affected-groups DELETE+recompute SQL
    // (refresh_compiler.cpp:372-387). Without this, openivm reads its empty
    // compile-time delta tables and picks the insert-only MERGE fast path
    // (refresh_delta_fast_paths.cpp:80-126), which silently produces wrong
    // values once a delete or update of the current min/max arrives at refresh
    // time. EXCEPTION: when the caller's WorkloadFacts proves the batch is
    // append-only (`assumeInsertOnly`), the MIN/MAX fast path is safe and the
    // facts re-enable it — so we must NOT force it off here, or the bridge SET
    // would override the proven-safe fast path (read after compile in
    // refresh_delta_fast_paths.cpp).
    if (!req.facts.assumeInsertOnly) {
      sb ++= "SET openivm_minmax_incremental=false;\n"
    }
    if (req.facts.scd2RangeJoinAccel) {
      sb ++= "SET openivm_scd2_range_join_accel=true;\n"
    }
    sb ++= s"SET openivm_files_path='${escapeSql(tmpDir.toAbsolutePath.toString)}';\n"
    sb ++= s"DROP VIEW IF EXISTS ${req.viewName};\n"
    for ((tableName, _) <- tableDdls) sb ++= s"DROP TABLE IF EXISTS $tableName;\n"
    for ((_, ddl)       <- tableDdls) sb ++= s"$ddl;\n"
    declareRelyFkStatements(req).foreach(stmt => sb ++= s"$stmt\n")
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
    sb ++= s"CREATE OR REPLACE MATERIALIZED VIEW ${req.viewName} AS $normalizedViewSql;\n"
    // openivm_compile_with_facts is the per-call compile entry point. It
    // takes the view name plus a JSON CompileFacts payload and returns one
    // row per top-level refresh statement without mutating openivm aux
    // state. The CompileFacts surface is documented in
    // `openivm/src/include/compile_facts.hpp`.
    //
    // - target_dialect="spark":         emit Spark/Delta SQL.
    // - compile_only=true:              preserve inclusion-exclusion terms for empty
    //                                   compile-time deltas + skip aux-state mutation.
    // - force_view_delta_cascade=true:  always emit openivm_delta_<view> cascade rows
    //                                   for AGGREGATE_GROUP / AGGREGATE_HAVING /
    //                                   WINDOW_PARTITION / GROUP_RECOMPUTE so depth-2
    //                                   MV-over-MV chains never fall back to FULL_REFRESH.
    sb ++= s"SELECT * FROM openivm_compile_with_facts('${escapeSql(req.viewName)}', '${escapeSql(req.facts.toJson)}');\n"
    sb.toString
  }

  /** Rewrites occurrences of a tracked qualified source name (`<db>.<table>`)
    * to its short form (`<table>`) in the supplied SQL.
    *
    * Spark stores `originalQueryText` verbatim as the user wrote it; when a
    * dbt-style workload references tables as `<db>.<table>` the resulting
    * SQL flows into the DuckDB compiler subprocess, which only has the short
    * tables registered (`CREATE TABLE <table> (...)`). Without this rewrite,
    * `openivm_compile_with_facts` fails with `Catalog Error: Table with
    * name "<db>.<table>" does not exist because schema "<db>" does not
    * exist.`
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

  /** Rewrites Spark backtick-quoted identifiers into a form DuckDB's parser
    * accepts, so the compile-bridge copy of the MV body binds cleanly.
    *
    * dbt-style Spark SQL qualifies sources as `` `db`.table `` (the whole
    * dbt-server corpus does this). DuckDB uses double-quote identifier quoting
    * and rejects backticks outright with `Parser Error: syntax error at or near
    * "`"`, which silently demotes every affected view to
    * COMPILE_FAILED -> FULL_REFRESH — defeating incremental maintenance. The
    * backtick also hides the `db.` prefix from [[stripDbQualifiers]] (its
    * pattern matches the unquoted qualified name), so the source is never
    * reduced to the short table registered in DuckDB.
    *
    * A simple bare identifier (`[A-Za-z_][A-Za-z0-9_]*`) is emitted unquoted so
    * the downstream [[stripDbQualifiers]] rewrite still matches the now-plain
    * `db.table` reference; anything else is emitted double-quoted (with internal
    * double-quotes escaped). Backticks inside single-quoted string literals are
    * left untouched. `` `` `` (a doubled backtick) is the Spark escape for a
    * literal backtick within an identifier.
    */
  private[compiler] def stripSparkBacktickIdentifiers(sql: String): String = {
    val out = new StringBuilder(sql.length)
    val n   = sql.length
    var i   = 0
    while (i < n) {
      sql.charAt(i) match {
        case '-' if i + 1 < n && sql.charAt(i + 1) == '-' =>
          // Copy a -- line comment verbatim so apostrophes/backticks inside it
          // never desync the string-literal / identifier scanner.
          while (i < n && sql.charAt(i) != '\n') {
            out += sql.charAt(i)
            i += 1
          }
        case '/' if i + 1 < n && sql.charAt(i + 1) == '*' =>
          // Copy a /* */ block comment verbatim.
          out += '/'
          out += '*'
          i += 2
          var closed = false
          while (i < n && !closed) {
            if (sql.charAt(i) == '*' && i + 1 < n && sql.charAt(i + 1) == '/') {
              out += '*'
              out += '/'
              i += 2
              closed = true
            } else {
              out += sql.charAt(i)
              i += 1
            }
          }
        case '\'' =>
          // Copy a Spark-dialect single-quoted string literal verbatim,
          // honoring BOTH '' escapes and Spark's backslash escapes (e.g.
          // `\'`) so an escaped quote inside the literal is never mistaken
          // for its closing quote.
          val litEnd = SparkFunctionShimSql.consumeSparkSingleQuoted(sql, i)
          out ++= sql.substring(i, litEnd)
          i = litEnd
        case '`' =>
          val ident = new StringBuilder
          i += 1
          var closed = false
          while (i < n && !closed) {
            val ch = sql.charAt(i)
            if (ch == '`' && i + 1 < n && sql.charAt(i + 1) == '`') {
              ident += '`'
              i += 2
            } else if (ch == '`') {
              closed = true
              i += 1
            } else {
              ident += ch
              i += 1
            }
          }
          val id = ident.toString
          if (id.matches("[A-Za-z_][A-Za-z0-9_]*")) out ++= id
          else { out += '"'; out ++= id.replace("\"", "\"\""); out += '"' }
        case other =>
          out += other
          i += 1
      }
    }
    out.toString
  }

  /** Spark-specific pre-normalizations applied before the MV body is handed to
    * DuckDB's parser/binder.
    *
    * Currently translated:
    *   - Spark single-quoted string literal escaping (both `''` and
    *     backslash escapes like `\'`, `\\`, `\n`, ...) → DuckDB's
    *     doubled-quote-only convention, so literals DuckDB would otherwise
    *     reject (`\'`) or silently misvalue (`\\`) parse correctly. Must run
    *     FIRST, before any other pass scans for quoted regions, since every
    *     later pass assumes DuckDB-dialect literal syntax.
    *   - Spark 1-arg / 2-arg `to_date` / `to_timestamp` and 2-arg
    *     `date_format`, 7-arg `make_interval`, and 2-arg `get_json_object` →
    *     collision-free `__sparkfn_*` names so DuckDB binds our shim macro
    *     instead of its own incompatible built-in overloads / arities (or,
    *     for Spark-only functions, a missing DuckDB function).
    *   - Spark literal-boolean `last_value(expr, true|false)` /
    *     `first_value(expr, true|false)` → DuckDB's native window spelling
    *     (`last_value(expr IGNORE NULLS)` or `last_value(expr)`, and the
    *     `first_value` equivalents) so null-handling semantics survive
    *     planning.
    *   - 0-arg `current_date()` / `current_timestamp()` → collision-free
    *     `__sparkfn_*` names bound to macros that produce a deterministic,
    *     compile-time-stable value (DuckDB's own `current_date`/
    *     `current_timestamp` are non-deterministic builtins that
    *     `openivm_compile_with_facts` rejects during planning).
    *   - `LEFT SEMI JOIN` → `SEMI JOIN` (DuckDB does not accept the LEFT prefix)
    *   - `LEFT ANTI JOIN` → `ANTI JOIN`
    *
    * The original Spark SQL is still stored in `MvMetadata.querySql` and used
    * for FULL_REFRESH `INSERT OVERWRITE`; this normalization only affects the
    * compile-bridge copy sent to DuckDB.
    */
  private[compiler] def normalizeSparkSqlForDuckdb(sql: String): String = {
    val leftSemi          = """(?i)\bLEFT\s+SEMI\s+JOIN\b""".r
    val leftAnti          = """(?i)\bLEFT\s+ANTI\s+JOIN\b""".r
    val literalsRewritten = SparkFunctionShimSql.translateSparkStringLiteralEscapes(sql)
    val renamedFns        = OpenIvmCompiler.renameSparkFunctionShimCalls(literalsRewritten)
    leftAnti.replaceAllIn(
      leftSemi.replaceAllIn(renamedFns, "SEMI JOIN"),
      "ANTI JOIN"
    )
  }

  private[compiler] def declareRelyFkStatements(req: CompileRequest): Seq[String] = {
    if (!req.facts.declareRelyFk) return Seq.empty

    def normalizeTableName(table: String): Option[String] = {
      val exactShort = req.sources.get(table).map(_ => table)
      val fromQual = req.sourceQualifiedNames.collectFirst {
        case (short, qualified) if qualified.equalsIgnoreCase(table) && req.sources.contains(short) => short
      }
      val trailing = table.split('.').lastOption.filter(req.sources.contains)
      exactShort.orElse(fromQual).orElse(trailing)
    }

    req.facts.fkRelations
      .filter(fk => fk.rely && fk.childColumns.nonEmpty && fk.parentColumns.nonEmpty)
      .flatMap { fk =>
        for {
          child  <- normalizeTableName(fk.childTable)
          parent <- normalizeTableName(fk.parentTable)
        } yield declareRelyFkStatement(fk.copy(childTable = child, parentTable = parent))
      }
      .distinct
  }

  private[compiler] def declareRelyFkStatement(fk: ForeignKeyRelation): String =
    s"PRAGMA openivm_declare_rely_fk('${escapeSql(fk.childTable)}','${escapeSql(jsonArray(fk.childColumns))}'," +
      s"'${escapeSql(fk.parentTable)}','${escapeSql(jsonArray(fk.parentColumns))}');"

  private def jsonArray(values: Seq[String]): String =
    values.map(v => "\"" + v.flatMap(jsonEscapeChar) + "\"").mkString("[", ",", "]")

  private def jsonEscapeChar(ch: Char): String = ch match {
    case '"'          => "\\\""
    case '\\'         => "\\\\"
    case '\b'         => "\\b"
    case '\f'         => "\\f"
    case '\n'         => "\\n"
    case '\r'         => "\\r"
    case '\t'         => "\\t"
    case c if c < ' ' => f"\\u${c.toInt}%04x"
    case c            => c.toString
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
      val stage = OpenIvmCompiler.classifyCompileFailureStage(stdout)
      val hint  = if (stderr.nonEmpty) s"\nCLI stderr:\n$stderr" else ""
      throw new OpenIvmCompileException(
        s"openivm_compile_with_facts('$viewName', ...) produced no result (failed during ${stage.label})$hint",
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

    // openivm always emits a LIVE read of each source. When the user pinned a
    // source to a Delta snapshot, re-attach that pin here — otherwise the CTAS
    // at CREATE would load the current table instead of the version the view
    // body asked for. Every `memory.main.<source>` reference openivm emits sits
    // in a FROM position, where Spark's `temporalClause` is grammatical.
    val pinBySource: Map[String, String] =
      SparkTimeTravelSql.split(req.viewSql).pins.map(pin => pin.shortName -> pin.clause).toMap
    for (short <- req.sourceQualifiedNames.keySet ++ pinBySource.keySet) {
      val qualified = req.sourceQualifiedNames.getOrElse(short, short)
      val pin       = pinBySource.get(short.toLowerCase(Locale.ROOT)).map(clause => s" $clause").getOrElse("")
      if (qualified != short || pin.nonEmpty) sql = sql.replace(s"memory.main.$short", s"$qualified$pin")
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

  /** Persists a diagnostic bundle for a failed compile when a failure-bundle
    * directory is configured (via the `failureBundleDir` constructor
    * parameter, which [[OpenIvmCompiler.build]] defaults to the
    * `OPENIVM_COMPILE_FAILURE_BUNDLE_DIR` environment variable), so a
    * failure can be triaged without reproducing it interactively. Off by
    * default (unset → no-op), so normal operation is unaffected.
    *
    * Writes, under a unique subdirectory of the configured root
    * (`<viewName>-<nanoTime>/`):
    *   - `original.sql`   — `req.viewSql` as supplied by the caller
    *   - `normalized.sql` — the compile-bridge copy actually sent to DuckDB
    *   - `facts.json`     — the exact `WorkloadFacts` JSON payload used
    *   - `stdout.log` / `stderr.log` — the DuckDB CLI subprocess output
    *   - `stage.txt`      — [[OpenIvmCompiler.classifyCompileFailureStage]]'s
    *     label, so a CREATE/bind failure is distinguished from an
    *     `openivm_compile_with_facts` failure without re-parsing stdout
    *
    * Called from the `catch` branches in [[compile]], strictly before the
    * `finally` block's `deleteDirRecursively(tmpDir)` runs, so the bundle
    * reflects this attempt's state.  None of the persisted content includes
    * credentials or environment secrets — it is exactly the SQL/JSON/CLI
    * output already visible in the thrown exception and DuckDB CLI streams.
    */
  private def persistFailureBundleIfConfigured(
      req: CompileRequest,
      normalizedViewSql: String,
      stdout: String,
      stderr: String
  ): Unit =
    failureBundleDir.foreach { rootDir =>
      try {
        val safeName  = req.viewName.replaceAll("[^A-Za-z0-9_.-]", "_")
        val bundleDir = Paths.get(rootDir, s"$safeName-${System.nanoTime()}")
        Files.createDirectories(bundleDir)
        val stage = OpenIvmCompiler.classifyCompileFailureStage(stdout)
        Files.write(bundleDir.resolve("original.sql"), req.viewSql.getBytes("UTF-8"))
        Files.write(bundleDir.resolve("normalized.sql"), normalizedViewSql.getBytes("UTF-8"))
        Files.write(bundleDir.resolve("facts.json"), req.facts.toJson.getBytes("UTF-8"))
        Files.write(bundleDir.resolve("stdout.log"), stdout.getBytes("UTF-8"))
        Files.write(bundleDir.resolve("stderr.log"), stderr.getBytes("UTF-8"))
        Files.write(bundleDir.resolve("stage.txt"), stage.label.getBytes("UTF-8"))
      } catch {
        // Narrow on purpose: a bundle-write failure must be logged, not
        // swallowed into the original compile exception being diagnosed, and
        // must not mask it either — so only the I/O failure mode particular
        // to this best-effort persistence step is caught here.
        case e: java.io.IOException =>
          System.err.println(s"openivm compiler: failed to persist compile-failure bundle: ${e.getMessage}")
      }
    }

  private def deleteDirRecursively(dir: Path): Unit = {
    val stream = Files.walk(dir)
    try stream.sorted(Comparator.reverseOrder[Path]()).forEach(p => Files.delete(p))
    finally stream.close()
  }
}

object OpenIvmCompiler {

  /** Distinguishes which stage of the compile script a DuckDB CLI failure
    * occurred in, for [[classifyCompileFailureStage]].
    */
  sealed trait CompileFailureStage { def label: String }
  object CompileFailureStage       {

    /** The `CREATE OR REPLACE MATERIALIZED VIEW ... AS <sql>` statement
      * itself failed to parse/bind (e.g. a syntax error in the normalized
      * SQL, or an unresolvable column/table reference).
      */
    case object CreateOrBind extends CompileFailureStage { val label = "CREATE MATERIALIZED VIEW (bind)" }

    /** The view bound successfully but `openivm_compile_with_facts` itself
      * failed (e.g. an unsupported refresh shape, or a malformed facts
      * payload).
      */
    case object CompileWithFacts extends CompileFailureStage { val label = "openivm_compile_with_facts" }
  }

  /** Classifies a failed compile attempt's stage from the DuckDB CLI's
    * `-jsonlines` stdout alone.
    *
    * `buildScript` always emits `CREATE OR REPLACE MATERIALIZED VIEW ...`
    * immediately followed by `SELECT * FROM openivm_compile_with_facts(...)`.
    * When `CREATE` fails, the DuckDB CLI reports the error on stderr and
    * (in `-jsonlines` mode) simply continues to the next statement without
    * emitting any output line for the failed one — so stdout never contains
    * the view-creation confirmation row. When `CREATE` succeeds but
    * `openivm_compile_with_facts` itself fails, that confirmation row IS
    * present, followed by nothing further (no `refresh_type` rows). This
    * gives a reliable, non-heuristic signal from stdout content alone,
    * without parsing or pattern-matching stderr text.
    */
  private[compiler] def classifyCompileFailureStage(stdout: String): CompileFailureStage =
    if (stdout.contains("\"MATERIALIZED VIEW CREATION\"")) CompileFailureStage.CompileWithFacts
    else CompileFailureStage.CreateOrBind

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
    * @param failureBundleDir Directory to persist a per-failure diagnostic
    *                       bundle under (see `persistFailureBundleIfConfigured`).
    *                       Defaults to the `OPENIVM_COMPILE_FAILURE_BUNDLE_DIR`
    *                       environment variable; `None`/unset disables the
    *                       feature entirely (the default, zero-overhead path).
    * @throws IllegalArgumentException if `extensionPath` or `cliPath` does not exist on disk.
    * @throws NotImplementedError      if `isolation == ChildProcess`.
    */
  def build(
      extensionPath: String = sys.env.getOrElse(
        "OPENIVM_EXTENSION_PATH",
        "/opt/openivm/openivm.duckdb_extension"
      ),
      cliPath: String = sys.env.getOrElse("OPENIVM_CLI_PATH", defaultCliPath()),
      isolation: Isolation = InProcess,
      failureBundleDir: Option[String] = sys.env.get("OPENIVM_COMPILE_FAILURE_BUNDLE_DIR").filter(_.nonEmpty)
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
      new OpenIvmCompiler(extensionPath, cliPath, isolation, failureBundleDir)
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
    * Collision-prone, arity-incompatible, or Spark-only built-ins (1-arg /
    * 2-arg `to_date`, 1-arg / 2-arg `to_timestamp`, 2-arg `date_format`,
    * 7-arg `make_interval`, 2-arg `get_json_object`, 0-arg `current_date`,
    * 0-arg `current_timestamp`) are renamed to `__sparkfn_*` by
    * [[renameSparkFunctionShimCalls]] before the SQL reaches DuckDB. The
    * 1-arg date/time spellings use dedicated `*_1arg` macro names because
    * DuckDB macros do not overload by arity. The macros below define the
    * corresponding DuckDB-side bodies that openivm will inline.
    * `LptsSparkDialect.rewriteSparkFunctionInlinings` reverses the inlinings
    * back to Spark's original functions in the emitted refresh SQL.
    *
    * Literal-boolean `last_value(expr, true|false)` / `first_value(expr,
    * true|false)` calls are normalized to DuckDB's native window syntax
    * before compile. `__sparkfn_last_value` / `__sparkfn_first_value` remain
    * as compatibility fallbacks for non-literal second arguments; they still
    * use DuckDB's `last(expr)` / `first(expr)` stand-ins and therefore cannot
    * preserve dynamic ignore-null flags.
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
      // The nonnumeric marker branch is always NULL, but keeps all seven
      // arguments visible to LPTS for exact scanner-based recovery. The
      // interval fallback keeps the DuckDB macro type- and value-correct.
      s"CREATE OR REPLACE MACRO __sparkfn_make_interval(years, months, weeks, days, hours, mins, secs) AS COALESCE(to_seconds(TRY_CAST(concat('${SparkFunctionShimSql.MakeIntervalMarker}', years, '${SparkFunctionShimSql.MarkerArgSeparator}', months, '${SparkFunctionShimSql.MarkerArgSeparator}', weeks, '${SparkFunctionShimSql.MarkerArgSeparator}', days, '${SparkFunctionShimSql.MarkerArgSeparator}', hours, '${SparkFunctionShimSql.MarkerArgSeparator}', mins, '${SparkFunctionShimSql.MarkerArgSeparator}', secs) AS DOUBLE)), to_years(CAST(years AS BIGINT)) + to_months(CAST(months AS BIGINT)) + to_days(CAST(weeks AS BIGINT) * 7) + to_days(CAST(days AS BIGINT)) + to_hours(CAST(hours AS BIGINT)) + to_minutes(CAST(mins AS BIGINT)) + to_seconds(CAST(secs AS DOUBLE)));",
      // The pinned DuckDB binary has no JSON extension. Encode both arguments
      // in a collision-free VARCHAR marker, then restore Spark's exact call.
      s"CREATE OR REPLACE MACRO __sparkfn_get_json_object(json_text, path) AS concat('${SparkFunctionShimSql.GetJsonObjectMarker}', json_text, '${SparkFunctionShimSql.MarkerArgSeparator}', path);",
      // Fallback for non-literal Spark `last_value(expr, ignoreNulls)` calls.
      // Literal boolean flags are handled in the pre-pass with DuckDB's native
      // `IGNORE NULLS` modifier; dynamic flags keep the legacy compile-time
      // stand-in and cannot preserve ignore-null semantics.
      "CREATE OR REPLACE MACRO __sparkfn_last_value(expr, ignore_nulls) AS last(expr);",
      // Fallback for non-literal Spark `first_value(expr, ignoreNulls)` calls,
      // mirroring `__sparkfn_last_value` above.
      "CREATE OR REPLACE MACRO __sparkfn_first_value(expr, ignore_nulls) AS first(expr);",
      // Spark's `current_date()` / `current_timestamp()` are non-deterministic
      // and would otherwise be rejected by `openivm_compile_with_facts`
      // planning (or worse, bound to DuckDB's own volatile builtins and
      // evaluated at a different time than Spark would). `get_current_timestamp()`
      // is used as the inlined body's distinguishing marker (rather than
      // `now()`) specifically because Spark has no function of that name, so
      // the LptsSparkDialect post-pass can recognize the inlined shape without
      // risk of misfiring on genuine user SQL. DuckDB has no direct
      // TIMESTAMPTZ -> DATE cast, hence the intermediate TIMESTAMP cast.
      "CREATE OR REPLACE MACRO __sparkfn_current_timestamp() AS CAST(get_current_timestamp() AS TIMESTAMP);",
      "CREATE OR REPLACE MACRO __sparkfn_current_date() AS CAST(CAST(get_current_timestamp() AS TIMESTAMP) AS DATE);"
    )
    macros.mkString("", "\n", "\n")
  }
}
