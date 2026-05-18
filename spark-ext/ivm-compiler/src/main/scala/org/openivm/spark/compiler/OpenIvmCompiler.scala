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
    sb ++= "SET openivm_target_dialect='spark';\n"
    sb ++= "SET openivm_compile_only=true;\n"
    sb ++= "SET openivm_enable_view_matching=false;\n"
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
    sb ++= s"CREATE OR REPLACE MATERIALIZED VIEW ${req.viewName} AS ${normalizeSparkSqlForDuckdb(req.viewSql)};\n"
    sb ++= s"PRAGMA compile_refresh('${escapeSql(req.viewName)}');\n"
    sb.toString
  }

  /** Spark-specific join syntaxes that DuckDB does not parse.  Translates them
    * to the equivalent DuckDB form before the view definition is sent to
    * `CREATE MATERIALIZED VIEW`.
    *
    * Currently translated:
    *   - `LEFT SEMI JOIN` → `SEMI JOIN` (DuckDB does not accept the LEFT prefix)
    *   - `LEFT ANTI JOIN` → `ANTI JOIN`
    *
    * Both forms have identical semantics on the Spark side (`LEFT SEMI`/`LEFT
    * ANTI` are the documented Spark spellings; bare `SEMI`/`ANTI` is also
    * accepted by Spark) so the translation is round-trip safe — the original
    * Spark SQL is still stored in `MvMetadata.querySql` and used for
    * FULL_REFRESH `INSERT OVERWRITE`.
    */
  private[compiler] def normalizeSparkSqlForDuckdb(sql: String): String = {
    val leftSemi = """(?i)\bLEFT\s+SEMI\s+JOIN\b""".r
    val leftAnti = """(?i)\bLEFT\s+ANTI\s+JOIN\b""".r
    leftAnti.replaceAllIn(
      leftSemi.replaceAllIn(sql, "SEMI JOIN"),
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
    stdout.linesIterator.find(_.contains("\"refresh_type\"")) match {
      case None =>
        val hint = if (stderr.nonEmpty) s"\nCLI stderr:\n$stderr" else ""
        throw new OpenIvmCompileException(
          s"PRAGMA compile_refresh('$viewName') produced no result$hint",
          null
        )
      case Some(line) => parseRefreshLine(line)
    }
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
}
