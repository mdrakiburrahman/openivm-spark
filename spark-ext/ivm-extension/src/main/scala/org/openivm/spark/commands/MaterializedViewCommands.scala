package org.openivm.spark.commands

import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, NamedExpression, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{
  Aggregate,
  Filter,
  GlobalLimit,
  LocalLimit,
  LogicalPlan,
  Offset,
  Sort,
  Tail
}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.StructType
import org.openivm.spark.analyzer.IvmDmlInterceptorRule
import org.openivm.spark.common._
import org.openivm.spark.compiler.{CompileRequest, OpenIvmCompiler}

import java.sql.Timestamp
import java.util.{Collections, UUID}

// ---------------------------------------------------------------------------
// Compiler singleton — one OpenIvmCompiler per SparkSession, lazily created.
// ---------------------------------------------------------------------------
private[commands] object OpenIvmCompilers {

  private val cache: java.util.Map[SparkSession, OpenIvmCompiler] =
    Collections.synchronizedMap(new java.util.WeakHashMap[SparkSession, OpenIvmCompiler]())

  def forSession(spark: SparkSession): OpenIvmCompiler = {
    val existing = cache.get(spark)
    if (existing != null) return existing
    cache.synchronized {
      val existing2 = cache.get(spark)
      if (existing2 != null) return existing2
      val c = OpenIvmCompiler.build()
      cache.put(spark, c)
      Runtime.getRuntime.addShutdownHook(new Thread(() => c.close()))
      c
    }
  }
}

// ---------------------------------------------------------------------------
// Per-MV refresh mutex — JVM-wide. openivm itself uses a per-view mutex
// (see openivm/test/sql/concurrency.test prologue), and we replicate that
// invariant here because the Spark-side incremental refresh path is NOT
// safe under naive Delta OCC + retry: the per-statement retry harness
// re-executes the same MERGE without re-reading the staging-delta
// snapshot, so two threads that both observed the same unconsumed delta
// can each apply it once, double-counting the count-monoid aggregates.
// ---------------------------------------------------------------------------
private[commands] object RefreshMutex {

  private val locks: java.util.Map[String, AnyRef] =
    Collections.synchronizedMap(new java.util.HashMap[String, AnyRef]())

  /** Acquire (creating if absent) and synchronize on the lock object that
    * keys this MV. The lock identity is the fully-qualified MV name so two
    * refreshes targeting the SAME logical MV serialise, even if they
    * originate from different Spark sessions in the same JVM.
    */
  def withLock[A](mvKey: String)(body: => A): A = {
    val existing = locks.get(mvKey)
    val lock =
      if (existing != null) existing
      else
        locks.synchronized {
          val again = locks.get(mvKey)
          if (again != null) again
          else {
            val l = new Object
            locks.put(mvKey, l)
            l
          }
        }
    lock.synchronized(body)
  }
}

// ---------------------------------------------------------------------------
// Structured per-refresh timing telemetry — emits `[openivm-perf]` log lines
// that a downstream parser can use to break a refresh down into phases and
// per-statement times.  Distinct from the existing `[openivm-mv]` lines
// (which carry decision/outcome metadata at second-level resolution) so the
// two parsers don't have to disambiguate fields.
//
// Line shapes (all single-line, INFO level):
//
//   [openivm-perf] refresh_id='<uuid>' view='<sqlIdent>' phase='start' \
//       thread='<thread-name>'
//   [openivm-perf] refresh_id='<uuid>' view='<sqlIdent>' phase='<name>' \
//       elapsed_ms=<long> [<extra k=v pairs>]
//   [openivm-perf] refresh_id='<uuid>' view='<sqlIdent>' phase='stmt' \
//       stmt_idx=<int> stmt_kind='<classifier>' elapsed_ms=<long>
//   [openivm-perf] refresh_id='<uuid>' view='<sqlIdent>' phase='end' \
//       total_ms=<long> refresh_type='<RefreshTypeCode name>' \
//       outcome='<outcome>' pending_deltas=<int>
//
// Disable with `OPENIVM_PERF_DISABLED=1` or `-Dopenivm.perf.disabled=1`.
// ---------------------------------------------------------------------------
private[commands] object RefreshPerf extends org.apache.spark.internal.Logging {

  def enabled: Boolean = {
    try {
      val raw = sys.env
        .get("OPENIVM_PERF_DISABLED")
        .orElse(sys.props.get("openivm.perf.disabled"))
        .getOrElse("0")
      !(raw.trim == "1" || raw.trim.equalsIgnoreCase("true"))
    } catch { case _: Throwable => true }
  }

  def timePhase[A](refreshId: String, view: String, phase: String, extraFields: String = "")(
      body: => A
  ): A = {
    val t0 = System.nanoTime()
    try body
    finally {
      val elapsedMs = (System.nanoTime() - t0) / 1000000L
      if (enabled) {
        val extra = if (extraFields.nonEmpty) " " + extraFields else ""
        logInfo(
          s"[openivm-perf] refresh_id='$refreshId' view='$view' phase='$phase' " +
            s"elapsed_ms=$elapsedMs$extra"
        )
      }
    }
  }

  def emit(refreshId: String, view: String, phase: String, extraFields: String = ""): Unit =
    if (enabled) {
      val extra = if (extraFields.nonEmpty) " " + extraFields else ""
      logInfo(s"[openivm-perf] refresh_id='$refreshId' view='$view' phase='$phase'$extra")
    }

  def logStmt(
      refreshId: String,
      view: String,
      stmtIdx: Int,
      stmtKind: String,
      elapsedMs: Long,
      extra: Option[String] = None
  ): Unit =
    if (enabled) {
      val tail = extra.filter(_.nonEmpty).map(" " + _).getOrElse("")
      logInfo(
        s"[openivm-perf] refresh_id='$refreshId' view='$view' phase='stmt' " +
          s"stmt_idx=$stmtIdx stmt_kind='$stmtKind' elapsed_ms=$elapsedMs$tail"
      )
    }

  def timeStmt[A](refreshId: String, view: String, stmtIdx: Int, stmtKind: String)(body: => A): A = {
    val t0 = System.nanoTime()
    try body
    finally {
      val elapsedMs = (System.nanoTime() - t0) / 1000000L
      logStmt(refreshId, view, stmtIdx, stmtKind, elapsedMs)
    }
  }

  /** Classify a Spark SQL statement by its leading tokens.  Used to label
    * `phase='stmt'` lines so the parser can tell view-delta CTAS from MERGE
    * from INSERT OVERWRITE without re-parsing the SQL.  When `viewDeltaPath`
    * is non-empty, a CTAS whose SQL references that path is tagged
    * `view_delta_ctas`.
    */
  def classify(sql: String, viewDeltaPath: String): String = {
    val trimmed = sql.replaceAll("(?s)^\\s*(--[^\\n]*\\n|/\\*.*?\\*/)+", "").trim
    val upper   = trimmed.toUpperCase
    if (upper.startsWith("MERGE")) "merge"
    else if (upper.startsWith("DELETE")) "delete"
    else if (upper.startsWith("INSERT OVERWRITE")) "insert_overwrite"
    else if (upper.startsWith("INSERT INTO")) "insert_into"
    else if (upper.startsWith("UPDATE")) "update"
    else if (
      upper.startsWith("CREATE OR REPLACE TEMPORARY VIEW") ||
      upper.startsWith("CREATE TEMPORARY VIEW") ||
      upper.startsWith("CREATE OR REPLACE TEMP VIEW") ||
      upper.startsWith("CREATE TEMP VIEW")
    ) "temp_view"
    else if (
      upper.startsWith("CREATE TABLE") ||
      upper.startsWith("CREATE OR REPLACE TABLE")
    ) {
      if (viewDeltaPath.nonEmpty && sql.contains(viewDeltaPath)) "view_delta_ctas"
      else "ctas"
    } else if (upper.startsWith("DROP")) "drop"
    else if (upper.startsWith("SELECT")) "select"
    else "other"
  }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------
private[commands] object MvCommandHelper {

  /** Fully-qualified dot-separated name used in MvMetadata and SQL strings. */
  def metaName(id: TableIdentifier): String =
    id.database.fold(id.table)(db => s"$db.${id.table}")

  /** Backtick-quoted SQL identifier, including optional catalog and database. */
  def sqlIdent(id: TableIdentifier): String = {
    val parts = id.catalog.toSeq ++ id.database.toSeq ++ Seq(id.table)
    parts.map(p => s"`${p.replace("`", "``")}`").mkString(".")
  }

  /** Physical path for the MV's Delta table inside `<warehouse>/_ivm/views/`. */
  def mvLocation(spark: SparkSession, id: TableIdentifier): String = {
    val warehouse = spark.conf.get("spark.sql.warehouse.dir").stripSuffix("/")
    val segment   = id.database.fold(id.table)(db => s"$db/${id.table}")
    s"$warehouse/_ivm/views/$segment"
  }

  /**
   * Best-effort cleanup of a stale, non-Delta MV location before CREATE.
   *
   * Background: `CREATE TABLE ... USING DELTA LOCATION '<path>'` fails with
   * `DELTA_CREATE_TABLE_WITH_NON_EMPTY_LOCATION` when `<path>` already
   * contains files but lacks a `_delta_log/` subdirectory. This happens
   * when a previous CREATE attempt aborted mid-flight (e.g. driver OOM
   * during the initial-load CTAS write, leaving Parquet files behind but
   * no Delta log). Dbt then retries the CREATE for many minutes (24
   * retries × 60s) and every retry hits the same error because the dir
   * is still non-empty.
   *
   * mvLocation is always under `<warehouse>/_ivm/views/`, which is an
   * openivm-managed namespace, so it is safe to wipe any stray files
   * that pre-date a Delta commit. If the dir has `_delta_log/` we leave
   * it alone — that's a legitimate Delta table and Delta's own
   * idempotency takes over.
   */
  def cleanupStaleMvLocation(spark: SparkSession, location: String): Unit = {
    try {
      val path  = new Path(location)
      val hconf = spark.sparkContext.hadoopConfiguration
      val fs    = path.getFileSystem(hconf)
      if (!fs.exists(path)) return
      val deltaLog = new Path(path, "_delta_log")
      if (fs.exists(deltaLog)) return
      val children = fs.listStatus(path)
      if (children == null || children.isEmpty) return
      fs.delete(path, true)
    } catch {
      case _: Throwable => ()
    }
  }

  /**
   * Analyze `querySql` in the current session and return
   * (qualifiedNames, qualifiedSchemas, compileSchemas, shortToQualMap).
   *
   * qualifiedNames go into MvMetadata.sourceTables and the fingerprint.
   * shortNames go into CompileRequest.sources so DuckDB can resolve them
   * against the view body (which uses unqualified table references).
   * The shortToQual map lets the compiler bridge rewrite emitted SQL that
   * references DuckDB's `memory.main.<short>` qualifier back to the Spark
   * fully-qualified table name.
   */
  def collectSourceSchemas(
      spark: SparkSession,
      querySql: String
  ): (Seq[String], Map[String, StructType], Map[String, StructType], Map[String, String]) = {
    val analyzed = spark.sql(querySql).queryExecution.analyzed

    // Extract (qualifiedName, shortName) pairs from a single plan, non-recursively.
    def pairsFromPlan(plan: LogicalPlan): Seq[(String, String)] =
      plan.collect {
        case r: LogicalRelation if r.catalogTable.isDefined =>
          val id        = r.catalogTable.get.identifier
          val qualified = id.database.fold(id.table)(db => s"$db.${id.table}")
          (qualified, id.table)
        case r: DataSourceV2Relation if r.identifier.isDefined =>
          val ident     = r.identifier.get
          val ns        = ident.namespace()
          val short     = ident.name()
          val qualified = if (ns.nonEmpty) (ns :+ short).mkString(".") else short
          (qualified, short)
      }

    // Collect table pairs from a plan AND from any SubqueryExpression plans nested
    // within node expressions (covers WHERE EXISTS / IN subqueries whose inner plan
    // is not reachable via LogicalPlan.children alone).
    def collectAllPairs(plan: LogicalPlan): Seq[(String, String)] = {
      val direct = pairsFromPlan(plan)
      // plan.collect { case p => p } enumerates every LogicalPlan node in the tree.
      // For each node we look inside its expressions for SubqueryExpression instances
      // (Exists, ListQuery, ScalarSubquery, etc.) and recurse into their inner plans.
      val fromSubqueries = plan
        .collect { case p: LogicalPlan => p }
        .flatMap { node =>
          node.expressions.flatMap { expr =>
            expr.collect { case s: SubqueryExpression => s }
          }
        }
        .flatMap(s => collectAllPairs(s.plan))
      (direct ++ fromSubqueries).distinct
    }

    val pairs: Seq[(String, String)] = collectAllPairs(analyzed)

    val qualNames = pairs.map(_._1).distinct
    // Fetch full table schemas from the Spark catalog (not projected/pruned).
    val qualSchemas = qualNames.map(n => n -> spark.table(n).schema).toMap
    val compileSchemas: Map[String, StructType] =
      pairs.map { case (q, s) => s -> qualSchemas(q) }.toMap
    val shortToQual: Map[String, String] =
      pairs.map { case (q, s) => s -> q }.toMap
    (qualNames, qualSchemas, compileSchemas, shortToQual)
  }

  /** Extract the GROUP BY key column names from an analyzed LogicalPlan. */
  def extractGroupKeys(analyzed: LogicalPlan): Seq[String] =
    analyzed
      .collect { case agg: Aggregate => agg }
      .headOption
      .map(_.groupingExpressions.collect { case ne: NamedExpression => ne.name })
      .getOrElse(Nil)

  /** Detects a top-level Top-K wrapper (`ORDER BY … [LIMIT k] [OFFSET m]`) in the
    * user-supplied view body.
    *
    * Parses `querySql` with Spark's unresolved parser and inspects the root of
    * the resulting [[LogicalPlan]].  We look for any combination of
    * [[GlobalLimit]], [[LocalLimit]], [[Sort]], [[Offset]], or [[Tail]] at the
    * top of the plan — a Top-K view always presents at least one of these as
    * the outermost operator (e.g. `GlobalLimit -> LocalLimit -> Sort -> …` for
    * `ORDER BY … LIMIT k`; `Offset -> GlobalLimit -> LocalLimit -> Sort -> …`
    * for `ORDER BY … LIMIT k OFFSET m`).
    *
    * Returns `false` if parsing fails for any reason (e.g. dialect-specific
    * syntax), in which case the caller falls through to openivm's
    * classification.
    */
  def hasTopLevelTopK(spark: SparkSession, querySql: String): Boolean = {
    try {
      val parsed = spark.sessionState.sqlParser.parsePlan(querySql)
      parsed match {
        case _: GlobalLimit | _: LocalLimit | _: Sort | _: Offset | _: Tail => true
        case _                                                              => false
      }
    } catch { case _: Throwable => false }
  }

  /** If the view's aggregate has a `COUNT(*)` aggregate expression, return the
    * alias it projects under (e.g. `cnt` from `COUNT(*) AS cnt`).  This alias
    * — when present — doubles as the openivm bookkeeping count column, so it
    * is used by the incremental-refresh post-pass DELETE to remove rows whose
    * group has been fully retracted.
    */
  def extractCountStarAlias(analyzed: LogicalPlan): Option[String] = {
    import org.apache.spark.sql.catalyst.expressions.Alias
    import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count}
    analyzed.collect { case agg: Aggregate => agg }.headOption.flatMap { agg =>
      agg.aggregateExpressions.collectFirst {
        case Alias(AggregateExpression(c: Count, _, _, _, _), alias) if isCountStar(c) => alias
      }
    }
  }

  private def isCountStar(c: org.apache.spark.sql.catalyst.expressions.aggregate.Count): Boolean = {
    import org.apache.spark.sql.catalyst.expressions.Literal
    c.children.forall {
      case _: Literal => true
      case _          => false
    }
  }

  /** Sibling Delta table name for an AGGREGATE_HAVING materialized view. The
    * data table stores every group (no HAVING filter) so that a group whose
    * aggregate later crosses the threshold can be promoted back into the
    * HAVING-passing set via the incremental MERGE. The user-facing object is
    * a Spark VIEW that applies the HAVING predicate at read time.
    *
    * Convention: `<table>__ivm_data` in the same database. Stays in lock-step
    * between CREATE / REFRESH / DROP so the three commands address the same
    * Delta table.
    */
  def dataTableId(id: TableIdentifier): TableIdentifier =
    id.copy(table = id.table + "__ivm_data")

  /** Extract the HAVING predicate from an analyzed plan as a Spark-SQL string,
    * with each aggregate function reference rewritten to the SELECT-list alias
    * that materialises it on the data table.
    *
    * Returns None if no `Filter` sits directly above an `Aggregate` (the
    * canonical HAVING shape after analysis), or if the analyzer wrapped the
    * pattern in an operator we don't recognise.
    *
    * Caveat: HAVING aggregates not also exposed in the SELECT list (e.g.
    * `SELECT region, SUM(x) ... HAVING COUNT(*) > 5` where COUNT(*) is not
    * selected) end up referencing the analyzer's hidden alias for the
    * aggregate; the rewritten SQL will reference a column that does not
    * exist on the data table. Such views are caught at CREATE time and fall
    * back to FULL_REFRESH with an explicit warning.
    */
  def extractHavingPredicateSql(analyzed: LogicalPlan): Option[String] = {
    analyzed
      .collectFirst { case Filter(cond, agg: Aggregate) =>
        (cond, agg)
      }
      .map { case (cond, agg) =>
        val aliasMap: Map[Expression, String] = agg.aggregateExpressions.collect {
          case al: Alias if al.child.find(_.isInstanceOf[AggregateExpression]).isDefined =>
            al.child.canonicalized -> al.name
        }.toMap

        val rewritten = cond.transform {
          case e: Expression if aliasMap.contains(e.canonicalized) =>
            UnresolvedAttribute(Seq(aliasMap(e.canonicalized)))
        }
        rewritten.sql
      }
  }

  /** True when the rewritten HAVING predicate references only attributes that
    * exist as columns on the data table. The check is approximate (regex over
    * backtick-quoted identifiers) but catches the failure mode where the
    * analyzer left a synthetic name like `count(1)` in the predicate.
    */
  def havingPredicateIsSafe(predicateSql: String, dataTableColumns: Set[String]): Boolean = {
    val ref  = "`([^`]+)`".r
    val refs = ref.findAllMatchIn(predicateSql).map(_.group(1)).toSet
    refs.forall(dataTableColumns.contains)
  }

  def createStagingViews(
      spark: SparkSession,
      deltas: Seq[StagingDelta],
      refreshType: Int
  ): Seq[String] = {
    if (refreshType == RefreshTypeCode.FullRefresh || deltas.isEmpty) return Nil
    val byTable = deltas.groupBy(_.baseTable)
    byTable.toSeq.map { case (qualifiedTable, tableDeltas) =>
      val shortName  = qualifiedTable.split("\\.").last
      val unionParts = tableDeltas.map(d => s"SELECT * FROM delta.`${escapePath(d.stagingPath)}`")
      spark.sql(
        s"CREATE OR REPLACE TEMP VIEW `$shortName` AS ${unionParts.mkString(" UNION ALL ")}"
      )
      shortName
    }
  }

  /** Drop temp views that were created by [[createStagingViews]]. */
  def dropStagingViews(spark: SparkSession, viewNames: Seq[String]): Unit =
    viewNames.foreach(n => spark.sql(s"DROP VIEW IF EXISTS `$n`"))

  private def escapePath(p: String): String = p.replace("`", "``")
}

// ---------------------------------------------------------------------------
// CreateMaterializedViewCommand
// ---------------------------------------------------------------------------

/**
 * Logical plan node for CREATE MATERIALIZED VIEW.
 *
 * @param originalQueryText  Raw SQL of the SELECT body, captured by the parser.
 *                           Stored verbatim in MvMetadata and passed to the compiler.
 */
case class CreateMaterializedViewCommand(
    name: TableIdentifier,
    query: LogicalPlan,
    properties: Map[String, String],
    ifNotExists: Boolean,
    provider: Option[String],
    originalQueryText: String
) extends LeafRunnableCommand {

  override def run(spark: SparkSession): Seq[Row] = {
    import MvCommandHelper._

    val createT0 = System.nanoTime()
    val profile  = RefreshProfile.start(spark, metaName(name), RefreshProfile.Mode.Create)
    val sqlLog =
      RefreshSqlLog.start(spark, profile.refreshId, metaName(name), RefreshSqlLog.ModeCreate)
    // Record the user-supplied CREATE-MV body up front. Not executed by us
    // (stmt_order = -1, duration = -1) but invaluable for the benchmarker
    // because it's the verbatim user query.
    sqlLog.record(
      category = "original_query",
      stmtOrder = -1,
      attemptIdx = 0,
      stmtKind = "select",
      sql = originalQueryText,
      durationMs = -1L
    )
    try runCreate(spark, profile, sqlLog)
    finally {
      val totalMs = (System.nanoTime() - createT0) / 1000000L
      profile.appendStep("create_mv_total", s"view=${sqlIdent(name)}", totalMs)
      profile.flush()
      sqlLog.flush()
    }
  }

  private def runCreate(
      spark: SparkSession,
      profile: RefreshProfile,
      sqlLog: RefreshSqlLog
  ): Seq[Row] = {
    import MvCommandHelper._

    profile.timeStep("create_mv_system_tables", "scope=mv_and_staging") {
      MvCatalog.ensureTables(spark)
      StagingCatalog.ensureTables(spark)
    }

    // Existence guard
    MvCatalog.lookup(spark, name) match {
      case Some(_) if ifNotExists => return Seq.empty
      case Some(_) =>
        throw new AnalysisException(
          "TABLE_OR_VIEW_ALREADY_EXISTS",
          Map("relationName" -> sqlIdent(name))
        )
      case None => // proceed
    }

    // Resolve source schemas
    val (qualNames, qualSchemas, compileSchemas, shortToQual) =
      collectSourceSchemas(spark, originalQueryText)

    // Extract GROUP BY keys and other optional metadata from the analyzed plan
    val analyzed       = spark.sql(originalQueryText).queryExecution.analyzed
    val groupKeys      = extractGroupKeys(analyzed)
    val countStarAlias = extractCountStarAlias(analyzed)

    // Compile the view via OpenIVM. If openivm's DuckDB subprocess cannot
    // compile the query (e.g. the user's view body references Spark-only
    // functions like `regexp_like`, `to_date(string)`, etc. that DuckDB does
    // not recognise), demote the view to FULL_REFRESH so each refresh
    // re-executes the original Spark SQL via INSERT OVERWRITE. This trades
    // incrementality for correctness: the MV stays bag-equal to the live
    // query while the user retains source-of-truth control over the SQL
    // they wrote.
    val compiler = OpenIvmCompilers.forSession(spark)
    val compiled = profile.timeStep(
      "create_compile_classification",
      s"sources=${qualNames.size};group_cols=${groupKeys.size}"
    ) {
      try
        compiler.compile(
          CompileRequest(
            viewName = name.table,
            viewSql = originalQueryText,
            sources = compileSchemas,
            sourceQualifiedNames = shortToQual
          )
        )
      catch {
        case e: org.openivm.spark.compiler.OpenIvmCompileException =>
          // ERROR-level so the demotion is visible in the dbt-server / Livy
          // container log. Structured shape — operator can grep on
          // `[openivm-mv]` to enumerate all demotions for a run.
          logError(
            s"[openivm-mv] view='${sqlIdent(name)}' compiled_refresh_type='COMPILE_FAILED' " +
              s"effective_refresh_type='FULL_REFRESH' reason='compile_failed' cause=${e.getMessage}"
          )
          org.openivm.spark.compiler.CompiledRefresh(
            refreshType = RefreshTypeCode.FullRefresh,
            refreshTypeName = "FULL_REFRESH",
            sql = "",
            initialLoadSql = ""
          )
      }
    }

    val windowInitialLoadMatchesUserQuery: Boolean =
      if (compiled.refreshType != RefreshTypeCode.WindowPartition || compiled.initialLoadSql.isEmpty) true
      else {
        val translatedInitialLoadSql = org.openivm.spark.compiler.LptsSparkDialect.translate(compiled.initialLoadSql)
        try {
          val expected    = spark.sql(originalQueryText)
          val userCols    = expected.columns.toSeq
          val initialLoad = spark.sql(translatedInitialLoadSql).selectExpr(userCols.map(c => s"`$c`"): _*)
          // Order-independent multiset equality via (COUNT(*), SUM(xxhash64(*))) digest.
          // EXCEPT ALL of two large window bags OOMs the driver at benchmark scale
          // (see SF=175 OAT failure where stage at MaterializedViewCommands.scala:553
          // throws SparkOutOfMemoryError); this single-pass aggregation is O(scan).
          //
          // FP-tolerant column expression: openivm's LPTS rewrite of STDDEV/VARIANCE
          // over DOUBLE re-derives the aggregate via the "naive variance" identity
          // sqrt((Σx² − (Σx)²/n)/(n−1)) rather than Spark's Welford evaluator, which
          // produces values that differ at the ULP level (decimals 14+) from the
          // straight-through user-query evaluation. Both formulas are mathematically
          // equivalent and the per-row downstream MERGE/recompute is unaffected, so
          // the initial-load equality check normalises numeric columns to a canonical
          // DOUBLE with 4 decimal places of precision before hashing — well above
          // ULP drift, well below the magnitude of any structural mismatch (wrong
          // formula, off-by-one grouping, missing rows would drift > 10⁻⁴ and still
          // get caught). The CAST → DOUBLE normalisation also defends against the
          // case where one side produces DECIMAL(_,6) (because of an explicit ROUND
          // in the body) and the other side produces DOUBLE (e.g. an unrounded LAG
          // intermediate) — xxhash64 hashes bytes, not values, so types must agree
          // before hashing. INT / STRING / DATE columns continue to be hashed
          // byte-exactly. NULLs are preserved (xxhash64(NULL)=0 on both sides).
          import org.apache.spark.sql.types._
          val schemaTypes = expected.schema.fields.map(f => f.name -> f.dataType).toMap
          val colsExpr = userCols
            .map { c =>
              schemaTypes.get(c) match {
                case Some(DoubleType) | Some(FloatType) | Some(_: DecimalType) =>
                  s"round(CAST(`$c` AS DOUBLE), 4)"
                case _ => s"`$c`"
              }
            }
            .mkString(", ")
          def digest(df: org.apache.spark.sql.DataFrame): (Long, Long) = {
            val row = df
              .selectExpr("COUNT(*) AS __ivm_cnt", s"COALESCE(SUM(xxhash64($colsExpr)), 0L) AS __ivm_hash")
              .head()
            (row.getLong(0), row.getLong(1))
          }
          val initDigest = digest(initialLoad)
          val userDigest = digest(expected)
          val isEqual    = initDigest == userDigest
          if (!isEqual) {
            logError(
              s"[openivm-mv-debug] view='${sqlIdent(name)}' init_digest=$initDigest user_digest=$userDigest " +
                s"cols_expr='$colsExpr'"
            )
          }
          isEqual
        } catch {
          case _: Throwable => false
        }
      }
    // Storage location
    val location = mvLocation(spark, name)
    val aggregateHavingDataColumns: Option[Set[String]] =
      if (compiled.refreshType != RefreshTypeCode.AggregateHaving) None
      else {
        val incrementalViewBodySql =
          if (compiled.initialLoadSql.isEmpty) originalQueryText
          else org.openivm.spark.compiler.LptsSparkDialect.translate(compiled.initialLoadSql)
        try {
          Some(
            spark
              .sql(s"SELECT * FROM ($incrementalViewBodySql) __openivm_having_preview LIMIT 0")
              .schema
              .fieldNames
              .toSet
          )
        } catch {
          case _: Throwable => None
        }
      }

    val simpleProjectionHasDataApply: Boolean =
      if (compiled.refreshType != RefreshTypeCode.SimpleProjection || compiled.sql.isEmpty) true
      else {
        val probeViewDeltaPath = s"${location.stripSuffix("/")}/__openivm_rewrite_probe"
        try {
          val rewritten = SparkRefreshRewriter.rewrite(
            compiledSql = compiled.sql,
            mvName = name,
            mvLocation = location,
            viewLogicalName = name.table,
            sourceTempViews = Map.empty,
            viewDeltaPath = probeViewDeltaPath,
            postProcess = org.openivm.spark.compiler.LptsSparkDialect.translate,
            sourceSchemas = qualSchemas.map { case (qual, schema) =>
              qual.split("\\.").last -> schema.fieldNames.toSeq
            },
            sourceQualifiedNames = shortToQual
          )
          rewritten.statements.size > 1
        } catch {
          case _: Throwable => false
        }
      }

    // Move the fingerprint computation below the upstream-MV enumeration so we
    // can include each upstream MV's identity hash. This way DROP + recreate
    // of an upstream MV with the same user schema but a different body
    // triggers `INCOMPATIBLE_VIEW_SCHEMA_CHANGE` at the next REFRESH.
    // (Definition of `fingerprint` deferred to line ~525 below.)

    // If openivm emits only an empty-placeholder delta (e.g. for multi-source JOINs
    // that it cannot compute incrementally), classify the MV as FULL_REFRESH so every
    // refresh re-executes the original view query from live tables instead of silently
    // performing a no-op incremental update.
    //
    // WINDOW_PARTITION (RefreshType 5) is still EXEMPT from the `hasRealDelta`
    // demotion, but the reason changed in openivm
    // `4471f4e929fd3b21ac55ea0c47249d4716853c98`
    // ("feat: emit openivm_delta_<view> from recompute paths"). With
    // `openivm_emit_cascade_delta_for_recompute=true`,
    // `CompileWindowRecompute()` now snapshots the affected pre-refresh rows and
    // recomputed post-refresh rows before mutating `openivm_data_<view>`, then
    // appends them as `-1/+1` rows into `openivm_delta_<view>`. The PRIMARY
    // execution shape is still the partition-scoped DELETE+INSERT that refreshes
    // this MV in place; the new view-delta exists so downstream MV-over-MV chains
    // can consume the recompute incrementally. The rewriter therefore handles
    // both pieces: the direct DELETE/INSERT plus the auxiliary cascade delta.
    //
    // GROUP_RECOMPUTE (RefreshType 6) likewise stopped being a "no view-delta"
    // exception once the same pragma is enabled. `CompileGroupRecompute()` still
    // materialises the affected-key set and refreshes the MV by DELETEing and
    // re-INSERTing those groups, but openivm now also snapshots the old/new group
    // rows and emits a signed `INSERT INTO openivm_delta_<view>` companion. That
    // extra delta is not the authoritative local refresh mechanism — it is the
    // downstream cascade feed. The Spark side therefore keeps the dedicated
    // GROUP_RECOMPUTE rewrites for the affected-group program while also
    // persisting the new signed view-delta. See CLAUDE.md: "Do not fix OpenIVM
    // correctness bugs by avoiding incrementalization."
    //
    // Top-K views (`ORDER BY … [LIMIT k] [OFFSET m]`) are also routed to FULL_REFRESH.
    // openivm classifies the *inner* stripped query as SIMPLE_PROJECTION (2) or
    // AGGREGATE_GROUP (0) and applies the ORDER BY/LIMIT in a thin user-facing VIEW
    // wrapper at read time (parser.cpp:239-291). The Spark side has no equivalent
    // table+view split today: the MV is a single Delta table addressed by `<name>`,
    // and the refresh rewriter writes directly into that table by name. Storing the
    // *unlimited* inner result there would make `SELECT * FROM <mv>` return every
    // row, ignoring the user's LIMIT; storing the *limited* result would produce an
    // incrementally-broken state (rows that fell out of the top-k can't come back
    // after a DELETE/UPDATE without reading the live source). FULL_REFRESH avoids
    // both pitfalls: every refresh runs `INSERT OVERWRITE TABLE <mv> SELECT * FROM
    // (<originalQueryText>)`, which evaluates `ORDER BY … LIMIT k` over the live
    // source and atomically replaces the MV with the correct k-row snapshot.
    //
    // This is an explicit (NOT silent) demotion. A future refinement could mirror
    // openivm's pattern — maintain an inner data table incrementally and create a
    // Spark VIEW on top with the ORDER BY/LIMIT applied at read time — but that
    // requires schema changes to `MvMetadata`, the refresh rewriter, and the drop
    // path that are out of scope here.
    val isTopKView = hasTopLevelTopK(spark, originalQueryText)
    // For AGGREGATE_HAVING (type 4) we need to extract the HAVING predicate up
    // front so we can build the user-facing VIEW that filters the data table.
    // If extraction fails (e.g. HAVING references a hidden aggregate not in
    // SELECT), demote to FULL_REFRESH — never silently drop the predicate.
    val rawHavingPred: Option[String] =
      if (compiled.refreshType == RefreshTypeCode.AggregateHaving)
        extractHavingPredicateSql(analyzed)
      else None
    // MV-over-MV chains: enumerate every upstream source that resolves to a
    // tracked materialized view, capturing the upstream's effective refresh
    // type so the demotion rule below can be capability-driven (not blanket).
    //
    // Lookup is symmetric on db.table vs bare-name matching to handle MVs
    // created with or without an explicit db prefix.
    val upstreamMvByQual: Map[String, MvMetadata] = {
      val all: Seq[MvMetadata] =
        try MvCatalog.list(spark)
        catch { case _: Throwable => Seq.empty[MvMetadata] }
      val byMeta: Map[String, MvMetadata] = all.map(m => metaName(m.name) -> m).toMap
      qualNames.flatMap { qn =>
        val short = qn.split("\\.").last
        byMeta.get(qn).orElse(byMeta.get(short)).map(m => qn -> m)
      }.toMap
    }
    val sourceIsMv: Boolean = upstreamMvByQual.nonEmpty
    val distinctUpstreamMvCount: Int =
      upstreamMvByQual.values.map(m => metaName(m.name)).toSet.size
    val nonCascadeUpstreams: Seq[(String, String)] =
      upstreamMvByQual.toSeq.collect {
        case (q, m) if !m.emitsCascadeViewDelta => q -> "non_cascade"
        // NOTE: an earlier `aggregate_group_into_simple_projection` guard demoted any SIMPLE_PROJECTION
        // whose upstream is AGGREGATE_GROUP, because openivm's NULL-companion retract
        // (`openivm/src/upsert/refresh_sql.cpp:898-960`) emits `(group_keys, NULL, NULL, ..., -1)`
        // rows that the downstream's value-equality MERGE cannot match. The guard was relaxed
        // because duckdb-openivm's compile path emits SIMPLE_PROJECTION refresh as a POSITIVE-ONLY
        // bag-apply for the data-table path: `INSERT INTO openivm_data_<v> ... WHERE openivm_multiplicity > 0`
        // — no DELETE/MERGE arm for negatives is generated. Negative-multiplicity rows from the
        // upstream retract are dropped at the join filter (BETWEEN over NULL → UNKNOWN) before
        // reaching the apply step. This matches duckdb-openivm's actual behavior, so Spark and
        // DuckDB agree on the bag at every batch boundary; both engines have the same
        // correctness footprint at scales where upstream dim deltas remain empty across batches
        // (the TPC-DI 100/1/1 layout: only fact tables change in batches 2/3). At larger scales
        // where dimension SCD-2 retracts manifest, both engines exhibit the same drift documented
        // in `.scratch/OPENIVM_VALIDATE.md` — Failure Mode 1, and the fix lives in openivm itself
        // (emit pre-merge snapshot retracts instead of NULL-companion). The fact that this
        // demotion existed only on the Spark side broke binary parity with duckdb-openivm.
        // NOTE: an earlier `multi_mv_simple_projection` guard demoted every SIMPLE_PROJECTION whose
        // upstream MV count was >= 2. That guard was over-defensive: openivm emits multi-source
        // SIMPLE_PROJECTION refresh as ONE `INSERT INTO openivm_delta_<view>` with a UNION ALL of
        // (Δup1 × cur_up2) ⊎ (cur_up1 × Δup2) ⊎ (-1 × Δup1 × Δup2) arms — all delta terms are
        // consumed in a single statement. The Spark rewriter handles this shape today:
        // `MaterializedViewCommands.runRefresh` (L962-972) registers one `openivm_delta_<source>`
        // temp view per source in `meta.sourceTables`, `SparkRefreshRewriter.rewriteMemoryMainPrefix`
        // substitutes every `memory.main.openivm_delta_<X>` reference (no single-source assumption),
        // and `StagingDeltaView.buildSourceDeltaViewSql` UNION-ALL-s base-table staging deltas with
        // the upstream MV's persisted view-delta (via the `MvViewDelta` opType branch). The TPC-DI
        // parity suite verifies bag-equality across batch-1/2/3 for 16 MVs that were previously
        // demoted by these two guards.
      }
    val nonCascadeUpstreamReason: String =
      nonCascadeUpstreams
        .groupBy(_._2)
        .toSeq
        .sortBy(_._1)
        .map { case (reason, entries) => s"${reason}:${entries.map(_._1).mkString(",")}" }
        .mkString(";")
    // ── Effective refresh-type classification with structured logging ──────
    //
    // Every demotion of `compiled.refreshType` to FULL_REFRESH below is
    // surfaced via [[logError]] with a `reason=<key>` tag so the operator can
    // see WHY each MV ended up FULL_REFRESH in the spark-ext / dbt-server
    // container log. The reason keys mirror the if-else branches:
    //   - top_k                       Top-K view (ORDER BY ... LIMIT ...) forced to FULL_REFRESH
    //   - simple_projection_no_apply  compiler emitted a SIMPLE_PROJECTION delta
    //                                 feed but no data-table apply statement after
    //                                 rewrite, so REFRESH would be a no-op
    //   - non_cascade_upstream        Upstream MV instance either does NOT emit a
    //                                 persisted `openivm_delta_<view>` downstream can
    //                                 consume incrementally. Specifically, an upstream MV
    //                                 currently classified as FULL_REFRESH is the only
    //                                 documented trigger today; the interpolated reason is
    //                                 `non_cascade:<upstream>`.
    //   - window_initial_load_mismatch translated WINDOW_PARTITION initial-load SQL
    //                                 is not bag-equal to the user query on current
    //                                 data, so the MV is demoted to FULL_REFRESH
    //                                 for correctness (e.g. ignoreNulls semantics)
    //   - window_partition_kept       compiler classified WINDOW_PARTITION; kept
    //                                 (primary refresh is still partition recompute,
    //                                 now with an auxiliary cascade view-delta)
    //   - group_recompute_kept        compiler classified GROUP_RECOMPUTE; kept
    //                                 (affected-group recompute plus auxiliary
    //                                 cascade view-delta)
    //   - having_pred_empty           AggregateHaving with no extractable HAVING predicate
    //   - no_real_delta               openivm emitted only empty-placeholder delta
    //   - kept                        compiled type is preserved verbatim (incremental MV-over-MV
    //                                 case included — upstream is cascade-delta-capable)
    //
    // INFO-level for "kept" so the per-MV decision is always visible during
    // normal operation; ERROR-level for any demotion so even
    // production-tuned log filters surface it.
    val (effectiveRefreshType, classifyReason) = {
      if (isTopKView) (RefreshTypeCode.FullRefresh, "top_k")
      else if (!simpleProjectionHasDataApply)
        (RefreshTypeCode.FullRefresh, "simple_projection_no_apply")
      else if (nonCascadeUpstreams.nonEmpty)
        (RefreshTypeCode.FullRefresh, s"non_cascade_upstream:${nonCascadeUpstreamReason}")
      else if (compiled.refreshType == RefreshTypeCode.WindowPartition && !windowInitialLoadMatchesUserQuery)
        (RefreshTypeCode.FullRefresh, "window_initial_load_mismatch")
      else if (compiled.refreshType == RefreshTypeCode.WindowPartition)
        (compiled.refreshType, "window_partition_kept")
      else if (compiled.refreshType == RefreshTypeCode.GroupRecompute)
        (compiled.refreshType, "group_recompute_kept")
      else if (compiled.refreshType == RefreshTypeCode.AggregateHaving && rawHavingPred.isEmpty)
        (RefreshTypeCode.FullRefresh, "having_pred_empty")
      else if (
        compiled.refreshType == RefreshTypeCode.AggregateHaving && rawHavingPred
          .exists(pred => aggregateHavingDataColumns.forall(cols => !havingPredicateIsSafe(pred, cols)))
      )
        (RefreshTypeCode.FullRefresh, "having_pred_hidden_agg")
      else if (!SparkRefreshRewriter.hasRealDelta(compiled.sql, name.table))
        (RefreshTypeCode.FullRefresh, "no_real_delta")
      else (compiled.refreshType, "kept")
    }
    // `sourceIsMv` is computed for logging visibility only; the demotion is
    // now driven by `nonCascadeUpstreams.nonEmpty` (see above).
    val _ = (sourceIsMv, distinctUpstreamMvCount)
    val effectiveRefreshTypeName =
      if (effectiveRefreshType == RefreshTypeCode.FullRefresh) "FULL_REFRESH"
      else compiled.refreshTypeName
    val emitsCascadeViewDelta =
      RefreshTypeCode.emitsCascadeViewDelta(effectiveRefreshType) &&
        SparkRefreshRewriter.hasRealDelta(compiled.sql, name.table)

    {
      val msg =
        s"[openivm-mv] view='${sqlIdent(name)}' compiled_refresh_type='${compiled.refreshTypeName}' " +
          s"effective_refresh_type='$effectiveRefreshTypeName' reason='$classifyReason' " +
          s"emits_cascade_view_delta='$emitsCascadeViewDelta'"
      if (effectiveRefreshType == RefreshTypeCode.FullRefresh && compiled.refreshType != RefreshTypeCode.FullRefresh)
        logError(msg)
      else
        logInfo(msg)
    }

    // AGGREGATE_HAVING split: the data table stores ALL groups (so a group
    // whose aggregate later crosses the threshold can be re-promoted by an
    // incremental MERGE), and the user-facing object is a Spark VIEW that
    // applies the HAVING predicate at read time. Mirrors openivm's
    // `openivm_data_<v>` + user-facing VIEW pattern in `CompileAggregateGroups`.
    val isHavingViewIncremental = effectiveRefreshType == RefreshTypeCode.AggregateHaving
    val dataIdent: TableIdentifier =
      if (isHavingViewIncremental) dataTableId(name) else name
    val havingPred: Option[String] = if (isHavingViewIncremental) rawHavingPred else None
    val userOutputCols: Seq[String] =
      if (isHavingViewIncremental) analyzed.output.map(_.name) else Nil

    // Persist internal metadata alongside any user-provided properties
    val baseProps         = Map("_ivm_group_keys" -> groupKeys.mkString(","))
    val countProp         = countStarAlias.map(a => "_ivm_count_col" -> a).toMap
    val havingProp        = havingPred.map(p => "_ivm_having_pred" -> p).toMap
    val cascadeDeltaProps = MvMetadata.cascadeViewDeltaProperties(emitsCascadeViewDelta)
    // Persist the raw DuckDB-CLI compile result so REFRESH can skip the
    // subprocess fork + extension load + binder (≈2-5s per call) on every
    // incremental refresh. Suppressed for FULL_REFRESH (no incremental SQL
    // is used) and for compile_failed views (compiled.sql is empty).
    val compiledProps =
      if (effectiveRefreshType == RefreshTypeCode.FullRefresh) Map.empty[String, String]
      else MvMetadata.compiledProperties(compiled.sql, compiled.initialLoadSql)
    // Capture per-source watermarks BEFORE the MV's initial CTAS so the first
    // REFRESH ignores any staging rows that pre-date this MV (otherwise we'd
    // double-apply upstream view-deltas this MV already absorbed via the CTAS).
    // See `StagingCatalog.currentWatermarks` + `StagingCatalog.collectFor`.
    val watermarks     = StagingCatalog.currentWatermarks(spark, qualNames)
    val watermarkProps = MvMetadata.watermarkProperties(watermarks)
    val allProps =
      properties ++ baseProps ++ countProp ++ havingProp ++ cascadeDeltaProps ++
        compiledProps ++ watermarkProps
    val now = new Timestamp(System.currentTimeMillis())

    // Fingerprint the current source schemas + every upstream MV's identity
    // hash. Captures schema drift AND upstream-body drift (DROP + recreate
    // with same schema but different body).
    val mvIdentityBySource: Map[String, String] =
      upstreamMvByQual.map { case (qn, m) => qn -> MvCatalog.mvIdentity(m) }
    val fingerprint = MvCatalog.schemaFingerprint(qualSchemas, mvIdentityBySource)

    val meta = MvMetadata(
      name = name,
      querySql = originalQueryText,
      refreshType = effectiveRefreshType,
      refreshTypeName = effectiveRefreshTypeName,
      lastVersion = -1L,
      sourceTables = qualNames,
      sourceSchemaFingerprint = fingerprint,
      location = location,
      createdAt = now,
      properties = allProps
    )

    // Materialize the MV with an initial full-load CREATE TABLE AS SELECT.
    // For FULL_REFRESH (type 3) always use the original user query: the
    // openivm-emitted LPTS SQL may contain DuckDB-specific syntax (e.g. SEMI JOIN
    // instead of LEFT SEMI JOIN, or correlated subquery rewrites) that Spark
    // cannot parse, and FULL_REFRESH data tables carry no hidden bookkeeping
    // columns — the FullRefreshAssembler already re-executes the original query
    // at refresh time via INSERT OVERWRITE.
    // For incremental types (0, 1, 2, …), prefer the LPTS SQL when available
    // because it includes openivm_count_star and other hidden columns required
    // by the incremental MERGE program.
    val viewBodySql =
      if (effectiveRefreshType == RefreshTypeCode.FullRefresh || compiled.initialLoadSql.isEmpty)
        originalQueryText
      else
        org.openivm.spark.compiler.LptsSparkDialect.translate(compiled.initialLoadSql)

    val escaped  = location.replace("'", "\\'")
    val tblProps = FeatureGate.buildMvDataTblProperties(spark)
    val tblPropsClause =
      if (tblProps.nonEmpty) s"TBLPROPERTIES (${tblProps.mkString(", ")}) " else ""
    val initSql =
      s"CREATE TABLE IF NOT EXISTS ${sqlIdent(dataIdent)} USING DELTA " +
        s"${tblPropsClause}LOCATION '$escaped' AS $viewBodySql"
    // Wipe stray files from a previous aborted CREATE so Delta's
    // "non-empty location, not a Delta table" check does not fail dbt
    // retries after an OOM-aborted initial load (see exp-000 SF=100
    // forensics — trades_history failed 24× over 87 minutes because the
    // location had non-Delta Parquet from a prior partial write).
    cleanupStaleMvLocation(spark, location)
    IvmDmlInterceptorRule.bypass.set(true)
    try {
      profile.timeStep(
        "create_mv_initial_load",
        s"refresh_type=${compiled.refreshTypeName};init_sql_bytes=${initSql.length}"
      ) {
        val t0 = System.nanoTime()
        try {
          spark.sql(initSql)
        } finally {
          val ms = (System.nanoTime() - t0) / 1000000L
          sqlLog.record(
            category = "initial_load_ctas",
            stmtOrder = 0,
            attemptIdx = 0,
            stmtKind = RefreshPerf.classify(initSql, ""),
            sql = initSql,
            durationMs = ms
          )
        }
      }

      // For AGGREGATE_HAVING we additionally create the user-facing Spark VIEW
      // that projects only the user columns from the data table and applies the
      // HAVING predicate. The post-pass DELETE during refresh removes zero-count
      // rows from the data table so this VIEW does not need a separate
      // `openivm_count_star > 0` guard.
      if (isHavingViewIncremental) {
        profile.timeStep("create_mv_user_view", "kind=aggregate_having") {
          val dataCols = spark.table(sqlIdent(dataIdent)).schema.fieldNames.toSet
          val pred     = havingPred.getOrElse("TRUE")
          if (!havingPredicateIsSafe(pred, dataCols)) {
            throw new RuntimeException(
              s"HAVING predicate '$pred' references columns not present on data table " +
                s"${sqlIdent(dataIdent)} (columns: ${dataCols.mkString(", ")}). " +
                "This indicates the HAVING clause uses an aggregate not in SELECT; " +
                "such views are not currently supported in the incremental AGGREGATE_HAVING " +
                "path — they will be retried as FULL_REFRESH on the next CREATE."
            )
          }
          val colList = userOutputCols
            .map(c => s"`${c.replace("`", "``")}`")
            .mkString(", ")
          val viewSql =
            s"CREATE OR REPLACE VIEW ${sqlIdent(name)} AS " +
              s"SELECT $colList FROM ${sqlIdent(dataIdent)} WHERE ($pred)"
          val t0 = System.nanoTime()
          try {
            spark.sql(viewSql)
          } finally {
            val ms = (System.nanoTime() - t0) / 1000000L
            sqlLog.record(
              category = "aggregate_having_view",
              stmtOrder = 1,
              attemptIdx = 0,
              stmtKind = "ddl",
              sql = viewSql,
              durationMs = ms
            )
          }
        }
      }

      profile.timeStep("create_view_index", s"sources=${meta.sourceTables.size}") {
        // Write metadata catalog entry
        MvCatalog.upsert(spark, meta)
      }

      profile.timeStep("create_mv_publish_metadata", "phase=advance_version") {
        // Record the Delta version of the initial snapshot
        val version =
          DeltaTable.forPath(spark, location).history(1).collect().head.getAs[Long]("version")
        MvCatalog.advance(spark, name, version)
      }
    } finally {
      IvmDmlInterceptorRule.bypass.set(false)
    }

    Seq.empty
  }
}

// ---------------------------------------------------------------------------
// RefreshMaterializedViewCommand
// ---------------------------------------------------------------------------

case class RefreshMaterializedViewCommand(
    name: TableIdentifier
) extends LeafRunnableCommand {

  override def run(spark: SparkSession): Seq[Row] = {
    import MvCommandHelper._

    // Serialize concurrent REFRESHes against the same MV — see comment on
    // RefreshMutex above. Without this, two threads that both read the
    // same unconsumed staging-delta snapshot each apply it once, doubling
    // count-monoid aggregates.
    val lockT0 = System.nanoTime()
    RefreshMutex.withLock(metaName(name)) {
      val lockAcqMs = (System.nanoTime() - lockT0) / 1000000L
      // Clone the SparkSession so every refresh gets its own temp-view
      // namespace.  Concurrent refresh waves (e.g. TpcDiSpec's
      // `runWaveParallel`) routinely register session-global
      // `openivm_delta_<source>` temp views; without isolation, sibling MVs
      // racing through CREATE OR REPLACE TEMP VIEW + DROP VIEW can yank or
      // replace a temp view mid-MERGE in another refresh, surfacing as
      // `[TABLE_OR_VIEW_NOT_FOUND] openivm_delta_<source>` or returning
      // rows from the wrong refresh's deltas.  `cloneSession` copies
      // SessionState (temp catalog, SQLConf, registered extensions) but
      // shares SparkContext and table cache, so the cost is microseconds
      // per refresh.
      runUnderLock(org.apache.spark.sql.openivm.SparkSessionAccess.cloneSession(spark), lockAcqMs)
    }
  }

  private def runUnderLock(spark: SparkSession, lockAcqMs: Long): Seq[Row] = {
    import MvCommandHelper._
    import org.openivm.spark.compiler.LptsSparkDialect

    val refreshT0  = System.nanoTime()
    val viewLabel  = sqlIdent(name)
    val viewMeta   = metaName(name)
    val threadName = Thread.currentThread().getName
    val profile    = RefreshProfile.start(spark, viewMeta, RefreshProfile.Mode.Refresh)
    val refreshId  = profile.refreshId
    val sqlLog     = RefreshSqlLog.start(spark, refreshId, viewMeta, RefreshSqlLog.ModeRefresh)
    // A query-log-only monotonic counter. Independent of the perf
    // `stmtCounter` so source-delta registrations, count-monoid cleanup,
    // post-cleanup, and drop-cleanup get clean sequential row ids.
    val qlogOrder = new java.util.concurrent.atomic.AtomicInteger(0)
    profile.appendStep("acquire_locks", s"thread=$threadName", lockAcqMs)
    RefreshPerf.emit(refreshId, viewLabel, "start", s"thread='$threadName'")

    def emitEnd(outcome: String, refreshTypeName: String, pendingDeltas: Int): Unit = {
      val totalMs = (System.nanoTime() - refreshT0) / 1000000L
      RefreshPerf.emit(
        refreshId,
        viewLabel,
        "end",
        s"total_ms=$totalMs refresh_type='$refreshTypeName' " +
          s"outcome='$outcome' pending_deltas=$pendingDeltas"
      )
      profile.appendStep(
        "total_refresh",
        s"refresh_type=$refreshTypeName;outcome=$outcome;pending_deltas=$pendingDeltas",
        totalMs
      )
      profile.flush()
      sqlLog.flush()
    }

    val meta = MvCatalog
      .lookup(spark, name)
      .getOrElse {
        emitEnd("metadata_not_found", "UNKNOWN", 0)
        throw new AnalysisException(
          "TABLE_OR_VIEW_NOT_FOUND",
          Map("relationName" -> sqlIdent(name))
        )
      }

    val viewNameStr      = metaName(name)
    val sourceWatermarks = meta.sourceWatermarks

    // Phase D: memoize MvCatalog.list within this refresh.  The catalog is
    // read in three places (schema_resolve, hasNoDownstreamConsumer probe,
    // and record_cascade trigger-key resolution).  A RocksDB prefix scan
    // each time costs 5-30 ms × 3 across heavy refresh programs.  Read once
    // and reuse.  Defensive try/catch matches the original call sites.
    lazy val allMvsCached: Seq[MvMetadata] =
      try MvCatalog.list(spark)
      catch { case _: Throwable => Seq.empty[MvMetadata] }

    if (
      meta.refreshType != RefreshTypeCode.FullRefresh &&
      !StagingCatalog.hasPendingDeltas(spark, viewNameStr, meta.sourceTables, sourceWatermarks)
    ) {
      logInfo(
        s"[openivm-mv] refresh view='${sqlIdent(name)}' refresh_type='${meta.refreshTypeName}' " +
          "outcome='no_pending_deltas'"
      )
      emitEnd("no_pending_deltas", meta.refreshTypeName, 0)
      return Seq.empty
    }

    val stagingDeltas = profile.timeStep("metadata_pre_sql", "phase=collect_staging") {
      RefreshPerf.timePhase(refreshId, viewLabel, "collect_staging") {
        StagingCatalog.collectFor(
          spark,
          viewNameStr,
          meta.sourceTables,
          sourceWatermarks
        )
      }
    }

    // Defensive backstop: the cheap existence probe above and the full collect
    // can diverge if another refresh consumes the same rows before we collect.
    if (meta.refreshType != RefreshTypeCode.FullRefresh && stagingDeltas.isEmpty) {
      logInfo(
        s"[openivm-mv] refresh view='${sqlIdent(name)}' refresh_type='${meta.refreshTypeName}' " +
          "outcome='no_pending_deltas'"
      )
      emitEnd("no_pending_deltas", meta.refreshTypeName, 0)
      return Seq.empty
    }

    // Once we know we'll be doing real work, record the user-supplied CREATE-MV
    // body as the leading row of this refresh's query log. Not executed by us
    // (stmt_order = -1, duration_ms = -1) but invaluable for the benchmarker
    // because it pins "what the user asked for" alongside "what we actually ran"
    // in a single refresh-id folder.
    sqlLog.record(
      category = "original_query",
      stmtOrder = -1,
      attemptIdx = 0,
      stmtKind = "select",
      sql = meta.querySql,
      durationMs = -1L
    )

    logInfo(
      s"[openivm-mv] refresh view='${sqlIdent(name)}' refresh_type='${meta.refreshTypeName}' " +
        s"pending_deltas=${stagingDeltas.size} source_tables=${meta.sourceTables.mkString(",")}"
    )
    RefreshPerf.emit(
      refreshId,
      viewLabel,
      "deltas_resolved",
      s"refresh_type='${meta.refreshTypeName}' pending_deltas=${stagingDeltas.size}"
    )
    profile.appendStep(
      "generate_refresh_sql.dispatch",
      s"refresh_type=${meta.refreshTypeName};pending_deltas=${stagingDeltas.size}",
      0L
    )

    // Resolve current source schemas and check for schema drift. Include
    // upstream MV identity hashes so a DROP + recreate-with-same-schema
    // upstream is also caught.
    val (freshSchemas, freshMvIdentityBySource, freshFingerprint) =
      profile.timeStep("metadata_pre_sql", "phase=schema_resolve") {
        RefreshPerf.timePhase(refreshId, viewLabel, "schema_resolve") {
          val schemas = meta.sourceTables.map(t => t -> spark.table(t).schema).toMap
          val identityMap: Map[String, String] = {
            val all                             = allMvsCached
            val byMeta: Map[String, MvMetadata] = all.map(m => metaName(m.name) -> m).toMap
            meta.sourceTables.flatMap { qn =>
              val short = qn.split("\\.").last
              byMeta.get(qn).orElse(byMeta.get(short)).map(m => qn -> MvCatalog.mvIdentity(m))
            }.toMap
          }
          val fp = MvCatalog.schemaFingerprint(schemas, identityMap)
          (schemas, identityMap, fp)
        }
      }
    if (freshFingerprint != meta.sourceSchemaFingerprint) {
      emitEnd("schema_drift", meta.refreshTypeName, stagingDeltas.size)
      throw new AnalysisException(
        "INCOMPATIBLE_VIEW_SCHEMA_CHANGE",
        Map(
          "viewName"    -> sqlIdent(name),
          "colName"     -> "source schema fingerprint",
          "expectedNum" -> meta.sourceSchemaFingerprint,
          "actualCols"  -> freshFingerprint,
          "suggestion"  -> "DROP and recreate the materialized view"
        )
      )
    }
    // Reference the identity map so the unused-binding inference doesn't trip
    // the -Ywarn-unused:imports compile flag (the value is intentionally kept
    // local — debugging will want it).
    val _ = freshMvIdentityBySource

    // -----------------------------------------------------------------------
    // FullRefresh path — recompute INSERT OVERWRITE from the live tables.
    // -----------------------------------------------------------------------
    if (meta.refreshType == RefreshTypeCode.FullRefresh) {
      val input = AssemblyInput(
        refreshType = RefreshTypeCode.FullRefresh,
        refreshTypeName = "FULL_REFRESH",
        deltaSql = meta.querySql,
        mvName = metaName(name),
        mvLocation = meta.location
      )
      val assembled = SparkMergeAssembler.assemble(input)
      IvmDmlInterceptorRule.bypass.set(true)
      var stmtCounter = 0
      try {
        assembled.statements.foreach { sql =>
          val kind     = RefreshPerf.classify(sql, "")
          val sqlBytes = sql.length
          val qOrder   = qlogOrder.getAndIncrement()
          profile.timeStep(
            "execute_refresh_sql_stmt",
            s"statement=${stmtCounter + 1}/${assembled.statements.size};bytes=$sqlBytes;stmt_kind=$kind"
          ) {
            RefreshPerf.timeStmt(refreshId, viewLabel, stmtCounter, kind) {
              RetryPolicy.DeltaConflicts.executeWithAttempt { attempt =>
                val t0 = System.nanoTime()
                try {
                  val r  = spark.sql(sql).collect()
                  val ms = (System.nanoTime() - t0) / 1000000L
                  sqlLog.record("full_refresh_stmt", qOrder, attempt - 1, kind, sql, ms)
                  r
                } catch {
                  case t: Throwable =>
                    val ms = (System.nanoTime() - t0) / 1000000L
                    sqlLog.record("full_refresh_stmt", qOrder, attempt - 1, kind, sql, ms)
                    throw t
                }
              }
            }
          }
          stmtCounter += 1
        }
        profile.timeStep("metadata_post_sql", "phase=post_cleanup") {
          RefreshPerf.timePhase(refreshId, viewLabel, "post_cleanup") {
            postRefreshCleanup(spark, name, meta, stagingDeltas, viewNameStr, sqlLog, qlogOrder)
          }
        }
        emitEnd("full_refresh_executed", "FULL_REFRESH", stagingDeltas.size)
      } catch {
        case t: Throwable =>
          emitEnd("full_refresh_failed", "FULL_REFRESH", stagingDeltas.size)
          val sqlSnippet = assembled.statements.mkString(";\n---\n")
          throw new RuntimeException(
            s"Full refresh of '${sqlIdent(name)}' failed: ${t.getMessage}\nAssembled SQL:\n$sqlSnippet",
            t
          )
      } finally {
        IvmDmlInterceptorRule.bypass.set(false)
      }
      return Seq.empty
    }

    // -----------------------------------------------------------------------
    // Incremental path — recompile, register source temp views, rewrite the
    // openivm-emitted multi-statement program, execute it.
    // -----------------------------------------------------------------------
    val compileSchemas: Map[String, StructType] = freshSchemas.map { case (q, schema) =>
      q.split("\\.").last -> schema
    }
    val shortToQual: Map[String, String] = freshSchemas.map { case (q, _) =>
      q.split("\\.").last -> q
    }
    // Reuse the cached compile result if CREATE persisted it. The source
    // schema fingerprint above (lines 728-738) has already guaranteed the
    // cache is still valid for this REFRESH — schema drift triggers
    // INCOMPATIBLE_VIEW_SCHEMA_CHANGE and never reaches here.
    //
    // Falls back to invoking the DuckDB CLI on a cache miss (legacy MVs
    // created before this caching was added) and back-fills the metadata
    // so subsequent REFRESHes skip the subprocess. The back-fill uses
    // `MvCatalog.updateProperties` — a property-only update that preserves
    // the row's `last_version`, unlike a full `upsert(meta.copy(...))`
    // which would race with the end-of-refresh `advance` call.
    val cachedCompiledSql = meta.properties.get(MvMetadata.CompiledSqlKey).filter(_.nonEmpty)
    val cachedInitialLoadSql =
      meta.properties.get(MvMetadata.CompiledInitialLoadSqlKey).getOrElse("")
    val compileCacheHit = cachedCompiledSql.isDefined
    val compiled = profile.timeStep(
      "generate_refresh_sql.compile",
      s"compile_cache_hit=$compileCacheHit"
    ) {
      RefreshPerf.timePhase(
        refreshId,
        viewLabel,
        "compile",
        s"compile_cache_hit=$compileCacheHit"
      ) {
        cachedCompiledSql match {
          case Some(sql) =>
            org.openivm.spark.compiler.CompiledRefresh(
              refreshType = meta.refreshType,
              refreshTypeName = meta.refreshTypeName,
              sql = sql,
              initialLoadSql = cachedInitialLoadSql
            )
          case None =>
            val compiler = OpenIvmCompilers.forSession(spark)
            val fresh = compiler.compile(
              CompileRequest(
                viewName = name.table,
                viewSql = meta.querySql,
                sources = compileSchemas,
                sourceQualifiedNames = shortToQual
              )
            )
            if (fresh.sql.nonEmpty) {
              val backfilled = meta.properties ++
                MvMetadata.compiledProperties(fresh.sql, fresh.initialLoadSql)
              try MvCatalog.updateProperties(spark, name, backfilled)
              catch {
                case t: Throwable =>
                  logWarning(
                    s"[openivm-mv] refresh view='${sqlIdent(name)}' compile_cache_backfill_failed: " +
                      s"${t.getClass.getName}: ${t.getMessage}"
                  )
              }
            }
            fresh
        }
      }
    }

    // Per-refresh view-delta path under a STABLE per-MV namespace
    // (`<warehouse>/_ivm/view_deltas/<safe-qualified-mv-name>/<txn-ts-uuid>`).
    // The path is uniquely named per refresh, but the parent directory is
    // shared across all of this MV's refreshes — so the Phase-7 orphan sweep
    // and the MV's DROP path can clean up the entire namespace at once.
    //
    // The fully-qualified MV name (db.table) is used (not just the short
    // name) so two MVs with the same short name in different databases
    // don't collide on disk.
    val warehouse     = spark.conf.get("spark.sql.warehouse.dir").stripSuffix("/")
    val safeMvName    = metaName(name).replace(".", "_").replace(" ", "_")
    val viewDeltaPath = s"$warehouse/_ivm/view_deltas/$safeMvName/${java.util.UUID.randomUUID()}"

    val byTable                          = stagingDeltas.groupBy(_.baseTable)
    val tempViewShortNames               = scala.collection.mutable.ArrayBuffer[String]()
    var fusedScratchView: Option[String] = None

    IvmDmlInterceptorRule.bypass.set(true)
    try {
      // Register a delta temp view for every source table.  Tables that have
      // pending staging deltas get a real view; tables with no pending deltas
      // get an empty view so that multi-source compiled SQL (e.g. UNION DISTINCT
      // across two tables) can reference all delta views without a NOT_FOUND error.
      profile.timeStep("metadata_pre_sql", "phase=register_views") {
        RefreshPerf.timePhase(refreshId, viewLabel, "register_views") {
          for (qualTable <- meta.sourceTables) {
            val schema      = freshSchemas(qualTable)
            val tableDeltas = byTable.getOrElse(qualTable, Seq.empty)
            val viewSql     = StagingDeltaView.buildSourceDeltaViewSql(qualTable, schema, tableDeltas)
            val t0          = System.nanoTime()
            try {
              spark.sql(viewSql)
            } finally {
              val ms = (System.nanoTime() - t0) / 1000000L
              sqlLog.record(
                category = "register_source_delta",
                stmtOrder = qlogOrder.getAndIncrement(),
                attemptIdx = 0,
                stmtKind = "temp_view",
                sql = viewSql,
                durationMs = ms
              )
            }
            tempViewShortNames += qualTable.split("\\.").last

            if (diagnosticsEnabled) {
              val short = qualTable.split("\\.").last
              try {
                val counts = spark
                  .sql(s"""SELECT
                        |  COUNT(*) AS total,
                        |  COUNT(CASE WHEN `openivm_multiplicity` > 0 THEN 1 END) AS pos,
                        |  COUNT(CASE WHEN `openivm_multiplicity` < 0 THEN 1 END) AS neg
                        |FROM `openivm_delta_$short`""".stripMargin)
                  .head()
                logInfo(
                  s"[openivm-mv-diag] refresh view='${sqlIdent(name)}' source_delta source='$qualTable' " +
                    s"deltas=${tableDeltas.size} total=${counts.getLong(0)} " +
                    s"pos=${counts.getLong(1)} neg=${counts.getLong(2)}"
                )
              } catch {
                case t: Throwable =>
                  logInfo(
                    s"[openivm-mv-diag] refresh view='${sqlIdent(name)}' source_delta source='$qualTable' " +
                      s"error='${t.getClass.getSimpleName}: ${t.getMessage}'"
                  )
              }
            }
          }
        }
      }

      // For AGGREGATE_HAVING the user-facing object is a Spark VIEW; the actual
      // Delta data lives in a sibling table that stores ALL groups (no HAVING
      // filter). Redirect MERGE/DELETE statements to the sibling table so a
      // group whose aggregate later crosses the threshold can be re-promoted
      // back into the HAVING-passing set incrementally.
      val mergeTargetId: TableIdentifier =
        if (meta.refreshType == RefreshTypeCode.AggregateHaving) dataTableId(name)
        else name

      val rewritten = profile.timeStep(
        "generate_refresh_sql.assembly",
        s"compiled_sql_bytes=${compiled.sql.length}"
      ) {
        RefreshPerf.timePhase(refreshId, viewLabel, "rewrite") {
          SparkRefreshRewriter.rewrite(
            compiledSql = compiled.sql,
            mvName = mergeTargetId,
            mvLocation = meta.location,
            viewLogicalName = name.table,
            sourceTempViews = tempViewShortNames.map(n => n -> s"openivm_delta_$n").toMap,
            viewDeltaPath = viewDeltaPath,
            postProcess = LptsSparkDialect.translate,
            // Pass the user-facing column list for each source so the rewriter can
            // expand DuckDB-style `SELECT * EXCEPT (openivm_multiplicity, openivm_timestamp)`
            // into an explicit column list (Spark 3.5 does not support that syntax).
            sourceSchemas = freshSchemas.map { case (qual, schema) =>
              qual.split("\\.").last -> schema.fieldNames.toSeq
            },
            // Pass the short → qualified source name map so the rewriter can
            // expand `memory.main.<short>` to the fully-qualified Spark name
            // when the user's view body referenced a Hive-qualified table.
            // Live-source refs would otherwise hit DELTA_TABLE_NOT_FOUND because
            // Spark would resolve `<short>` against the current_schema.
            sourceQualifiedNames = shortToQual,
            mvVersionBeforeRefresh = Some(meta.lastVersion)
          )
        }
      }

      var cleanupMeta           = meta
      var spFullRefreshFallback = false
      def deletePathIfExists(pathStr: String): Unit =
        try {
          val hadoopPath = new Path(pathStr)
          val fs         = hadoopPath.getFileSystem(spark.sessionState.newHadoopConf())
          if (fs.exists(hadoopPath)) fs.delete(hadoopPath, /* recursive = */ true)
        } catch { case _: Throwable => () }

      // Eligibility for the scratch-CTAS fuse fast path: SIMPLE_PROJECTION
      // MVs whose short name does not appear in any other MV's `sourceTables`
      // (i.e. they have no current downstream consumer) write the per-refresh
      // view-delta to a Delta scratch table that is then consumed exactly
      // once by the INSERT INTO mv_data (and, in the negative-row case, the
      // value-equality DELETE MERGE). Materialising that scratch as a
      // cached DataFrame + temp view skips the per-table Delta commit
      // overhead and the second on-disk read.
      def hasNoDownstreamConsumer: Boolean = {
        val mvShortName = name.identifier
        !allMvsCached
          .exists(other =>
            metaName(other.name) != metaName(name) &&
              other.sourceTables.exists(_.split("\\.").last == mvShortName)
          )
      }

      val fuseEligible =
        FeatureGate.fuseScratchEnabled(spark) &&
          meta.refreshType == RefreshTypeCode.SimpleProjection &&
          rewritten.statements.nonEmpty &&
          hasNoDownstreamConsumer

      try {
        lazy val hasSimpleProjectionDeletes = hasNegativeSimpleProjectionRows(spark, viewDeltaPath)

        // Log the rewritten SQL at DEBUG so cascade-related issues are
        // observable when -Dlog4j2.logger.org.openivm.spark.commands=DEBUG
        // is set, without polluting the default INFO output.
        rewritten.statements.zipWithIndex.foreach { case (stmt, i) =>
          val sql = SparkRefreshRewriter.stripExecutionMarker(stmt)
          val limit =
            try {
              sys.env
                .get("OPENIVM_LOG_SQL_LIMIT")
                .orElse(sys.props.get("openivm.log.sql.limit"))
                .map(_.toInt)
                .getOrElse(4000)
            } catch { case _: Throwable => 4000 }
          logInfo(
            s"[openivm-mv] refresh view='${sqlIdent(name)}' stmt[$i]=" +
              sql.replace('\n', ' ').take(limit)
          )
        }
        // Wraps every spark.sql(...).collect() under DeltaConflict retry, AND
        // emits an `[openivm-perf] phase='stmt'` line with a kind classifier
        // plus elapsed_ms so a parser can attribute time to view-delta CTAS /
        // MERGE / DELETE / INSERT OVERWRITE / etc. The stmt_idx is monotonic
        // across all statements executed by this refresh (rewritten + any
        // fallback + count-monoid cleanup).
        val stmtCounter = new java.util.concurrent.atomic.AtomicInteger(0)
        def advanceStmtCounterPast(stmtIdx: Int): Unit = {
          var done = false
          while (!done) {
            val current = stmtCounter.get()
            done = current > stmtIdx || stmtCounter.compareAndSet(current, stmtIdx + 1)
          }
        }
        def executeSqlAt(sql: String, stmtIdx: Int): Unit = {
          advanceStmtCounterPast(stmtIdx)
          val kind     = RefreshPerf.classify(sql, viewDeltaPath)
          val sqlBytes = sql.length
          val qOrder   = qlogOrder.getAndIncrement()
          profile.timeStep(
            "execute_refresh_sql_stmt",
            s"statement=${stmtIdx + 1};bytes=$sqlBytes;stmt_kind=$kind"
          ) {
            RefreshPerf.timeStmt(refreshId, viewLabel, stmtIdx, kind) {
              RetryPolicy.DeltaConflicts.executeWithAttempt { attempt =>
                val t0 = System.nanoTime()
                try {
                  val r  = spark.sql(sql).collect()
                  val ms = (System.nanoTime() - t0) / 1000000L
                  sqlLog.record("rewritten_stmt", qOrder, attempt - 1, kind, sql, ms)
                  r
                } catch {
                  case t: Throwable =>
                    val ms = (System.nanoTime() - t0) / 1000000L
                    sqlLog.record("rewritten_stmt", qOrder, attempt - 1, kind, sql, ms)
                    throw t
                }
              }
            }
          }
        }
        def executeSql(sql: String): Unit = {
          val idx = stmtCounter.getAndIncrement()
          executeSqlAt(sql, idx)
        }
        def logSkippedDeleteMerge(stmtIdx: Int): Unit = {
          advanceStmtCounterPast(stmtIdx)
          RefreshPerf.logStmt(refreshId, viewLabel, stmtIdx, "merge_skipped", 0L)
          profile.appendStep(
            "execute_refresh_sql_stmt",
            s"statement=${stmtIdx + 1};stmt_kind=merge_skipped",
            0L
          )
        }

        if (meta.refreshType == RefreshTypeCode.SimpleProjection && rewritten.statements.nonEmpty) {
          // ── Scratch-CTAS fuse fast path ────────────────────────────────────
          //
          // openivm emits stmt[0] as `CREATE OR REPLACE TABLE delta.\`<path>\`
          // USING DELTA AS WITH … SELECT … openivm_multiplicity FROM …` and
          // stmt[1] as `INSERT INTO mv SELECT … FROM delta.\`<path>\` …`
          // (the value-equality DELETE MERGE is stmt[2] when negatives exist).
          //
          // For leaf MVs (no downstream consumer), the scratch is consumed
          // exactly once or twice on-disk. Materialising it as a cached
          // DataFrame + temp view skips the per-table Delta commit overhead
          // AND keeps subsequent reads in-memory. The cascade record block
          // is skipped because there is no on-disk path to record — safe
          // because a downstream MV created later does its own initial CTAS.
          val fusedView: Option[String] =
            if (fuseEligible)
              SparkRefreshRewriter
                .extractViewDeltaCtasBody(
                  SparkRefreshRewriter.stripExecutionMarker(rewritten.statements.head),
                  viewDeltaPath
                )
                .flatMap { selectBody =>
                  val scratchView = s"openivm_scratch_${java.util.UUID.randomUUID().toString.replace("-", "_")}"
                  try {
                    val t0 = System.nanoTime()
                    val df = spark.sql(selectBody)
                    df.cache()
                    df.createOrReplaceTempView(scratchView)
                    // Force materialisation so the cache holds the rows before
                    // any negative-row probe / INSERT read. count() is the
                    // cheapest force-eval action that respects the cache.
                    val rowCount  = df.count()
                    val elapsedMs = (System.nanoTime() - t0) / 1000000L
                    advanceStmtCounterPast(0)
                    RefreshPerf.logStmt(
                      refreshId,
                      viewLabel,
                      0,
                      "view_delta_ctas",
                      elapsedMs,
                      extra = Some(s"fused='true' rows=$rowCount")
                    )
                    profile.appendStep(
                      "execute_refresh_sql_stmt",
                      s"statement=1;stmt_kind=view_delta_ctas;fused=true;rows=$rowCount",
                      elapsedMs
                    )
                    sqlLog.record(
                      category = "fused_view_delta_select",
                      stmtOrder = qlogOrder.getAndIncrement(),
                      attemptIdx = 0,
                      stmtKind = "view_delta_ctas",
                      sql = selectBody,
                      durationMs = elapsedMs
                    )
                    fusedScratchView = Some(scratchView)
                    Some(scratchView)
                  } catch {
                    case t: Throwable =>
                      // Best-effort cleanup and fall through to the on-disk path
                      try spark.catalog.dropTempView(scratchView)
                      catch { case _: Throwable => () }
                      logInfo(
                        s"[openivm-mv] refresh view='${sqlIdent(name)}' fused_fallback='${t.getClass.getSimpleName}: ${t.getMessage}'"
                      )
                      None
                  }
                }
            else None

          // Negative-row + conflict probes operate against either the cached
          // temp view (fuse) or the on-disk scratch (existing path).
          lazy val hasNegativesHere: Boolean = fusedView match {
            case Some(view) =>
              spark
                .sql(
                  s"SELECT 1 FROM `$view` WHERE `openivm_multiplicity` < 0 LIMIT 1"
                )
                .head(1)
                .nonEmpty
            case None => hasSimpleProjectionDeletes
          }

          if (fusedView.isEmpty) {
            executeSqlAt(SparkRefreshRewriter.stripExecutionMarker(rewritten.statements.head), 0)
            logViewDeltaDiagnostics(spark, name, viewDeltaPath, 0)
          }

          val usesValueEqualityDeleteMerge =
            rewritten.statements.exists(SparkRefreshRewriter.isSimpleProjectionDeleteMerge)
          val hasConflictingRows =
            usesValueEqualityDeleteMerge && hasNegativesHere && {
              fusedView match {
                case Some(view) => hasConflictingFusedRows(spark, mergeTargetId, view)
                case None       => hasConflictingSimpleProjectionRows(spark, mergeTargetId, viewDeltaPath)
              }
            }
          if (hasConflictingRows) {
            logInfo(
              s"[openivm-mv] refresh view='${sqlIdent(name)}' " +
                "outcome='simple_projection_full_refresh' reason='conflicting_signed_rows'"
            )
            RefreshPerf.emit(
              refreshId,
              viewLabel,
              "fallback",
              "outcome='simple_projection_full_refresh' reason='conflicting_signed_rows'"
            )
            profile.appendStep(
              "fallback",
              "outcome=simple_projection_full_refresh;reason=conflicting_signed_rows",
              0L
            )
            val fullRefreshMeta = meta.copy(
              refreshType = RefreshTypeCode.FullRefresh,
              refreshTypeName = "FULL_REFRESH",
              properties = meta.properties ++ MvMetadata.cascadeViewDeltaProperties(false)
            )
            val fullRefresh = SparkMergeAssembler.assemble(
              AssemblyInput(
                refreshType = RefreshTypeCode.FullRefresh,
                refreshTypeName = "FULL_REFRESH",
                deltaSql = meta.querySql,
                mvName = metaName(name),
                mvLocation = meta.location
              )
            )
            cleanupMeta = fullRefreshMeta
            spFullRefreshFallback = true
            fullRefresh.statements.foreach(executeSql)
            deletePathIfExists(viewDeltaPath)
          } else {
            rewritten.statements.tail.zipWithIndex.foreach { case (stmt, idx) =>
              val stmtIdx = 1 + idx
              val sql     = SparkRefreshRewriter.stripExecutionMarker(stmt)
              val skipDeleteMerge =
                SparkRefreshRewriter.isSimpleProjectionDeleteMerge(stmt) && !hasNegativesHere

              if (skipDeleteMerge) {
                logInfo(
                  s"[openivm-mv] refresh view='${sqlIdent(name)}' " +
                    "outcome='skip_simple_projection_delete_merge' reason='no_negative_rows'"
                )
                logSkippedDeleteMerge(stmtIdx)
              } else {
                val sqlForExec = fusedView match {
                  case Some(view) =>
                    SparkRefreshRewriter.substituteViewDeltaPath(sql, viewDeltaPath, view)
                  case None => sql
                }
                executeSqlAt(sqlForExec, stmtIdx)
              }
            }
          }
        } else {
          rewritten.statements.zipWithIndex.foreach { case (stmt, idx) =>
            val sql = SparkRefreshRewriter.stripExecutionMarker(stmt)
            val skipDeleteMerge =
              SparkRefreshRewriter.isSimpleProjectionDeleteMerge(stmt) && !hasSimpleProjectionDeletes

            if (skipDeleteMerge) {
              logInfo(
                s"[openivm-mv] refresh view='${sqlIdent(name)}' " +
                  "outcome='skip_simple_projection_delete_merge' reason='no_negative_rows'"
              )
              logSkippedDeleteMerge(idx)
            } else {
              executeSqlAt(sql, idx)
              // After any CTAS that wrote to the view-delta path, log a diagnostic
              // (multiplicity-sign counts + small JSON sample). Cheap: bounded to 8
              // rows. Gated by OPENIVM_REFRESH_DIAGNOSTICS=1.
              if (diagnosticsEnabled && sql.contains(s"`$viewDeltaPath`")) {
                logViewDeltaDiagnostics(spark, name, viewDeltaPath, idx)
              }
            }
          }
        }

        // For count-monoid refresh types, the openivm-emitted MERGE leaves
        // zero-count rows behind when a group retracts to 0. Clean them up
        // so the user-visible MV reflects the live aggregate. The column
        // used as the bookkeeping count is either openivm_count_star
        // (added by openivm when the user query has no COUNT(*)) or the
        // user's COUNT(*) alias (extracted at CREATE time).
        if (isCountMonoid(meta.refreshType)) {
          countMonoidColumn(spark, mergeTargetId, meta).foreach { col =>
            val q         = col.replace("`", "``")
            val deleteSql = s"DELETE FROM ${sqlIdent(mergeTargetId)} WHERE `$q` = 0"
            val idx       = stmtCounter.getAndIncrement()
            val qOrder    = qlogOrder.getAndIncrement()
            profile.timeStep(
              "execute_refresh_sql_stmt",
              s"statement=${idx + 1};bytes=${deleteSql.length};stmt_kind=count_monoid_cleanup"
            ) {
              RefreshPerf.timeStmt(refreshId, viewLabel, idx, "count_monoid_cleanup") {
                RetryPolicy.DeltaConflicts.executeWithAttempt { attempt =>
                  val t0 = System.nanoTime()
                  try {
                    val r  = spark.sql(deleteSql).collect()
                    val ms = (System.nanoTime() - t0) / 1000000L
                    sqlLog.record(
                      "count_monoid_cleanup",
                      qOrder,
                      attempt - 1,
                      "delete",
                      deleteSql,
                      ms
                    )
                    r
                  } catch {
                    case t: Throwable =>
                      val ms = (System.nanoTime() - t0) / 1000000L
                      sqlLog.record(
                        "count_monoid_cleanup",
                        qOrder,
                        attempt - 1,
                        "delete",
                        deleteSql,
                        ms
                      )
                      throw t
                  }
                }
              }
            }
          }
        }

        // MV-over-MV cascade: persist this MV's view-delta as a
        // `StagingDelta` row so any downstream MV's next REFRESH consumes it.
        // Only refresh types that actually emit `INSERT INTO openivm_delta_<view>`
        // (per RefreshTypeCode.emitsCascadeViewDelta) produce a view-delta on
        // disk; for the others the rewriter writes the MV directly without a
        // view-delta CTAS, so there is nothing to persist.
        //
        // Strict ordering for crash safety:
        //   1. refresh program executes (writes data table + view-delta CTAS)
        //   2. countMonoid cleanup
        //   3. record(MV_VIEW_DELTA, viewDeltaPath) — MUST precede step 4
        //   4. postRefreshCleanup → markConsumed → MvCatalog.advance
        //
        // If we crash between (1) and (3): the data table is updated and
        // input staging is still unconsumed (a retry replays it — at-least-once
        // semantics; see PRE-EXISTING idempotency gap). The view-delta on disk
        // is orphan (no catalog row) and is collected by Phase 7's orphan sweep.
        //
        // One MV_VIEW_DELTA row is recorded per distinct downstream
        // `sourceTables` form referencing this MV. Downstream MVs may
        // reference this MV as `db.name` or bare `name` depending on the
        // session's current schema at downstream CREATE time; each form
        // becomes a separate trigger key so `StagingCatalog.collectFor`
        // (which matches `base_table` exactly against the downstream's
        // `meta.sourceTables`) finds it.
        if (cleanupMeta.emitsCascadeViewDelta && fusedScratchView.isEmpty) {
          profile.timeStep("metadata_post_sql", "phase=record_cascade") {
            RefreshPerf.timePhase(refreshId, viewLabel, "record_cascade") {
              val mvShortName = name.identifier
              val triggerKeys: Set[String] = allMvsCached
                .filter(_.sourceTables.exists(_.split("\\.").last == mvShortName))
                .flatMap(_.sourceTables.filter(_.split("\\.").last == mvShortName))
                .toSet
              val keysToRecord =
                if (triggerKeys.isEmpty) Set(viewNameStr) // record under our own name even with no downstream yet
                else triggerKeys
              val txnTs = new Timestamp(System.currentTimeMillis())
              keysToRecord.foreach { triggerKey =>
                StagingCatalog.record(
                  spark,
                  StagingDelta(
                    baseTable = triggerKey,
                    opType = StagingDelta.OpTypes.MvViewDelta,
                    stagingPath = viewDeltaPath,
                    txnTs = txnTs,
                    consumedBy = Seq.empty
                  )
                )
              }
            }
          }
        }
      } catch {
        case t: Throwable =>
          // Best-effort cleanup of any partial view-delta on failure. Phase 7
          // orphan-sweep is the long-tail safety net.
          deletePathIfExists(viewDeltaPath)
          emitEnd(
            "incremental_failed",
            meta.refreshTypeName,
            stagingDeltas.size
          )
          val sqlSnippet = rewritten.statements.zipWithIndex
            .map { case (s, i) => s"[${i + 1}] ${SparkRefreshRewriter.stripExecutionMarker(s)}" }
            .mkString("\n---\n")
          throw new RuntimeException(
            s"Incremental refresh of '${sqlIdent(name)}' failed: ${t.getMessage}\n" +
              s"Rewritten SQL:\n$sqlSnippet",
            t
          )
      }

      profile.timeStep("metadata_post_sql", "phase=post_cleanup") {
        RefreshPerf.timePhase(refreshId, viewLabel, "post_cleanup") {
          postRefreshCleanup(spark, name, cleanupMeta, stagingDeltas, viewNameStr, sqlLog, qlogOrder)
        }
      }
      emitEnd(
        if (spFullRefreshFallback) "simple_projection_full_refresh_fallback"
        else "incremental_executed",
        if (spFullRefreshFallback) "FULL_REFRESH" else meta.refreshTypeName,
        stagingDeltas.size
      )
    } finally {
      IvmDmlInterceptorRule.bypass.set(false)
      tempViewShortNames.foreach { n =>
        val dropSql = StagingDeltaView.dropSourceDeltaViewSql(n)
        val t0      = System.nanoTime()
        try {
          spark.sql(dropSql)
        } catch { case _: Throwable => () }
        finally {
          val ms = (System.nanoTime() - t0) / 1000000L
          sqlLog.record(
            category = "drop_cleanup",
            stmtOrder = qlogOrder.getAndIncrement(),
            attemptIdx = 0,
            stmtKind = "drop",
            sql = dropSql,
            durationMs = ms
          )
        }
      }
      fusedScratchView.foreach { view =>
        try {
          // Unpersist the cached scratch DataFrame before dropping the temp
          // view so the SparkSession's cache manager releases storage
          // memory immediately rather than waiting for GC.
          spark.catalog.uncacheTable(view)
        } catch { case _: Throwable => () }
        try spark.catalog.dropTempView(view)
        catch { case _: Throwable => () }
      }
      // emitEnd() above already flushed sqlLog, but drop_cleanup rows
      // are appended after that flush (this finally block runs after the
      // try-body returns). A second flush is required so those rows land
      // in RocksDB before SHOW OPENIVM QUERY LOG observes the lifecycle.
      sqlLog.flush()
    }

    Seq.empty
  }

  private def hasNegativeSimpleProjectionRows(spark: SparkSession, viewDeltaPath: String): Boolean = {
    val escapedPath = viewDeltaPath.replace("`", "``")
    spark
      .sql(
        s"""SELECT 1
           |FROM delta.`$escapedPath`
           |WHERE `openivm_multiplicity` < 0
           |LIMIT 1""".stripMargin
      )
      .head(1)
      .nonEmpty
  }

  /** Diagnostic gated by OPENIVM_REFRESH_DIAGNOSTICS=1 or
    * -Dopenivm.refresh.diagnostics=1. After the view-delta CTAS, log row counts
    * by multiplicity sign + a small sample of rows. Useful when CI surfaces a
    * downstream MV miss and we need to know whether the row is present in the
    * upstream cascade-delta vs dropped by the downstream's inclusion-exclusion
    * CTAS.
    */
  private def diagnosticsEnabled: Boolean = {
    try {
      val raw = sys.env
        .get("OPENIVM_REFRESH_DIAGNOSTICS")
        .orElse(sys.props.get("openivm.refresh.diagnostics"))
        .getOrElse("0")
      raw.trim == "1" || raw.trim.equalsIgnoreCase("true")
    } catch { case _: Throwable => false }
  }

  private def logViewDeltaDiagnostics(
      spark: SparkSession,
      viewName: TableIdentifier,
      viewDeltaPath: String,
      stmtIdx: Int
  ): Unit = {
    if (!diagnosticsEnabled) return
    val escapedPath = viewDeltaPath.replace("`", "``")
    val viewId      = MvCommandHelper.sqlIdent(viewName)
    try {
      // Row counts by multiplicity sign.
      val counts = spark
        .sql(
          s"""SELECT
             |  COUNT(*) AS total,
             |  COUNT(CASE WHEN `openivm_multiplicity` > 0 THEN 1 END) AS pos,
             |  COUNT(CASE WHEN `openivm_multiplicity` < 0 THEN 1 END) AS neg,
             |  COUNT(CASE WHEN `openivm_multiplicity` = 0 THEN 1 END) AS zero
             |FROM delta.`$escapedPath`""".stripMargin
        )
        .head()
      logInfo(
        s"[openivm-mv-diag] refresh view='$viewId' stmt[$stmtIdx]_view_delta " +
          s"path='$viewDeltaPath' total=${counts.getLong(0)} pos=${counts.getLong(1)} " +
          s"neg=${counts.getLong(2)} zero=${counts.getLong(3)}"
      )
      // Sample the first 8 rows as JSON (avoids depending on the view's column shape).
      val sample = spark
        .sql(
          s"""SELECT to_json(struct(*)) AS row_json
             |FROM delta.`$escapedPath`
             |LIMIT 8""".stripMargin
        )
        .collect()
        .map(_.getString(0))
      sample.zipWithIndex.foreach { case (json, idx) =>
        logInfo(
          s"[openivm-mv-diag] refresh view='$viewId' stmt[$stmtIdx]_view_delta " +
            s"sample[$idx]=$json"
        )
      }
    } catch {
      case t: Throwable =>
        logInfo(
          s"[openivm-mv-diag] refresh view='$viewId' stmt[$stmtIdx]_view_delta " +
            s"error='${t.getClass.getSimpleName}: ${t.getMessage}'"
        )
    }
  }

  private def simpleProjectionUserCols(spark: SparkSession, targetId: TableIdentifier): Seq[String] =
    spark
      .table(MvCommandHelper.metaName(targetId))
      .columns
      .filterNot(_.startsWith("openivm_"))
      .map(c => s"`${c.replace("`", "``")}`")
      .toSeq

  private def hasConflictingSimpleProjectionRows(
      spark: SparkSession,
      targetId: TableIdentifier,
      viewDeltaPath: String
  ): Boolean = {
    val escapedPath = viewDeltaPath.replace("`", "``")
    val colList     = simpleProjectionUserCols(spark, targetId).mkString(", ")
    spark
      .sql(
        s"""SELECT 1
           |FROM delta.`$escapedPath`
           |GROUP BY $colList
           |HAVING SUM(CASE WHEN `openivm_multiplicity` > 0 THEN `openivm_multiplicity` ELSE 0 END) > 0
           |   AND SUM(CASE WHEN `openivm_multiplicity` < 0 THEN -`openivm_multiplicity` ELSE 0 END) > 0
           |LIMIT 1""".stripMargin
      )
      .head(1)
      .nonEmpty
  }

  /** Same as [[hasConflictingSimpleProjectionRows]] but reads from a cached
    * temp view (the scratch-CTAS fuse fast path).
    */
  private def hasConflictingFusedRows(
      spark: SparkSession,
      targetId: TableIdentifier,
      scratchView: String
  ): Boolean = {
    val colList = simpleProjectionUserCols(spark, targetId).mkString(", ")
    spark
      .sql(
        s"""SELECT 1
           |FROM `$scratchView`
           |GROUP BY $colList
           |HAVING SUM(CASE WHEN `openivm_multiplicity` > 0 THEN `openivm_multiplicity` ELSE 0 END) > 0
           |   AND SUM(CASE WHEN `openivm_multiplicity` < 0 THEN -`openivm_multiplicity` ELSE 0 END) > 0
           |LIMIT 1""".stripMargin
      )
      .head(1)
      .nonEmpty
  }

  /** Advance the MV's tracked Delta version and prune fully-consumed staging
    * rows. Shared between the FullRefresh and incremental paths.
    *
    * MV-over-MV cascade trigger:
    *
    * For every other tracked MV that lists `name` as a source table, this
    * method ensures the downstream MV's next REFRESH does not short-circuit
    * on the `stagingDeltas.isEmpty` guard at the top of [[runUnderLock]].
    *
    * Two paths depending on `meta.emitsCascadeViewDelta`:
    *
    *  - **Cascade-delta-capable** (per [[MvMetadata.emitsCascadeViewDelta]]):
    *    nothing to synthesise here — the
    *    `MV_VIEW_DELTA` staging row was already recorded inside the
    *    incremental refresh's success block ([[runUnderLock]] step 3).
    *    Downstream MVs whose `MvMetadata.sourceTables` references this MV
    *    pick up that row via `StagingCatalog.collectFor`.
    *
    *  - **NOT cascade-delta-capable** (e.g. FullRefresh,
    *    SIMPLE_AGGREGATE, DISTINCT_INCREMENTAL, SEMI_ANTI_RECOMPUTE, TOP_K,
    *    or a recompute MV whose compiled SQL emitted no real view-delta):
    *    is no persisted upstream delta downstream can consume. Synthesise a
    *    **unique per-refresh trigger** row so the downstream's
    *    next REFRESH fires. The unique suffix prevents
    *    `StagingCatalog.record`'s `(base_table, staging_path)` idempotency
    *    key from collapsing multiple consecutive triggers into one row with
    *    stale `consumed_by`. Downstream MVs over these upstream types are
    *    themselves FullRefresh-demoted at CREATE time, so they never
    *    interpret the trigger row's content — only its presence.
    *
    * Match the downstream's source-table entry by its trailing short-name
    * segment so the synthetic staging uses the exact form recorded in the
    * downstream's `MvMetadata.sourceTables` (the DML-interceptor convention
    * stores sources with their Spark-resolved namespace prefix, e.g.
    * `default.ch_sales_by_region`, but [[TableIdentifier]] for a
    * `CREATE MATERIALIZED VIEW` issued without a db prefix is db-less).
    */
  private def postRefreshCleanup(
      spark: SparkSession,
      name: TableIdentifier,
      meta: MvMetadata,
      stagingDeltas: Seq[StagingDelta],
      viewNameStr: String,
      sqlLog: RefreshSqlLog = RefreshSqlLog.NoOp,
      qlogOrder: java.util.concurrent.atomic.AtomicInteger = new java.util.concurrent.atomic.AtomicInteger(0)
  ): Unit = {
    import MvCommandHelper._
    val newVersion =
      DeltaTable.forPath(spark, meta.location).history(1).collect().head.getAs[Long]("version")
    MvCatalog.advance(spark, name, newVersion)

    val consumedPaths = stagingDeltas.map(_.stagingPath)
    StagingCatalog.markConsumed(spark, viewNameStr, consumedPaths)

    val allMvs = MvCatalog.list(spark)
    val viewsByTable = allMvs
      .flatMap(m => m.sourceTables.map(t => t -> metaName(m.name)))
      .groupBy(_._1)
      .map { case (t, pairs) => t -> pairs.map(_._2) }
    StagingCatalog.pruneFullyConsumed(spark, viewsByTable)

    if (!meta.emitsCascadeViewDelta) {
      val upstreamShortName = name.identifier
      val downstreamSourceKeys = allMvs
        .filterNot(m => metaName(m.name) == viewNameStr)
        .flatMap(_.sourceTables.filter(_.split("\\.").last == upstreamShortName))
        .distinct

      if (downstreamSourceKeys.nonEmpty) {
        val warehouse   = spark.conf.get("spark.sql.warehouse.dir").stripSuffix("/")
        val safeName    = viewNameStr.replaceAll("[^A-Za-z0-9_.-]", "_")
        val triggerPath = s"$warehouse/_openivm/triggers/$safeName/${UUID.randomUUID().toString}"
        // The actual write goes through the DataFrame API (Delta needs the
        // `.write.format("delta")…save(triggerPath)` shape, not `spark.sql`).
        // Synthesize an INSERT OVERWRITE representation so the query log
        // captures the user-readable intent.
        val syntheticSql =
          s"-- synthetic representation of postRefreshCleanup trigger write\n" +
            s"INSERT OVERWRITE delta.`$triggerPath`\n" +
            s"SELECT * FROM ${sqlIdent(name)} WHERE 1 = 0"
        val t0 = System.nanoTime()
        try {
          spark
            .sql(s"SELECT * FROM ${sqlIdent(name)} WHERE 1 = 0")
            .write
            .format("delta")
            .mode("overwrite")
            .save(triggerPath)
        } finally {
          val ms = (System.nanoTime() - t0) / 1000000L
          sqlLog.record(
            category = "post_cleanup_stage",
            stmtOrder = qlogOrder.getAndIncrement(),
            attemptIdx = 0,
            stmtKind = "insert_overwrite",
            sql = syntheticSql,
            durationMs = ms
          )
        }

        val txnTs = new Timestamp(System.currentTimeMillis())
        downstreamSourceKeys.foreach { sourceKey =>
          StagingCatalog.record(
            spark,
            StagingDelta(
              baseTable = sourceKey,
              opType = StagingDelta.OpTypes.Overwrite,
              stagingPath = triggerPath,
              txnTs = txnTs,
              consumedBy = Seq.empty
            )
          )
        }
      }
    }
  }

  /** True for refresh types whose openivm-emitted MERGE preserves rows whose
    * count monoid has retracted to zero. The Spark-side cleanup pass then
    * removes those rows so the user-visible MV reflects the live aggregate.
    */
  private def isCountMonoid(refreshType: Int): Boolean =
    refreshType == RefreshTypeCode.AggregateGroup ||
      refreshType == RefreshTypeCode.AggregateHaving ||
      refreshType == RefreshTypeCode.DistinctIncremental

  /** Resolve the column that tracks group cardinality for an incremental
    * aggregate refresh:
    *  - Prefer `openivm_count_star` when openivm emitted it (no user COUNT(*)
    *    in the view body).
    *  - Otherwise fall back to the user-supplied COUNT(*) alias captured at
    *    CREATE time (stored in `MvMetadata.properties` under `_ivm_count_col`).
    *
    *  Returns None when neither is available — in which case the post-pass
    *  DELETE is skipped (no safe column to filter on).
    */
  private def countMonoidColumn(
      spark: SparkSession,
      name: TableIdentifier,
      meta: MvMetadata
  ): Option[String] = {
    import MvCommandHelper._
    val cols: Set[String] =
      try spark.table(sqlIdent(name)).schema.fieldNames.toSet
      catch { case _: Throwable => Set.empty[String] }
    // openivm injects `openivm_count_star` for AGGREGATE_GROUP / AGGREGATE_HAVING, and
    // `openivm_distinct_count` for views compiled from SELECT DISTINCT (which OpenIVM also
    // classifies as AGGREGATE_GROUP with a different hidden-column name).
    if (cols.contains("openivm_count_star")) Some("openivm_count_star")
    else if (cols.contains("openivm_distinct_count")) Some("openivm_distinct_count")
    else meta.properties.get("_ivm_count_col").filter(cols.contains)
  }
}

// ---------------------------------------------------------------------------
// DropMaterializedViewCommand
// ---------------------------------------------------------------------------

case class DropMaterializedViewCommand(
    name: TableIdentifier,
    ifExists: Boolean
) extends LeafRunnableCommand {

  override def run(spark: SparkSession): Seq[Row] = {
    import MvCommandHelper._

    MvCatalog.lookup(spark, name) match {
      case None if ifExists =>
        return Seq.empty
      case None =>
        throw new AnalysisException(
          "TABLE_OR_VIEW_NOT_FOUND",
          Map("relationName" -> sqlIdent(name))
        )
      case Some(meta) =>
        // For AGGREGATE_HAVING the user-facing name is a Spark VIEW and the
        // data lives in a sibling Delta table. Drop both so no orphan storage
        // or stale catalog entry survives.
        if (meta.refreshType == RefreshTypeCode.AggregateHaving) {
          spark.sql(s"DROP VIEW IF EXISTS ${sqlIdent(name)}")
          spark.sql(s"DROP TABLE IF EXISTS ${sqlIdent(dataTableId(name))}")
        } else {
          // Drop the catalog table entry (Delta table registration in Spark)
          spark.sql(s"DROP TABLE IF EXISTS ${sqlIdent(name)}")
        }

        // Delete the physical Delta files
        val hadoopPath = new Path(meta.location)
        val fs         = hadoopPath.getFileSystem(spark.sessionState.newHadoopConf())
        if (fs.exists(hadoopPath)) fs.delete(hadoopPath, /* recursive = */ true)

        // MV-over-MV cleanup: remove every `StagingCatalog` row whose
        // `base_table` could reference this MV. Without this, a subsequent
        // CREATE of the SAME name with a different body could consume stale
        // view-deltas from the old incarnation. Two forms are pruned:
        //   - exact match on `metaName(name)` (qualified `db.table`)
        //   - bare short-name match (downstream MVs created without a db
        //     prefix store their source as the bare name)
        //
        // Also delete the per-MV view-delta namespace on disk so view-delta
        // Delta paths from previous refreshes are gone.
        val mvQual  = metaName(name)
        val mvShort = name.identifier
        StagingCatalog.removeForBaseTable(spark, mvQual)
        if (mvShort != mvQual) StagingCatalog.removeForBaseTable(spark, mvShort)

        val warehouse       = spark.conf.get("spark.sql.warehouse.dir").stripSuffix("/")
        val safeMvName      = mvQual.replace(".", "_").replace(" ", "_")
        val viewDeltaNsPath = new Path(s"$warehouse/_ivm/view_deltas/$safeMvName")
        try {
          val vdFs = viewDeltaNsPath.getFileSystem(spark.sessionState.newHadoopConf())
          if (vdFs.exists(viewDeltaNsPath)) vdFs.delete(viewDeltaNsPath, /* recursive = */ true)
        } catch { case _: Throwable => () }

        // Remove the tracking row from the MV catalog
        MvCatalog.remove(spark, name)
    }

    Seq.empty
  }
}
