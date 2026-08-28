package org.openivm.spark.commands

import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  AttributeReference,
  Expression,
  NamedExpression,
  RowOrdering,
  SubqueryExpression
}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.WindowExpression
import org.apache.spark.sql.catalyst.plans.logical.{
  Aggregate,
  Filter,
  GlobalLimit,
  Join,
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
import org.openivm.spark.common.rocksdb.OpenIvmStateSync
import org.openivm.spark.compiler.{CompiledRefresh, CompileRequest, OpenIvmCompiler}

import java.sql.Timestamp
import java.util.{Collections, UUID}
import scala.util.control.NonFatal

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
      val c = buildForSession(spark)
      cache.put(spark, c)
      Runtime.getRuntime.addShutdownHook(new Thread(() => c.close()))
      c
    }
  }

  /** Build the compiler, resolving the DuckDB CLI + OpenIVM extension binaries:
    *
    *  1. on-disk `OPENIVM_CLI_PATH` / `OPENIVM_EXTENSION_PATH` (default
    *     `/opt/openivm/…`) — the local spark-openivm container symlinks both
    *     here, so this path is unchanged.
    *  2. otherwise the binaries baked into the assembly JAR under
    *     `/openivm-native/` are extracted to a per-app local temp dir (chmod +x).
    *     Managed Fabric Spark has neither binary on local disk but loads the JAR
    *     on the driver classpath (`spark.jars`), so the compile bridge stays
    *     self-contained with no OneLake round-trip.
    */
  private def buildForSession(spark: SparkSession): OpenIvmCompiler = {
    val extEnv = sys.env.getOrElse(
      "OPENIVM_EXTENSION_PATH",
      "/opt/openivm/openivm.duckdb_extension"
    )
    val cliEnv = sys.env.getOrElse(
      "OPENIVM_CLI_PATH",
      Option(new java.io.File(extEnv).getParentFile)
        .map(dir => new java.io.File(dir, "duckdb").getAbsolutePath)
        .getOrElse("/opt/openivm/duckdb")
    )
    if (new java.io.File(extEnv).exists() && new java.io.File(cliEnv).exists())
      OpenIvmCompiler.build(extensionPath = extEnv, cliPath = cliEnv)
    else {
      val (extPath, cliPath) = extractBundledAssets(spark)
      OpenIvmCompiler.build(extensionPath = extPath, cliPath = cliPath)
    }
  }

  /** Extract the DuckDB CLI + OpenIVM extension baked into the assembly JAR
    * (`/openivm-native/…`) to a per-app local temp dir; chmod +x the CLI. */
  private def extractBundledAssets(spark: SparkSession): (String, String) = {
    val localDir = new java.io.File(s"/tmp/openivm-assets-${spark.sparkContext.applicationId}")
    localDir.mkdirs()
    val ext = new java.io.File(localDir, "openivm.duckdb_extension")
    val cli = new java.io.File(localDir, "duckdb")

    def extract(resource: String, dst: java.io.File): Unit =
      if (!dst.exists() || dst.length() == 0L) {
        val in = Option(getClass.getResourceAsStream(resource)).getOrElse(
          throw new IllegalStateException(
            s"bundled compile asset $resource not found on the classpath — the " +
              "openivm-spark assembly JAR must embed it under /openivm-native/ " +
              "(set OPENIVM_NATIVE_DIR at build time), or provide it on disk via " +
              "OPENIVM_CLI_PATH / OPENIVM_EXTENSION_PATH"
          )
        )
        try
          java.nio.file.Files.copy(
            in,
            dst.toPath,
            java.nio.file.StandardCopyOption.REPLACE_EXISTING
          )
        finally in.close()
      }

    extract("/openivm-native/openivm.duckdb_extension", ext)
    extract("/openivm-native/duckdb", cli)
    cli.setExecutable(true, /* ownerOnly = */ false)
    (ext.getAbsolutePath, cli.getAbsolutePath)
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

private[spark] object RefreshFailureInjection {

  private val FailWindowCascadeInsertKey = "spark.openivm.test.failWindowCascadeInsert"

  def failNextWindowCascadeInsert(spark: SparkSession): Unit =
    spark.sparkContext.setLocalProperty(FailWindowCascadeInsertKey, "true")

  private[commands] def maybeFailWindowCascadeInsert(spark: SparkSession): Unit =
    if (spark.sparkContext.getLocalProperty(FailWindowCascadeInsertKey) == "true") {
      spark.sparkContext.setLocalProperty(FailWindowCascadeInsertKey, null)
      throw new RuntimeException("injected failure after WINDOW_PARTITION target delete")
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
    if (SparkRefreshRewriter.isMergeStatement(trimmed)) "merge"
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

  /** Whether the pending changes replace a source snapshot rather than
    * describe an incremental delta.
    *
    * CDF batches expose replacement semantics through their classified
    * verdict. Intercepted staging batches carry the operation type directly.
    * In particular, MV_VIEW_DELTA is a complete signed delta and must not be
    * treated like OVERWRITE merely because it is delivered through staging.
    */
  def hasReplacementBatch(
      changeBatches: Seq[ChangeBatch],
      cdfBatchVerdicts: Iterable[BatchVerdict]
  ): Boolean =
    cdfBatchVerdicts.exists(_ == BatchVerdict.Replace) ||
      changeBatches.exists {
        case batch: StagingChangeBatch =>
          batch.deltas.exists(_.opType == StagingDelta.OpTypes.Overwrite)
        case _: CdfChangeBatch => false
      }

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

  /** Resolved simple-column window partition key for liquid-clustering
    * WINDOW_PARTITION MV data tables. Skips safely when any window PARTITION BY
    * expression is not a plain output column, when windows use different keys,
    * or when the key is not projected by the MV data table.
    */
  def resolvedWindowPartitionColumns(plan: LogicalPlan): Option[Seq[String]] = {
    val windowPartSpecs = plan.collect { case node =>
      node.expressions.flatMap(_.collect { case w: WindowExpression => w.windowSpec.partitionSpec })
    }.flatten
    if (windowPartSpecs.isEmpty) return None

    val parsedSpecs = windowPartSpecs.map { spec =>
      spec.map {
        case a: AttributeReference => Some(a.name)
        case _                     => None
      }
    }
    if (parsedSpecs.exists(parts => parts.isEmpty || parts.exists(_.isEmpty))) return None

    val keys     = parsedSpecs.map(_.flatten)
    val firstKey = keys.head
    val sameKey = keys.forall { key =>
      key.map(_.toLowerCase(java.util.Locale.ROOT)) == firstKey.map(_.toLowerCase(java.util.Locale.ROOT))
    }
    if (!sameKey) return None

    val outputCols = plan.output.map(_.name.toLowerCase(java.util.Locale.ROOT)).toSet
    if (firstKey.forall(c => outputCols.contains(c.toLowerCase(java.util.Locale.ROOT)))) Some(firstKey) else None
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

  // ---------------------------------------------------------------------------
  // Shared refresh-type classification.
  //
  // These helpers are the SINGLE source of truth for the effective refresh-type
  // decision. They are invoked by both the live CREATE path
  // ([[CreateMaterializedViewCommand.runCreate]]) and the side-effect-free dry
  // compile ([[MvDryCompile]]) that backs `EXPLAIN CREATE MATERIALIZED VIEW` and
  // `SHOW REFRESH SQL`. Keeping the decision in one place guarantees the EXPLAIN
  // verdict is byte-identical to what a real CREATE would classify — so we never
  // report a query as incrementalizable when CREATE would demote it (or vice
  // versa). The ivm-it parity suite plus the explicit EXPLAIN==CREATE parity
  // test guard against drift.
  // ---------------------------------------------------------------------------

  /** The effective refresh-type verdict for a materialized view. */
  final case class EffectiveClassification(
      refreshType: Int,
      refreshTypeName: String,
      reason: String,
      emitsCascadeViewDelta: Boolean
  )

  /** AGGREGATE_HAVING data-table columns, discovered by a schema-only (`LIMIT 0`)
    * probe of the incremental view body. Empty for non-AGGREGATE_HAVING views.
    * Read-only: `LIMIT 0` triggers no scan/write.
    */
  def computeAggregateHavingDataColumns(
      spark: SparkSession,
      compiled: CompiledRefresh,
      originalQueryText: String
  ): Option[Set[String]] =
    if (compiled.refreshType != RefreshTypeCode.AggregateHaving) None
    else {
      val incrementalViewBodySql =
        if (compiled.initialLoadSql.isEmpty) originalQueryText
        else org.openivm.spark.compiler.LptsSparkDialect.translate(compiled.initialLoadSql)
      try
        Some(
          spark
            .sql(s"SELECT * FROM ($incrementalViewBodySql) __openivm_having_preview LIMIT 0")
            .schema
            .fieldNames
            .toSet
        )
      catch { case _: Throwable => None }
    }

  /** True unless the compiled SIMPLE_PROJECTION delta feed rewrites to a single
    * statement (i.e. a delta feed with no data-table apply, which would make
    * REFRESH a no-op). Non-SIMPLE_PROJECTION views short-circuit to `true`.
    * Read-only: the rewrite is a pure string transform against a probe path.
    */
  def computeSimpleProjectionHasDataApply(
      spark: SparkSession,
      compiled: CompiledRefresh,
      name: TableIdentifier,
      location: String,
      qualSchemas: Map[String, StructType],
      shortToQual: Map[String, String]
  ): Boolean =
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
      } catch { case _: Throwable => false }
    }

  /** Enumerate every source that resolves to a tracked materialized view,
    * keyed by the qualified source name. Symmetric on `db.table` vs bare-name
    * so it matches MVs created with or without a db prefix. Read-only.
    */
  def computeUpstreamMvByQual(
      spark: SparkSession,
      qualNames: Seq[String]
  ): Map[String, MvMetadata] = {
    val all: Seq[MvMetadata] =
      try MvCatalog.list(spark)
      catch { case _: Throwable => Seq.empty[MvMetadata] }
    val byMeta: Map[String, MvMetadata] = all.map(m => metaName(m.name) -> m).toMap
    qualNames.flatMap { qn =>
      val short = qn.split("\\.").last
      byMeta.get(qn).orElse(byMeta.get(short)).map(m => qn -> m)
    }.toMap
  }

  /** `Some(reason)` when any upstream MV cannot feed a downstream-consumable
    * `openivm_delta_<view>` (currently: an upstream classified FULL_REFRESH),
    * else `None`. Drives the `non_cascade_upstream` demotion below.
    */
  def computeNonCascadeUpstreamReason(upstreamMvByQual: Map[String, MvMetadata]): Option[String] = {
    val nonCascadeUpstreams: Seq[(String, String)] =
      upstreamMvByQual.toSeq.collect {
        case (q, m) if !m.emitsCascadeViewDelta => q -> "non_cascade"
        // Deliberately NOT demoting on `upstream is AGGREGATE_GROUP` or
        // `upstream MV count >= 2`: two historical guards
        // (`aggregate_group_into_simple_projection`, `multi_mv_simple_projection`)
        // were removed because duckdb-openivm emits SIMPLE_PROJECTION refresh as a
        // positive-only bag-apply and multi-source refresh as ONE UNION-ALL delta
        // statement — both shapes the Spark rewriter already handles at binary
        // parity. Re-adding either guard silently regresses incrementalization; the
        // full rationale lives in git history at this line.
      }
    if (nonCascadeUpstreams.isEmpty) None
    else
      Some(
        nonCascadeUpstreams
          .groupBy(_._2)
          .toSeq
          .sortBy(_._1)
          .map { case (reason, entries) => s"$reason:${entries.map(_._1).mkString(",")}" }
          .mkString(";")
      )
  }

  /** The effective refresh-type decision. Every FULL_REFRESH demotion here is
    * the ONLY place a compiled incremental type is downgraded, so this is the
    * function that must never regress. See the reason-key documentation inline.
    */
  def classifyEffectiveRefreshType(
      compiled: CompiledRefresh,
      viewShortName: String,
      isTopKView: Boolean,
      simpleProjectionHasDataApply: Boolean,
      nonCascadeUpstreamReason: Option[String],
      rawHavingPred: Option[String],
      aggregateHavingDataColumns: Option[Set[String]]
  ): EffectiveClassification = {
    val (effectiveRefreshType, reason) = {
      if (isTopKView) (RefreshTypeCode.FullRefresh, "top_k")
      else if (!simpleProjectionHasDataApply)
        (RefreshTypeCode.FullRefresh, "simple_projection_no_apply")
      else if (nonCascadeUpstreamReason.nonEmpty)
        (RefreshTypeCode.FullRefresh, s"non_cascade_upstream:${nonCascadeUpstreamReason.get}")
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
      else if (!SparkRefreshRewriter.hasRealDelta(compiled.sql, viewShortName))
        (RefreshTypeCode.FullRefresh, "no_real_delta")
      else (compiled.refreshType, "kept")
    }
    val effectiveRefreshTypeName =
      if (effectiveRefreshType == RefreshTypeCode.FullRefresh) "FULL_REFRESH"
      else compiled.refreshTypeName
    val emitsCascadeViewDelta =
      RefreshTypeCode.emitsCascadeViewDelta(effectiveRefreshType) &&
        SparkRefreshRewriter.hasRealDelta(compiled.sql, viewShortName)
    EffectiveClassification(effectiveRefreshType, effectiveRefreshTypeName, reason, emitsCascadeViewDelta)
  }
}

// ---------------------------------------------------------------------------
// CreateMaterializedViewCommand
// ---------------------------------------------------------------------------

/**
 * Logical plan node for CREATE MATERIALIZED VIEW.
 *
 * @param originalQueryText  Raw SQL of the SELECT body, captured by the parser.
 *                           Stored verbatim in MvMetadata and passed to the compiler.
 * @param clusterColumns     User-supplied `CLUSTER BY (...)` columns (declaration
 *                           order); empty when the DDL had no `CLUSTER BY` clause.
 */
case class CreateMaterializedViewCommand(
    name: TableIdentifier,
    query: LogicalPlan,
    properties: Map[String, String],
    ifNotExists: Boolean,
    provider: Option[String],
    originalQueryText: String,
    clusterColumns: Seq[String] = Seq.empty
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
    try {
      val rows = runCreate(spark, profile, sqlLog)
      OpenIvmStateSync.backupAsync(spark)
      rows
    } finally {
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
      CdfWatermarkCatalog.ensureTables(spark)
    }

    val propagation = ChangePropagationFactory.forSession(spark)

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

    // Validate that every source is configured correctly for the active
    // change-propagation mode (e.g. under CDF mode: requires
    // `delta.enableChangeDataFeed = true` on every source).  Fails fast at
    // CREATE so users see a clear error before paying for the openivm
    // compile + initial CTAS.
    propagation.validateSources(spark, qualNames)

    // Extract GROUP BY keys and other optional metadata from the analyzed plan
    val analyzed       = spark.sql(originalQueryText).queryExecution.analyzed
    val groupKeys      = extractGroupKeys(analyzed)
    val countStarAlias = extractCountStarAlias(analyzed)
    val queryShapeProps = MvMetadata.queryShapeProperties(analyzed.exists {
      case _: Join => true
      case _       => false
    })

    // Compile the view via OpenIVM. If openivm's DuckDB subprocess cannot
    // compile the query (e.g. the user's view body references Spark-only
    // functions like `regexp_like`, `to_date(string)`, etc. that DuckDB does
    // not recognise), demote the view to FULL_REFRESH so each refresh
    // re-executes the original Spark SQL via INSERT OVERWRITE. This trades
    // incrementality for correctness: the MV stays bag-equal to the live
    // query while the user retains source-of-truth control over the SQL
    // they wrote.
    val constraintFacts = WorkloadFactsRegistry.forRefresh().discover(spark, qualNames)
    val statsFacts      = SparkDeltaStatsService.forRefresh().workloadFactsFor(spark, qualNames)
    val workloadFacts = statsFacts.copy(
      fkRelations = constraintFacts.fkRelations,
      uniqueKeys = constraintFacts.uniqueKeys,
      declareRelyFk = FeatureGate.declareRelyFkEnabled(spark)
    )
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
            sourceQualifiedNames = shortToQual,
            facts = workloadFacts
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

    // Storage location
    val location = mvLocation(spark, name)
    val aggregateHavingDataColumns: Option[Set[String]] =
      computeAggregateHavingDataColumns(spark, compiled, originalQueryText)

    val simpleProjectionHasDataApply: Boolean =
      computeSimpleProjectionHasDataApply(spark, compiled, name, location, qualSchemas, shortToQual)

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
    // `force_view_delta_cascade=true` in CompileFacts (which openivm-spark
    // always sets), `CompileWindowRecompute()` now snapshots the affected
    // pre-refresh rows and recomputed post-refresh rows before mutating
    // `openivm_data_<view>`, then appends them as `-1/+1` rows into
    // `openivm_delta_<view>`. For a bounded affected-key set and the raw
    // old×-1/new×+1 cascade shape, Spark materializes the affected-key set and
    // signed snapshot once. Small literal key sets use one partition-scoped
    // `REPLACE WHERE`; larger sets defer the compiler's key-based DELETE until
    // after the cascade is persisted, then insert the cascade's positive rows.
    // The signed snapshot remains the downstream MV-over-MV feed in both cases.
    //
    // GROUP_RECOMPUTE (RefreshType 6) likewise stopped being a "no view-delta"
    // exception once that CompileFacts flag is set. `CompileGroupRecompute()`
    // still materialises the affected-key set and refreshes the MV by
    // DELETEing and
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
    val upstreamMvByQual: Map[String, MvMetadata] = computeUpstreamMvByQual(spark, qualNames)
    val sourceIsMv: Boolean                       = upstreamMvByQual.nonEmpty
    val distinctUpstreamMvCount: Int =
      upstreamMvByQual.values.map(m => metaName(m.name)).toSet.size
    // Non-cascade-upstream demotion reason (None when every upstream MV can feed
    // a downstream-consumable view-delta). See computeNonCascadeUpstreamReason.
    val nonCascadeUpstreamReason: Option[String] =
      computeNonCascadeUpstreamReason(upstreamMvByQual)
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
    val classification = classifyEffectiveRefreshType(
      compiled = compiled,
      viewShortName = name.table,
      isTopKView = isTopKView,
      simpleProjectionHasDataApply = simpleProjectionHasDataApply,
      nonCascadeUpstreamReason = nonCascadeUpstreamReason,
      rawHavingPred = rawHavingPred,
      aggregateHavingDataColumns = aggregateHavingDataColumns
    )
    val effectiveRefreshType     = classification.refreshType
    val classifyReason           = classification.reason
    val effectiveRefreshTypeName = classification.refreshTypeName
    val emitsCascadeViewDelta    = classification.emitsCascadeViewDelta
    // `sourceIsMv`/`distinctUpstreamMvCount` are computed for logging visibility
    // only; the demotion is driven by the shared classifier above.
    val _ = (sourceIsMv, distinctUpstreamMvCount)

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

    // Persist internal metadata alongside any user-provided properties.
    val baseProps         = Map("_ivm_group_keys" -> groupKeys.mkString(","))
    val countProp         = countStarAlias.map(a => "_ivm_count_col" -> a).toMap
    val havingProp        = havingPred.map(p => "_ivm_having_pred" -> p).toMap
    val clusterColsProp   = MvMetadata.clusterColumnsProperties(clusterColumns)
    val cascadeDeltaProps = MvMetadata.cascadeViewDeltaProperties(emitsCascadeViewDelta)

    // Fingerprint the current source schemas + every upstream MV's identity
    // hash. Captures schema drift AND upstream-body drift (DROP + recreate
    // with same schema but different body).
    val mvIdentityBySource: Map[String, String] =
      upstreamMvByQual.map { case (qn, m) => qn -> MvCatalog.mvIdentity(m) }
    val fingerprint = MvCatalog.schemaFingerprint(qualSchemas, mvIdentityBySource)

    // Persist the raw DuckDB-CLI compile result behind the schema/tier-keyed
    // cache gate.  The cached SQL is shape-stable only: REFRESH still invokes
    // SparkRefreshRewriter every time, so per-refresh snapshot temp views and
    // scratch Delta paths are always recreated.
    val createCompileTier = MvMetadata.compileCacheTier(workloadFacts)
    // The compiled initial-load SQL is ALWAYS persisted at CREATE: the FULL_REFRESH
    // arity fix (count-monoid MVs re-routed to FULL_REFRESH) reads
    // `CompiledInitialLoadSqlKey` to reproduce hidden bookkeeping columns,
    // independent of the compile-cache feature flag. Only the per-(fingerprint,tier)
    // cache properties are gated by the flag.
    val initialLoadProps =
      if (compiled.initialLoadSql.nonEmpty) Map(MvMetadata.CompiledInitialLoadSqlKey -> compiled.initialLoadSql)
      else Map.empty[String, String]
    val cacheCompiledProps =
      if (!FeatureGate.compileClassificationCacheEnabled(spark) || effectiveRefreshType == RefreshTypeCode.FullRefresh)
        Map.empty[String, String]
      else
        MvMetadata.compiledProperties(
          fingerprint,
          createCompileTier,
          compiled.sql,
          compiled.initialLoadSql,
          compiled.refreshType,
          compiled.refreshTypeName
        )
    val compiledProps = initialLoadProps ++ cacheCompiledProps
    // Capture per-source watermarks BEFORE the MV's initial CTAS so the first
    // REFRESH ignores any staging rows / Delta versions that pre-date this MV
    // (otherwise we'd double-apply upstream view-deltas this MV already
    // absorbed via the CTAS).  Encoded opaquely so the same property key
    // round-trips both `intercept`-mode timestamps and `cdf`-mode versions.
    val watermarks     = propagation.currentWatermarks(spark, qualNames)
    val watermarkProps = MvMetadata.changeWatermarkProperties(watermarks)
    val allProps =
      properties ++ baseProps ++ countProp ++ havingProp ++ clusterColsProp ++ cascadeDeltaProps ++
        queryShapeProps ++ compiledProps ++ watermarkProps
    val now = new Timestamp(System.currentTimeMillis())

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

    val windowClusterCols =
      if (effectiveRefreshType == RefreshTypeCode.WindowPartition && FeatureGate.windowClusterPruneEnabled(spark))
        resolvedWindowPartitionColumns(analyzed)
      else None
    // A user-supplied `CLUSTER BY` (#24) overrides the window-partition
    // auto-prune columns; otherwise fall back to the window-prune behaviour.
    val effectiveClusterCols: Option[Seq[String]] =
      if (clusterColumns.nonEmpty) Some(clusterColumns)
      else windowClusterCols
    val clusterClause =
      effectiveClusterCols
        .filter(_.nonEmpty)
        .map(cols => s"CLUSTER BY (${cols.map(c => s"`${c.replace("`", "``")}`").mkString(", ")}) ")
        .getOrElse("")

    val escaped  = location.replace("'", "\\'")
    val tblProps = FeatureGate.buildMvDataTblProperties(spark)
    val tblPropsClause =
      if (tblProps.nonEmpty) s"TBLPROPERTIES (${tblProps.mkString(", ")}) " else ""
    val initSql =
      s"CREATE TABLE IF NOT EXISTS ${sqlIdent(dataIdent)} USING DELTA " +
        s"$clusterClause${tblPropsClause}LOCATION '$escaped' AS $viewBodySql"
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
    val rows = RefreshMutex.withLock(metaName(name)) {
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
    OpenIvmStateSync.backupAsync(spark)
    rows
  }

  private def runUnderLock(spark: SparkSession, lockAcqMs: Long): Seq[Row] = {
    import MvCommandHelper._
    import org.openivm.spark.compiler.LptsSparkDialect

    // Refresh-scoped AQE broadcast cap.
    //
    // openivm-emitted recompute programs (SIMPLE_PROJECTION view-delta CTAS,
    // GROUP_RECOMPUTE / AGGREGATE_GROUP INSERT MERGE, WINDOW_PARTITION refresh)
    // compose long CTE chains over multi-table joins. Catalyst's plan-time
    // estimates for these chains are derived from compressed Parquet file sizes
    // and don't account for row-expansion through SCD2 range joins, count
    // monoids, or LEFT SEMI / LEFT OUTER joins that may not push down through
    // the chain.
    //
    // With AQE enabled, the runtime post-shuffle stats for an intermediate
    // stage can come back well under `spark.sql.autoBroadcastJoinThreshold`
    // (100 MiB in the bench config). AQE then PROMOTES the next join to
    // BroadcastHashJoin. When the upstream operator materialises that
    // intermediate, the actual relation exceeds Spark's hard-coded 8 GiB
    // BroadcastExchangeExec cap and the whole refresh fails with
    // `Cannot broadcast the table that is larger than 8.0 GiB: <N> GiB`.
    // dbt-spark-livy's `retry_all` retries the same deterministic failure
    // for hours, making it look "flaky" while it's actually a plan pathology.
    //
    // We disable AQE's runtime broadcast PROMOTION for the refresh scope
    // (sets `spark.sql.adaptive.autoBroadcastJoinThreshold` to -1) while
    // preserving the plan-time `spark.sql.autoBroadcastJoinThreshold` so the
    // WINDOW_PARTITION / AGGREGATE_GROUP MERGE patterns that depend on a
    // genuinely-small-side broadcast at plan time still get one. Net effect:
    // Catalyst's initial cost-based plan stays intact, but AQE will not
    // escalate a join to broadcast at runtime based on post-shuffle stats
    // from an openivm CTE chain whose true row count it can't predict.
    //
    // Scope: `spark` here is the per-refresh cloneSession (see entry point
    // above), so this override applies only to this refresh and does not
    // leak to sibling refreshes or to user queries.
    spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")

    // Per-stmt PLAN-TIME broadcast disable.
    //
    // The AQE override above only blocks runtime PROMOTION of joins to
    // broadcast based on post-shuffle stats. Catalyst's `JoinSelection`
    // strategy independently selects broadcast at PLAN TIME using row-count
    // estimates derived from compressed Parquet stats. For a multi-table
    // join over an openivm view body that includes SCD2 range joins (e.g.
    // `CAST(d.dt AS TIMESTAMP) BETWEEN s.effective_timestamp AND
    // s.end_timestamp`), Catalyst's plan-time estimate of an intermediate
    // relation can be tiny (file-size-derived) while the actual row count
    // explodes through the SCD2 multiplicity at execution. The resulting
    // BroadcastExchange then exceeds Spark's hard-coded 8 GiB build cap
    // (`BroadcastExchangeExec.scala:166`) and the refresh fails with
    // `Cannot broadcast the table that is larger than 8.0 GiB: <N> GiB`.
    //
    // We must NOT globally disable plan-time broadcast — small-side
    // broadcast for DELETE MERGEs (whose source is a `SELECT DISTINCT
    // openivm_left_key FROM <view_deltas>` tiny relation) is a desirable
    // optimisation. So this disable is narrowly scoped to the two
    // statement shapes that actually wrap the full MV body:
    //
    //   (a) the view-delta CTAS (refresh stmt[0]) — fused fast path AND
    //       on-disk CTAS fallback;
    //   (b) every openivm-emitted recompute INSERT MERGE
    //       (`MERGE INTO … USING (… <full view body> …) AS d ON false
    //       WHEN NOT MATCHED THEN INSERT`) — used by SIMPLE_PROJECTION,
    //       AGGREGATE_GROUP, WINDOW_PARTITION, GROUP_RECOMPUTE, etc.
    //
    // The conf must remain set across the DataFrame action (`.cache()` +
    // `.count()` on the fused path, `.collect()` on the on-disk path), not
    // just construction — Catalyst plans lazily inside the action.
    def withPlanTimeBroadcastDisabled[A](body: => A): A = {
      val key         = "spark.sql.autoBroadcastJoinThreshold"
      val adaptiveKey = "spark.sql.adaptive.autoBroadcastJoinThreshold"
      val sessionStatic =
        spark.conf.getOption(key).flatMap(v => scala.util.Try(v.toLong).toOption)

      val overrides = scala.collection.mutable.LinkedHashMap.empty[String, String]
      // (1) Disable PLAN-TIME broadcast: Catalyst under-estimates the SCD2
      // multiplicity and would auto-broadcast a relation that explodes past the
      // 8 GiB build cap at execution.
      overrides += (key -> "-1")
      // (2) But let AQE convert the small-side SCD2 dimension sort-merge joins
      // to broadcast-hash joins at RUNTIME, keyed on the ACTUAL materialised
      // shuffle size — safe against the explosion (AQE measures the exploding
      // intermediate as large and keeps the sort-merge join). Result-invariant.
      // Gated by `spark.openivm.refresh.adaptiveBroadcast.*`.
      if (FeatureGate.adaptiveBroadcastEnabled(spark)) {
        overrides += (adaptiveKey ->
          FeatureGate.adaptiveBroadcastThresholdBytes(spark.sparkContext.getConf, sessionStatic).toString)
      }
      // (3) Runtime-filter (bloom / semi-join) pushdown: every IVM view-delta is
      // a union of delta-rule terms, one of which is `FULL_SOURCE ⋈ Δdimension`
      // (e.g. `gold.fact_market_history` scanning the whole `daily_market`
      // against the handful of changed `dim_security` rows). A runtime filter
      // built from the tiny Δ side prunes the full-source scan to the affected
      // keys — the dominant SF10 cost. Result-invariant (an exact/superset
      // filter), gated by `spark.openivm.refresh.runtimeFilter.*`.
      overrides ++= FeatureGate.runtimeFilterConfOverrides(spark)

      val prev = overrides.keys.map(k => k -> spark.conf.getOption(k)).toList
      overrides.foreach { case (k, v) => spark.conf.set(k, v) }
      try body
      finally
        prev.foreach {
          case (k, Some(v)) => spark.conf.set(k, v)
          case (k, None)    => spark.conf.unset(k)
        }
    }

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
    val propagation      = ChangePropagationFactory.forSession(spark)
    val sourceWatermarks = meta.changeWatermarks

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
      !propagation.hasPendingChanges(spark, viewNameStr, meta.sourceTables, sourceWatermarks)
    ) {
      logInfo(
        s"[openivm-mv] refresh view='${sqlIdent(name)}' refresh_type='${meta.refreshTypeName}' " +
          "outcome='no_pending_deltas'"
      )
      emitEnd("no_pending_deltas", meta.refreshTypeName, 0)
      return Seq.empty
    }

    val changeBatches = profile.timeStep("metadata_pre_sql", "phase=collect_staging") {
      RefreshPerf.timePhase(refreshId, viewLabel, "collect_staging") {
        propagation.collectChanges(
          spark,
          viewNameStr,
          meta.sourceTables,
          sourceWatermarks
        )
      }
    }

    // Defensive backstop: the cheap existence probe above and the full collect
    // can diverge if another refresh consumes the same rows before we collect.
    if (meta.refreshType != RefreshTypeCode.FullRefresh && changeBatches.isEmpty) {
      logInfo(
        s"[openivm-mv] refresh view='${sqlIdent(name)}' refresh_type='${meta.refreshTypeName}' " +
          "outcome='no_pending_deltas'"
      )
      emitEnd("no_pending_deltas", meta.refreshTypeName, 0)
      return Seq.empty
    }

    val cdfChangeBatches = changeBatches.collect { case b: CdfChangeBatch => b }

    // Regular N-term SQL reads unchanged relations from the snapshot immediately
    // before this refresh. Metadata watermarks describe MV creation and become
    // stale after the first CDF refresh, so reconstruct that pre-refresh snapshot:
    // changed sources start at their consumed version, while unchanged sources
    // are already at their current version.
    lazy val sourceSnapshotWatermarks: Map[String, ChangeWatermark] =
      if (cdfChangeBatches.nonEmpty) {
        val current = propagation.currentWatermarks(spark, meta.sourceTables)
        val changed = cdfChangeBatches
          .groupBy(_.baseTable)
          .map { case (source, batches) =>
            source -> ChangeWatermark.DeltaVersion(batches.map(_.startVersionExclusive).min)
          }
        current ++ changed
      } else {
        sourceWatermarks
      }

    lazy val cdfBatchVerdicts: Map[String, BatchVerdict] =
      cdfChangeBatches
        .groupBy(_.baseTable)
        .map { case (source, batches) =>
          val startVersion = batches.map(_.startVersionExclusive).min
          val verdict =
            try DeltaCommitClassifier.classify(spark, source, startVersion)
            catch { case _: Throwable => BatchVerdict.Replace }
          source -> verdict
        }

    if (
      FeatureGate.noopFastExitEnabled(spark) &&
      meta.refreshType != RefreshTypeCode.FullRefresh &&
      cdfChangeBatches.size == changeBatches.size &&
      cdfChangeBatches.nonEmpty &&
      cdfBatchVerdicts.values.forall(_ == BatchVerdict.Noop)
    ) {
      propagation.markConsumed(spark, viewNameStr, changeBatches)
      logInfo(
        s"[openivm-mv] refresh view='${sqlIdent(name)}' refresh_type='${meta.refreshTypeName}' " +
          "outcome='noop_fast_exit'"
      )
      emitEnd("noop_fast_exit", meta.refreshTypeName, changeBatches.size)
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
        s"pending_deltas=${changeBatches.size} source_tables=${meta.sourceTables.mkString(",")}"
    )
    RefreshPerf.emit(
      refreshId,
      viewLabel,
      "deltas_resolved",
      s"refresh_type='${meta.refreshTypeName}' pending_deltas=${changeBatches.size}"
    )
    profile.appendStep(
      "generate_refresh_sql.dispatch",
      s"refresh_type=${meta.refreshTypeName};pending_deltas=${changeBatches.size}",
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
      emitEnd("schema_drift", meta.refreshTypeName, changeBatches.size)
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

    def verdictForSource(source: String): Option[BatchVerdict] =
      cdfBatchVerdicts
        .get(source)
        .orElse {
          val short = source.split("\\.").last
          cdfBatchVerdicts.collectFirst { case (candidate, verdict) if candidate.split("\\.").last == short => verdict }
        }

    lazy val stagingBatchShapes: Map[String, DeltaShape] =
      changeBatches
        .collect { case b: StagingChangeBatch => b }
        .map { batch =>
          val insertOnly = batch.deltas.nonEmpty && batch.deltas.forall(_.opType == StagingDelta.OpTypes.Insert)
          batch.baseTable -> (if (insertOnly) DeltaShape.InsertOnly else DeltaShape.General)
        }
        .toMap

    def stagingShapeForSource(source: String): Option[DeltaShape] =
      stagingBatchShapes
        .get(source)
        .orElse {
          val short = source.split("\\.").last
          stagingBatchShapes.collectFirst { case (candidate, shape) if candidate.split("\\.").last == short => shape }
        }

    lazy val sourceDeltaShape: Map[String, DeltaShape] =
      if (cdfChangeBatches.nonEmpty) {
        meta.sourceTables.map { source =>
          source -> verdictForSource(source).map(DeltaCommitClassifier.shapeOf).getOrElse(DeltaShape.Unchanged)
        }.toMap
      } else if (stagingBatchShapes.nonEmpty) {
        meta.sourceTables.map { source =>
          source -> stagingShapeForSource(source).getOrElse(DeltaShape.Unchanged)
        }.toMap
      } else Map.empty

    lazy val batchInsertOnly: Boolean =
      sourceDeltaShape.nonEmpty &&
        sourceDeltaShape.values.exists(_ == DeltaShape.InsertOnly) &&
        sourceDeltaShape.values.forall(_ != DeltaShape.General)

    def downstreamSourceKeysForThisMv: Set[String] = {
      val mvShortName = name.identifier
      allMvsCached
        .filter(other => metaName(other.name) != metaName(name))
        .filter(_.sourceTables.exists(_.split("\\.").last == mvShortName))
        .flatMap(_.sourceTables.filter(_.split("\\.").last == mvShortName))
        .toSet
    }

    // Base-table inserts are not sufficient to prove a view delta monotone:
    // an outer join can retract a NULL-padded row, and multi-source joins can
    // generate cross terms. Restrict insert-only aggregate compilation to the
    // single-source, join-free shape whose output is monotone under inserts.
    // Downstream consumers do not affect this proof; they only determine
    // whether the cascade delta must remain materialized.
    lazy val insertOnlyAggregateShapeCandidate: Boolean =
      !propagation.requiresDmlInterception &&
        meta.refreshType == RefreshTypeCode.AggregateGroup &&
        meta.sourceTables.size == 1 &&
        !meta.queryHasJoin

    // Commit metrics classify most insert-only MERGEs without reading CDF.
    // For an otherwise eligible aggregate, retain a bounded fallback
    // for older Delta writers that omit those metrics. Failure is conservative.
    lazy val cdfRowsInsertOnly: Boolean =
      insertOnlyAggregateShapeCandidate &&
        !batchInsertOnly &&
        cdfChangeBatches.nonEmpty &&
        cdfChangeBatches.size == changeBatches.size &&
        profile.timeStep(
          "generate_refresh_sql.cdf_insert_only_probe",
          s"sources=${cdfChangeBatches.map(_.baseTable).distinct.size}"
        ) {
          RefreshPerf.timePhase(refreshId, viewLabel, "cdf_insert_only_probe") {
            cdfBatchesContainOnlyInserts(spark, cdfChangeBatches)
          }
        }

    lazy val insertOnlyAggregate: Boolean =
      insertOnlyAggregateShapeCandidate &&
        (batchInsertOnly || cdfRowsInsertOnly) &&
        changeBatches.nonEmpty

    // CDF consumers discover an upstream MV through its Delta change feed.
    // Only a grouped aggregate with no consumer may inline and discard its
    // durable cascade delta. Non-terminal aggregates still use the same
    // insert-only MIN/MAX merge, but retain OpenIVM's signed cascade output.
    lazy val terminalInsertOnlyAggregate: Boolean =
      insertOnlyAggregate && downstreamSourceKeysForThisMv.isEmpty

    profile.appendStep(
      "generate_refresh_sql.eligibility",
      s"batch_insert_only=$batchInsertOnly;cdf_rows_insert_only=$cdfRowsInsertOnly;" +
        s"insert_only_aggregate_shape_candidate=$insertOnlyAggregateShapeCandidate;" +
        s"insert_only_aggregate=$insertOnlyAggregate;" +
        s"terminal_insert_only_aggregate=$terminalInsertOnlyAggregate;" +
        s"requires_dml_interception=${propagation.requiresDmlInterception};" +
        s"source_delta_shape=${sourceDeltaShape.toSeq.sortBy(_._1).mkString("|")};" +
        s"downstream_sources=${downstreamSourceKeysForThisMv.toSeq.sorted.mkString("|")}",
      0L
    )

    // -----------------------------------------------------------------------
    // FullRefresh path — recompute INSERT OVERWRITE from the live tables.
    // A REPLACE/OVERWRITE/TRUNCATE on any source invalidates incremental
    // semantics for THIS batch, so route it through a full recompute (the MV
    // stays incremental for subsequent append batches). Classifier consulted
    // once; conservative (failure => treat as Replace).
    lazy val replaceBatch: Boolean =
      cdfBatchVerdicts.values.exists(_ == BatchVerdict.Replace)
    if (meta.refreshType == RefreshTypeCode.FullRefresh || replaceBatch) {
      if (meta.refreshType != RefreshTypeCode.FullRefresh)
        logInfo(
          s"[openivm-mv] refresh view='${sqlIdent(name)}' outcome='replace_full_refresh' reason='source_overwritten'"
        )
      // An incremental MV (e.g. AGGREGATE_GROUP count-monoid) routed here by
      // `replaceBatch` has hidden bookkeeping columns (e.g. openivm_count_star)
      // in its data table, created from the openivm initial-load SQL. The raw
      // user query (`meta.querySql`) omits those columns, so an
      // `INSERT OVERWRITE ... SELECT *` from it trips
      // DELTA_INSERT_COLUMN_ARITY_MISMATCH. Use the compiled initial-load SQL
      // (which reproduces the hidden columns) for any non-FULL_REFRESH MV; a
      // genuinely FULL_REFRESH MV has no hidden columns and uses its body.
      val fullRefreshSql = {
        val persistedInitialLoad = meta.properties.get(MvMetadata.CompiledInitialLoadSqlKey).filter(_.nonEmpty)
        val cachedInitialLoad    = MvMetadata.anyCachedInitialLoadSql(meta.properties, meta.sourceSchemaFingerprint)
        val initialLoad          = persistedInitialLoad.orElse(cachedInitialLoad).getOrElse("")
        if (meta.refreshType != RefreshTypeCode.FullRefresh && initialLoad.nonEmpty)
          org.openivm.spark.compiler.LptsSparkDialect.translate(initialLoad)
        else meta.querySql
      }
      val input = AssemblyInput(
        refreshType = RefreshTypeCode.FullRefresh,
        refreshTypeName = "FULL_REFRESH",
        deltaSql = fullRefreshSql,
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
            postRefreshCleanup(spark, name, meta, changeBatches, viewNameStr, sqlLog, qlogOrder)
          }
        }
        emitEnd("full_refresh_executed", "FULL_REFRESH", changeBatches.size)
      } catch {
        case t: Throwable =>
          emitEnd("full_refresh_failed", "FULL_REFRESH", changeBatches.size)
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
    val compileCacheEnabled = FeatureGate.compileClassificationCacheEnabled(spark)
    val constraintFacts     = WorkloadFactsRegistry.forRefresh().discover(spark, meta.sourceTables)
    val cacheTierFacts = WorkloadFacts(
      forceViewDeltaCascade = !terminalInsertOnlyAggregate,
      assumeInsertOnly = insertOnlyAggregate ||
        (FeatureGate.windowRunningIncrementalEnabled(spark) &&
          meta.refreshType == RefreshTypeCode.WindowPartition && batchInsertOnly),
      deltaShape = sourceDeltaShape,
      fkRelations = constraintFacts.fkRelations,
      uniqueKeys = constraintFacts.uniqueKeys,
      scd2RangeJoinAccel = FeatureGate.scd2RangeJoinAccelEnabled(spark),
      declareRelyFk = FeatureGate.declareRelyFkEnabled(spark)
    )
    val compileCacheTier                            = MvMetadata.compileCacheTier(cacheTierFacts)
    var refreshProperties                           = meta.properties
    var observedCompileFacts: Option[WorkloadFacts] = None
    def refreshCompileFacts(): WorkloadFacts = {
      val statsFacts = SparkDeltaStatsService
        .forRefresh()
        .workloadFactsFor(spark, meta.sourceTables, changeBatches)
      val facts = statsFacts.copy(
        deltaShape = sourceDeltaShape,
        fkRelations = constraintFacts.fkRelations,
        uniqueKeys = constraintFacts.uniqueKeys,
        declareRelyFk = FeatureGate.declareRelyFkEnabled(spark),
        runningWindowIncremental = FeatureGate.windowRunningIncrementalEnabled(spark),
        scd2RangeJoinAccel = FeatureGate.scd2RangeJoinAccelEnabled(spark),
        forceViewDeltaCascade = !terminalInsertOnlyAggregate,
        assumeInsertOnly = insertOnlyAggregate ||
          (FeatureGate.windowRunningIncrementalEnabled(spark) &&
            meta.refreshType == RefreshTypeCode.WindowPartition && batchInsertOnly)
      )
      observedCompileFacts = Some(facts)
      facts
    }

    // Reuse only the schema/tier-keyed, shape-stable compiled SQL.  The cached
    // text is intentionally NOT rewritten SQL: every REFRESH below still calls
    // SparkRefreshRewriter.rewrite, which recreates the per-refresh
    // `openivm_old_*` / `openivm_new_*` snapshot temp views and substitutes a
    // fresh view-delta path before execution.
    val cachedCompiledSql =
      if (compileCacheEnabled)
        MvMetadata.cachedCompiledSql(refreshProperties, meta.sourceSchemaFingerprint, compileCacheTier)
      else None
    val cachedInitialLoadSql =
      if (compileCacheEnabled) {
        MvMetadata
          .cachedInitialLoadSql(refreshProperties, meta.sourceSchemaFingerprint, compileCacheTier)
          .getOrElse("")
      } else ""
    val compileCacheHit = cachedCompiledSql.isDefined
    val compiled = profile.timeStep(
      "generate_refresh_sql.compile",
      s"compile_cache_hit=$compileCacheHit;compile_cache_tier=$compileCacheTier"
    ) {
      RefreshPerf.timePhase(
        refreshId,
        viewLabel,
        "compile",
        s"compile_cache_hit=$compileCacheHit compile_cache_tier=$compileCacheTier"
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
            val compiler     = OpenIvmCompilers.forSession(spark)
            val compileFacts = refreshCompileFacts()
            val fresh = compiler.compile(
              CompileRequest(
                viewName = name.table,
                viewSql = meta.querySql,
                sources = compileSchemas,
                sourceQualifiedNames = shortToQual,
                facts = compileFacts
              )
            )
            if (compileCacheEnabled && fresh.sql.nonEmpty) {
              val backfilled = refreshProperties ++
                MvMetadata.compiledProperties(
                  meta.sourceSchemaFingerprint,
                  compileCacheTier,
                  fresh.sql,
                  fresh.initialLoadSql,
                  fresh.refreshType,
                  fresh.refreshTypeName
                )
              try {
                MvCatalog.updateProperties(spark, name, backfilled)
                refreshProperties = backfilled
              } catch {
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

    val byTable                                        = changeBatches.groupBy(_.baseTable)
    val tempViewShortNames                             = scala.collection.mutable.ArrayBuffer[String]()
    var fusedScratchView: Option[String]               = None
    var fusedScratchRecordedForCascade: Boolean        = false
    var materializedWindowAffectedView: Option[String] = None
    var cascadeProducedChanges: Boolean                = true

    IvmDmlInterceptorRule.bypass.set(true)
    try {
      // Register a delta temp view for every source table.  Tables that have
      // pending staging deltas get a real view; tables with no pending deltas
      // get an empty view so that multi-source compiled SQL (e.g. UNION DISTINCT
      // across two tables) can reference all delta views without a NOT_FOUND error.
      profile.timeStep("metadata_pre_sql", "phase=register_views") {
        RefreshPerf.timePhase(refreshId, viewLabel, "register_views") {
          for (qualTable <- meta.sourceTables) {
            val schema       = freshSchemas(qualTable)
            val tableBatches = byTable.getOrElse(qualTable, Seq.empty)
            val t0           = System.nanoTime()
            val viewSql =
              try {
                propagation.registerSourceDeltaView(spark, qualTable, schema, tableBatches)
              } finally {
                val ms = (System.nanoTime() - t0) / 1000000L
                sqlLog.record(
                  category = "register_source_delta",
                  stmtOrder = qlogOrder.getAndIncrement(),
                  attemptIdx = 0,
                  stmtKind = "temp_view",
                  sql = propagation.buildSourceDeltaViewSql(qualTable, schema, tableBatches),
                  durationMs = ms
                )
              }
            // viewSql is the exact SQL the impl executed (used by impl-specific diagnostics).
            val _ = viewSql
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
                    s"deltas=${tableBatches.size} total=${counts.getLong(0)} " +
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

      val unifiedIntelligenceEnabled = FeatureGate.unifiedRefreshIntelligenceEnabled(spark)
      val runtimeDeltaSizeForDecision =
        if (unifiedIntelligenceEnabled) {
          val rowsBySource = meta.sourceTables.map { qualTable =>
            val deltaView = StagingDeltaView.deltaViewName(qualTable)
            val rows = spark
              .sql(s"SELECT COUNT(*) FROM `${deltaView.replace("`", "``")}`")
              .head()
              .getLong(0)
            qualTable -> rows
          }.toMap
          Some(RuntimeDeltaSize(rowsBySource))
        } else None

      if (unifiedIntelligenceEnabled || FeatureGate.costModelEnabled(spark)) {
        val facts    = observedCompileFacts.getOrElse(refreshCompileFacts())
        val estimate = RefreshCostModel.estimate(facts)
        val decision = RefreshIntelligence.decide(facts, estimate, runtimeDeltaSizeForDecision)
        val intelligenceProps =
          (if (FeatureGate.costModelEnabled(spark)) Map(MvMetadata.LastCostModelHintKey -> estimate.hint)
           else Map.empty[String, String]) ++
            (if (unifiedIntelligenceEnabled) Map(MvMetadata.RefreshDecisionKey -> decision.toJson)
             else Map.empty[String, String])
        if (intelligenceProps.nonEmpty) {
          val updated = refreshProperties ++ intelligenceProps
          try {
            MvCatalog.updateProperties(spark, name, updated)
            refreshProperties = updated
          } catch {
            case t: Throwable =>
              logWarning(
                s"[openivm-mv] refresh view='${sqlIdent(name)}' refresh_intelligence_property_update_failed: " +
                  s"${t.getClass.getName}: ${t.getMessage}"
              )
          }
        }
        if (unifiedIntelligenceEnabled) {
          logInfo(
            s"[openivm-mv] refresh view='${sqlIdent(name)}' refresh_decision='${decision.route.name}' " +
              s"reasons='${decision.reasons.mkString(",")}' cost_hint='${estimate.hint}'"
          )
        }
      }

      if (FeatureGate.runtimeEmptyDeltaSkipEnabled(spark)) {
        val nonEmptySources = runtimeDeltaSizeForDecision match {
          case Some(size) => size.rowsBySource.collect { case (source, rows) if rows > 0L => source }.toSeq
          case None =>
            meta.sourceTables.flatMap { qualTable =>
              val deltaView = StagingDeltaView.deltaViewName(qualTable)
              val hasRows = spark
                .sql(s"SELECT 1 FROM `${deltaView.replace("`", "``")}` LIMIT 1")
                .head(1)
                .nonEmpty
              if (hasRows) Some(qualTable) else None
            }
        }
        if (nonEmptySources.isEmpty) {
          profile.appendStep(
            "runtime_empty_delta_skip",
            s"sources=${meta.sourceTables.mkString(",")};signal=limit1_empty",
            0L
          )
          RefreshPerf.emit(
            refreshId,
            viewLabel,
            "fast_path",
            "outcome='runtime_empty_delta_skip' signal='limit1_empty'"
          )
          logInfo(
            s"[openivm-mv] refresh view='${sqlIdent(name)}' outcome='runtime_empty_delta_skip' " +
              "reason='all_source_delta_views_empty'"
          )
          consumeRefreshChangesWithoutMvWrite(spark, viewNameStr, changeBatches)
          emitEnd("runtime_empty_delta_skip", meta.refreshTypeName, changeBatches.size)
          return Seq.empty
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

      // Workload-aware insert-only fast path. For a SIMPLE_PROJECTION on the
      // recompute path (DELETE by openivm_left_key + recompute), when this batch
      // changes NO existing MV row, openivm's view-delta is purely net-new rows,
      // so the correct refresh is to INSERT the view-delta and SKIP the DELETE +
      // recompute tail — which otherwise deletes+recomputes the entire
      // LEFT-JOIN-key group (e.g. a whole `sk_company_id`) for a few appended
      // fact rows, a near-FULL recompute.
      //
      // "Changes no existing MV row" is proven by THREE conditions, checked at
      // the use site:
      //   (1) the view-delta has no negative multiplicities (`!hasNegativesHere`)
      //       — an INNER-side DELETE/UPDATE retracts rows ⇒ negatives;
      //   (2) no changed source is on the NULL-producing side of an outer join
      //       (`!batchTouchesOuterNullableSource`) — an insert there re-affects
      //       existing rows (NULL→value) which openivm does NOT emit as a negative;
      //   (3) no source was overwritten/replaced (`!batchHasReplace`) — a REPLACE
      //       invalidates incremental semantics wholesale.
      // The classifier is consulted ONLY for (3); a MERGE/append commit (e.g. a
      // dimension MV refreshing) does not block the fast path, because the
      // view-delta sign (1) is the authoritative signal for the FACT.
      lazy val batchHasReplace: Boolean =
        hasReplacementBatch(changeBatches, cdfBatchVerdicts.values)

      // True when a changed source is on the NULL-producing (optional) side of an
      // outer join in the MV body. An INSERT there re-affects EXISTING MV rows
      // (e.g. a previously NULL-extended left row gains a right match), which
      // openivm handles via the DELETE + recompute and which a plain insert of
      // the view-delta would get wrong. Conservative: any RIGHT/FULL join (whose
      // nullable side is harder to pin down by name) disables the fast path.
      lazy val batchTouchesOuterNullableSource: Boolean = {
        val body = meta.querySql
        if ("(?i)(RIGHT|FULL)\\s+(OUTER\\s+)?JOIN".r.findFirstIn(body).isDefined) true
        else {
          val nullable =
            "(?i)LEFT\\s+(?:OUTER\\s+)?JOIN\\s+`?\"?([\\w.]+)`?\"?".r
              .findAllMatchIn(body)
              .map(_.group(1).split("\\.").last.toLowerCase)
              .toSet
          nullable.nonEmpty && changeBatches.exists { b =>
            nullable.contains(b.baseTable.split("\\.").last.toLowerCase)
          }
        }
      }

      lazy val selectiveBroadcastTables: Seq[SparkRefreshRewriter.SelectiveBroadcastTable] =
        if (!FeatureGate.selectiveBroadcastEnabled(spark)) Seq.empty
        else {
          val key = "spark.sql.autoBroadcastJoinThreshold"
          val thresholdBytes = FeatureGate.adaptiveBroadcastThresholdBytes(
            spark.sparkContext.getConf,
            spark.conf.getOption(key).flatMap(v => scala.util.Try(v.toLong).toOption)
          )
          val stats = SparkDeltaStatsService.forRefresh()
          freshSchemas.keys.toSeq.flatMap { qualifiedName =>
            scala.util.Try(stats.statsFor(spark, qualifiedName)).toOption.flatMap { sourceStats =>
              val sizeBytes = sourceStats.tableStats.sizeBytes
              if (sizeBytes <= thresholdBytes)
                Some(
                  SparkRefreshRewriter.SelectiveBroadcastTable(
                    shortName = qualifiedName.split("\\.").last,
                    qualifiedName = qualifiedName,
                    sizeBytes = sizeBytes
                  )
                )
              else None
            }
          }
        }

      lazy val skewFanoutDeltaBroadcasts: Seq[SparkRefreshRewriter.SkewFanoutDeltaBroadcast] =
        if (!FeatureGate.skewFanoutEnabled(spark)) Seq.empty
        else {
          val statsFacts = SparkDeltaStatsService
            .forRefresh()
            .workloadFactsFor(spark, meta.sourceTables, changeBatches)
          SparkRefreshRewriter.planSkewFanoutDeltaBroadcasts(
            statsFacts,
            maxDeltaRows = FeatureGate.skewFanoutNarrowDeltaRows(spark.sparkContext.getConf),
            maxOverlapRatio = FeatureGate.skewFanoutNarrowOverlapRatio(spark.sparkContext.getConf)
          )
        }

      def refreshPostProcess(sql: String): String = {
        val translated = LptsSparkDialect.translate(sql)
        val withSelectiveBroadcast =
          SparkRefreshRewriter.injectSelectiveBroadcastHints(translated, selectiveBroadcastTables)
        val withSkewFanout =
          SparkRefreshRewriter.injectSkewFanoutBroadcastHints(withSelectiveBroadcast, skewFanoutDeltaBroadcasts)
        if (FeatureGate.scd2RangeAccelEnabled(spark))
          SparkRefreshRewriter.injectScd2RangeAcceleration(withSkewFanout)
        else
          withSkewFanout
      }

      val uniqueJoinSimplifyEnabled = FeatureGate.uniqueJoinSimplifyEnabled(spark)
      val fkTermPruneEnabled        = FeatureGate.fkTermPruneEnabled(spark)
      val rewriteConstraintFacts =
        if (uniqueJoinSimplifyEnabled || fkTermPruneEnabled)
          WorkloadFactsRegistry.forRefresh().discover(spark, meta.sourceTables)
        else WorkloadConstraintFacts()

      val rewrittenBase = profile.timeStep(
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
            postProcess = refreshPostProcess,
            // Pass the user-facing column list for each source so the rewriter can
            // expand DuckDB-style `SELECT * EXCEPT (openivm_multiplicity, openivm_timestamp)`
            // into an explicit column list (Spark 3.5 does not support that syntax).
            sourceSchemas = freshSchemas.map { case (qual, schema) =>
              qual.split("\\.").last -> schema.fieldNames.toSeq
            },
            deltaShape = sourceDeltaShape,
            semiJoinPruneEnabled = FeatureGate.semiJoinPruneEnabled(spark),
            fkTermPruneEnabled = fkTermPruneEnabled,
            fkRelations = rewriteConstraintFacts.fkRelations,
            uniqueKeys = rewriteConstraintFacts.uniqueKeys,
            uniqueJoinSimplifyEnabled = uniqueJoinSimplifyEnabled,
            windowPartitionSingleDeleteMergeEnabled = FeatureGate.windowPartitionSingleDeleteMergeEnabled(spark),
            // Pass the short → qualified source name map so the rewriter can
            // expand `memory.main.<short>` to the fully-qualified Spark name
            // when the user's view body referenced a Hive-qualified table.
            // Live-source refs would otherwise hit DELTA_TABLE_NOT_FOUND because
            // Spark would resolve `<short>` against the current_schema.
            sourceQualifiedNames = shortToQual,
            sourceSnapshotVersions = sourceSnapshotWatermarks.collect {
              case (source, ChangeWatermark.DeltaVersion(version)) => source -> version
            },
            mvVersionBeforeRefresh = Some(meta.lastVersion)
          )
        }
      }

      val rewritten =
        if (FeatureGate.regularNtermLiteralPruneEnabled(spark)) {
          val requests = rewrittenBase.statements.flatMap(SparkRefreshRewriter.regularNtermKeyRequests).distinct
          val literals = collectRegularNtermLiteralKeys(spark, requests, freshSchemas)
          if (literals.nonEmpty) {
            logInfo(
              s"[openivm-mv] refresh view='${sqlIdent(name)}' " +
                s"outcome='regular_nterm_literal_prune' key_sets='${literals.size}'"
            )
            rewrittenBase.copy(
              statements = rewrittenBase.statements.map(
                SparkRefreshRewriter.pruneRegularNtermWithLiteralKeys(_, literals)
              )
            )
          } else rewrittenBase
        } else rewrittenBase

      var preserveViewDeltaOnFailure = false
      def deletePathIfExists(pathStr: String): Unit =
        try {
          val hadoopPath = new Path(pathStr)
          val fs         = hadoopPath.getFileSystem(spark.sessionState.newHadoopConf())
          if (fs.exists(hadoopPath)) fs.delete(hadoopPath, /* recursive = */ true)
        } catch { case _: Throwable => () }

      val fuseEligible =
        FeatureGate.fuseScratchEnabled(spark) &&
          meta.refreshType == RefreshTypeCode.SimpleProjection &&
          rewritten.statements.nonEmpty

      try {
        lazy val hasSimpleProjectionDeletes = hasNegativeSimpleProjectionRows(spark, viewDeltaPath)

        val directAggregateMerge: Option[String] =
          if (terminalInsertOnlyAggregate && rewritten.statements.size == 2)
            SparkRefreshRewriter.inlineViewDeltaCtasIntoMerge(
              rewritten.statements.head,
              rewritten.statements(1),
              viewDeltaPath
            )
          else None

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
        def executeSqlRowsAt(sql: String, stmtIdx: Int): Array[Row] = {
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
                  val df = spark.sql(sql)
                  val r  = df.collect()
                  val ms = (System.nanoTime() - t0) / 1000000L
                  sqlLog.record("rewritten_stmt", qOrder, attempt - 1, kind, sql, ms)
                  // Diagnostic-only physical-plan capture (FeatureGate default OFF).
                  // After the timer + reusing the executed plan, so zero overhead
                  // unless explicitly enabled for a diagnostic refresh.
                  if (sqlLog.isActive && FeatureGate.explainCaptureEnabled(spark)) {
                    try
                      sqlLog.record(
                        "explain_formatted",
                        qOrder,
                        attempt - 1,
                        kind,
                        df.queryExecution.explainString(org.apache.spark.sql.execution.FormattedMode),
                        0L
                      )
                    catch { case _: Throwable => () }
                  }
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
        def executeSqlAt(sql: String, stmtIdx: Int): Unit = {
          executeSqlRowsAt(sql, stmtIdx)
          ()
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
        def logSkippedWindowStmt(stmtIdx: Int, kind: String): Unit = {
          advanceStmtCounterPast(stmtIdx)
          RefreshPerf.logStmt(refreshId, viewLabel, stmtIdx, kind, 0L)
          profile.appendStep(
            "execute_refresh_sql_stmt",
            s"statement=${stmtIdx + 1};stmt_kind=$kind",
            0L
          )
        }

        if (directAggregateMerge.isDefined) {
          advanceStmtCounterPast(0)
          RefreshPerf.logStmt(refreshId, viewLabel, 0, "view_delta_inlined", 0L)
          profile.appendStep(
            "execute_refresh_sql_stmt",
            "statement=1;stmt_kind=view_delta_inlined;terminal=true;insert_only=true",
            0L
          )
          RefreshPerf.emit(refreshId, viewLabel, "fast_path", "outcome='direct_insert_only_aggregate'")
          logInfo(
            s"[openivm-mv] refresh view='${sqlIdent(name)}' " +
              "outcome='direct_insert_only_aggregate' reason='terminal_insert_only_batch'"
          )
          withPlanTimeBroadcastDisabled {
            executeSqlAt(directAggregateMerge.get, 1)
          }
        } else if (meta.refreshType == RefreshTypeCode.SimpleProjection && rewritten.statements.nonEmpty) {
          // ── Scratch-CTAS fuse fast path ────────────────────────────────────
          //
          // openivm emits stmt[0] as `CREATE OR REPLACE TABLE delta.\`<path>\`
          // USING DELTA AS WITH … SELECT … openivm_multiplicity FROM …` and
          // stmt[1] as `INSERT INTO mv SELECT … FROM delta.\`<path>\` …`
          // (the value-equality DELETE MERGE is stmt[2] when negatives exist).
          //
          // Materialising the scratch as a cached global-temp view skips the
          // per-table Delta commit overhead AND keeps subsequent DELETE/INSERT
          // reads in-memory. If downstream MVs exist, the global-temp view name
          // is recorded as an MV_VIEW_DELTA staging ref so the cascade input is
          // still readable without writing the scratch to disk.
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
                    val rowCount = withPlanTimeBroadcastDisabled {
                      val d = spark.sql(selectBody)
                      d.createOrReplaceGlobalTempView(scratchView)
                      spark.catalog.cacheTable(s"global_temp.$scratchView")
                      // Force materialisation so the cache holds the rows before
                      // any negative-row probe / INSERT read. count() is the
                      // cheapest force-eval action that respects the cache.
                      spark.table(s"global_temp.$scratchView").count()
                    }
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
                      try spark.catalog.uncacheTable(s"global_temp.$scratchView")
                      catch { case _: Throwable => () }
                      try spark.catalog.dropGlobalTempView(scratchView)
                      catch { case _: Throwable => () }
                      logInfo(
                        s"[openivm-mv] refresh view='${sqlIdent(name)}' fused_fallback='${t.getClass.getSimpleName}: ${t.getMessage}'"
                      )
                      None
                  }
                }
            else None

          // The negative-row probe operates against either the cached temp
          // view (fuse) or the on-disk scratch (existing path).
          lazy val hasNegativesHere: Boolean = fusedView match {
            case Some(view) =>
              spark
                .sql(
                  s"SELECT 1 FROM ${StagingDeltaView.CachedViewDeltaRef.sqlRef(view)} " +
                    "WHERE `openivm_multiplicity` < 0 LIMIT 1"
                )
                .head(1)
                .nonEmpty
            case None => hasSimpleProjectionDeletes
          }

          if (fusedView.isEmpty) {
            withPlanTimeBroadcastDisabled {
              executeSqlAt(SparkRefreshRewriter.stripExecutionMarker(rewritten.statements.head), 0)
            }
            logViewDeltaDiagnostics(spark, name, viewDeltaPath, 0)
          }

          // Insert-only fast path SQL (see `batchInsertOnly`). Built only for a
          // proven append-only batch: the view-delta (already materialised by
          // stmt[0] / the fuse) is purely net-new rows, so the correct refresh is
          // to INSERT them (multiplicity-expanded) and skip the DELETE +
          // company-recompute. `None` (e.g. schema mismatch) falls back to the
          // general program. stmt[0] still runs, so cascade view-deltas are intact.
          val insertOnlyInsertSql: Option[String] =
            if (
              batchHasReplace || batchTouchesOuterNullableSource || hasNegativesHere ||
              // Only the recompute path (openivm_left_key DELETE + recompute) needs
              // this. Value-equality SIMPLE_PROJECTION MVs already handle insert-only
              // optimally via their no-negative-rows DELETE-skip + EXPLODE INSERT, so
              // leave them (and their telemetry) untouched.
              rewritten.statements.exists(SparkRefreshRewriter.isSimpleProjectionDeleteMerge)
            ) None
            else
              try {
                val src = fusedView match {
                  case Some(view) => StagingDeltaView.CachedViewDeltaRef.sqlRef(view)
                  case None       => s"delta.`${viewDeltaPath.replace("`", "``")}`"
                }
                val deltaCols =
                  (fusedView match {
                    case Some(view) => spark.table(s"global_temp.$view")
                    case None       => spark.read.format("delta").load(viewDeltaPath)
                  }).columns.toSet
                val mvCols = spark.table(sqlIdent(mergeTargetId)).columns.toSeq
                if (
                  deltaCols.contains("openivm_multiplicity") && mvCols.nonEmpty && mvCols.forall(deltaCols.contains)
                ) {
                  val colList = mvCols.map(c => s"`$c`").mkString(", ")
                  Some(
                    s"""|INSERT INTO ${sqlIdent(mergeTargetId)} ($colList)
                        |SELECT $colList FROM $src
                        |LATERAL VIEW EXPLODE(SEQUENCE(CAST(1 AS BIGINT), CAST(`openivm_multiplicity` AS BIGINT)))
                        |  _ivm_lv AS _ivm_i
                        |WHERE `openivm_multiplicity` > 0""".stripMargin
                  )
                } else None
              } catch { case _: Throwable => None }

          if (insertOnlyInsertSql.isDefined) {
            RefreshPerf.emit(refreshId, viewLabel, "fast_path", "outcome='insert_only_simple_projection'")
            logInfo(
              s"[openivm-mv] refresh view='${sqlIdent(name)}' " +
                "outcome='insert_only_simple_projection' reason='additive_view_delta' " +
                s"skipped_tail_stmts=${rewritten.statements.size - 1}"
            )
            withPlanTimeBroadcastDisabled {
              executeSqlAt(insertOnlyInsertSql.get, 1)
            }
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
                    SparkRefreshRewriter.substituteViewDeltaPath(sql, viewDeltaPath, s"global_temp`.`$view")
                  case None => sql
                }
                // Wrap EVERY openivm-emitted MERGE in the SIMPLE_PROJECTION
                // tail (not just the recompute INSERT MERGE). The value-equality
                // DELETE MERGE (`WHEN MATCHED THEN DELETE`) on a SCD2-shaped
                // MV body (e.g. `gold.fact_market_history` joining
                // `daily_market` × `dim_security` on
                // `CAST(dm_date AS TIMESTAMP) BETWEEN effective_timestamp
                // AND end_timestamp`) is rewritten by Delta into a plan that
                // builds an "affected target files" probe on the outer view
                // body, which trips Spark's 8 GiB `BroadcastExchangeExec`
                // cap. See `SparkRefreshRewriter.isMergeStatement` for the
                // full rationale.
                if (SparkRefreshRewriter.isMergeStatement(sqlForExec)) {
                  withPlanTimeBroadcastDisabled {
                    executeSqlAt(sqlForExec, stmtIdx)
                  }
                } else {
                  executeSqlAt(sqlForExec, stmtIdx)
                }
              }
            }
          }
        } else {
          val windowSuffixSql: Option[WindowSuffixSql] =
            if (
              FeatureGate.windowSuffixSkipEnabled(spark) &&
              meta.refreshType == RefreshTypeCode.WindowPartition &&
              batchInsertOnly
            )
              buildWindowSuffixSql(spark, meta, mergeTargetId, viewDeltaPath)
            else None
          val windowSuffixSafe =
            windowSuffixSql.isDefined && windowSuffixBatchIsStrictSuffix(spark, meta, mergeTargetId)
          val windowSuffixEmitsCascade =
            windowSuffixSafe && downstreamSourceKeysForThisMv.nonEmpty && meta.emitsCascadeViewDelta
          var windowSuffixCascadeWritten = false
          val boundedRankInsertSql: Option[String] =
            if (
              !windowSuffixSafe &&
              !propagation.requiresDmlInterception &&
              FeatureGate.boundedRankEnabled(spark) &&
              meta.refreshType == RefreshTypeCode.WindowPartition
            ) buildBoundedRankInsertSql(spark, meta, mergeTargetId)
            else None
          val rewrittenSql = rewritten.statements.map(SparkRefreshRewriter.stripExecutionMarker)
          val windowCascadeMergeShape: Option[WindowCascadeMergeShape] =
            if (
              FeatureGate.windowCascadeMergeEnabled(spark) &&
              meta.refreshType == RefreshTypeCode.WindowPartition &&
              !windowSuffixSafe &&
              boundedRankInsertSql.isEmpty
            )
              buildWindowCascadeMergeShape(
                mergeTargetId,
                rewrittenSql,
                viewDeltaPath
              )
            else None
          var windowSinglePassPlan: Option[WindowSinglePassPlan] =
            if (
              windowCascadeMergeShape.isEmpty &&
              FeatureGate.windowSinglePassReplaceEnabled(spark) &&
              meta.refreshType == RefreshTypeCode.WindowPartition &&
              !windowSuffixSafe &&
              boundedRankInsertSql.isEmpty
            )
              buildWindowSinglePassPlan(
                spark,
                meta,
                mergeTargetId,
                rewrittenSql,
                viewDeltaPath
              )
            else None
          var windowCascadeMergePlan: Option[WindowCascadeMergePlan] = None
          val windowCascadeCtasIdx =
            rewrittenSql.indexWhere(isRawWindowSnapshotCtas(_, mergeTargetId, viewDeltaPath))
          val windowInsertIdx = rewrittenSql.indexWhere(isWindowNewSnapshotInsertSql(_, mergeTargetId))
          def useCascadeFirstWindowPlan: Boolean =
            windowSinglePassPlan.exists(_.isInstanceOf[WindowSinglePassWrite]) &&
              !FeatureGate.windowSnapshotCacheEnabled(spark) &&
              windowCascadeCtasIdx > windowInsertIdx &&
              windowInsertIdx >= 0
          var deferredWindowTargetSql: Option[(Int, String)] = None

          def activateMaterializedWindowKeys(shape: WindowCascadeMergeShape, stmtIdx: Int): Unit = {
            executeSqlAt(s"CACHE TABLE ${quoteCol(shape.affectedViewName)}", stmtIdx)
            materializedWindowAffectedView = Some(shape.affectedViewName)
            windowSinglePassPlan =
              if (FeatureGate.windowSinglePassReplaceEnabled(spark))
                withPlanTimeBroadcastDisabled {
                  buildWindowSinglePassPlanFromMaterializedKeys(
                    spark,
                    meta,
                    mergeTargetId,
                    shape.affectedViewName,
                    shape.affectedCol,
                    shape.partitionCol,
                    viewDeltaPath
                  )
                }
              else None
            if (windowSinglePassPlan.isEmpty) {
              windowCascadeMergePlan = buildWindowCascadeMergePlan(spark, mergeTargetId, shape, viewDeltaPath)
            }
            logInfo(
              s"[openivm-mv] refresh view='${sqlIdent(name)}' " +
                s"outcome='window_affected_keys_materialized' key_view='${shape.affectedViewName}'"
            )
          }

          windowCascadeMergeShape.filter(_.affectedStmtIdx.isEmpty).foreach { shape =>
            executeSqlAt(shape.affectedCreateSql.get, shape.deleteStmtIdx)
            activateMaterializedWindowKeys(shape, shape.deleteStmtIdx)
          }

          rewritten.statements.zipWithIndex.foreach { case (stmt, idx) =>
            val sql = SparkRefreshRewriter.stripExecutionMarker(stmt)
            val skipDeleteMerge =
              SparkRefreshRewriter.isSimpleProjectionDeleteMerge(stmt) && !hasSimpleProjectionDeletes
            val skipWindowPartitionAux =
              windowSuffixSafe && isWindowPartitionAuxSql(sql, mergeTargetId)
            val skipWindowPartitionDelete =
              windowSuffixSafe && isWindowPartitionDeleteSql(sql, mergeTargetId)
            val skipWindowPartitionInsert =
              windowSuffixSafe && isWindowPartitionInsertSql(sql, mergeTargetId)
            val replaceWithWindowSuffixCascadeCtas =
              windowSuffixEmitsCascade && !windowSuffixCascadeWritten &&
                SparkRefreshRewriter.extractViewDeltaCtasBody(sql, viewDeltaPath).isDefined
            val skipBoundedRankAux =
              boundedRankInsertSql.isDefined && isWindowPartitionAuxSql(sql, mergeTargetId)
            val replaceWithBoundedRankInsert =
              boundedRankInsertSql.isDefined && isWindowPartitionInsertSql(sql, mergeTargetId)
            val skipWindowSinglePassDelete =
              isWindowPartitionDeleteSql(sql, mergeTargetId) &&
                (windowSinglePassPlan.isDefined || windowCascadeMergePlan.isDefined)
            val replaceWithWindowCascadeMerge =
              isWindowPartitionInsertSql(sql, mergeTargetId) && windowCascadeMergePlan.isDefined
            val replaceWithWindowSinglePassInsert =
              isWindowPartitionInsertSql(sql, mergeTargetId) && windowSinglePassPlan.isDefined
            val materializeWindowAffectedKeys =
              windowCascadeMergeShape.exists(_.affectedStmtIdx.contains(idx))
            val cacheWindowSinglePassSnapshot =
              isWindowNewSnapshotCreateSql(sql, mergeTargetId) &&
                windowSinglePassPlan.isDefined &&
                !useCascadeFirstWindowPlan &&
                FeatureGate.windowSnapshotCacheEnabled(spark)

            if (materializeWindowAffectedKeys) {
              executeSqlAt(sql, idx)
              val shape = windowCascadeMergeShape.get
              activateMaterializedWindowKeys(shape, idx)
            } else if (skipDeleteMerge) {
              logInfo(
                s"[openivm-mv] refresh view='${sqlIdent(name)}' " +
                  "outcome='skip_simple_projection_delete_merge' reason='no_negative_rows'"
              )
              logSkippedDeleteMerge(idx)
            } else if (replaceWithWindowSuffixCascadeCtas) {
              withPlanTimeBroadcastDisabled {
                executeSqlAt(windowSuffixSql.get.viewDeltaCtasSql, idx)
              }
              windowSuffixCascadeWritten = true
              logViewDeltaDiagnostics(spark, name, viewDeltaPath, idx)
            } else if (skipWindowPartitionAux) {
              logSkippedWindowStmt(idx, "window_suffix_aux_skipped")
            } else if (skipWindowPartitionDelete) {
              logSkippedWindowStmt(idx, "window_suffix_delete_skipped")
            } else if (skipWindowPartitionInsert) {
              RefreshPerf.emit(refreshId, viewLabel, "fast_path", "outcome='window_suffix_skip'")
              logInfo(
                s"[openivm-mv] refresh view='${sqlIdent(name)}' " +
                  "outcome='window_suffix_skip' reason='append_only_strict_suffix' " +
                  s"emits_cascade_view_delta='$windowSuffixEmitsCascade'"
              )
              if (windowSuffixEmitsCascade && !windowSuffixCascadeWritten) {
                withPlanTimeBroadcastDisabled {
                  executeSql(windowSuffixSql.get.viewDeltaCtasSql)
                }
                windowSuffixCascadeWritten = true
                logViewDeltaDiagnostics(spark, name, viewDeltaPath, idx)
              }
              withPlanTimeBroadcastDisabled {
                executeSqlAt(
                  if (windowSuffixEmitsCascade) windowSuffixSql.get.insertFromViewDeltaSql
                  else windowSuffixSql.get.insertSql,
                  idx
                )
              }
            } else if (skipBoundedRankAux) {
              logSkippedWindowStmt(idx, "bounded_rank_aux_skipped")
            } else if (replaceWithBoundedRankInsert) {
              RefreshPerf.emit(refreshId, viewLabel, "fast_path", "outcome='bounded_rank_topk'")
              logInfo(
                s"[openivm-mv] refresh view='${sqlIdent(name)}' " +
                  "outcome='bounded_rank_topk' reason='topk_rank_partition_recompute'"
              )
              withPlanTimeBroadcastDisabled {
                executeSqlAt(boundedRankInsertSql.get, idx)
              }
            } else if (skipWindowSinglePassDelete) {
              windowCascadeMergePlan match {
                case Some(_) =>
                  logInfo(
                    s"[openivm-mv] refresh view='${sqlIdent(name)}' " +
                      "outcome='window_target_delete_deferred' reason='reuse_materialized_cascade'"
                  )
                case None =>
                  logSkippedWindowStmt(idx, "window_replace_delete_skipped")
              }
            } else if (replaceWithWindowCascadeMerge) {
              logInfo(
                s"[openivm-mv] refresh view='${sqlIdent(name)}' " +
                  "outcome='window_target_insert_deferred' reason='reuse_materialized_cascade'"
              )
            } else if (replaceWithWindowSinglePassInsert) {
              windowSinglePassPlan.get match {
                case WindowSinglePassNoAffectedPartitions =>
                  logSkippedWindowStmt(idx, "window_replace_empty_skipped")
                case plan: WindowSinglePassWrite if useCascadeFirstWindowPlan =>
                  deferredWindowTargetSql = Some(idx -> plan.cascadeSql)
                  logInfo(
                    s"[openivm-mv] refresh view='${sqlIdent(name)}' " +
                      "outcome='window_target_write_deferred' reason='reuse_cascade_snapshot'"
                  )
                case plan: WindowSinglePassWrite =>
                  RefreshPerf.emit(refreshId, viewLabel, "fast_path", "outcome='window_single_pass_replace'")
                  logInfo(
                    s"[openivm-mv] refresh view='${sqlIdent(name)}' " +
                      "outcome='window_single_pass_replace' reason='small_literal_partition_set'"
                  )
                  withPlanTimeBroadcastDisabled {
                    executeSqlAt(plan.directSql, idx)
                  }
              }
            } else {
              // Apply the per-statement plan-time broadcast disable to BOTH
              // statement shapes that wrap the full MV body, matching the
              // intent documented at `withPlanTimeBroadcastDisabled` above:
              //   (a) the view-delta CTAS (refresh stmt[0]) emitted by openivm
              //       under `force_view_delta_cascade=true` for
              //       JOIN_DELTA / AGGREGATE_GROUP / WINDOW_PARTITION / GROUP_RECOMPUTE
              //       cascade producers — this is the on-disk
              //       `CREATE OR REPLACE TABLE delta.`<viewDeltaPath>` USING
              //       DELTA AS WITH … <multi-source view-delta join> …` shape;
              //   (b) every openivm-emitted MERGE (recompute INSERT MERGE,
              //       value-equality DELETE MERGE, aggregate UPSERT MERGE).
              //
              // Without (a), a JOIN_DELTA MV over a SCD2-shaped join (e.g.
              // `gold.fact_market_history` joining `daily_market` × `dim_security`
              // on `CAST(dm_date AS TIMESTAMP) BETWEEN effective_timestamp
              // AND end_timestamp`) explodes through SCD2 multiplicity at
              // execution and trips Spark's 8 GiB BroadcastExchangeExec cap —
              // a deterministic failure that `dbt-spark-livy`'s `retry_all`
              // then loops on for hours.
              //
              // Without (b), Delta's MERGE rewrite (specifically its
              // "find affected target files" subquery) can broadcast the
              // outer view body even when the USING source itself is tiny.
              val isViewDeltaCtas =
                SparkRefreshRewriter.extractViewDeltaCtasBody(stmt, viewDeltaPath).isDefined
              if (SparkRefreshRewriter.isMergeStatement(sql) || isViewDeltaCtas) {
                withPlanTimeBroadcastDisabled {
                  executeSqlAt(sql, idx)
                }
              } else {
                executeSqlAt(sql, idx)
              }
              if (isViewDeltaCtas && idx == windowCascadeCtasIdx) {
                val targetWritePending = windowCascadeMergePlan.nonEmpty || deferredWindowTargetSql.nonEmpty
                val hasNetChanges =
                  !targetWritePending || buildWindowNetChangeProbeSql(spark, mergeTargetId, viewDeltaPath).forall {
                    probeSql =>
                      withPlanTimeBroadcastDisabled {
                        val probeIdx = stmtCounter.getAndIncrement()
                        executeSqlRowsAt(probeSql, probeIdx).nonEmpty
                      }
                  }
                if (hasNetChanges) {
                  windowCascadeMergePlan.foreach { plan =>
                    RefreshPerf.emit(refreshId, viewLabel, "fast_path", "outcome='window_cascade_merge'")
                    logInfo(
                      s"[openivm-mv] refresh view='${sqlIdent(name)}' " +
                        "outcome='window_cascade_merge' reason='reuse_materialized_cascade'"
                    )
                    try {
                      withPlanTimeBroadcastDisabled {
                        executeSqlAt(plan.deleteSql, plan.deleteStmtIdx)
                      }
                      RefreshFailureInjection.maybeFailWindowCascadeInsert(spark)
                      withPlanTimeBroadcastDisabled {
                        executeSqlAt(plan.insertSql, plan.insertStmtIdx)
                      }
                    } catch {
                      case targetError: Throwable =>
                        try {
                          withPlanTimeBroadcastDisabled {
                            executeSqlAt(plan.deleteSql, plan.deleteStmtIdx)
                            executeSqlAt(plan.restoreSql, plan.insertStmtIdx)
                          }
                          logInfo(
                            s"[openivm-mv] refresh view='${sqlIdent(name)}' " +
                              "outcome='window_cascade_compensated' reason='target_write_failed'"
                          )
                        } catch {
                          case compensationError: Throwable =>
                            preserveViewDeltaOnFailure = true
                            targetError.addSuppressed(compensationError)
                            logError(
                              s"[openivm-mv] refresh view='${sqlIdent(name)}' " +
                                s"outcome='window_cascade_compensation_failed' cascade_path='$viewDeltaPath'",
                              compensationError
                            )
                        }
                        throw targetError
                    }
                  }
                  deferredWindowTargetSql.foreach { case (targetIdx, targetSql) =>
                    RefreshPerf.emit(refreshId, viewLabel, "fast_path", "outcome='window_cascade_first_replace'")
                    logInfo(
                      s"[openivm-mv] refresh view='${sqlIdent(name)}' " +
                        "outcome='window_cascade_first_replace' reason='reuse_materialized_cascade_snapshot'"
                    )
                    withPlanTimeBroadcastDisabled {
                      executeSqlAt(targetSql, targetIdx)
                    }
                  }
                } else {
                  cascadeProducedChanges = false
                  windowCascadeMergePlan.foreach { plan =>
                    logSkippedWindowStmt(plan.deleteStmtIdx, "window_cascade_noop_skipped")
                    logSkippedWindowStmt(plan.insertStmtIdx, "window_cascade_noop_skipped")
                  }
                  deferredWindowTargetSql.foreach { case (targetIdx, _) =>
                    logSkippedWindowStmt(targetIdx, "window_replace_noop_skipped")
                  }
                  RefreshPerf.emit(refreshId, viewLabel, "fast_path", "outcome='window_cascade_noop'")
                  logInfo(
                    s"[openivm-mv] refresh view='${sqlIdent(name)}' " +
                      "outcome='window_cascade_noop' reason='old_new_bags_equal'"
                  )
                }
                deferredWindowTargetSql = None
                materializedWindowAffectedView.foreach { affectedView =>
                  try spark.catalog.uncacheTable(affectedView)
                  catch { case _: Throwable => () }
                  try spark.catalog.dropTempView(affectedView)
                  catch { case _: Throwable => () }
                }
                materializedWindowAffectedView = None
              }
              if (cacheWindowSinglePassSnapshot) {
                executeSqlAt(s"CACHE TABLE `openivm_new_${mergeTargetId.table.replace("`", "``")}`", idx)
              }
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
        if (isCountMonoid(meta.refreshType) && !terminalInsertOnlyAggregate) {
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
        // Only matters for the intercept mode: under CDF the downstream MV
        // discovers our update via the MV data table's own change feed, so
        // there is no need to write an MV_VIEW_DELTA staging row.
        if (propagation.requiresDmlInterception && meta.emitsCascadeViewDelta && cascadeProducedChanges) {
          profile.timeStep("metadata_post_sql", "phase=record_cascade") {
            RefreshPerf.timePhase(refreshId, viewLabel, "record_cascade") {
              val triggerKeys: Set[String] = downstreamSourceKeysForThisMv
              val keysToRecord =
                if (triggerKeys.isEmpty && fusedScratchView.isEmpty) {
                  // Keep the legacy on-disk breadcrumb for non-fused refreshes.
                  Set(viewNameStr)
                } else triggerKeys
              val cascadeCacheOn = FeatureGate.fuseScratchCascadeCacheEnabled(spark)
              val stagingPathForCascade =
                fusedScratchView match {
                  case Some(view) if cascadeCacheOn => StagingDeltaView.CachedViewDeltaRef.encode(view)
                  case Some(view) if triggerKeys.nonEmpty =>
                    spark
                      .table(s"global_temp.$view")
                      .write
                      .format("delta")
                      .mode("overwrite")
                      .save(viewDeltaPath)
                    viewDeltaPath
                  case Some(_) => viewDeltaPath
                  case None    => viewDeltaPath
                }
              val txnTs = new Timestamp(System.currentTimeMillis())
              keysToRecord.foreach { triggerKey =>
                StagingCatalog.record(
                  spark,
                  StagingDelta(
                    baseTable = triggerKey,
                    opType = StagingDelta.OpTypes.MvViewDelta,
                    stagingPath = stagingPathForCascade,
                    txnTs = txnTs,
                    consumedBy = Seq.empty
                  )
                )
              }
              fusedScratchRecordedForCascade = fusedScratchView.isDefined && keysToRecord.nonEmpty
            }
          }
        }
      } catch {
        case t: Throwable =>
          // Best-effort cleanup of any partial view-delta on failure. Phase 7
          // orphan-sweep is the long-tail safety net.
          if (!preserveViewDeltaOnFailure) deletePathIfExists(viewDeltaPath)
          emitEnd(
            "incremental_failed",
            meta.refreshTypeName,
            changeBatches.size
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
          postRefreshCleanup(spark, name, meta, changeBatches, viewNameStr, sqlLog, qlogOrder)
        }
      }
      emitEnd(
        "incremental_executed",
        meta.refreshTypeName,
        changeBatches.size
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
        if (!fusedScratchRecordedForCascade) {
          try {
            // Unpersist the cached scratch DataFrame before dropping the global
            // temp view so the SparkSession's cache manager releases storage
            // memory immediately rather than waiting for GC. When a downstream
            // cascade staging row references it, StagingCatalog.pruneConsumed
            // owns this cleanup after every downstream MV has consumed the row.
            spark.catalog.uncacheTable(s"global_temp.$view")
          } catch { case _: Throwable => () }
          try spark.catalog.dropGlobalTempView(view)
          catch { case _: Throwable => () }
        }
      }
      materializedWindowAffectedView.foreach { affectedView =>
        try spark.catalog.uncacheTable(affectedView)
        catch { case _: Throwable => () }
        try spark.catalog.dropTempView(affectedView)
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

  private def cdfBatchesContainOnlyInserts(spark: SparkSession, batches: Seq[CdfChangeBatch]): Boolean =
    try {
      batches
        .groupBy(_.baseTable)
        .forall { case (source, sourceBatches) =>
          val startVersion = sourceBatches.map(_.startVersionExclusive).min + 1L
          val endVersion   = sourceBatches.map(_.endVersionInclusive).max
          spark.read
            .format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", startVersion)
            .option("endingVersion", endVersion)
            .table(source)
            .filter("_change_type IS NULL OR _change_type <> 'insert'")
            .head(1)
            .isEmpty
        }
    } catch {
      case NonFatal(error) =>
        logWarning("[openivm-mv] CDF insert-only probe failed; using the conservative refresh path", error)
        false
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

  private case class WindowSuffixShape(sourceShort: String, partitionCols: Seq[String], orderCol: String)

  private case class WindowSuffixSql(insertSql: String, viewDeltaCtasSql: String, insertFromViewDeltaSql: String)

  private case class WindowReplaceKeySet(targetCol: String, literals: Seq[String], hasNull: Boolean)

  private sealed trait WindowSinglePassPlan

  private case object WindowSinglePassNoAffectedPartitions extends WindowSinglePassPlan

  private case class WindowSinglePassWrite(
      directSql: String,
      cascadeSql: String
  ) extends WindowSinglePassPlan

  private case class WindowDeleteMerge(targetCol: String, sourceCol: String, subquery: String)

  private case class WindowCascadeMergeShape(
      affectedViewName: String,
      affectedStmtIdx: Option[Int],
      affectedCreateSql: Option[String],
      deleteStmtIdx: Int,
      insertStmtIdx: Int,
      partitionCol: String,
      affectedCol: String
  )

  private case class WindowCascadeMergePlan(
      deleteStmtIdx: Int,
      insertStmtIdx: Int,
      deleteSql: String,
      insertSql: String,
      restoreSql: String
  )

  private case class BoundedRankShape(
      sourceShort: String,
      partitionCols: Seq[String],
      orderCol: String,
      orderDirection: String,
      rankFunction: String,
      limit: Int
  )

  private val WindowReplaceMaxLiteralKeys    = 1000
  private val RegularNtermMaxLiteralKeys     = 10000
  private val WindowReplaceMaxPredicateBytes = 1024 * 1024

  private def buildWindowNetChangeProbeSql(
      spark: SparkSession,
      targetId: TableIdentifier,
      viewDeltaPath: String
  ): Option[String] = {
    val targetSchema = spark.table(MvCommandHelper.sqlIdent(targetId)).schema
    if (targetSchema.isEmpty || !targetSchema.forall(field => RowOrdering.isOrderable(field.dataType))) None
    else {
      val targetColumns    = targetSchema.fieldNames.toSeq.map(quoteCol).mkString(", ")
      val escapedDeltaPath = viewDeltaPath.replace("`", "``")
      Some(
        s"""|SELECT 1
            |FROM delta.`$escapedDeltaPath`
            |GROUP BY $targetColumns
            |HAVING SUM(CAST(`openivm_multiplicity` AS BIGINT)) <> 0
            |LIMIT 1""".stripMargin
      )
    }
  }

  private def collectRegularNtermLiteralKeys(
      spark: SparkSession,
      requests: Seq[SparkRefreshRewriter.RegularNtermKeyRequest],
      sourceSchemas: Map[String, StructType]
  ): Map[SparkRefreshRewriter.RegularNtermKeyRequest, Seq[String]] = {
    val schemasByShort = sourceSchemas.map { case (table, schema) =>
      table.split("\\.").last.toLowerCase(java.util.Locale.ROOT) -> schema
    }
    requests.flatMap { request =>
      val sourceSchema = schemasByShort.get(request.sourceShortName.toLowerCase(java.util.Locale.ROOT))
      val outputColumn = request.outputColumn.toLowerCase(java.util.Locale.ROOT)
      val sourceColumn = sourceSchema.flatMap { schema =>
        schema.fieldNames
          .filter { field =>
            val normalized = field.toLowerCase(java.util.Locale.ROOT)
            outputColumn == normalized || outputColumn.endsWith(s"_$normalized")
          }
          .sortBy(_.length)
          .lastOption
      }
      val safeCastType = request.castType
        .map(_.trim)
        .filter(_.matches("(?i)[A-Z][A-Z0-9_]*(?:\\s*\\([0-9,\\s]+\\))?"))
      sourceColumn.flatMap { column =>
        if (request.castType.isDefined && safeCastType.isEmpty) None
        else {
          val quotedColumn = quoteCol(column)
          val keyExpression = safeCastType
            .map(dataType => s"CAST($quotedColumn AS $dataType)")
            .getOrElse(quotedColumn)
          val deltaView = quoteCol(s"openivm_delta_${request.sourceShortName}")
          try {
            val keys = spark.sql(
              s"SELECT DISTINCT $keyExpression AS openivm_nterm_key FROM $deltaView " +
                s"WHERE $quotedColumn IS NOT NULL LIMIT ${RegularNtermMaxLiteralKeys + 1}"
            )
            keys.persist()
            try
              collectWindowReplaceKeySet(keys, "openivm_nterm_key", RegularNtermMaxLiteralKeys)
                .map(keySet => request -> keySet.literals)
            finally keys.unpersist()
          } catch {
            case NonFatal(e) =>
              logWarning(
                s"[openivm-mv] regular N-term key collection skipped for " +
                  s"${request.sourceShortName}.${request.outputColumn}: ${e.getMessage}"
              )
              None
          }
        }
      }
    }.toMap
  }

  private def buildWindowSinglePassPlan(
      spark: SparkSession,
      meta: MvMetadata,
      targetId: TableIdentifier,
      rewrittenStatements: Seq[String],
      viewDeltaPath: String
  ): Option[WindowSinglePassPlan] = {
    val insertIdx = rewrittenStatements.indexWhere(isWindowNewSnapshotInsertSql(_, targetId))
    if (insertIdx < 0) return None

    val deleteSqls = rewrittenStatements.take(insertIdx).filter(isWindowPartitionDeleteSql(_, targetId))
    if (deleteSqls.isEmpty) return None

    val keySets = deleteSqls.flatMap(collectWindowReplaceKeySet(spark, _))
    if (keySets.size != deleteSqls.size || keySets.isEmpty) return None
    buildWindowSinglePassPlanFromKeySets(spark, meta, targetId, keySets, viewDeltaPath)
  }

  private def buildWindowSinglePassPlanFromMaterializedKeys(
      spark: SparkSession,
      meta: MvMetadata,
      targetId: TableIdentifier,
      affectedViewName: String,
      affectedCol: String,
      targetCol: String,
      viewDeltaPath: String
  ): Option[WindowSinglePassPlan] = {
    val affectedRef = quoteCol(affectedViewName)
    val keys = collectWindowReplaceKeySet(
      spark.sql(s"SELECT $affectedCol FROM $affectedRef"),
      targetCol
    ).getOrElse(return None)
    buildWindowSinglePassPlanFromKeySets(spark, meta, targetId, Seq(keys), viewDeltaPath)
  }

  private def buildWindowSinglePassPlanFromKeySets(
      spark: SparkSession,
      meta: MvMetadata,
      targetId: TableIdentifier,
      keySets: Seq[WindowReplaceKeySet],
      viewDeltaPath: String
  ): Option[WindowSinglePassPlan] = {
    if (keySets.forall(keys => keys.literals.isEmpty && !keys.hasNull))
      return Some(WindowSinglePassNoAffectedPartitions)

    val predicates = keySets.flatMap { keys =>
      val inPred =
        if (keys.literals.nonEmpty) Some(s"${keys.targetCol} IN (${keys.literals.mkString(", ")})")
        else None
      val nullPred = if (keys.hasNull) Some(s"${keys.targetCol} IS NULL") else None
      (inPred ++ nullPred).toSeq match {
        case Nil      => None
        case Seq(one) => Some(one)
        case many     => Some(many.mkString("(", " OR ", ")"))
      }
    }
    if (predicates.isEmpty) return Some(WindowSinglePassNoAffectedPartitions)

    val predicate = predicates.mkString("(", " OR ", ")")
    if (predicate.length > WindowReplaceMaxPredicateBytes) return None

    val escapedLocation = meta.location.replace("`", "``")
    val view            = targetId.table.replace("`", "``")
    val targetRef       = MvCommandHelper.sqlIdent(targetId)
    val targetColumns   = spark.table(targetRef).columns.toSeq.map(quoteCol).mkString(", ")
    val directSql =
      s"""|INSERT INTO delta.`$escapedLocation`
          |REPLACE WHERE $predicate
          |SELECT * FROM `openivm_new_$view`""".stripMargin
    val escapedDeltaPath = viewDeltaPath.replace("`", "``")
    val cascadeSql =
      s"""|INSERT INTO delta.`$escapedLocation`
          |REPLACE WHERE $predicate
          |SELECT $targetColumns
          |FROM delta.`$escapedDeltaPath`
          |WHERE `openivm_multiplicity` > 0""".stripMargin
    Some(WindowSinglePassWrite(directSql, cascadeSql))
  }

  private def buildWindowCascadeMergeShape(
      targetId: TableIdentifier,
      rewrittenStatements: Seq[String],
      viewDeltaPath: String
  ): Option[WindowCascadeMergeShape] = {
    val insertIdx = rewrittenStatements.indexWhere(isWindowNewSnapshotInsertSql(_, targetId))
    if (insertIdx < 0) return None
    val cascadeIdx = rewrittenStatements.indexWhere(isRawWindowSnapshotCtas(_, targetId, viewDeltaPath))
    if (cascadeIdx <= insertIdx) return None

    val deleteStatements = rewrittenStatements.zipWithIndex
      .take(insertIdx)
      .filter { case (sql, _) => isWindowPartitionDeleteSql(sql, targetId) }
    if (deleteStatements.size != 1) return None
    val (deleteSql, deleteIdx) = deleteStatements.head
    val parsedDelete           = parseWindowDeleteMerge(deleteSql).getOrElse(return None)
    val existingAffectedView = rewrittenStatements.zipWithIndex
      .take(deleteIdx)
      .flatMap { case (sql, idx) =>
        parseWindowAffectedViewCreate(sql).map(viewName => viewName -> idx)
      }
      .find { case (viewName, _) =>
        parsedDelete.subquery
          .toUpperCase(java.util.Locale.ROOT)
          .contains(viewName.toUpperCase(java.util.Locale.ROOT))
      }
    val affectedViewName =
      existingAffectedView.map(_._1).getOrElse(s"openivm_affected_${targetId.table}")
    val affectedCreateSql =
      if (existingAffectedView.isDefined) None
      else
        Some(
          s"""CREATE OR REPLACE TEMPORARY VIEW ${quoteCol(affectedViewName)} AS
             |${parsedDelete.subquery}""".stripMargin
        )

    Some(
      WindowCascadeMergeShape(
        affectedViewName = affectedViewName,
        affectedStmtIdx = existingAffectedView.map(_._2),
        affectedCreateSql = affectedCreateSql,
        deleteStmtIdx = deleteIdx,
        insertStmtIdx = insertIdx,
        partitionCol = parsedDelete.targetCol,
        affectedCol = parsedDelete.sourceCol
      )
    )
  }

  private def buildWindowCascadeMergePlan(
      spark: SparkSession,
      targetId: TableIdentifier,
      shape: WindowCascadeMergeShape,
      viewDeltaPath: String
  ): Option[WindowCascadeMergePlan] = {
    val targetRef     = MvCommandHelper.sqlIdent(targetId)
    val targetColumns = spark.table(targetRef).columns.toSeq.map(quoteCol).mkString(", ")
    val escapedPath   = viewDeltaPath.replace("`", "``")
    val affectedRef   = quoteCol(shape.affectedViewName)
    val deleteSql =
      s"""|MERGE INTO $targetRef AS v
          |USING (
          |  SELECT DISTINCT ${shape.affectedCol}
          |  FROM $affectedRef
          |) AS d
          |ON v.${shape.partitionCol} IS NOT DISTINCT FROM d.${shape.affectedCol}
          |WHEN MATCHED THEN DELETE""".stripMargin
    val insertSql =
      s"""|INSERT INTO $targetRef
          |SELECT $targetColumns
          |FROM delta.`$escapedPath`
          |WHERE `openivm_multiplicity` > 0""".stripMargin
    val restoreSql =
      s"""|INSERT INTO $targetRef
          |SELECT $targetColumns
          |FROM delta.`$escapedPath`
          |WHERE `openivm_multiplicity` < 0""".stripMargin
    Some(
      WindowCascadeMergePlan(
        deleteStmtIdx = shape.deleteStmtIdx,
        insertStmtIdx = shape.insertStmtIdx,
        deleteSql = deleteSql,
        insertSql = insertSql,
        restoreSql = restoreSql
      )
    )
  }

  private def isWindowNewSnapshotInsertSql(sql: String, targetId: TableIdentifier): Boolean = {
    val upper = sql.trim.toUpperCase(java.util.Locale.ROOT)
    upper.startsWith(s"INSERT INTO ${MvCommandHelper.sqlIdent(targetId).toUpperCase(java.util.Locale.ROOT)}") &&
    upper.contains(s"FROM OPENIVM_NEW_${targetId.table.toUpperCase(java.util.Locale.ROOT)}")
  }

  private def collectWindowReplaceKeySet(spark: SparkSession, deleteMergeSql: String): Option[WindowReplaceKeySet] = {
    val parsed    = parseWindowDeleteMerge(deleteMergeSql).getOrElse(return None)
    val rawKeySql = rawWindowAffectedKeySql(parsed.subquery).getOrElse(return None)
    val probeSql =
      s"""SELECT * FROM (
         |$rawKeySql
         |) __openivm_window_replace_keys
         |LIMIT ${WindowReplaceMaxLiteralKeys + 1}""".stripMargin
    val df = spark.sql(probeSql)
    df.persist()
    try collectWindowReplaceKeySet(df, parsed.targetCol)
    finally df.unpersist()
  }

  private def collectWindowReplaceKeySet(
      df: org.apache.spark.sql.DataFrame,
      targetCol: String,
      maxLiteralKeys: Int = WindowReplaceMaxLiteralKeys
  ): Option[WindowReplaceKeySet] = {
    if (df.schema.fields.length != 1) return None
    val sourceCol = quoteCol(df.schema.fields.head.name)
    val stats = df
      .selectExpr(
        "COUNT(*) AS openivm_key_count",
        s"COALESCE(SUM(LENGTH(CAST($sourceCol AS STRING))), 0) AS openivm_key_bytes"
      )
      .head()
    if (
      stats.getAs[Long]("openivm_key_count") > maxLiteralKeys ||
      stats.getAs[Long]("openivm_key_bytes") > WindowReplaceMaxPredicateBytes
    ) return None

    val rows     = df.collect()
    val literals = scala.collection.mutable.ArrayBuffer.empty[String]
    var hasNull  = false
    rows.foreach { row =>
      if (row.isNullAt(0)) hasNull = true
      else
        literalSql(row.get(0)) match {
          case Some(lit) => literals += lit
          case None      => return None
        }
    }
    Some(WindowReplaceKeySet(targetCol, literals.distinct.toSeq, hasNull))
  }

  private def rawWindowAffectedKeySql(subquery: String): Option[String] = {
    val hasRecognizedSource =
      "(?is)\\bFROM\\s+`?OPENIVM_(?:DELTA|AFFECTED)_[A-Za-z0-9_]+`?".r.findFirstIn(subquery).isDefined
    val hasExpensiveOperator =
      "(?is)\\b(?:UNION|INTERSECT|EXCEPT|JOIN)\\b|\\b(?:GROUP|ORDER)\\s+BY\\b|\\bOVER\\s*\\(".r
        .findFirstIn(subquery)
        .isDefined
    if (!hasRecognizedSource || hasExpensiveOperator) return None

    val distinctSelect = "(?is)^\\s*SELECT\\s+DISTINCT\\s+".r
    distinctSelect.findPrefixMatchOf(subquery).map(m => "SELECT " + subquery.substring(m.end))
  }

  private def parseWindowAffectedViewCreate(sql: String): Option[String] = {
    val ident = """(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)"""
    val createRe =
      ("""(?is)^\s*CREATE\s+OR\s+REPLACE\s+TEMPORARY\s+VIEW\s+(""" + ident +
        """)\s+AS\s+SELECT\s+DISTINCT\b""").r
    createRe
      .findFirstMatchIn(sql)
      .map(m => stripSqlIdent(m.group(1)))
      .filter(_.toUpperCase(java.util.Locale.ROOT).startsWith("OPENIVM_AFFECTED_"))
  }

  private def parseWindowDeleteMerge(sql: String): Option[WindowDeleteMerge] = {
    val ident     = """(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)"""
    val usingOpen = "(?is)\\bUSING\\s*\\(".r.findFirstMatchIn(sql)
    val (sourceAlias, subquery, directSource, tail) = usingOpen match {
      case Some(using) =>
        val openIdx  = using.end - 1
        val closeIdx = matchingCloseParen(sql, openIdx)
        if (closeIdx < 0) return None
        val after   = sql.substring(closeIdx + 1)
        val aliasRe = ("(?is)^\\s+(?:AS\\s+)?(" + ident + ")\\s+ON\\s+").r
        val alias   = aliasRe.findFirstMatchIn(after).map(_.group(1)).getOrElse(return None)
        (stripSqlIdent(alias), sql.substring(openIdx + 1, closeIdx).trim, None, after)
      case None =>
        val directRe = ("(?is)\\bUSING\\s+(" + ident + ")\\s+(?:AS\\s+)?(" + ident + ")\\s+ON\\s+").r
        val direct   = directRe.findFirstMatchIn(sql).getOrElse(return None)
        val source   = direct.group(1)
        (stripSqlIdent(direct.group(2)), "", Some(source), sql.substring(direct.start))
    }

    val comparisonRe =
      ("(?is)(" + ident + ")\\.\\s*(" + ident + ")\\s*(?:<=>|IS\\s+NOT\\s+DISTINCT\\s+FROM)\\s*(" +
        ident + ")\\.\\s*(" + ident + ")").r
    comparisonRe.findFirstMatchIn(tail).flatMap { m =>
      val leftAlias  = stripSqlIdent(m.group(1))
      val rightAlias = stripSqlIdent(m.group(3))
      val columns =
        if (leftAlias.equalsIgnoreCase(sourceAlias)) Some(m.group(4).trim -> m.group(2).trim)
        else if (rightAlias.equalsIgnoreCase(sourceAlias)) Some(m.group(2).trim -> m.group(4).trim)
        else None
      columns.map { case (targetCol, sourceCol) =>
        val affectedSql = directSource.map(source => s"SELECT DISTINCT $sourceCol FROM $source").getOrElse(subquery)
        WindowDeleteMerge(targetCol, sourceCol, affectedSql)
      }
    }
  }

  private def matchingCloseParen(sql: String, openIdx: Int): Int = {
    var depth    = 0
    var i        = openIdx
    var inSingle = false
    var inTick   = false
    while (i < sql.length) {
      val c = sql.charAt(i)
      if (inSingle) {
        if (c == '\'' && i + 1 < sql.length && sql.charAt(i + 1) == '\'') i += 1
        else if (c == '\'') inSingle = false
      } else if (inTick) {
        if (c == '`') inTick = false
      } else {
        c match {
          case '\'' => inSingle = true
          case '`'  => inTick = true
          case '('  => depth += 1
          case ')' =>
            depth -= 1
            if (depth == 0) return i
          case _ =>
        }
      }
      i += 1
    }
    -1
  }

  private def literalSql(value: Any): Option[String] =
    value match {
      case s: String                 => Some("'" + s.replace("'", "''") + "'")
      case d: java.sql.Date          => Some("DATE '" + d.toString + "'")
      case t: java.sql.Timestamp     => Some("TIMESTAMP '" + t.toString.replace("'", "''") + "'")
      case b: java.lang.Boolean      => Some(if (b.booleanValue()) "TRUE" else "FALSE")
      case bd: java.math.BigDecimal  => Some(bd.toPlainString)
      case bd: scala.math.BigDecimal => Some(bd.bigDecimal.toPlainString)
      case n: java.lang.Byte         => Some(n.toString)
      case n: java.lang.Short        => Some(n.toString)
      case n: java.lang.Integer      => Some(n.toString)
      case n: java.lang.Long         => Some(n.toString)
      case n: java.lang.Float        => if (java.lang.Float.isFinite(n)) Some(n.toString) else None
      case n: java.lang.Double       => if (java.lang.Double.isFinite(n)) Some(n.toString) else None
      case _                         => None
    }

  private def buildWindowSuffixSql(
      spark: SparkSession,
      meta: MvMetadata,
      targetId: TableIdentifier,
      viewDeltaPath: String
  ): Option[WindowSuffixSql] =
    windowSuffixShape(meta).flatMap { shape =>
      try {
        val mvCols     = spark.table(MvCommandHelper.sqlIdent(targetId)).columns.toSeq
        val sourceCols = spark.table(meta.sourceTables.head).columns.toSeq
        if (
          mvCols.nonEmpty &&
          sourceCols.nonEmpty &&
          (shape.partitionCols :+ shape.orderCol).forall(c => mvCols.exists(_.equalsIgnoreCase(c))) &&
          sourceCols.forall(c => mvCols.exists(_.equalsIgnoreCase(c)))
        ) {
          val targetRef  = MvCommandHelper.sqlIdent(targetId)
          val colList    = mvCols.map(quoteCol).mkString(", ")
          val sourceList = sourceCols.map(quoteCol).mkString(", ")
          val partList   = shape.partitionCols.map(quoteCol).mkString(", ")
          val deltaRef   = s"`openivm_delta_${shape.sourceShort.replace("`", "``")}`"
          val orderCol   = quoteCol(shape.orderCol)
          val partExists = partitionMatch("openivm_suffix", "a", shape.partitionCols)
          val targetContextPartExists =
            partitionMatch("openivm_target_context", "a", shape.partitionCols)
          val maxExists = partitionMatch("openivm_suffix", "m", shape.partitionCols)
          replaceWindowSuffixSource(meta.querySql, "context_source").map { suffixBody =>
            def suffixQuery(selectList: String): String =
              s"""|WITH affected AS (
                  |  SELECT DISTINCT $partList
                  |  FROM $deltaRef
                  |  WHERE `openivm_multiplicity` > 0
                  |),
                  |current_max AS (
                  |  SELECT $partList, MAX($orderCol) AS `openivm_max_order`
                  |  FROM $targetRef
                  |  GROUP BY $partList
                  |),
                  |context_source AS (
                  |  SELECT $sourceList
                  |  FROM $targetRef openivm_target_context
                  |  WHERE EXISTS (SELECT 1 FROM affected a WHERE $targetContextPartExists)
                  |  UNION ALL
                  |  SELECT $sourceList
                  |  FROM $deltaRef
                  |  WHERE `openivm_multiplicity` > 0
                  |)
                  |SELECT $selectList
                  |FROM ($suffixBody) openivm_suffix
                  |WHERE EXISTS (SELECT 1 FROM affected a WHERE $partExists)
                  |  AND (
                  |    NOT EXISTS (SELECT 1 FROM current_max m WHERE $maxExists)
                  |    OR openivm_suffix.$orderCol > (
                  |      SELECT MAX(m.`openivm_max_order`) FROM current_max m WHERE $maxExists
                  |    )
                  |  )""".stripMargin
            val suffixSelect         = suffixQuery(colList)
            val suffixViewDeltaQuery = suffixQuery(s"$colList, CAST(1 AS INT) AS `openivm_multiplicity`")
            val escapedViewDeltaPath = viewDeltaPath.replace("`", "``")
            WindowSuffixSql(
              insertSql = s"INSERT INTO $targetRef ($colList)\n$suffixSelect",
              viewDeltaCtasSql = s"""|CREATE OR REPLACE TABLE delta.`$escapedViewDeltaPath` USING DELTA AS
                    |$suffixViewDeltaQuery""".stripMargin,
              insertFromViewDeltaSql = s"""|INSERT INTO $targetRef ($colList)
                    |SELECT $colList
                    |FROM delta.`$escapedViewDeltaPath`
                    |WHERE `openivm_multiplicity` > 0""".stripMargin
            )
          }
        } else None
      } catch { case _: Throwable => None }
    }

  private def windowSuffixBatchIsStrictSuffix(
      spark: SparkSession,
      meta: MvMetadata,
      targetId: TableIdentifier
  ): Boolean =
    windowSuffixShape(meta).exists { shape =>
      try {
        val targetRef = MvCommandHelper.sqlIdent(targetId)
        val deltaRef  = s"`openivm_delta_${shape.sourceShort.replace("`", "``")}`"
        val partList  = shape.partitionCols.map(quoteCol).mkString(", ")
        val orderCol  = quoteCol(shape.orderCol)
        val maxMatch  = partitionMatch("d", "m", shape.partitionCols)
        val hasBadSign = spark
          .sql(
            s"""SELECT 1
               |FROM $deltaRef
               |WHERE `openivm_multiplicity` IS NULL OR `openivm_multiplicity` <= 0
               |LIMIT 1""".stripMargin
          )
          .head(1)
          .nonEmpty
        if (hasBadSign) false
        else {
          val badPartition = spark
            .sql(
              s"""|SELECT 1
                  |FROM (
                  |  SELECT $partList,
                  |         MIN($orderCol) AS `openivm_min_order`,
                  |         SUM(CASE WHEN $orderCol IS NULL THEN 1 ELSE 0 END) AS `openivm_null_orders`
                  |  FROM $deltaRef
                  |  WHERE `openivm_multiplicity` > 0
                  |  GROUP BY $partList
                  |) d
                  |LEFT JOIN (
                  |  SELECT $partList, MAX($orderCol) AS `openivm_max_order`
                  |  FROM $targetRef
                  |  GROUP BY $partList
                  |) m
                  |ON $maxMatch
                  |WHERE d.`openivm_null_orders` > 0
                  |   OR (m.`openivm_max_order` IS NOT NULL AND NOT (d.`openivm_min_order` > m.`openivm_max_order`))
                  |LIMIT 1""".stripMargin
            )
            .head(1)
            .nonEmpty
          !badPartition
        }
      } catch { case _: Throwable => false }
    }

  private def buildBoundedRankInsertSql(
      spark: SparkSession,
      meta: MvMetadata,
      targetId: TableIdentifier
  ): Option[String] =
    boundedRankShape(meta).flatMap { shape =>
      try {
        val mvCols     = spark.table(MvCommandHelper.sqlIdent(targetId)).columns.toSeq
        val sourceCols = spark.table(meta.sourceTables.head).columns.toSeq
        if (
          mvCols.nonEmpty &&
          sourceCols.nonEmpty &&
          shape.partitionCols.nonEmpty &&
          (shape.partitionCols :+ shape.orderCol).forall(c => sourceCols.exists(_.equalsIgnoreCase(c)))
        ) {
          val targetRef   = MvCommandHelper.sqlIdent(targetId)
          val sourceRef   = quoteIdentPath(meta.sourceTables.head)
          val colList     = mvCols.map(quoteCol).mkString(", ")
          val sourceList  = sourceCols.map(quoteCol).mkString(", ")
          val partList    = shape.partitionCols.map(quoteCol).mkString(", ")
          val orderExpr   = s"${quoteCol(shape.orderCol)} ${shape.orderDirection}"
          val deltaRef    = s"`openivm_delta_${shape.sourceShort.replace("`", "``")}`"
          val baseMatch   = partitionMatch("openivm_base", "a", shape.partitionCols)
          val resultMatch = partitionMatch("openivm_bounded", "a", shape.partitionCols)
          replaceWindowSuffixSource(meta.querySql, "bounded_source").map { boundedBody =>
            s"""|INSERT INTO $targetRef ($colList)
                |WITH affected AS (
                |  SELECT DISTINCT $partList
                |  FROM $deltaRef
                |  WHERE `openivm_multiplicity` != 0
                |),
                |bounded_source AS (
                |  SELECT $sourceList
                |  FROM (
                |    SELECT openivm_base.*,
                |           ${shape.rankFunction}() OVER (PARTITION BY $partList ORDER BY $orderExpr) AS `__openivm_bound_rank`
                |    FROM $sourceRef openivm_base
                |    WHERE EXISTS (SELECT 1 FROM affected a WHERE $baseMatch)
                |  ) openivm_ranked
                |  WHERE `__openivm_bound_rank` <= ${shape.limit}
                |)
                |SELECT $colList
                |FROM ($boundedBody) openivm_bounded
                |WHERE EXISTS (SELECT 1 FROM affected a WHERE $resultMatch)""".stripMargin
          }
        } else None
      } catch { case _: Throwable => None }
    }

  private def windowSuffixShape(meta: MvMetadata): Option[WindowSuffixShape] = {
    val sql = meta.querySql
    if (
      meta.sourceTables.size != 1 ||
      "(?is)\\bJOIN\\b".r.findFirstIn(sql).isDefined ||
      "(?is)\\bLEAD\\s*\\(|\\bFOLLOWING\\b|\\bROWS\\s+BETWEEN\\s+CURRENT\\s+ROW\\s+AND\\b|\\bRANGE\\s+BETWEEN\\s+CURRENT\\s+ROW\\s+AND\\b".r
        .findFirstIn(sql)
        .isDefined
    ) return None

    val overSpecs = "(?is)\\bOVER\\s*\\((.*?)\\)".r.findAllMatchIn(sql).map(_.group(1)).toVector
    if (overSpecs.isEmpty) return None

    val parsedSpecs = overSpecs.flatMap { spec =>
      val m = "(?is)\\bPARTITION\\s+BY\\s+(.+?)\\s+ORDER\\s+BY\\s+(.+?)(?:\\bROWS\\b|\\bRANGE\\b|$)".r
        .findFirstMatchIn(spec.trim)
      m.flatMap { hit =>
        val parts = splitIdentifierList(hit.group(1))
        val order = splitIdentifierList(hit.group(2)).headOption
        order.map(o => parts -> o)
      }
    }
    if (parsedSpecs.size != overSpecs.size) return None

    val (parts, order) = parsedSpecs.head
    if (parts.isEmpty || parsedSpecs.exists { case (p, o) => p != parts || o != order }) None
    else {
      val upperOrder = order.toUpperCase(java.util.Locale.ROOT)
      if (upperOrder.contains(" DESC") || upperOrder.contains(" NULLS ")) None
      else Some(WindowSuffixShape(meta.sourceTables.head.split("\\.").last, parts, order.stripSuffix(" ASC").trim))
    }
  }

  private def boundedRankShape(meta: MvMetadata): Option[BoundedRankShape] = {
    val sql = meta.querySql
    if (meta.sourceTables.size != 1 || "(?is)\\bJOIN\\b".r.findFirstIn(sql).isDefined) return None

    val rankRe =
      """(?is)\b(ROW_NUMBER|RANK)\s*\(\s*\)\s+OVER\s*\((.*?)\)\s+AS\s+(`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)""".r
    val rankMatches = rankRe.findAllMatchIn(sql).toVector
    if (rankMatches.isEmpty) return None

    val parsed = rankMatches.flatMap { hit =>
      val spec = hit.group(2).trim
      val m = "(?is)\\bPARTITION\\s+BY\\s+(.+?)\\s+ORDER\\s+BY\\s+(.+?)(?:\\bROWS\\b|\\bRANGE\\b|$)".r
        .findFirstMatchIn(spec)
      m.flatMap { specHit =>
        val parts = splitIdentifierList(specHit.group(1))
        val order = parseSingleOrderKey(specHit.group(2))
        order.map { case (col, dir) =>
          (hit.group(1).toUpperCase(java.util.Locale.ROOT), parts, col, dir, stripSqlIdent(hit.group(3)))
        }
      }
    }
    if (parsed.size != rankMatches.size) return None

    val (fn, parts, orderCol, orderDir, alias) = parsed.head
    val sameWindow = parsed.forall { case (f, p, o, d, a) =>
      f == fn && p == parts && o == orderCol && d == orderDir && a == alias
    }
    if (!sameWindow || parts.isEmpty) return None

    val aliasPattern         = java.util.regex.Pattern.quote(alias)
    val backtickAliasPattern = java.util.regex.Pattern.quote(s"`$alias`")
    val limitRe              = (s"(?is)(?:`?$aliasPattern`?|$backtickAliasPattern)\\s*<=\\s*(\\d+)").r
    limitRe.findFirstMatchIn(sql).flatMap { m =>
      scala.util.Try(m.group(1).toInt).toOption.filter(_ > 0).map { limit =>
        BoundedRankShape(
          sourceShort = meta.sourceTables.head.split("\\.").last,
          partitionCols = parts,
          orderCol = orderCol,
          orderDirection = orderDir,
          rankFunction = if (fn == "ROW_NUMBER") "RANK" else fn,
          limit = limit
        )
      }
    }
  }

  private def splitIdentifierList(csv: String): Seq[String] =
    csv
      .split(",")
      .map(_.trim.stripPrefix("`").stripSuffix("`"))
      .filter(s => s.matches("[A-Za-z_][A-Za-z0-9_]*\\s*(?i:ASC)?"))
      .map(_.replaceAll("(?i)\\s+ASC\\s*$", ""))
      .toSeq

  private def parseSingleOrderKey(orderSql: String): Option[(String, String)] = {
    val parts = orderSql.split(",").map(_.trim).filter(_.nonEmpty)
    if (parts.length != 1) None
    else {
      val raw   = parts.head
      val upper = raw.toUpperCase(java.util.Locale.ROOT)
      if (upper.contains(" NULLS ")) None
      else {
        val direction =
          if (upper.endsWith(" DESC")) "DESC"
          else "ASC"
        val col = raw.replaceAll("(?i)\\s+(ASC|DESC)\\s*$", "").trim.stripPrefix("`").stripSuffix("`")
        if (col.matches("[A-Za-z_][A-Za-z0-9_]*")) Some(col -> direction) else None
      }
    }
  }

  private def stripSqlIdent(ident: String): String =
    ident.trim.stripPrefix("`").stripSuffix("`")

  private def quoteIdentPath(path: String): String =
    path.split("\\.").map(quoteCol).mkString(".")

  private def partitionMatch(leftAlias: String, rightAlias: String, partitionCols: Seq[String]): String =
    partitionCols
      .map(c => s"$leftAlias.${quoteCol(c)} <=> $rightAlias.${quoteCol(c)}")
      .mkString(" AND ")

  private def quoteCol(col: String): String =
    s"`${col.replace("`", "``")}`"

  private def replaceWindowSuffixSource(sql: String, replacement: String): Option[String] = {
    val fromRe =
      ("(?is)\\bFROM\\s+((?:`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)(?:\\.(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_]*))?)" +
        "(\\s+(?:AS\\s+)?[A-Za-z_][A-Za-z0-9_]*)?").r
    val matches = fromRe.findAllMatchIn(sql).toVector
    if (matches.size != 1) None
    else {
      val m      = matches.head
      val alias  = Option(m.group(2)).getOrElse("")
      val before = sql.substring(0, m.start)
      val after  = sql.substring(m.end)
      Some(s"${before}FROM $replacement$alias$after")
    }
  }

  private def isWindowPartitionAuxSql(sql: String, targetId: TableIdentifier): Boolean = {
    val upper = sql.trim.toUpperCase(java.util.Locale.ROOT)
    val view  = targetId.table.toUpperCase(java.util.Locale.ROOT)
    upper.startsWith(s"CREATE OR REPLACE TEMPORARY VIEW OPENIVM_OLD_$view") ||
    upper.startsWith(s"CREATE OR REPLACE TEMPORARY VIEW OPENIVM_NEW_$view") ||
    ((upper.startsWith("CREATE OR REPLACE TABLE DELTA.") || upper.startsWith("CREATE OR REPLACE TABLE DELTA.`")) &&
      upper.contains(s"FROM OPENIVM_OLD_$view") &&
      upper.contains(s"FROM OPENIVM_NEW_$view"))
  }

  private def isWindowNewSnapshotCreateSql(sql: String, targetId: TableIdentifier): Boolean = {
    val upper = sql.trim.toUpperCase(java.util.Locale.ROOT)
    upper.startsWith(
      s"CREATE OR REPLACE TEMPORARY VIEW OPENIVM_NEW_${targetId.table.toUpperCase(java.util.Locale.ROOT)}"
    )
  }

  private def isWindowPartitionDeleteSql(sql: String, targetId: TableIdentifier): Boolean = {
    val upper = sql.trim.toUpperCase(java.util.Locale.ROOT)
    val view  = targetId.table.toUpperCase(java.util.Locale.ROOT)
    upper.startsWith(s"MERGE INTO ${MvCommandHelper.sqlIdent(targetId).toUpperCase(java.util.Locale.ROOT)} AS ") &&
    (upper.contains("OPENIVM_DELTA_") || upper.contains(s"OPENIVM_AFFECTED_$view")) &&
    parseWindowDeleteMerge(sql).isDefined
  }

  private def isRawWindowSnapshotCtas(
      sql: String,
      targetId: TableIdentifier,
      viewDeltaPath: String
  ): Boolean = {
    val view = targetId.table.toUpperCase(java.util.Locale.ROOT)
    SparkRefreshRewriter
      .extractViewDeltaCtasBody(sql, viewDeltaPath)
      .exists { body =>
        val upper = body.toUpperCase(java.util.Locale.ROOT)
        upper.contains(s"FROM OPENIVM_OLD_$view") &&
        upper.contains(s"FROM OPENIVM_NEW_$view") &&
        upper.contains("UNION ALL") &&
        upper.contains("CAST(-1 AS INTEGER) AS OPENIVM_MULTIPLICITY") &&
        upper.contains("CAST(1 AS INTEGER) AS OPENIVM_MULTIPLICITY") &&
        !upper.contains("EXCEPT")
      }
  }

  private def isWindowPartitionInsertSql(sql: String, targetId: TableIdentifier): Boolean = {
    val upper = sql.trim.toUpperCase(java.util.Locale.ROOT)
    upper.startsWith(s"INSERT INTO ${MvCommandHelper.sqlIdent(targetId).toUpperCase(java.util.Locale.ROOT)}") &&
    ((upper.contains("OPENIVM_RECOMPUTE") &&
      "\\bIN\\s*\\(\\s*SELECT\\s+DISTINCT\\b".r.findFirstIn(upper).isDefined &&
      upper.contains("OPENIVM_DELTA_")) ||
      upper.contains(s"FROM OPENIVM_NEW_${targetId.table.toUpperCase(java.util.Locale.ROOT)}"))
  }

  /** Advance the MV's tracked Delta version and prune fully-consumed staging
    * rows. Shared between the FullRefresh and incremental paths.
    *
    * MV-over-MV cascade trigger:
    *
    * For every other tracked MV that lists `name` as a source table, this
    * method ensures the downstream MV's next REFRESH does not short-circuit
    * on the `changeBatches.isEmpty` guard at the top of [[runUnderLock]].
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
      changeBatches: Seq[ChangeBatch],
      viewNameStr: String,
      sqlLog: RefreshSqlLog = RefreshSqlLog.NoOp,
      qlogOrder: java.util.concurrent.atomic.AtomicInteger = new java.util.concurrent.atomic.AtomicInteger(0)
  ): Unit = {
    import MvCommandHelper._
    val propagation = ChangePropagationFactory.forSession(spark)
    val newVersion =
      DeltaTable.forPath(spark, meta.location).history(1).collect().head.getAs[Long]("version")
    MvCatalog.advance(spark, name, newVersion)

    propagation.markConsumed(spark, viewNameStr, changeBatches)

    val allMvs = MvCatalog.list(spark)
    val viewsByTable = allMvs
      .flatMap(m => m.sourceTables.map(t => t -> metaName(m.name)))
      .groupBy(_._1)
      .map { case (t, pairs) => t -> pairs.map(_._2) }
    propagation.pruneConsumed(spark, viewsByTable)

    // Non-cascade trigger synthesis is intercept-mode only.  Under CDF mode
    // the downstream MV's next REFRESH naturally sees the new MV-data Delta
    // version via its own [[CdfChangePropagation.hasPendingChanges]] probe.
    if (propagation.requiresDmlInterception && !meta.emitsCascadeViewDelta) {
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

  /** Consume source changes after proving their materialized runtime delta views
    * are empty. Unlike [[postRefreshCleanup]], this intentionally does not
    * advance the MV Delta version or synthesize downstream cascade triggers,
    * because the MV table was not written and its logical contents did not
    * change.
    */
  private def consumeRefreshChangesWithoutMvWrite(
      spark: SparkSession,
      viewNameStr: String,
      changeBatches: Seq[ChangeBatch]
  ): Unit = {
    import MvCommandHelper._

    val propagation = ChangePropagationFactory.forSession(spark)
    propagation.markConsumed(spark, viewNameStr, changeBatches)
    val viewsByTable = MvCatalog
      .list(spark)
      .flatMap(m => m.sourceTables.map(t => t -> metaName(m.name)))
      .groupBy(_._1)
      .map { case (t, pairs) => t -> pairs.map(_._2) }
    propagation.pruneConsumed(spark, viewsByTable)
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
        val mvQual      = metaName(name)
        val mvShort     = name.identifier
        val propagation = ChangePropagationFactory.forSession(spark)
        propagation.removeForBaseTable(spark, mvQual)
        if (mvShort != mvQual) propagation.removeForBaseTable(spark, mvShort)
        // Also evict any CDF watermark rows scoped to this MV instance.  These
        // are independent of intercept-mode staging rows and are pruned even
        // if the active mode is `intercept` (defensive cleanup so a later
        // mode flip never re-uses stale watermarks).
        CdfWatermarkCatalog.removeForView(spark, mvQual)
        if (mvShort != mvQual) CdfWatermarkCatalog.removeForView(spark, mvShort)
        CdfWatermarkCatalog.removeForBaseTable(spark, mvQual)
        if (mvShort != mvQual) CdfWatermarkCatalog.removeForBaseTable(spark, mvShort)

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
