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
import java.util.Collections

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

    MvCatalog.ensureTables(spark)
    StagingCatalog.ensureTables(spark)

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
    val compiled =
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

    // Storage location
    val location = mvLocation(spark, name)

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
    val nonCascadeUpstreams: Seq[String] =
      upstreamMvByQual.toSeq.collect {
        case (q, m) if !m.emitsCascadeViewDelta => q
      }
    // ── Effective refresh-type classification with structured logging ──────
    //
    // Every demotion of `compiled.refreshType` to FULL_REFRESH below is
    // surfaced via [[logError]] with a `reason=<key>` tag so the operator can
    // see WHY each MV ended up FULL_REFRESH in the spark-ext / dbt-server
    // container log. The reason keys mirror the if-else branches:
    //   - top_k                       Top-K view (ORDER BY ... LIMIT ...) forced to FULL_REFRESH
    //   - non_cascade_upstream        Upstream MV instance does NOT emit a
    //                                 persisted `openivm_delta_<view>` downstream can
    //                                 consume incrementally (e.g. SIMPLE_AGGREGATE,
    //                                 DISTINCT_INCREMENTAL, SEMI_ANTI_RECOMPUTE, TOP_K,
    //                                 FULL_REFRESH, or a recompute MV whose compiled SQL
    //                                 lacked a real view-delta). WINDOW_PARTITION /
    //                                 GROUP_RECOMPUTE only escape this bucket when their
    //                                 concrete compiled SQL actually emitted the cascade
    //                                 delta guarded by `_ivm_emits_cascade_view_delta`.
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
      else if (nonCascadeUpstreams.nonEmpty)
        (RefreshTypeCode.FullRefresh, s"non_cascade_upstream:${nonCascadeUpstreams.mkString(",")}")
      else if (compiled.refreshType == RefreshTypeCode.WindowPartition)
        (compiled.refreshType, "window_partition_kept")
      else if (compiled.refreshType == RefreshTypeCode.GroupRecompute)
        (compiled.refreshType, "group_recompute_kept")
      else if (compiled.refreshType == RefreshTypeCode.AggregateHaving && rawHavingPred.isEmpty)
        (RefreshTypeCode.FullRefresh, "having_pred_empty")
      else if (!SparkRefreshRewriter.hasRealDelta(compiled.sql, name.table))
        (RefreshTypeCode.FullRefresh, "no_real_delta")
      else (compiled.refreshType, "kept")
    }
    // `sourceIsMv` is computed for logging visibility only; the demotion is
    // now driven by `nonCascadeUpstreams.nonEmpty` (see above).
    val _ = sourceIsMv
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
    // Capture per-source watermarks BEFORE the MV's initial CTAS so the first
    // REFRESH ignores any staging rows that pre-date this MV (otherwise we'd
    // double-apply upstream view-deltas this MV already absorbed via the CTAS).
    // See `StagingCatalog.currentWatermarks` + `StagingCatalog.collectFor`.
    val watermarks     = StagingCatalog.currentWatermarks(spark, qualNames)
    val watermarkProps = MvMetadata.watermarkProperties(watermarks)
    val allProps =
      properties ++ baseProps ++ countProp ++ havingProp ++ cascadeDeltaProps ++ watermarkProps
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

    val escaped = location.replace("'", "\\'")
    val initSql =
      s"CREATE TABLE IF NOT EXISTS ${sqlIdent(dataIdent)} USING DELTA LOCATION '$escaped' AS $viewBodySql"
    IvmDmlInterceptorRule.bypass.set(true)
    try {
      spark.sql(initSql)

      // For AGGREGATE_HAVING we additionally create the user-facing Spark VIEW
      // that projects only the user columns from the data table and applies the
      // HAVING predicate. The post-pass DELETE during refresh removes zero-count
      // rows from the data table so this VIEW does not need a separate
      // `openivm_count_star > 0` guard.
      if (isHavingViewIncremental) {
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
        spark.sql(
          s"CREATE OR REPLACE VIEW ${sqlIdent(name)} AS " +
            s"SELECT $colList FROM ${sqlIdent(dataIdent)} WHERE ($pred)"
        )
      }

      // Write metadata catalog entry
      MvCatalog.upsert(spark, meta)

      // Record the Delta version of the initial snapshot
      val version =
        DeltaTable.forPath(spark, location).history(1).collect().head.getAs[Long]("version")
      MvCatalog.advance(spark, name, version)
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
    RefreshMutex.withLock(metaName(name)) {
      runUnderLock(spark)
    }
  }

  private def runUnderLock(spark: SparkSession): Seq[Row] = {
    import MvCommandHelper._
    import org.openivm.spark.compiler.LptsSparkDialect

    val meta = MvCatalog
      .lookup(spark, name)
      .getOrElse(
        throw new AnalysisException(
          "TABLE_OR_VIEW_NOT_FOUND",
          Map("relationName" -> sqlIdent(name))
        )
      )

    val viewNameStr = metaName(name)
    val stagingDeltas = StagingCatalog.collectFor(
      spark,
      viewNameStr,
      meta.sourceTables,
      meta.sourceWatermarks
    )

    // No pending deltas → nothing to do
    if (stagingDeltas.isEmpty) {
      logInfo(
        s"[openivm-mv] refresh view='${sqlIdent(name)}' refresh_type='${meta.refreshTypeName}' " +
          "outcome='no_pending_deltas'"
      )
      return Seq.empty
    }

    logInfo(
      s"[openivm-mv] refresh view='${sqlIdent(name)}' refresh_type='${meta.refreshTypeName}' " +
        s"pending_deltas=${stagingDeltas.size} source_tables=${meta.sourceTables.mkString(",")}"
    )

    // Resolve current source schemas and check for schema drift. Include
    // upstream MV identity hashes so a DROP + recreate-with-same-schema
    // upstream is also caught.
    val freshSchemas = meta.sourceTables.map(t => t -> spark.table(t).schema).toMap
    val freshMvIdentityBySource: Map[String, String] = {
      val all: Seq[MvMetadata] =
        try MvCatalog.list(spark)
        catch { case _: Throwable => Seq.empty[MvMetadata] }
      val byMeta: Map[String, MvMetadata] = all.map(m => metaName(m.name) -> m).toMap
      meta.sourceTables.flatMap { qn =>
        val short = qn.split("\\.").last
        byMeta.get(qn).orElse(byMeta.get(short)).map(m => qn -> MvCatalog.mvIdentity(m))
      }.toMap
    }
    val freshFingerprint = MvCatalog.schemaFingerprint(freshSchemas, freshMvIdentityBySource)
    if (freshFingerprint != meta.sourceSchemaFingerprint)
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
      try {
        assembled.statements.foreach { sql =>
          RetryPolicy.DeltaConflicts.execute { spark.sql(sql).collect() }
        }
        postRefreshCleanup(spark, name, meta, stagingDeltas, viewNameStr)
      } catch {
        case t: Throwable =>
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
    val compiler = OpenIvmCompilers.forSession(spark)
    val compiled = compiler.compile(
      CompileRequest(
        viewName = name.table,
        viewSql = meta.querySql,
        sources = compileSchemas,
        sourceQualifiedNames = shortToQual
      )
    )

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

    val byTable            = stagingDeltas.groupBy(_.baseTable)
    val tempViewShortNames = scala.collection.mutable.ArrayBuffer[String]()

    IvmDmlInterceptorRule.bypass.set(true)
    try {
      // Register a delta temp view for every source table.  Tables that have
      // pending staging deltas get a real view; tables with no pending deltas
      // get an empty view so that multi-source compiled SQL (e.g. UNION DISTINCT
      // across two tables) can reference all delta views without a NOT_FOUND error.
      for (qualTable <- meta.sourceTables) {
        val schema      = freshSchemas(qualTable)
        val tableDeltas = byTable.getOrElse(qualTable, Seq.empty)
        val viewSql     = StagingDeltaView.buildSourceDeltaViewSql(qualTable, schema, tableDeltas)
        spark.sql(viewSql)
        tempViewShortNames += qualTable.split("\\.").last
      }

      // For AGGREGATE_HAVING the user-facing object is a Spark VIEW; the actual
      // Delta data lives in a sibling table that stores ALL groups (no HAVING
      // filter). Redirect MERGE/DELETE statements to the sibling table so a
      // group whose aggregate later crosses the threshold can be re-promoted
      // back into the HAVING-passing set incrementally.
      val mergeTargetId: TableIdentifier =
        if (meta.refreshType == RefreshTypeCode.AggregateHaving) dataTableId(name)
        else name

      val rewritten = SparkRefreshRewriter.rewrite(
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

      try {
        lazy val hasSimpleProjectionDeletes = hasNegativeSimpleProjectionRows(spark, viewDeltaPath)

        // Log the rewritten SQL at DEBUG so cascade-related issues are
        // observable when -Dlog4j2.logger.org.openivm.spark.commands=DEBUG
        // is set, without polluting the default INFO output.
        rewritten.statements.zipWithIndex.foreach { case (stmt, i) =>
          val sql = SparkRefreshRewriter.stripExecutionMarker(stmt)
          logInfo(
            s"[openivm-mv] refresh view='${sqlIdent(name)}' stmt[$i]=" +
              sql.replace('\n', ' ').take(4000)
          )
        }
        rewritten.statements.foreach { stmt =>
          val sql = SparkRefreshRewriter.stripExecutionMarker(stmt)
          val skipDeleteMerge =
            SparkRefreshRewriter.isSimpleProjectionDeleteMerge(stmt) && !hasSimpleProjectionDeletes

          if (skipDeleteMerge) {
            logInfo(
              s"[openivm-mv] refresh view='${sqlIdent(name)}' " +
                "outcome='skip_simple_projection_delete_merge' reason='no_negative_rows'"
            )
          } else {
            RetryPolicy.DeltaConflicts.execute { spark.sql(sql).collect() }
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
            RetryPolicy.DeltaConflicts.execute { spark.sql(deleteSql).collect() }
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
        if (meta.emitsCascadeViewDelta) {
          val mvShortName = name.identifier
          val triggerKeys: Set[String] = MvCatalog
            .list(spark)
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
      } catch {
        case t: Throwable =>
          // Best-effort cleanup of any partial view-delta on failure. Phase 7
          // orphan-sweep is the long-tail safety net.
          try {
            val hadoopPath = new Path(viewDeltaPath)
            val fs         = hadoopPath.getFileSystem(spark.sessionState.newHadoopConf())
            if (fs.exists(hadoopPath)) fs.delete(hadoopPath, /* recursive = */ true)
          } catch { case _: Throwable => () }
          val sqlSnippet = rewritten.statements.zipWithIndex
            .map { case (s, i) => s"[${i + 1}] ${SparkRefreshRewriter.stripExecutionMarker(s)}" }
            .mkString("\n---\n")
          throw new RuntimeException(
            s"Incremental refresh of '${sqlIdent(name)}' failed: ${t.getMessage}\n" +
              s"Rewritten SQL:\n$sqlSnippet",
            t
          )
      }

      postRefreshCleanup(spark, name, meta, stagingDeltas, viewNameStr)
    } finally {
      IvmDmlInterceptorRule.bypass.set(false)
      tempViewShortNames.foreach { n =>
        try spark.sql(StagingDeltaView.dropSourceDeltaViewSql(n))
        catch { case _: Throwable => () }
      }
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
      viewNameStr: String
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

    // MV-over-MV cascade trigger (non-cascade-capable upstream only — see
    // method docstring). For cascade-delta-capable types the MV_VIEW_DELTA
    // row is already recorded; emitting an OVERWRITE trigger here would
    // pile a +1 multiplicity full-table delta on top of the +/- signed
    // view-delta and silently double-add downstream rows.
    if (!meta.emitsCascadeViewDelta) {
      val mvShortName = name.identifier
      val triggerKeys: Set[String] = allMvs
        .filter(_.sourceTables.exists(_.split("\\.").last == mvShortName))
        .flatMap(_.sourceTables.filter(_.split("\\.").last == mvShortName))
        .toSet
      triggerKeys.foreach { triggerKey =>
        // Unique per-refresh path under the MV's location so repeats don't
        // collapse on (base_table, staging_path) idempotency.
        val triggerPath = s"${meta.location}/_trigger/${java.util.UUID.randomUUID()}"
        StagingCatalog.record(
          spark,
          StagingDelta(
            baseTable = triggerKey,
            opType = StagingDelta.OpTypes.Overwrite,
            stagingPath = triggerPath,
            txnTs = new Timestamp(System.currentTimeMillis()),
            consumedBy = Seq.empty
          )
        )
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
