package org.openivm.spark.common

import org.apache.spark.sql.catalyst.TableIdentifier

/** Output of [[SparkRefreshRewriter.rewrite]]: the ordered list of Spark-SQL
  * statements that, when executed, perform one incremental refresh.
  */
final case class RewrittenRefresh(statements: Seq[String])

/** Rewrites the openivm-emitted multi-statement refresh SQL into a sequence of
  * Spark-executable statements.
  *
  * The openivm bridge produces, for an AGGREGATE_GROUP (type 0) view, a 7-statement
  * program (in order):
  *   {{{
  *     A. UPDATE openivm_views SET refresh_in_progress = true ...
  *     B. WITH ... INSERT INTO openivm_delta_<view> ...
  *     C. WITH refresh_cte AS (... FROM openivm_delta_<view> WHERE openivm_timestamp > ...) MERGE INTO openivm_data_<view> ...
  *     D. DELETE FROM openivm_delta_<view>;
  *     E. DELETE FROM openivm_delta_<source> WHERE openivm_timestamp < ...;
  *     F. UPDATE openivm_delta_tables SET ...;
  *     G. UPDATE openivm_views SET refresh_in_progress = false ...
  *   }}}
  *
  * For a SIMPLE_AGGREGATE (type 1) view, openivm emits UPDATE-based statements
  * instead of MERGE for the data-modification step.  The program is:
  *   {{{
  *     A. UPDATE openivm_views SET refresh_in_progress = true ...
  *     B. WITH ... INSERT INTO openivm_delta_<view> ...          (kept, rewritten as CTAS)
  *     C. WITH openivm_delta AS (SELECT SUM(mult*col) AS d_col FROM openivm_delta_<view> WHERE ...)
  *            UPDATE openivm_data_<view> SET col = COALESCE(col,0) + COALESCE((SELECT d_col FROM openivm_delta),0)
  *        (kept, CTE inlined — Spark SQL does not support WITH before UPDATE)
  *     D. UPDATE openivm_data_<view> SET col = <recompute_expr>   (kept — recompute visible columns from hidden ones, e.g. AVG from SUM/COUNT)
  *     E. UPDATE openivm_data_<view> SET col=NULL WHERE NOT EXISTS (SELECT 1 FROM <source> LIMIT 1)
  *        (kept — null-reset when source becomes empty)
  *     F–H. DELETE/UPDATE cleanup  (dropped)
  *   }}}
  *
  * Spark replays only the data-bearing statements (B plus C/D/E). The bookkeeping
  * statements (A, F–H) are owned by the Spark-side staging catalog and are
  * dropped here. Statement B is rewritten from `INSERT INTO openivm_delta_<v>`
  * into a `CREATE OR REPLACE TABLE delta.\`<viewDeltaPath>\` USING DELTA AS …`
  * CTAS so we can persist the per-refresh view delta to a fresh Delta path
  * without depending on a pre-existing target table. Statement C then reads
  * back from that path and updates the user-visible MV.
  */
object SparkRefreshRewriter {

  final case class SelectiveBroadcastTable(shortName: String, qualifiedName: String, sizeBytes: Long)
  final case class SkewFanoutDeltaBroadcast(
      shortName: String,
      qualifiedName: String,
      deltaRows: Long,
      signal: String
  )

  private final case class SqlRelationRef(relation: String, alias: String)
  private final case class Scd2RangeJoin(
      probeAlias: String,
      probeExpr: String,
      dimAlias: String,
      effectiveExpr: String,
      endExpr: String
  )
  private final case class UniqueJoinKey(shortName: String, columns: Set[String])
  private final case class UniqueJoinClause(
      removeStart: Int,
      removeEnd: Int,
      joinType: String,
      relation: String,
      alias: String,
      onCondition: String
  )

  /** Per-rewrite map of short source-table name → fully-qualified Spark name.
    *
    * Populated at the top of [[rewrite]] from the caller's
    * `sourceQualifiedNames` argument and consulted by
    * [[rewriteMemoryMainPrefix]] so live-source references like
    * `memory.main.<short>` are rewritten to `` `<db>`.`<table>` `` instead
    * of the unqualified `` `<short>` ``. Internal openivm names (e.g.
    * `openivm_delta_<short>`, `openivm_data_<view>`) are not in the map
    * and keep the default bare-backtick rewrite.
    *
    * Stored in a ThreadLocal so concurrent refreshes on different threads
    * don't trample each other's per-MV qualification context.
    */
  private val activeQualifiedNames: ThreadLocal[Map[String, String]] =
    new ThreadLocal[Map[String, String]] {
      override def initialValue(): Map[String, String] = Map.empty
    }

  private[spark] val SimpleProjectionDeleteMergeMarker: String =
    "/*OPENIVM_SIMPLE_PROJECTION_DELETE_MERGE*/"

  private def markSimpleProjectionDeleteMerge(sql: String): String =
    s"$SimpleProjectionDeleteMergeMarker\n$sql"

  private[spark] def isSimpleProjectionDeleteMerge(sql: String): Boolean =
    sql.contains(SimpleProjectionDeleteMergeMarker)

  private[spark] def stripExecutionMarker(sql: String): String =
    sql.replace(SimpleProjectionDeleteMergeMarker, "").trim

  private[spark] def injectSelectiveBroadcastHints(
      sql: String,
      tables: Seq[SelectiveBroadcastTable]
  ): String = {
    if (tables.isEmpty || !"(?is)\\bJOIN\\b".r.findFirstIn(sql).isDefined) sql
    else {
      val names = tables.flatMap(t => Seq(t.shortName, t.qualifiedName)).map(normalizeSqlIdentifier).toSet
      val edits = selectKeywordOffsets(sql).flatMap { selectIdx =>
        val blockEnd = selectBlockEnd(sql, selectIdx)
        val block    = sql.substring(selectIdx, blockEnd)
        val aliases  = broadcastAliasesInBlock(block, names)
        if (aliases.isEmpty) None
        else Some(selectIdx + "SELECT".length -> s" /*+ BROADCAST(${aliases.mkString(", ")}) */")
      }
      if (edits.isEmpty) sql
      else {
        val out  = new StringBuilder(sql)
        var bias = 0
        edits.foreach { case (idx, hint) =>
          out.insert(idx + bias, hint)
          bias += hint.length
        }
        out.toString()
      }
    }
  }

  private[spark] def planSkewFanoutDeltaBroadcasts(
      facts: WorkloadFacts,
      maxDeltaRows: Long,
      maxOverlapRatio: Double
  ): Seq[SkewFanoutDeltaBroadcast] =
    facts.deltaStats.toSeq
      .flatMap { case (table, delta) =>
        delta.rowCount.filter(rows => rows >= 0L && rows <= maxDeltaRows).map { rows =>
          val signal = narrowestOverlapSignal(table, delta, facts.columnStats, maxOverlapRatio)
            .getOrElse(s"delta_rows=$rows<=${maxDeltaRows};histogram_bins=unavailable")
          SkewFanoutDeltaBroadcast(shortTableName(table), table, rows, signal)
        }
      }
      .sortBy(b => (b.shortName.toLowerCase, b.qualifiedName.toLowerCase))

  private[spark] def injectSkewFanoutBroadcastHints(
      sql: String,
      broadcasts: Seq[SkewFanoutDeltaBroadcast]
  ): String = {
    if (broadcasts.isEmpty || sql.contains("OPENIVM_SKEW_FANOUT") || !"(?is)\\bJOIN\\b".r.findFirstIn(sql).isDefined)
      sql
    else {
      val deltaNames = broadcasts
        .flatMap { b =>
          val deltaShort = s"openivm_delta_${b.shortName}"
          Seq(deltaShort, s"memory.main.$deltaShort")
        }
        .map(normalizeSqlIdentifier)
        .toSet
      val byShort = broadcasts.map(b => b.shortName.toLowerCase -> b).toMap
      val edits = selectKeywordOffsets(sql).flatMap { selectIdx =>
        val blockEnd = selectBlockEnd(sql, selectIdx)
        val block    = sql.substring(selectIdx, blockEnd)
        val aliases  = broadcastAliasesInBlock(block, deltaNames)
        if (aliases.isEmpty) None
        else {
          val observed = relationRefsInBlock(block).flatMap { ref =>
            deltaSourceShortName(normalizeSqlIdentifier(ref.relation).split("\\.").lastOption.getOrElse(ref.relation))
              .flatMap(short => byShort.get(short.toLowerCase))
          }.distinct
          val signal = observed
            .map(b => s"${b.shortName}:rows=${b.deltaRows}:${b.signal}")
            .mkString(";")
            .replace("*/", "")
          Some(
            selectIdx + "SELECT".length -> s" /*+ BROADCAST(${aliases.mkString(", ")}) */ /*OPENIVM_SKEW_FANOUT $signal*/"
          )
        }
      }
      if (edits.isEmpty) sql
      else {
        val out  = new StringBuilder(sql)
        var bias = 0
        edits.foreach { case (idx, hint) =>
          out.insert(idx + bias, hint)
          bias += hint.length
        }
        out.toString()
      }
    }
  }

  private[spark] def injectScd2RangeAcceleration(sql: String): String = {
    if (
      sql.contains("__openivm_scd2_range_accel__") ||
      !"(?is)\\bJOIN\\b".r.findFirstIn(sql).isDefined ||
      !"(?is)\\bBETWEEN\\b".r.findFirstIn(sql).isDefined
    ) {
      sql
    } else {
      val joins = scd2RangeJoins(sql)
      if (joins.isEmpty) sql
      else {
        val hinted = injectBroadcastHintsForAliases(sql, joins.map(_.dimAlias).toSet)
        injectScd2OverlapPredicates(hinted, joins)
      }
    }
  }

  /** Detect the openivm-emitted **recompute INSERT MERGE** shape:
    *
    * {{{
    *   MERGE INTO <target> [AS] <alias>
    *   USING ( ... <recompute body> ... ) AS <alias>
    *   ON FALSE
    *   WHEN NOT MATCHED [AND ...] THEN INSERT ...
    * }}}
    *
    * This shape is used by every refresh path that needs to recompute and
    * re-insert *all* affected rows for the current delta — including
    * `SIMPLE_PROJECTION`, `AGGREGATE_GROUP`, `WINDOW_PARTITION`,
    * `GROUP_RECOMPUTE`, and DuckLake variants. The USING source typically
    * wraps openivm's full view body (or a subset of it) inside a deeply
    * nested CTE chain, and Catalyst's plan-time `JoinSelection` can pick a
    * BroadcastHashJoin for one of those inner joins based on
    * file-size-derived stats that under-estimate the actual row count
    * (notably SCD2 range joins amplify the intermediate by
    * `versions × dates`).
    *
    * Callers use this predicate to gate a per-statement
    * `spark.sql.autoBroadcastJoinThreshold = -1` override so that Catalyst
    * falls back to `ShuffledHashJoinExec` / `SortMergeJoinExec`, neither of
    * which can trip Spark's hard 8 GiB `BroadcastExchangeExec` cap.
    *
    * Paren-aware: skips parens, backticks, and single-quoted strings inside
    * the target identifier and the USING source body so a `(` inside a
    * `delta.`<path>`` qualifier or inside a string literal can't fool the
    * matcher.
    *
    * Returns false for:
    *   - non-MERGE statements (CTAS, INSERT, UPDATE, DELETE, …);
    *   - MERGEs whose USING source isn't parenthesised;
    *   - MERGEs whose ON clause is anything other than literal `FALSE`
    *     (e.g. a real equi-merge predicate);
    *   - MERGEs whose first matched clause is `WHEN MATCHED` (delete /
    *     update merges).
    */
  private[spark] def isRecomputeInsertMerge(sql: String): Boolean = {
    val stripped     = stripExecutionMarker(sql)
    val mergeHeader  = "(?is)\\bMERGE\\s+INTO\\s+".r.findFirstMatchIn(stripped).getOrElse(return false)
    val usingOpenIdx = findMergeUsingOpenParen(stripped, mergeHeader.end).getOrElse(return false)
    val closeIdx     = findMatchingCloseParen(stripped, usingOpenIdx)
    if (closeIdx < 0) return false
    val tail = stripped.substring(closeIdx + 1)
    val tailRe =
      "(?is)^\\s*(?:AS\\s+)?\\w+\\s+ON\\s+(?:FALSE|\\(\\s*FALSE\\s*\\))\\s+WHEN\\s+NOT\\s+MATCHED\\b".r
    tailRe.findFirstMatchIn(tail).isDefined
  }

  /** True iff `sql` is any `MERGE INTO …` statement (after stripping our
    * execution marker). Used by `MaterializedViewCommands` to wrap *every*
    * openivm-emitted MERGE in a per-statement plan-time broadcast disable
    * scope.
    *
    * Why broader than `isRecomputeInsertMerge`: openivm-emitted MERGEs
    * include not just the `ON FALSE WHEN NOT MATCHED INSERT` recompute shape
    * but also `WHEN MATCHED THEN DELETE` (SIMPLE_PROJECTION delete-merge)
    * and `WHEN MATCHED THEN UPDATE … WHEN NOT MATCHED THEN INSERT` (aggregate
    * upsert). Any of those can hit Spark's 8 GiB `BroadcastExchangeExec`
    * cap when Delta's MERGE rewrite plus DPP-style subquery broadcasts
    * combine on a SCD2-shaped MV body — even when the USING source itself
    * is tiny (e.g. `SELECT DISTINCT key FROM <view_deltas>`), because
    * Delta's "find affected target files" subquery may materialise the
    * outer view body for `IS NOT DISTINCT FROM` matching.
    *
    * Used only as a gating predicate; never mutates `sql`.
    */
  private[spark] def isMergeStatement(sql: String): Boolean = {
    val stripped = stripExecutionMarker(sql)
    findTopLevelSqlKeyword(stripped, 0, stripped.length, "MERGE").exists { mergeIdx =>
      "(?is)^MERGE\\s+INTO\\b".r.findFirstIn(stripped.substring(mergeIdx)).isDefined
    }
  }

  /** Match `CREATE OR REPLACE TABLE delta.`<viewDeltaPath>` USING DELTA AS`
    * (whitespace-tolerant, case-insensitive on keywords) and return the SELECT
    * body that follows the `AS` keyword. Used by the SimpleProjection fuse
    * optimization in `MaterializedViewCommands.refresh` to swap the on-disk
    * scratch Delta write for an in-memory cached temp view.
    *
    * Returns `None` if `stmt` is not a view-delta CTAS for `viewDeltaPath`
    * (e.g. a MERGE, INSERT, or some other CTAS pointing at a different path).
    */
  private[spark] def extractViewDeltaCtasBody(stmt: String, viewDeltaPath: String): Option[String] = {
    val escapedPath = viewDeltaPath.replace("`", "``")
    val literalPath = java.util.regex.Pattern.quote(escapedPath)
    val ctasRe =
      ("(?is)^\\s*CREATE\\s+OR\\s+REPLACE\\s+TABLE\\s+delta\\.`" + literalPath +
        "`\\s+USING\\s+DELTA\\s+AS\\s+(.+)$").r
    ctasRe.findFirstMatchIn(stripExecutionMarker(stmt)).map(_.group(1).trim)
  }

  /** Inline a view-delta CTAS body into its single aggregate MERGE consumer.
    *
    * Eligible insert-only terminal aggregates do not need a durable cascade
    * delta. Replacing the one scratch-path scan with the CTAS query turns the
    * two-statement write/read program into one Spark pipeline. Return `None`
    * unless the shape is exact so callers can execute the original program.
    */
  private[spark] def inlineViewDeltaCtasIntoMerge(
      ctas: String,
      merge: String,
      viewDeltaPath: String
  ): Option[String] = {
    val strippedMerge = stripExecutionMarker(merge)
    val isMerge       = isMergeStatement(strippedMerge)
    val escapedPath   = viewDeltaPath.replace("`", "``")
    val literalRef    = "delta.`" + escapedPath + "`"
    val refPattern    = java.util.regex.Pattern.compile(java.util.regex.Pattern.quote(literalRef))
    val refMatcher    = refPattern.matcher(strippedMerge)
    var refCount      = 0
    while (refMatcher.find()) refCount += 1

    if (!isMerge || refCount != 1) None
    else
      extractViewDeltaCtasBody(ctas, viewDeltaPath).map { body =>
        val subquery = body.stripSuffix(";").trim
        val replacement = java.util.regex.Matcher.quoteReplacement(
          s"($subquery) AS __openivm_direct_delta"
        )
        refPattern.matcher(strippedMerge).replaceFirst(replacement)
      }
  }

  /** Replace every occurrence of `` delta.`<viewDeltaPath>` `` in `sql` with
    * `` `<tempViewName>` ``. Anchored on the exact escaped path (the path is
    * UUID-suffixed so this is functionally safe) but uses a literal-quoted
    * regex so SQL operators in the path can't escape the substitution. Used
    * by the SimpleProjection fuse path to rewrite stmt[1] (INSERT) and
    * stmt[2] (MERGE) to read from a cached temp view instead of the on-disk
    * scratch Delta table.
    */
  private[spark] def substituteViewDeltaPath(
      sql: String,
      viewDeltaPath: String,
      tempViewName: String
  ): String = {
    val escapedPath = viewDeltaPath.replace("`", "``")
    val literalRef  = "delta.`" + escapedPath + "`"
    val re          = java.util.regex.Pattern.compile(java.util.regex.Pattern.quote(literalRef))
    val replacement = java.util.regex.Matcher.quoteReplacement(s"`$tempViewName`")
    re.matcher(sql).replaceAll(replacement)
  }

  /** Rewrite the openivm-emitted multi-statement refresh SQL into Spark-
    * executable statements.
    *
    * @param compiledSql      Full multi-statement refresh program as emitted
    *                         by openivm (after dialect translation has
    *                         already happened upstream — though [[postProcess]]
    *                         offers a final hook).
    * @param mvName           Fully-qualified MV identifier (e.g. `db.v`).
    * @param mvLocation       The MV's Delta path. Used for pragma-gated
    *                         recompute cascade support, where `openivm_old_<view>`
    *                         must read the pre-refresh snapshot via Delta time
    *                         travel while the live table is being mutated.
    * @param viewLogicalName  The bare view name openivm uses internally (the
    *                         `<view>` in `openivm_data_<view>` and
    *                         `openivm_delta_<view>`); typically `mvName.table`.
    * @param sourceTempViews  Map from short source-table name to the temp-view
    *                         name openivm references. Currently unused by the
    *                         rewriter (reserved for source-name remapping
    *                         beyond `memory.main.<short>`).
    * @param viewDeltaPath    Per-refresh scratch path where the view-delta
    *                         CTAS is materialised. Statement C reads back
    *                         from `delta.\`<viewDeltaPath>\``.
    * @param postProcess      Final dialect translation applied to each
    *                         surviving statement (e.g. [[org.openivm.spark.compiler.LptsSparkDialect.translate]]).
    * @param mvVersionBeforeRefresh Delta version visible before this refresh starts.
    *                         Required when pragma-gated recompute cascade emits an
    *                         `openivm_old_<view>` snapshot that must stay pinned to
    *                         the pre-refresh MV contents.
    */
  def rewrite(
      compiledSql: String,
      mvName: TableIdentifier,
      mvLocation: String,
      viewLogicalName: String,
      sourceTempViews: Map[String, String],
      viewDeltaPath: String,
      postProcess: String => String = identity,
      sourceSchemas: Map[String, Seq[String]] = Map.empty,
      sourceQualifiedNames: Map[String, String] = Map.empty,
      deltaShape: Map[String, DeltaShape] = Map.empty,
      semiJoinPruneEnabled: Boolean = false,
      fkTermPruneEnabled: Boolean = false,
      fkRelations: Seq[ForeignKeyRelation] = Seq.empty,
      uniqueKeys: Seq[UniqueKey] = Seq.empty,
      uniqueJoinSimplifyEnabled: Boolean = false,
      windowPartitionSingleDeleteMergeEnabled: Boolean = false,
      mvVersionBeforeRefresh: Option[Long] = None
  ): RewrittenRefresh = {
    val _ = sourceTempViews // reserved for future passes

    // Make qualified-name remapping visible to every private rewriter below,
    // so `memory.main.<short>` becomes the correct `<db>.<table>` reference
    // in all six call sites instead of a current-schema-bound `<short>`.
    val prior = activeQualifiedNames.get()
    activeQualifiedNames.set(sourceQualifiedNames)
    try {
      val stmts = splitStatements(compiledSql).map(_.trim).filter(_.nonEmpty)

      val rewritten: Seq[String] = stmts.flatMap { stmt =>
        classify(stmt, viewLogicalName) match {
          case StatementKind.InProgressFlag | StatementKind.Cleanup => Nil
          case StatementKind.ViewDeltaInsert =>
            Seq(
              rewriteViewDeltaInsert(
                stmt,
                viewLogicalName,
                viewDeltaPath,
                deltaShape,
                semiJoinPruneEnabled,
                fkTermPruneEnabled,
                fkRelations,
                uniqueKeys,
                uniqueJoinSimplifyEnabled
              )
            )
          case StatementKind.ViewDeltaCompanion =>
            Seq(rewriteViewDeltaCompanion(stmt, viewLogicalName, mvName, viewDeltaPath))
          case StatementKind.MvMerge =>
            Seq(rewriteMvMerge(stmt, viewLogicalName, mvName, viewDeltaPath))
          case StatementKind.SimpleProjectionDataInsert =>
            rewriteSimpleProjectionDataInsert(
              stmt,
              viewLogicalName,
              mvName,
              mvLocation,
              viewDeltaPath,
              mvVersionBeforeRefresh
            )
          case StatementKind.ScalarUpdate =>
            Seq(rewriteScalarUpdate(stmt, viewLogicalName, mvName, viewDeltaPath))
          case StatementKind.ScalarDeleteMv =>
            rewriteScalarDeleteMv(stmt, viewLogicalName, mvName, viewDeltaPath)
          case StatementKind.ScalarFullRecomputeInsert =>
            Seq(rewriteScalarFullRecomputeInsert(stmt, viewLogicalName, mvName, mvLocation, viewDeltaPath))
          case StatementKind.PartitionScopedDelete =>
            rewritePartitionScopedDelete(stmt, viewLogicalName, mvName, windowPartitionSingleDeleteMergeEnabled)
          case StatementKind.PartitionScopedInsert =>
            Seq(rewritePartitionScopedInsert(stmt, viewLogicalName, mvName))
          case StatementKind.RunningWindowTempCreate =>
            rewriteRunningWindowTempCreate(stmt, viewLogicalName, mvName, mvLocation, mvVersionBeforeRefresh)
          case StatementKind.RunningWindowFastInsert =>
            Seq(rewriteRunningWindowFastInsert(stmt, viewLogicalName, mvName))
          case StatementKind.RunningWindowCascadeInsert =>
            Seq(rewriteRunningWindowCascadeInsert(stmt, viewLogicalName, mvName, viewDeltaPath))
          case StatementKind.RunningWindowTempDrop =>
            Seq(rewriteRunningWindowTempDrop(stmt, viewLogicalName))
          case StatementKind.GroupRecomputeAffectedCreate =>
            Seq(rewriteGroupRecomputeAffectedCreate(stmt, viewLogicalName, mvLocation, mvVersionBeforeRefresh))
          case StatementKind.GroupRecomputeAffectedDrop =>
            Seq(rewriteGroupRecomputeAffectedDrop(stmt, viewLogicalName))
          case StatementKind.CurrentSnapshotCreate =>
            Seq(rewriteCurrentSnapshotCreate(stmt))
          case StatementKind.OldSnapshotCreate =>
            Seq(rewriteOldSnapshotCreate(stmt, viewLogicalName, mvLocation, mvVersionBeforeRefresh))
          case StatementKind.NewSnapshotCreate =>
            Seq(rewriteNewSnapshotCreate(stmt))
          case StatementKind.SnapshotDataInsert =>
            Seq(rewriteSnapshotDataInsert(stmt, viewLogicalName, mvName))
          case StatementKind.CurrentSnapshotDrop =>
            Seq(rewriteCurrentSnapshotDrop(viewLogicalName))
          case StatementKind.SnapshotDrop =>
            Seq(rewriteSnapshotDrop(stmt, viewLogicalName))
          case StatementKind.Unknown => Nil
        }
      }

      // Spark 3.5 does not support the DuckDB `SELECT * EXCEPT (col, ...)` column
      // exclusion syntax — `EXCEPT` is parsed as the set operator.  Expand any
      // surviving `SELECT * EXCEPT (col1, col2) FROM openivm_delta_<short>` form
      // (emitted by openivm for multi-source join-aware GROUP_RECOMPUTE and
      // similar refresh programs) into an explicit column list using the source
      // schemas supplied by the caller.
      val withExceptExpanded = rewritten.map(s => expandSelectStarExcept(s, sourceSchemas))
      val withAliasFixup     = withExceptExpanded.map(s => fixMergeAliasRefs(s, mvName))
      val withDedupedSource  = withAliasFixup.map(s => deduplicateNullSafeMergeSource(s, mvName))
      val withSemiJoinRewrite =
        withDedupedSource.map(s => rewriteRecomputeWhereExistsAsAffectedKeysJoin(s))
      RewrittenRefresh(withSemiJoinRewrite.map(postProcess))
    } finally {
      activeQualifiedNames.set(prior)
    }
  }

  // ── Statement classification ─────────────────────────────────────────────

  private sealed trait StatementKind
  private object StatementKind {
    case object InProgressFlag  extends StatementKind
    case object ViewDeltaInsert extends StatementKind

    /** AGGREGATE_GROUP / AGGREGATE_HAVING **retract companion** that openivm
      * emits when `force_view_delta_cascade=true` (or when a downstream
      * MV is registered in native mode). Shape:
      *
      *   INSERT INTO openivm_delta_<view> (cols)
      *   SELECT d.<key>, 0, …, 0, -1
      *   FROM   openivm_delta_<view> d
      *   WHERE  d.openivm_multiplicity > 0
      *     AND  d.openivm_timestamp > '…'::TIMESTAMP
      *     AND  EXISTS (SELECT 1 FROM openivm_data_<view> m
      *                  WHERE d.<key> IS NOT DISTINCT FROM m.<key>);
      *
      * Rewrite: APPEND retract rows to the view-delta path (the upstream
      * already CTAS'd it). Replace `openivm_delta_<view>` with
      * `delta.\`<viewDeltaPath>\`` and `openivm_data_<view>` with the MV
      * Spark identifier. Without this companion, downstream MVs over an
      * AGGREGATE_GROUP source compute wrong COUNT(*)/MIN/MAX deltas because
      * the additive view-delta lacks retraction rows for groups that already
      * exist in the data table. */
    case object ViewDeltaCompanion extends StatementKind

    case object MvMerge extends StatementKind

    /** SIMPLE_PROJECTION Statement C: `INSERT INTO openivm_data_<view> SELECT … FROM openivm_delta_<view>, generate_series(…)` */
    case object SimpleProjectionDataInsert extends StatementKind

    /** SIMPLE_AGGREGATE Statements C/D/E: any `UPDATE openivm_data_<view> SET …` form,
      * including the CTE-prefixed incremental-sum update, the hidden-column recompute,
      * and the null-reset for an empty source. */
    case object ScalarUpdate extends StatementKind

    /** SIMPLE_AGGREGATE full-recompute for non-additive aggregates (MIN/MAX):
      * `DELETE FROM openivm_data_<view>` — clears the MV before re-insert. */
    case object ScalarDeleteMv extends StatementKind

    /** SIMPLE_AGGREGATE full-recompute for non-additive aggregates (MIN/MAX):
      * `INSERT INTO openivm_data_<view> WITH scan_0 … SELECT … FROM memory.main.<src>`
      * — re-inserts by querying the live source table (not the delta); identified by
      * the presence of `memory.main.` which distinguishes it from SIMPLE_PROJECTION. */
    case object ScalarFullRecomputeInsert extends StatementKind

    /** GROUP_RECOMPUTE (type 6) Statement B: materialise the set of affected group keys
      * as a TEMP VIEW.  openivm emits this as
      *   `CREATE OR REPLACE TEMP TABLE openivm_affected_<view> AS
      *      SELECT DISTINCT <keys> FROM (<delta-substituted view query>);`
      * which is rewritten to `CREATE OR REPLACE TEMPORARY VIEW openivm_affected_<view>`
      * with DuckDB-isms (`SELECT * EXCLUDE`, `memory.main.<src>`, timestamp predicate)
      * normalised for Spark. */
    case object GroupRecomputeAffectedCreate extends StatementKind

    /** GROUP_RECOMPUTE (type 6) Statement E: drop the affected-keys scratch object.
      * `DROP TABLE IF EXISTS openivm_affected_<view>` → `DROP VIEW IF EXISTS …`. */
    case object GroupRecomputeAffectedDrop extends StatementKind

    /** Current-diff recompute snapshot of the full post-refresh query result.
      *
      * OpenIVM emits `openivm_current_<view>` before `openivm_affected_<view>`
      * for current-diff GROUP_RECOMPUTE plans such as HAVING CTEs.
      */
    case object CurrentSnapshotCreate extends StatementKind
    case object CurrentSnapshotDrop   extends StatementKind

    /** Recompute-cascade snapshot of the PRE-refresh MV rows.
      *
      * When openivm `4471f4e929fd3b21ac55ea0c47249d4716853c98` is enabled
      * and CompileFacts has `force_view_delta_cascade=true` (which
      * openivm-spark always sets), both WINDOW_PARTITION and
      * GROUP_RECOMPUTE emit
      * `CREATE OR REPLACE TEMP TABLE openivm_old_<view> AS SELECT … FROM openivm_data_<view> …`.
      * Spark must keep this pinned to the pre-refresh MV contents, so we rewrite
      * it to a TEMPORARY VIEW over `delta.<mvLocation> VERSION AS OF <pre-refresh-version>`.
      */
    case object OldSnapshotCreate extends StatementKind

    /** Recompute-cascade snapshot of the POST-refresh rows to be inserted into
      * the MV and the downstream view-delta.
      *
      * Rewritten to `CREATE OR REPLACE TEMPORARY VIEW openivm_new_<view> AS …`;
      * unlike `openivm_old_<view>`, this query reads only stable source staging / live
      * source tables during the refresh, so a temp view is sufficient.
      */
    case object NewSnapshotCreate extends StatementKind

    /** Recompute-cascade data-table INSERT fed by `openivm_new_<view>`.
      *
      * Shape (WINDOW_PARTITION / GROUP_RECOMPUTE with the new pragma enabled):
      * `INSERT INTO openivm_data_<view> SELECT * FROM openivm_new_<view>`.
      */
    case object SnapshotDataInsert extends StatementKind

    /** Drop the pragma-gated recompute snapshot scratch objects:
      * `DROP TABLE IF EXISTS openivm_old_<view>` / `openivm_new_<view>`.
      * They are rewritten to `DROP VIEW IF EXISTS` because Spark materialises
      * them as TEMPORARY VIEWs.
      */
    case object SnapshotDrop extends StatementKind

    /** WINDOW_PARTITION (type 5) DELETE: `DELETE FROM openivm_data_<view> WHERE
      * <part_col> IN (SELECT DISTINCT <src_col> FROM openivm_delta_<src> WHERE
      * openivm_timestamp > '<ts>'::TIMESTAMP)`, with one `IN (SELECT …)` clause
      * per partition column OR-joined together (see openivm
      * `refresh_compiler_aux.cpp:265-291`).  Delta Lake does NOT support
      * `IN (subquery)` in `DELETE`, so we rewrite each clause as a `MERGE INTO
      * <mv> AS v USING (<subquery>) AS d ON v.<col> <=> d.<col> WHEN MATCHED
      * THEN DELETE` and emit one MERGE per clause.  Running the per-clause
      * MERGEs in sequence is semantically equivalent to the original OR-joined
      * DELETE: any row matching ANY clause is deleted. */
    case object PartitionScopedDelete extends StatementKind

    /** WINDOW_PARTITION (type 5) INSERT: `INSERT INTO openivm_data_<view>
      * SELECT * FROM (<view_body referencing memory.main.<src>>) openivm_recompute
      * WHERE <part_col> IN (SELECT DISTINCT <src_col> FROM openivm_delta_<src>
      * WHERE openivm_timestamp > '<ts>'::TIMESTAMP)`.  Spark / Delta DO support
      * `IN (subquery)` in `INSERT … SELECT … WHERE`, so we keep the original
      * shape: substitute `openivm_data_<view>` → MV name, rewrite
      * `memory.main.<x>` → `` `<x>` ``, and strip the inner timestamp filter
      * (the Spark staging delta temp view already restricts visible rows). */
    case object PartitionScopedInsert      extends StatementKind
    case object RunningWindowTempCreate    extends StatementKind
    case object RunningWindowFastInsert    extends StatementKind
    case object RunningWindowCascadeInsert extends StatementKind
    case object RunningWindowTempDrop      extends StatementKind
    case object Cleanup                    extends StatementKind
    case object Unknown                    extends StatementKind
  }

  private def classify(stmt: String, viewLogicalName: String): StatementKind = {
    val upper             = stmt.toUpperCase.trim
    val affectedKeysName  = s"OPENIVM_AFFECTED_${viewLogicalName.toUpperCase}"
    val currentName       = s"OPENIVM_CURRENT_${viewLogicalName.toUpperCase}"
    val oldSnapshotName   = s"OPENIVM_OLD_${viewLogicalName.toUpperCase}"
    val newSnapshotName   = s"OPENIVM_NEW_${viewLogicalName.toUpperCase}"
    val runTempPrefix     = "OPENIVM_RUN_"
    val runTempViewSuffix = s"_${viewLogicalName.toUpperCase}"
    val compactName       = s"OPENIVM_OLD_COMPACT_${viewLogicalName.toUpperCase}"
    // openivm-side compact_delta_view cleanup statements:
    //   1. CREATE TEMP TABLE openivm_old_compact_<view> AS SELECT ... FROM openivm_delta_<view> GROUP BY ...
    //   2. DELETE FROM openivm_delta_<view> WHERE ...
    //   3. INSERT INTO openivm_delta_<view> SELECT ... FROM openivm_old_compact_<view>
    //   4. DROP TABLE openivm_old_compact_<view>
    // These collapse the duck-side delta table after the MERGE; the Spark side
    // already writes each refresh's view-delta to a fresh `viewDeltaPath`, so
    // none of them are relevant — drop unconditionally.
    if (upper.contains(compactName)) {
      return StatementKind.Cleanup
    }
    if (upper.startsWith("UPDATE OPENIVM_VIEWS")) {
      StatementKind.InProgressFlag
    } else if (
      upper.startsWith(s"CREATE OR REPLACE TEMP TABLE $runTempPrefix") && upper.contains(s"$runTempViewSuffix AS")
    ) {
      StatementKind.RunningWindowTempCreate
    } else if (upper.startsWith(s"CREATE OR REPLACE TEMP TABLE $affectedKeysName")) {
      // GROUP_RECOMPUTE Statement B: TEMP TABLE materialising affected group keys.
      StatementKind.GroupRecomputeAffectedCreate
    } else if (upper.startsWith(s"CREATE OR REPLACE TEMP TABLE $currentName")) {
      StatementKind.CurrentSnapshotCreate
    } else if (upper.startsWith(s"CREATE OR REPLACE TEMP TABLE $oldSnapshotName")) {
      StatementKind.OldSnapshotCreate
    } else if (upper.startsWith(s"CREATE OR REPLACE TEMP TABLE $newSnapshotName")) {
      StatementKind.NewSnapshotCreate
    } else if (upper.startsWith(s"DROP TABLE IF EXISTS $runTempPrefix") && upper.contains(runTempViewSuffix)) {
      StatementKind.RunningWindowTempDrop
    } else if (upper.startsWith(s"DROP TABLE IF EXISTS $affectedKeysName")) {
      // GROUP_RECOMPUTE Statement E: cleanup of the affected-keys scratch object.
      StatementKind.GroupRecomputeAffectedDrop
    } else if (upper.startsWith(s"DROP TABLE IF EXISTS $currentName")) {
      StatementKind.CurrentSnapshotDrop
    } else if (
      upper.startsWith(s"DROP TABLE IF EXISTS $oldSnapshotName") ||
      upper.startsWith(s"DROP TABLE IF EXISTS $newSnapshotName")
    ) {
      StatementKind.SnapshotDrop
    } else if (
      upper.contains(s"INSERT INTO OPENIVM_DELTA_${viewLogicalName.toUpperCase}") &&
      upper.contains("OPENIVM_RUN_FAST_")
    ) {
      // WINDOW running-suffix fast-path cascade delta: an APPEND of the
      // suffix-appended rows (multiplicity +1) into openivm_delta_<view>, whose
      // SELECT reads the source delta + the run_fast/run_state temp views. The
      // fallback cascade (openivm_old/openivm_new signed-multiset) is emitted
      // FIRST and CTAS-creates the view-delta path (ViewDeltaInsert); this
      // statement appends to it. Distinguished from the fallback cascade by the
      // OPENIVM_RUN_FAST_ reference (the fallback reads openivm_old/openivm_new).
      StatementKind.RunningWindowCascadeInsert
    } else if (upper.contains(s"INSERT INTO OPENIVM_DELTA_${viewLogicalName.toUpperCase}")) {
      // Distinguish the AGGREGATE_GROUP retract companion (refresh_sql.cpp:620,
      // emitted when `force_view_delta_cascade=true`) from the main
      // view-delta CTAS.  Both target `openivm_delta_<view>`.  The companion is
      // an APPEND of synthesized retract rows; its SELECT body references the
      // delta-view itself in the FROM clause (`FROM openivm_delta_<view> d
      // WHERE d.openivm_multiplicity > 0 AND EXISTS (… openivm_data_<view> …)`).
      // The main delta-query never reads back from openivm_delta_<view>.
      val deltaSelf = s"openivm_delta_${viewLogicalName}".toUpperCase
      val insertIdx = upper.indexOf(s"INSERT INTO OPENIVM_DELTA_${viewLogicalName.toUpperCase}")
      val tail      = if (insertIdx >= 0) upper.substring(insertIdx) else upper
      // First-occurrence is the INSERT keyword itself; check if the table is
      // referenced AGAIN after the column list — that indicates a self-join
      // companion shape rather than a delta-query CTAS.
      val nextOccurrence = tail.indexOf(deltaSelf, deltaSelf.length)
      if (nextOccurrence > 0) StatementKind.ViewDeltaCompanion
      else StatementKind.ViewDeltaInsert
    } else if (upper.contains(s"MERGE INTO OPENIVM_DATA_${viewLogicalName.toUpperCase}")) {
      StatementKind.MvMerge
    } else if (
      upper.startsWith(s"DELETE FROM OPENIVM_DATA_${viewLogicalName.toUpperCase}") &&
      containsInSubquery(upper)
    ) {
      // WINDOW_PARTITION (type 5) DELETE: `DELETE FROM openivm_data_<v> WHERE <part>
      // IN (SELECT DISTINCT <part> FROM openivm_delta_<src> WHERE …)`.  Delta does
      // not support `IN (subquery)` in DELETE; rewriter emits one MERGE per IN clause.
      // The `IN (SELECT` marker distinguishes this from the MIN/MAX bare DELETE
      // (handled by ScalarDeleteMv) and from GROUP_RECOMPUTE / AGGREGATE_GROUP+minmax
      // DELETEs (both use `WHERE EXISTS`, not `WHERE IN`).
      StatementKind.PartitionScopedDelete
    } else if (upper.contains(s"DELETE FROM OPENIVM_DATA_${viewLogicalName.toUpperCase}")) {
      StatementKind.ScalarDeleteMv
    } else if (
      upper.contains(s"INSERT INTO OPENIVM_DATA_${viewLogicalName.toUpperCase}") &&
      upper.contains(s"FROM $newSnapshotName")
    ) {
      StatementKind.SnapshotDataInsert
    } else if (
      upper.contains(s"INSERT INTO OPENIVM_DATA_${viewLogicalName.toUpperCase}") &&
      upper.contains(" OPENIVM_RUN_FAST_")
    ) {
      StatementKind.RunningWindowFastInsert
    } else if (
      upper.contains(s"INSERT INTO OPENIVM_DATA_${viewLogicalName.toUpperCase}") &&
      upper.contains("OPENIVM_RECOMPUTE") &&
      containsInSubquery(upper)
    ) {
      // WINDOW_PARTITION (type 5) INSERT: `INSERT INTO openivm_data_<v> SELECT * FROM
      // (<view body referencing memory.main.<src>>) openivm_recompute WHERE <part> IN
      // (SELECT DISTINCT <part> FROM openivm_delta_<src> WHERE …)`.  The `openivm_recompute`
      // alias is openivm's marker for `BuildDeleteInsertRefreshSQL` output
      // (refresh_helpers.cpp:178-184); together with an `IN (SELECT …)` clause it
      // identifies the WINDOW_PARTITION INSERT (vs the MIN/MAX full-recompute INSERT,
      // which has neither marker).
      StatementKind.PartitionScopedInsert
    } else if (
      upper.contains(s"INSERT INTO OPENIVM_DATA_${viewLogicalName.toUpperCase}") &&
      upper.contains("MEMORY.MAIN.")
    ) {
      // Full-recompute INSERT for non-additive aggregates (MIN/MAX) and GROUP_RECOMPUTE
      // statement D: reads from live source. The presence of `memory.main.` distinguishes
      // it from the SIMPLE_PROJECTION delta-fed INSERT.
      StatementKind.ScalarFullRecomputeInsert
    } else if (upper.contains(s"INSERT INTO OPENIVM_DATA_${viewLogicalName.toUpperCase}")) {
      StatementKind.SimpleProjectionDataInsert
    } else if (upper.contains(s"UPDATE OPENIVM_DATA_${viewLogicalName.toUpperCase}")) {
      StatementKind.ScalarUpdate
    } else if (upper.startsWith(s"DELETE FROM OPENIVM_DATA_${viewLogicalName.toUpperCase}")) {
      StatementKind.ScalarDeleteMv
    } else if (upper.startsWith("DELETE FROM") || upper.startsWith("UPDATE ")) {
      StatementKind.Cleanup
    } else {
      StatementKind.Unknown
    }
  }

  // ── Statement B rewrite (view-delta INSERT → CTAS) ───────────────────────

  private def rewriteViewDeltaInsert(
      stmt: String,
      viewLogicalName: String,
      viewDeltaPath: String,
      deltaShape: Map[String, DeltaShape],
      semiJoinPruneEnabled: Boolean,
      fkTermPruneEnabled: Boolean,
      fkRelations: Seq[ForeignKeyRelation],
      uniqueKeys: Seq[UniqueKey],
      uniqueJoinSimplifyEnabled: Boolean
  ): String = {
    var s = stmt
    s = pruneUnchangedDeltaUnionTerms(s, deltaShape)
    s = pruneFkRedundantDeltaUnionTerms(s, deltaShape, semiJoinPruneEnabled && fkTermPruneEnabled, fkRelations)
    s = semiJoinPruneFullSourceCtes(s, deltaShape, semiJoinPruneEnabled)
    s = simplifyUniqueKeyJoins(s, uniqueKeys, uniqueJoinSimplifyEnabled)
    s = deduplicateCteColumnAliases(s)
    s = stripTimestampPredicate(s)
    s = rewriteMemoryMainPrefix(s)
    s = rewriteInsertToCtas(s, viewLogicalName, viewDeltaPath)
    s = rewriteInsertNoColumnListToCtas(s, viewLogicalName, viewDeltaPath)
    s
  }

  /** Drop inclusion-exclusion UNION ALL arms that select a source delta proven
    * empty for this refresh. If every arm would be dropped, keep the original
    * SQL so the optimization is default-safe.
    */
  private[common] def pruneUnchangedDeltaUnionTerms(sql: String, deltaShape: Map[String, DeltaShape]): String = {
    val unchanged = deltaShape.collect { case (table, DeltaShape.Unchanged) => shortTableName(table).toLowerCase }.toSet
    val changed   = deltaShape.collect { case (table, shape) if shape != DeltaShape.Unchanged => shortTableName(table) }
    val enabled =
      sys.props.get("openivm.refresh.emptyDeltaSkip").orElse(sys.env.get("OPENIVM_EMPTY_DELTA_SKIP")).contains("true")
    if (!enabled || unchanged.isEmpty || changed.isEmpty) sql
    else pruneUnionTermsInSql(sql, unchanged)
  }

  private def shortTableName(table: String): String =
    table.split("\\.").last.replace("`", "").replace("\"", "")

  private def narrowestOverlapSignal(
      table: String,
      delta: WorkloadDeltaStats,
      columnStats: Map[String, WorkloadColumnStats],
      maxOverlapRatio: Double
  ): Option[String] = {
    val tableNorm = normalizeSqlIdentifier(table)
    val candidates = (delta.min.keySet ++ delta.max.keySet).flatMap { column =>
      for {
        deltaMin <- delta.min.get(column)
        deltaMax <- delta.max.get(column)
        base     <- columnStatsFor(columnStats, tableNorm, column)
        baseMin  <- base.min
        baseMax  <- base.max
        ratio    <- overlapRatio(deltaMin, deltaMax, baseMin, baseMax)
        if ratio <= maxOverlapRatio
      } yield s"min_max_overlap column=$column ratio=${"%.6f".formatLocal(java.util.Locale.ROOT, ratio)}<=${maxOverlapRatio}"
    }
    candidates.toSeq.sorted.headOption
  }

  private def columnStatsFor(
      columnStats: Map[String, WorkloadColumnStats],
      tableNorm: String,
      column: String
  ): Option[WorkloadColumnStats] = {
    val columnNorm = normalizeSqlIdentifier(column)
    columnStats
      .get(s"$tableNorm.$columnNorm")
      .orElse {
        columnStats.collectFirst {
          case (key, stat)
              if normalizeSqlIdentifier(key) == s"$tableNorm.$columnNorm" ||
                normalizeSqlIdentifier(key).endsWith(s".$tableNorm.$columnNorm") =>
            stat
        }
      }
  }

  private def overlapRatio(deltaMin: String, deltaMax: String, baseMin: String, baseMax: String): Option[Double] =
    (toDecimal(deltaMin), toDecimal(deltaMax), toDecimal(baseMin), toDecimal(baseMax)) match {
      case (Some(dMin), Some(dMax), Some(bMin), Some(bMax)) =>
        val deltaLo = dMin.min(dMax)
        val deltaHi = dMin.max(dMax)
        val baseLo  = bMin.min(bMax)
        val baseHi  = bMin.max(bMax)
        if (deltaHi < baseLo || baseHi < deltaLo) Some(0.0d)
        else {
          val baseWidth = baseHi - baseLo
          if (baseWidth == BigDecimal(0)) Some(if (deltaLo == baseLo && deltaHi == baseHi) 0.0d else 1.0d)
          else {
            val overlapLo = deltaLo.max(baseLo)
            val overlapHi = deltaHi.min(baseHi)
            Some(((overlapHi - overlapLo) / baseWidth).toDouble.max(0.0d))
          }
        }
      case _ if deltaMin == deltaMax && baseMin <= deltaMin && deltaMin <= baseMax =>
        Some(0.0d)
      case _ => None
    }

  private def toDecimal(value: String): Option[BigDecimal] =
    scala.util.Try(BigDecimal(value)).toOption

  private case class FkTermPruneRelation(
      childShortName: String,
      childColumns: Seq[String],
      parentShortName: String,
      parentColumns: Seq[String]
  )

  /** Drop higher-order inclusion-exclusion terms whose delta set contains both
    * sides of a declared child→parent FK when both sides are proven append-only
    * (or unchanged) for this refresh. The child-delta/full-parent arm is kept,
    * so inserted child rows still join to the post-refresh parent; this only
    * removes the FK-redundant correction arm and leaves the full-source Δparent
    * shape visible to [[semiJoinPruneFullSourceCtes]].
    */
  private[common] def pruneFkRedundantDeltaUnionTerms(
      sql: String,
      deltaShape: Map[String, DeltaShape],
      enabled: Boolean,
      fkRelations: Seq[ForeignKeyRelation]
  ): String = {
    if (!enabled || fkRelations.isEmpty) return sql

    val shapesByShort = deltaShape.map { case (table, shape) => shortTableName(table).toLowerCase -> shape }
    def appendOnlyOrUnchanged(short: String): Boolean =
      shapesByShort
        .get(short.toLowerCase)
        .exists(shape => shape == DeltaShape.InsertOnly || shape == DeltaShape.Unchanged)

    val eligible = fkRelations
      .filter(fk => fk.rely && fk.childColumns.nonEmpty && fk.parentColumns.nonEmpty)
      .map { fk =>
        FkTermPruneRelation(
          shortTableName(fk.childTable).toLowerCase,
          fk.childColumns.map(stripBackticks).map(_.toLowerCase),
          shortTableName(fk.parentTable).toLowerCase,
          fk.parentColumns.map(stripBackticks).map(_.toLowerCase)
        )
      }
      .filter(fk => appendOnlyOrUnchanged(fk.childShortName) && appendOnlyOrUnchanged(fk.parentShortName))

    if (eligible.isEmpty) sql
    else pruneUnionTermsInSql(sql, term => fkRedundantUnionTerm(term, eligible))
  }

  private[common] def semiJoinPruneFullSourceCtes(
      sql: String,
      deltaShape: Map[String, DeltaShape],
      enabled: Boolean
  ): String = {
    if (!enabled) return sql
    val changedShortNames =
      deltaShape.collect {
        case (table, shape) if shape != DeltaShape.Unchanged => shortTableName(table).toLowerCase
      }.toSet
    if (changedShortNames.isEmpty) return sql

    val predicates = semiJoinPrunePredicates(sql, changedShortNames)
    if (predicates.isEmpty) sql
    else rewriteFullSourceCteBodies(sql, predicates)
  }

  private case class Scd2RelationRef(
      ref: String,
      shortName: String,
      alias: String,
      deltaSourceShortName: Option[String]
  )

  private case class SemiJoinPrunePredicate(sourceShortName: String, fullSourceCol: String, deltaCol: String) {
    def sql(fullSourceAlias: String): String =
      s"$fullSourceAlias.$fullSourceCol IN (SELECT $deltaCol FROM memory.main.openivm_delta_$sourceShortName)"
  }

  private def semiJoinPrunePredicates(sql: String, changedShortNames: Set[String]): Seq[SemiJoinPrunePredicate] = {
    val relations = collectScd2RelationRefs(sql)
    val byAlias = relations
      .groupBy(r => stripBackticks(r.alias))
    val equalityRe =
      "(?is)(\\w+|`[^`]+`)\\s*\\.\\s*(\\w+|`[^`]+`)\\s*=\\s*(\\w+|`[^`]+`)\\s*\\.\\s*(\\w+|`[^`]+`)".r

    equalityRe
      .findAllMatchIn(sql)
      .flatMap { m =>
        val lhsAlias = stripBackticks(m.group(1))
        val lhsCol   = m.group(2)
        val rhsAlias = stripBackticks(m.group(3))
        val rhsCol   = m.group(4)
        for {
          lhsRefs <- byAlias.get(lhsAlias).toSeq
          rhsRefs <- byAlias.get(rhsAlias).toSeq
          lhs     <- lhsRefs
          rhs     <- rhsRefs
          pred    <- semiJoinPrunePredicate(lhs, lhsCol, rhs, rhsCol, changedShortNames)
        } yield pred
      }
      .toVector
      .distinct
  }

  private def semiJoinPrunePredicate(
      lhs: Scd2RelationRef,
      lhsCol: String,
      rhs: Scd2RelationRef,
      rhsCol: String,
      changedShortNames: Set[String]
  ): Option[SemiJoinPrunePredicate] = {
    (lhs.shortName.equalsIgnoreCase("full_source"), rhs.deltaSourceShortName) match {
      case (true, Some(deltaShort)) if changedShortNames(deltaShort.toLowerCase) =>
        Some(SemiJoinPrunePredicate(deltaShort, lhsCol, rhsCol))
      case _ =>
        (rhs.shortName.equalsIgnoreCase("full_source"), lhs.deltaSourceShortName) match {
          case (true, Some(deltaShort)) if changedShortNames(deltaShort.toLowerCase) =>
            Some(SemiJoinPrunePredicate(deltaShort, rhsCol, lhsCol))
          case _ => None
        }
    }
  }

  private def collectScd2RelationRefs(sql: String): Seq[Scd2RelationRef] = {
    val refs = scala.collection.mutable.ArrayBuffer.empty[Scd2RelationRef]
    val stop = Set(
      "ON",
      "WHERE",
      "JOIN",
      "LEFT",
      "RIGHT",
      "FULL",
      "INNER",
      "OUTER",
      "CROSS",
      "GROUP",
      "ORDER",
      "HAVING",
      "LIMIT",
      "UNION",
      "WHEN",
      "USING",
      "AS"
    )

    scanSql(sql) { (idx, _, _) =>
      if (isKeywordAt(sql, idx, "FROM") || isKeywordAt(sql, idx, "JOIN")) {
        val keywordEnd = idx + (if (isKeywordAt(sql, idx, "FROM")) "FROM".length else "JOIN".length)
        val refStart   = skipWhitespace(sql, keywordEnd)
        if (refStart < sql.length && sql.charAt(refStart) != '(') {
          val refEnd = scanSqlTableRef(sql, refStart)
          if (refEnd > refStart) {
            val ref        = sql.substring(refStart, refEnd).replaceAll("\\s+", "")
            var aliasStart = skipWhitespace(sql, refEnd)
            if (isKeywordAt(sql, aliasStart, "AS")) aliasStart = skipWhitespace(sql, aliasStart + "AS".length)
            val aliasEnd   = scanBareToken(sql, aliasStart)
            val normalized = normalizeSqlIdentifier(ref)
            val short      = normalized.split("\\.").lastOption.getOrElse(normalized)
            val alias =
              if (aliasEnd > aliasStart) sql.substring(aliasStart, aliasEnd)
              else short
            if (!stop(alias.toUpperCase)) {
              val deltaShort = deltaSourceShortName(short)
              refs += Scd2RelationRef(ref, short, alias, deltaShort)
            }
          }
        }
      }
    }
    refs.toVector
  }

  private def deltaSourceShortName(shortName: String): Option[String] = {
    val prefix = "openivm_delta_"
    if (shortName.toLowerCase.startsWith(prefix)) Some(shortName.substring(prefix.length))
    else None
  }

  private def fkRedundantUnionTerm(term: String, fks: Seq[FkTermPruneRelation]): Boolean = {
    val refs            = collectScd2RelationRefs(term)
    val deltaShortNames = refs.flatMap(_.deltaSourceShortName.map(_.toLowerCase)).toSet
    fks.exists { fk =>
      deltaShortNames(fk.childShortName) &&
      deltaShortNames(fk.parentShortName) &&
      termHasFkJoinPredicate(term, refs, fk)
    }
  }

  private def termHasFkJoinPredicate(
      term: String,
      refs: Seq[Scd2RelationRef],
      fk: FkTermPruneRelation
  ): Boolean = {
    val aliasesByShort = refs
      .flatMap { ref =>
        val short = ref.deltaSourceShortName.getOrElse(ref.shortName).toLowerCase
        Some(short -> stripBackticks(ref.alias).toLowerCase)
      }
      .groupBy(_._1)
      .map { case (short, pairs) => short -> pairs.map(_._2).toSet }
    val childAliases  = aliasesByShort.getOrElse(fk.childShortName, Set.empty)
    val parentAliases = aliasesByShort.getOrElse(fk.parentShortName, Set.empty)
    if (childAliases.isEmpty || parentAliases.isEmpty) return false

    val equalities = equalityPredicates(term).map { case (a1, c1, a2, c2) =>
      (a1.toLowerCase, stripBackticks(c1).toLowerCase, a2.toLowerCase, stripBackticks(c2).toLowerCase)
    }
    fk.childColumns.zip(fk.parentColumns).forall { case (childCol, parentCol) =>
      equalities.exists { case (a1, c1, a2, c2) =>
        (childAliases(a1) && c1 == childCol && parentAliases(a2) && c2 == parentCol) ||
        (parentAliases(a1) && c1 == parentCol && childAliases(a2) && c2 == childCol)
      }
    }
  }

  private def equalityPredicates(sql: String): Seq[(String, String, String, String)] = {
    val equalityRe =
      "(?is)(\\w+|`[^`]+`)\\s*\\.\\s*(\\w+|`[^`]+`)\\s*=\\s*(\\w+|`[^`]+`)\\s*\\.\\s*(\\w+|`[^`]+`)".r
    equalityRe
      .findAllMatchIn(sql)
      .map(m => (stripBackticks(m.group(1)), m.group(2), stripBackticks(m.group(3)), m.group(4)))
      .toVector
  }

  private[common] def simplifyUniqueKeyJoins(
      sql: String,
      uniqueKeys: Seq[UniqueKey],
      enabled: Boolean
  ): String = {
    val keys = uniqueKeys
      .filter(k => k.rely && k.columns.nonEmpty)
      .map(k =>
        UniqueJoinKey(shortTableName(k.table).toLowerCase, k.columns.map(stripBackticks).map(_.toLowerCase).toSet)
      )
    if (!enabled || keys.isEmpty || !"(?is)\\bJOIN\\b".r.findFirstIn(sql).isDefined) sql
    else rewriteSelectBlocks(sql)(block => simplifyUniqueKeyJoinsInBlock(block, keys))
  }

  private def simplifyUniqueKeyJoinsInBlock(block: String, uniqueKeys: Seq[UniqueJoinKey]): String = {
    if (selectListHasWildcard(block)) return block
    val clauses = uniqueJoinClauses(block).filter { clause =>
      val shortName = normalizeSqlIdentifier(clause.relation).split("\\.").lastOption.getOrElse("")
      !shortName.startsWith("openivm_delta_") &&
      rightAliasUnusedOutsideJoin(block, clause) &&
      uniqueKeys.exists(key =>
        key.shortName == shortName && joinConditionCoversKey(clause.alias, clause.onCondition, key)
      )
    }
    if (clauses.isEmpty) block
    else {
      val probes = clauses.filter(_.joinType == "inner").map { clause =>
        val relationAndAlias =
          if (
            clause.alias.equalsIgnoreCase(normalizeSqlIdentifier(clause.relation).split("\\.").lastOption.getOrElse(""))
          )
            clause.relation
          else s"${clause.relation} ${clause.alias}"
        s"EXISTS (SELECT 1 FROM $relationAndAlias WHERE ${clause.onCondition.trim})"
      }
      val withoutJoins = clauses.sortBy(-_.removeStart).foldLeft(block) { case (current, clause) =>
        current.substring(0, clause.removeStart) + current.substring(clause.removeEnd)
      }
      probes.foldLeft(withoutJoins)(addTopLevelWhereConjunct)
    }
  }

  private def rewriteSelectBlocks(sql: String)(rewrite: String => String): String = {
    val offsets = selectKeywordOffsets(sql).filter(idx => topLevelSelectFromIdx(sql, idx).isDefined)
    if (offsets.isEmpty) sql
    else {
      val out = new StringBuilder(sql)
      offsets.sortBy(-_).foreach { selectIdx =>
        val end     = selectBlockEnd(sql, selectIdx)
        val block   = sql.substring(selectIdx, end)
        val updated = rewrite(block)
        if (updated != block) out.replace(selectIdx, end, updated)
      }
      out.toString
    }
  }

  private def uniqueJoinClauses(block: String): Seq[UniqueJoinClause] = {
    val clauses = scala.collection.mutable.ArrayBuffer.empty[UniqueJoinClause]
    scanSql(block) { (idx, _, depth) =>
      if (depth == 0 && isKeywordAt(block, idx, "JOIN")) {
        val (joinType, removeStart) = joinTypeAndStart(block, idx)
        if (joinType == "inner" || joinType == "left") {
          val refStart = skipWhitespace(block, idx + "JOIN".length)
          if (refStart < block.length && block.charAt(refStart) != '(') {
            val refEnd   = scanSqlTableRef(block, refStart)
            val relation = block.substring(refStart, refEnd).replaceAll("\\s+", "")
            var aliasAt  = skipWhitespace(block, refEnd)
            if (isKeywordAt(block, aliasAt, "AS")) aliasAt = skipWhitespace(block, aliasAt + "AS".length)
            val aliasEnd = scanBareToken(block, aliasAt)
            val short    = normalizeSqlIdentifier(relation).split("\\.").lastOption.getOrElse(relation)
            val alias =
              if (aliasEnd > aliasAt) block.substring(aliasAt, aliasEnd)
              else short
            if (!sqlClauseKeywords.contains(alias.toUpperCase)) {
              val onIdx = findTopLevelSqlKeyword(block, aliasEnd.max(refEnd), block.length, "ON")
              onIdx.foreach { onStart =>
                val onBodyStart = skipWhitespace(block, onStart + "ON".length)
                val onEnd       = nextJoinBoundary(block, onBodyStart)
                clauses += UniqueJoinClause(
                  removeStart,
                  onEnd,
                  joinType,
                  relation,
                  alias,
                  block.substring(onBodyStart, onEnd)
                )
              }
            }
          }
        }
      }
    }
    clauses.toVector
  }

  private def joinTypeAndStart(block: String, joinIdx: Int): (String, Int) = {
    val beforeJoin = block.substring(0, joinIdx)
    "(?is)(LEFT\\s+OUTER|LEFT|INNER)\\s*$".r.findFirstMatchIn(beforeJoin) match {
      case Some(m) if m.group(1).toUpperCase.startsWith("LEFT") => "left"  -> m.start
      case Some(m) if m.group(1).equalsIgnoreCase("INNER")      => "inner" -> m.start
      case _                                                    => "inner" -> joinIdx
    }
  }

  private def nextJoinBoundary(block: String, from: Int): Int = {
    val keywords = Seq("JOIN", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "UNION")
    var end      = block.length
    scanSql(block, from) { (idx, _, depth) =>
      if (idx > from && depth == 0 && end == block.length && keywords.exists(k => isKeywordAt(block, idx, k))) {
        end = if (isKeywordAt(block, idx, "JOIN")) {
          val (_, start) = joinTypeAndStart(block, idx)
          start
        } else idx
      }
    }
    end
  }

  private def rightAliasUnusedOutsideJoin(block: String, clause: UniqueJoinClause): Boolean = {
    val outside = block.substring(0, clause.removeStart) + " " + block.substring(clause.removeEnd)
    !referencesAlias(outside, clause.alias)
  }

  private def joinConditionCoversKey(alias: String, condition: String, key: UniqueJoinKey): Boolean =
    key.columns.forall(col => conditionHasAliasColumnEquality(condition, alias, col))

  private def conditionHasAliasColumnEquality(condition: String, alias: String, col: String): Boolean = {
    val a        = java.util.regex.Pattern.quote(alias)
    val c        = java.util.regex.Pattern.quote(col)
    val aliasCol = s"`?$a`?\\s*\\.\\s*`?$c`?"
    val otherCol = """(?:`?[A-Za-z_]\w*`?\s*\.\s*`?[A-Za-z_]\w*`?)"""
    val re       = (s"(?is)(?:$aliasCol\\s*(?:=|<=>)\\s*$otherCol|$otherCol\\s*(?:=|<=>)\\s*$aliasCol)").r
    re.findFirstIn(condition).isDefined
  }

  private def referencesAlias(text: String, alias: String): Boolean = {
    val a = java.util.regex.Pattern.quote(alias)
    (s"(?is)(?:^|[^A-Za-z0-9_`])`?$a`?\\s*\\.").r.findFirstIn(text).isDefined
  }

  private def selectListHasWildcard(block: String): Boolean =
    topLevelSelectFromIdx(block, 0).exists { fromIdx =>
      val selectList = block.substring("SELECT".length, fromIdx)
      "(?is)(^|,)\\s*(?:`?[A-Za-z_]\\w*`?\\s*\\.\\s*)?\\*\\s*(?:,|$)".r.findFirstIn(selectList).isDefined
    }

  private def topLevelSelectFromIdx(block: String, selectIdx: Int): Option[Int] =
    findTopLevelSqlKeyword(block, selectIdx + "SELECT".length, selectBlockEnd(block, selectIdx), "FROM")

  private def addTopLevelWhereConjunct(block: String, predicate: String): String = {
    val fromIdx  = topLevelSelectFromIdx(block, 0).getOrElse(return block)
    val whereIdx = findTopLevelSqlKeyword(block, fromIdx + "FROM".length, block.length, "WHERE")
    whereIdx match {
      case Some(idx) =>
        val end = nextWhereBoundary(block, idx + "WHERE".length)
        block.substring(0, end) + s" AND $predicate" + block.substring(end)
      case None =>
        val insertAt = nextWhereBoundary(block, fromIdx + "FROM".length)
        block.substring(0, insertAt) + s" WHERE $predicate" + block.substring(insertAt)
    }
  }

  private def nextWhereBoundary(block: String, from: Int): Int = {
    val keywords = Seq("GROUP", "HAVING", "ORDER", "LIMIT", "UNION")
    var end      = block.length
    scanSql(block, from) { (idx, _, depth) =>
      if (idx > from && depth == 0 && end == block.length && keywords.exists(k => isKeywordAt(block, idx, k))) end = idx
    }
    end
  }

  private def rewriteFullSourceCteBodies(sql: String, predicates: Seq[SemiJoinPrunePredicate]): String = {
    val out  = new StringBuilder(sql.length)
    var from = 0
    var i    = 0
    while (i < sql.length) {
      if (startsWithSqlKeyword(sql, i, "full_source")) {
        val bodyOpen = fullSourceCteBodyOpen(sql, i + "full_source".length)
        bodyOpen match {
          case Some(openIdx) =>
            val closeIdx = findMatchingCloseParen(sql, openIdx)
            if (closeIdx > openIdx) {
              out.append(sql.substring(from, openIdx + 1))
              out.append(wrapFullSourceBody(sql.substring(openIdx + 1, closeIdx), predicates))
              from = closeIdx
              i = closeIdx + 1
            } else i += "full_source".length
          case None => i += "full_source".length
        }
      } else {
        sql.charAt(i) match {
          case '\'' => i = skipSingleQuoted(sql, i)
          case '"'  => i = skipDelimited(sql, i, '"')
          case '`'  => i = skipDelimited(sql, i, '`')
          case _    => i += 1
        }
      }
    }
    if (from == 0) sql
    else {
      out.append(sql.substring(from))
      out.toString
    }
  }

  private def fullSourceCteBodyOpen(sql: String, from: Int): Option[Int] = {
    var i = skipWhitespace(sql, from)
    if (i < sql.length && sql.charAt(i) == '(') {
      val colsClose = findMatchingCloseParen(sql, i)
      if (colsClose < 0) return None
      i = skipWhitespace(sql, colsClose + 1)
    }
    if (!startsWithSqlKeyword(sql, i, "AS")) None
    else {
      i = skipWhitespace(sql, i + "AS".length)
      if (i < sql.length && sql.charAt(i) == '(') Some(i) else None
    }
  }

  private def wrapFullSourceBody(body: String, predicates: Seq[SemiJoinPrunePredicate]): String = {
    val alias = "__openivm_full_source_pre"
    val grouped = predicates
      .groupBy(_.sourceShortName.toLowerCase)
      .toSeq
      .sortBy(_._1)
      .map { case (_, ps) =>
        ps.sortBy(p => (p.fullSourceCol.toLowerCase, p.deltaCol.toLowerCase))
          .map(_.sql(alias))
          .mkString("(", " AND ", ")")
      }
    val where = grouped.mkString(" OR ")
    s"SELECT * FROM ($body) $alias WHERE $where"
  }

  private def pruneUnionTermsInSql(sql: String, unchangedShortNames: Set[String]): String = {
    pruneUnionTermsInSql(sql, term => referencesAnyUnchangedDelta(term, unchangedShortNames))
  }

  private def pruneUnionTermsInSql(sql: String, pruneTerm: String => Boolean): String = {
    val withNestedPruned = rewriteParenthesizedSql(sql)(inner => pruneUnionTermsInSql(inner, pruneTerm))
    val terms            = splitTopLevelUnionAll(withNestedPruned)
    if (terms.lengthCompare(2) < 0) withNestedPruned
    else {
      val kept = terms.filterNot(pruneTerm)
      if (kept.nonEmpty && kept.size < terms.size) kept.mkString(" UNION ALL ")
      else withNestedPruned
    }
  }

  private def referencesAnyUnchangedDelta(sql: String, unchangedShortNames: Set[String]): Boolean =
    unchangedShortNames.exists { short =>
      val ref = ("(?i)(?:\\b|`)openivm_delta_" + java.util.regex.Pattern.quote(short) + "(?:\\b|`)").r
      ref.findFirstIn(sql).isDefined
    }

  private def rewriteParenthesizedSql(sql: String)(rewrite: String => String): String = {
    val out = new StringBuilder(sql.length)
    var i   = 0
    while (i < sql.length) {
      sql.charAt(i) match {
        case '\'' =>
          val end = copySingleQuoted(sql, i, out)
          i = end
        case '"' =>
          val end = copyDelimited(sql, i, '"', out)
          i = end
        case '`' =>
          val end = copyDelimited(sql, i, '`', out)
          i = end
        case '(' =>
          val close = findMatchingCloseParen(sql, i)
          if (close > i) {
            out.append('(')
            out.append(rewrite(sql.substring(i + 1, close)))
            out.append(')')
            i = close + 1
          } else {
            out.append(sql.charAt(i))
            i += 1
          }
        case c =>
          out.append(c)
          i += 1
      }
    }
    out.toString
  }

  private def copySingleQuoted(sql: String, start: Int, out: StringBuilder): Int = {
    var i    = start
    var done = false
    while (i < sql.length && !done) {
      out.append(sql.charAt(i))
      if (sql.charAt(i) == '\'' && i + 1 < sql.length && sql.charAt(i + 1) == '\'') {
        out.append(sql.charAt(i + 1))
        i += 2
      } else if (sql.charAt(i) == '\'') {
        i += 1
        done = true
      } else i += 1
    }
    i
  }

  private def copyDelimited(sql: String, start: Int, delimiter: Char, out: StringBuilder): Int = {
    var i    = start
    var done = false
    while (i < sql.length && !done) {
      out.append(sql.charAt(i))
      if (sql.charAt(i) == delimiter) {
        i += 1
        done = true
      } else i += 1
    }
    i
  }

  private def splitTopLevelUnionAll(sql: String): Seq[String] = {
    val parts = scala.collection.mutable.ArrayBuffer.empty[String]
    var start = 0
    var i     = 0
    var depth = 0
    while (i < sql.length) {
      sql.charAt(i) match {
        case '\'' => i = skipSingleQuoted(sql, i)
        case '"'  => i = skipDelimited(sql, i, '"')
        case '`'  => i = skipDelimited(sql, i, '`')
        case '(' =>
          depth += 1
          i += 1
        case ')' =>
          depth = math.max(0, depth - 1)
          i += 1
        case _ if depth == 0 && startsWithUnionAll(sql, i) =>
          parts += sql.substring(start, i).trim
          i = unionAllEnd(sql, i)
          start = i
        case _ => i += 1
      }
    }
    if (parts.nonEmpty) {
      parts += sql.substring(start).trim
      parts.toSeq.filter(_.nonEmpty)
    } else Seq(sql)
  }

  private def startsWithUnionAll(sql: String, idx: Int): Boolean = {
    val end = unionAllEnd(sql, idx)
    end > idx && isWordBoundary(sql, idx - 1) && isWordBoundary(sql, end)
  }

  private def unionAllEnd(sql: String, idx: Int): Int = {
    if (!regionMatchesIgnoreCase(sql, idx, "union")) idx
    else {
      var i = idx + "union".length
      i = skipUnionWhitespace(sql, i)
      if (!regionMatchesIgnoreCase(sql, i, "all")) idx
      else i + "all".length
    }
  }

  private def regionMatchesIgnoreCase(sql: String, idx: Int, needle: String): Boolean =
    idx >= 0 && idx + needle.length <= sql.length && sql.regionMatches(true, idx, needle, 0, needle.length)

  private def skipUnionWhitespace(sql: String, idx: Int): Int = {
    var i = idx
    while (i < sql.length && sql.charAt(i).isWhitespace) i += 1
    i
  }

  private def isWordBoundary(sql: String, idx: Int): Boolean =
    idx < 0 || idx >= sql.length || !sql.charAt(idx).isLetterOrDigit && sql.charAt(idx) != '_'

  private def skipSingleQuoted(sql: String, start: Int): Int = {
    var i = start + 1
    while (i < sql.length) {
      if (sql.charAt(i) == '\'' && i + 1 < sql.length && sql.charAt(i + 1) == '\'') i += 2
      else if (sql.charAt(i) == '\'') return i + 1
      else i += 1
    }
    sql.length
  }

  private def skipDelimited(sql: String, start: Int, delimiter: Char): Int = {
    var i = start + 1
    while (i < sql.length) {
      if (sql.charAt(i) == delimiter) return i + 1
      i += 1
    }
    sql.length
  }

  /** Strip `openivm_timestamp [op] '<ts>'::TIMESTAMP` predicates that openivm
    * emits at the source-scan level.  The Spark-side staging catalog already
    * controls which staging Delta paths are visible, so the inner timestamp
    * filter is redundant (and references a value that's only tracked by
    * openivm's own bookkeeping table).
    *
    * Three forms arise in practice:
    *   1. Standalone: `WHERE openivm_timestamp OP '...'::TIMESTAMP`
    *      — remove the entire WHERE clause.
    *   2. Trailing AND (CTE with filter): `… WHERE <filter> AND openivm_timestamp OP '...'::TIMESTAMP`
    *      — openivm appends the timestamp guard after the CTE predicate; strip
    *        only the `AND openivm_timestamp …` tail so the CTE predicate stays.
    *   3. Leading AND: `WHERE openivm_timestamp OP '...'::TIMESTAMP AND <filter>`
    *      — strip only the `openivm_timestamp … AND ` prefix.
    */
  private def stripTimestampPredicate(sql: String): String = {
    // The column can be optionally qualified with a table alias prefix
    // (e.g. `d.openivm_timestamp` in the AGGREGATE_GROUP retract companion
    // emitted by openivm when `force_view_delta_cascade=true`).
    val qcol      = "(?:`?\\w+`?\\.)?`?openivm_timestamp`?"
    val tsLiteral = "(?:'[^']*'::\\s*TIMESTAMP|CAST\\s*\\(\\s*'[^']*'\\s+AS\\s+TIMESTAMP\\s*\\))"
    val cmp       = "\\s*(?:>=|>|<=|<|=)\\s*"
    // LPTS rewrites parenthesises each WHERE conjunct
    // (`WHERE (`openivm_timestamp`>=CAST(...))`), so match the predicate either
    // wrapped in a balanced paren pair or bare (pre-merge single-line form).
    val tsPred = "(?:\\(\\s*" + qcol + cmp + tsLiteral + "\\s*\\)|" + qcol + cmp + tsLiteral + ")"
    // Case 1: standalone `WHERE [(]openivm_timestamp OP '...'::TIMESTAMP[)]`
    val standalone = ("(?i)\\s+WHERE\\s+" + tsPred).r
    // Case 2: trailing `AND [(]openivm_timestamp OP '...'::TIMESTAMP[)]`
    val trailingAnd = ("(?i)\\s+AND\\s+" + tsPred).r
    // Case 3: leading `[(]openivm_timestamp OP '...'::TIMESTAMP[)] AND `
    val leadingAnd = ("(?i)" + tsPred + "\\s+AND\\s+").r

    leadingAnd.replaceAllIn(
      trailingAnd.replaceAllIn(
        standalone.replaceAllIn(sql, ""),
        ""
      ),
      ""
    )
  }

  /** Replace `memory.main.<identifier>` (bare or backticked) → either
    *   - `` `<db>`.`<table>` `` if `<identifier>` is a tracked source whose
    *     fully-qualified name is registered in [[activeQualifiedNames]]; OR
    *   - `` `<identifier>` `` otherwise (the default openivm internal name
    *     e.g. `openivm_delta_<n>`, `openivm_data_<v>`).
    *
    * openivm usually emits the DuckDB catalog prefix `memory.main.` when the
    * SPARK target dialect is selected, but some translated statements arrive
    * as the equivalent backticked multipart identifier
    * `` `memory`.`main`.`<identifier>` ``. On the Spark side most of these
    * reference our internal temp views/tables (which we created with the
    * short name), but live-source references (`memory.main.<short>`) must
    * be expanded to `<db>.<table>` when the user's view body referenced a
    * Hive-qualified table — otherwise Spark resolves `<short>` against the
    * session's current_schema (typically `default`) and fails to find it.
    */
  private def rewriteMemoryMainPrefix(sql: String): String = {
    val qualifiedMap = activeQualifiedNames.get()
    val re =
      """(?i)(?:`?memory`?\s*\.\s*`?main`?\s*\.\s*`?([A-Za-z0-9_]+)`?)""".r
    re.replaceAllIn(
      sql,
      m => {
        val short = m.group(1)
        qualifiedMap.get(short) match {
          case Some(qual) if qual.contains(".") =>
            val parts = qual.split("\\.")
            // Wrap each segment in backticks so Spark resolves the table
            // against the specific database in the qualified name, not the
            // current session schema. `quoteReplacement` keeps regex meta-
            // characters (`$`, `\\`) in identifier strings inert.
            java.util.regex.Matcher.quoteReplacement(parts.map(p => s"`$p`").mkString("."))
          case _ =>
            java.util.regex.Matcher.quoteReplacement(s"`$short`")
        }
      }
    )
  }

  /** Transform openivm's `WITH ... INSERT INTO openivm_delta_<view> (cols) SELECT * FROM <lastCte>`
    * tail into a Spark CTAS that materialises the view delta to a per-refresh
    * Delta scratch path:
    *   {{{
    *     CREATE OR REPLACE TABLE delta.`<viewDeltaPath>` USING DELTA AS
    *       <CTEs>
    *       SELECT cteCol1 AS insertCol1, … FROM <lastCte>
    *   }}}
    * The aliases match openivm's INSERT column list so downstream readers see
    * the user-facing column names.
    */
  private def rewriteInsertToCtas(
      stmt: String,
      viewLogicalName: String,
      viewDeltaPath: String
  ): String = {
    val insertRe = ("(?is)\\bINSERT\\s+INTO\\s+(?:`?openivm_delta_" +
      java.util.regex.Pattern.quote(viewLogicalName) +
      "`?)\\s*\\(([^)]+)\\)\\s+SELECT\\s+\\*\\s+FROM\\s+(\\w+)\\s*$").r

    insertRe.findFirstMatchIn(stmt) match {
      case None =>
        rewriteInsertToCtasFallback(stmt, viewLogicalName, viewDeltaPath)
      case Some(m) =>
        val rawInsertCols = m.group(1).split(",").map(_.trim).toSeq
        // Normalise DuckDB double-quoted column names ("name") → backtick (`name`)
        val insertCols = rawInsertCols.map { col =>
          if (col.startsWith("\"") && col.endsWith("\""))
            s"`${col.substring(1, col.length - 1)}`"
          else col
        }
        val lastCteName = m.group(2).trim
        val ctePrefix   = stmt.substring(0, m.start).trim

        val cteColRe = ("(?i)\\b" + java.util.regex.Pattern.quote(lastCteName) +
          "\\s*\\(([^)]+)\\)\\s+AS\\s+\\(").r
        val cteCols = cteColRe
          .findFirstMatchIn(ctePrefix)
          .map(_.group(1).split(",").map(_.trim).toSeq)
          .getOrElse(insertCols)

        val selectCols = cteCols
          .zip(insertCols)
          .map { case (src, tgt) => s"$src AS $tgt" }
          .mkString(", ")

        val escapedPath = viewDeltaPath.replace("`", "``")
        s"""CREATE OR REPLACE TABLE delta.`$escapedPath` USING DELTA AS
           |$ctePrefix
           |SELECT $selectCols FROM $lastCteName""".stripMargin
    }
  }

  /** Fallback CTAS rewrite for the empty-placeholder INSERT form emitted by
    * openivm when one source table has no staging delta rows. openivm emits:
    * {{{
    *   INSERT INTO openivm_delta_<view> (col1, col2, …)
    *     SELECT CAST(NULL AS T1), CAST(NULL AS T2), … WHERE false
    * }}}
    * (no leading CTE, no `FROM` clause).  This wraps the SELECT in an inline
    * CTE so Spark can derive the schema and produce an empty-but-typed Delta
    * table, which statement C then reads (finding zero rows) to perform a
    * no-op refresh.
    */
  private def rewriteInsertToCtasFallback(
      stmt: String,
      viewLogicalName: String,
      viewDeltaPath: String
  ): String = {
    val fallbackRe = ("(?is)^\\s*INSERT\\s+INTO\\s+(?:`?openivm_delta_" +
      java.util.regex.Pattern.quote(viewLogicalName) +
      "`?)\\s*\\(([^)]+)\\)\\s+(SELECT\\b.+)$").r

    fallbackRe.findFirstMatchIn(stmt) match {
      case None => stmt
      case Some(m) =>
        val rawInsertCols = m.group(1).split(",").map(_.trim).toSeq
        val insertCols = rawInsertCols.map { col =>
          if (col.startsWith("\"") && col.endsWith("\""))
            s"`${col.substring(1, col.length - 1)}`"
          else col
        }
        val selectBody  = m.group(2).trim
        val colList     = insertCols.mkString(", ")
        val escapedPath = viewDeltaPath.replace("`", "``")
        s"""CREATE OR REPLACE TABLE delta.`$escapedPath` USING DELTA AS
           |WITH __openivm_placeholder ($colList) AS ($selectBody)
           |SELECT * FROM __openivm_placeholder""".stripMargin
    }
  }

  /** Fallback CTAS rewrite for pragma-gated recompute-cascade delta INSERTs
    * that omit the target column list and instead rely on positional insert
    * into the pre-created DuckDB `openivm_delta_<view>` table:
    *
    *   INSERT INTO openivm_delta_<view>
    *   SELECT *, CAST(-1 AS INTEGER), CURRENT_TIMESTAMP FROM openivm_old_<view>
    *   UNION ALL
    *   SELECT *, CAST(1 AS INTEGER), CURRENT_TIMESTAMP FROM openivm_new_<view>
    *
    * Spark CTAS has no pre-declared target schema, so the trailing metadata
    * expressions must be aliased explicitly as `openivm_multiplicity` /
    * `openivm_timestamp` before materialising the SELECT.
    */
  private def rewriteInsertNoColumnListToCtas(
      stmt: String,
      viewLogicalName: String,
      viewDeltaPath: String
  ): String = {
    val bareInsertRe = ("(?is)^\\s*INSERT\\s+INTO\\s+(?:`?openivm_delta_" +
      java.util.regex.Pattern.quote(viewLogicalName) +
      "`?)\\s+(SELECT\\b.+)$").r

    bareInsertRe.findFirstMatchIn(stmt) match {
      case None => stmt
      case Some(m) =>
        val selectBodyUpper = m.group(1).toUpperCase
        if (
          !selectBodyUpper.contains(s"OPENIVM_OLD_${viewLogicalName.toUpperCase}") &&
          !selectBodyUpper.contains(s"OPENIVM_NEW_${viewLogicalName.toUpperCase}")
        ) {
          stmt
        } else {
          val multAliased = """(?i)CAST\(\s*[+-]?\d+\s+AS\s+INTEGER\s*\)(?!\s+AS\s+openivm_multiplicity\b)""".r
            .replaceAllIn(m.group(1).trim, m0 => s"${m0.matched} AS openivm_multiplicity")
          val tsAliased = """(?i)\bCURRENT_TIMESTAMP(?:\(\))?(?!\s+AS\s+openivm_timestamp\b)""".r
            .replaceAllIn(multAliased, m0 => s"${m0.matched} AS openivm_timestamp")
          val escapedPath = viewDeltaPath.replace("`", "``")
          s"""CREATE OR REPLACE TABLE delta.`$escapedPath` USING DELTA AS
             |$tsAliased""".stripMargin
        }
    }
  }

  /** Rewrite the AGGREGATE_GROUP / AGGREGATE_HAVING retract companion INSERT
    * (emitted by openivm when `force_view_delta_cascade=true`):
    *
    *   INSERT INTO openivm_delta_<view> (cols)
    *   SELECT d.<key>, 0, …, -1
    *   FROM   openivm_delta_<view> d
    *   WHERE  d.openivm_multiplicity > 0 AND d.openivm_timestamp > '…'::TIMESTAMP
    *     AND  EXISTS (SELECT 1 FROM openivm_data_<view> m WHERE …);
    *
    * into:
    *
    *   INSERT INTO delta.`<viewDeltaPath>` (cols)
    *   SELECT d.<key>, 0, …, -1
    *   FROM   delta.`<viewDeltaPath>` d
    *   WHERE  d.openivm_multiplicity > 0
    *     AND  EXISTS (SELECT 1 FROM <mvName> m WHERE …);
    *
    * Notes:
    *  - The `openivm_timestamp` predicate is stripped (the per-refresh
    *    view-delta path contains only this refresh's rows, no historical
    *    timestamp filter needed).
    *  - Uses `INSERT INTO delta.\`<path>\`` (append) NOT `CREATE OR REPLACE`,
    *    so the retract rows are added to the existing CTAS output rather
    *    than replacing it.
    */
  private def rewriteViewDeltaCompanion(
      stmt: String,
      viewLogicalName: String,
      mvName: TableIdentifier,
      viewDeltaPath: String
  ): String = {
    var s           = stmt
    val escapedPath = viewDeltaPath.replace("`", "``")

    // Replace the INSERT target: `INSERT INTO openivm_delta_<view>` → `INSERT INTO delta.\`<path>\``
    val insertTargetRe = ("(?i)INSERT\\s+INTO\\s+`?openivm_delta_" +
      java.util.regex.Pattern.quote(viewLogicalName) + "`?").r
    s = insertTargetRe.replaceAllIn(
      s,
      java.util.regex.Matcher.quoteReplacement(s"INSERT INTO delta.`$escapedPath`")
    )

    // Replace the FROM/EXISTS source delta-view references: `openivm_delta_<view>` → `delta.\`<path>\``
    val deltaViewRe = ("(?i)\\bopenivm_delta_" +
      java.util.regex.Pattern.quote(viewLogicalName) + "\\b").r
    s = deltaViewRe.replaceAllIn(s, java.util.regex.Matcher.quoteReplacement(s"delta.`$escapedPath`"))

    // Replace data-table reference inside the EXISTS subquery: `openivm_data_<view>` → MV table name
    val dataViewRe = ("(?i)\\bopenivm_data_" +
      java.util.regex.Pattern.quote(viewLogicalName) + "\\b").r
    val mvSqlName = backtickMvName(mvName)
    s = dataViewRe.replaceAllIn(s, java.util.regex.Matcher.quoteReplacement(mvSqlName))

    // Strip the openivm_timestamp predicate (the view-delta path is per-refresh
    // scratch; no historical timestamp horizon to filter against).
    s = stripTimestampPredicate(s)

    s
  }

  // ── Statement C rewrite (MERGE INTO openivm_data_<view>) ─────────────────

  private def rewriteMvMerge(
      stmt: String,
      viewLogicalName: String,
      mvName: TableIdentifier,
      viewDeltaPath: String
  ): String = {
    var s = stmt
    s = stripTimestampPredicate(s)

    val deltaViewRe = ("(?i)\\bopenivm_delta_" +
      java.util.regex.Pattern.quote(viewLogicalName) + "\\b").r
    val escapedPath = viewDeltaPath.replace("`", "``")
    s = deltaViewRe.replaceAllIn(s, java.util.regex.Matcher.quoteReplacement(s"delta.`$escapedPath`"))

    val dataViewRe = ("(?i)\\bopenivm_data_" +
      java.util.regex.Pattern.quote(viewLogicalName) + "\\b").r
    val mvSqlName = backtickMvName(mvName)

    // First, replace all openivm_data_<view> references with the MV name.
    s = dataViewRe.replaceAllIn(s, java.util.regex.Matcher.quoteReplacement(mvSqlName))

    // After the replacement, if this is a MERGE with an alias, Spark/Delta
    // requires column references in the ON / WHEN clauses to use the alias
    // — not the fully-qualified table name.  Detect the alias from the
    // already-rewritten `MERGE INTO <mv> [AS] <alias> USING` and replace
    // any `<mv>.` column-ref prefixes in the body with `<alias>.`.
    // Note: the compiler may emit newlines between the alias and USING,
    // so whitespace matching must handle \n, not just spaces.
    val mergeIntoPrefix = s"MERGE INTO $mvSqlName"
    val usingRe         = "(?i)\\sUSING\\s".r
    val usingMatch      = usingRe.findFirstMatchIn(s)
    if (s.contains(mergeIntoPrefix) && usingMatch.isDefined) {
      val usingIdx = usingMatch.get.start
      val afterMv  = s.substring(mergeIntoPrefix.length, usingIdx).trim
      val alias = afterMv
        .replaceFirst("(?i)^AS\\s+", "")
        .replaceAll("\"", "")
        .trim
      if (alias.nonEmpty && alias.matches("\\w+")) {
        val mvDotPrefix = mvSqlName + "."
        val body        = s.substring(usingIdx)
        val fixedBody   = body.replace(mvDotPrefix, alias + ".")
        s = s.substring(0, usingIdx) + fixedBody
      }
    }

    s
  }

  // ── Statement C (SIMPLE_PROJECTION): bag-correct view-delta apply ─────────

  /** Rewrites the SIMPLE_PROJECTION view-delta apply (statement C of the SP
    * refresh program) into a Spark-executable **bag-correct** 3-statement
    * sequence.
    *
    * Input (single statement, DuckDB-target):
    * {{{
    *   INSERT INTO openivm_data_<view> SELECT <user_cols>
    *   FROM openivm_delta_<view>, generate_series(1, openivm_multiplicity::BIGINT)
    *   WHERE openivm_timestamp > '<ts>'::TIMESTAMP AND openivm_multiplicity > 0
    * }}}
    *
    * Output (three Spark statements, returned in execution order):
    *
    * Statement A — consolidate the view-delta into one net row per
    * `(user_cols)` group (mirroring DuckDB's `openivm_net` CTE at
    * `.temp/openivm/src/upsert/refresh_compiler.cpp:825-826`):
    * {{{
    *   CREATE OR REPLACE TEMPORARY VIEW `openivm_sp_net_<view>` AS
    *     SELECT <user_cols>, SUM(CAST(`openivm_multiplicity` AS BIGINT)) AS __openivm_net
    *     FROM delta.<viewDeltaPath>
    *     GROUP BY <user_cols>
    *     HAVING SUM(CAST(`openivm_multiplicity` AS BIGINT)) != 0
    * }}}
    *
    * Statement B — MERGE-DELETE all MV rows matching any negative-net group.
    * Delta `WHEN MATCHED THEN DELETE` deletes ALL matching MV rows per source
    * row by design here — this is the **over-delete** that Statement C
    * compensates for. Marked with `SimpleProjectionDeleteMergeMarker` so the
    * dispatcher can skip it when the view-delta has no negative-multiplicity
    * rows.
    * {{{
    *   /*OPENIVM_SIMPLE_PROJECTION_DELETE_MERGE*/
    *   MERGE INTO <mv> AS v
    *   USING (SELECT <user_cols> FROM `openivm_sp_net_<view>` WHERE __openivm_net < 0) AS d
    *   ON v.<c1> IS NOT DISTINCT FROM d.<c1> AND ...
    *   WHEN MATCHED THEN DELETE
    * }}}
    *
    * Statement C — INSERT the bag-correct count for every affected group:
    *   - For groups with `_net > 0`: insert `_net` copies (positive-side
    *     INSERT, equivalent to DuckDB's
    *     `INSERT … FROM openivm_net, generate_series(1, _net)` at
    *     `refresh_compiler.cpp:854-860`).
    *   - For groups with `_net < 0`: re-INSERT `max(0, _cur + _net)` copies
    *     where `_cur` is the **pre-DELETE** MV bag-count for the group, read
    *     via Delta time travel `delta.<mvLocation> VERSION AS OF <V>`. This
    *     restores the `(_cur - |_net|)` copies that Statement B over-deleted.
    *
    * Reference DuckDB-side `rowid + ROW_NUMBER()` per-group delete
    * (`refresh_compiler.cpp:828-852`) is functionally equivalent: it removes
    * exactly `|_net|` copies per group. Spark Delta has no exposed `rowid`
    * usable in a MERGE/DELETE predicate, so we instead over-delete and
    * replenish from a Delta-time-travel snapshot of the MV taken at
    * `mvVersionBeforeRefresh` — the version visible at refresh start, which
    * is the pre-DELETE state because no other statements in the SP refresh
    * program touch `<mv>` between refresh-start and Statement B.
    *
    * Bag-correctness guarantee: for every `(user_cols)` group `g`, letting
    * `cur_g` be the pre-refresh MV bag-count and `net_g` be the delta net,
    *   - `net_g >  0` → MV ends with `cur_g + net_g` copies of `g`
    *                   (DELETE skips `g`, INSERT adds `net_g` new copies).
    *   - `net_g <  0` → MV ends with `max(0, cur_g + net_g)` copies of `g`
    *                   (DELETE removes all `cur_g` copies, INSERT adds
    *                   `max(0, cur_g + net_g)` back).
    *   - `net_g == 0` → MV unchanged.
    *
    * Failure mode this fixes: when an upstream cascade compaction (or even
    * the initial CREATE, which inserts each source row independently) leaves
    * the MV with N value-equal copies of a tuple, and a subsequent
    * incremental refresh retracts ONLY M < N of those source rows (delta
    * carries `_net = -M`), the pre-fix value-equality
    * `MERGE … WHEN MATCHED THEN DELETE` removed ALL N copies, leaving
    * `N - M` MV rows missing. This was the SF=10 `gold.fact_holdings
    * diff=18` failure shape and the `fact_watches` SF=3 failure documented
    * in `.research/OPENIVM-BUG.md`. See `SimpleProjectionBagDeleteSpec`
    * for the unit-scale regression coverage.
    *
    * Multiplicity expansion via `LATERAL VIEW EXPLODE(SEQUENCE(...))` is the
    * Spark-side equivalent of openivm's `generate_series(1, mul::BIGINT)`
    * (`refresh_compiler.cpp:813-820, 854-860`). The positive-multiplicity
    * filter MUST stay inside the inner subquery so that `SEQUENCE(1, n)` is
    * never evaluated with `n <= 0` — Spark's `SEQUENCE` defaults step to
    * `-1` when `stop < start` and returns the descending `[1, 0]` /
    * `[1, 0, -1]` etc., which would over-insert.
    *
    * If `mvVersionBeforeRefresh` is `None`, falls back to reading the
    * post-DELETE MV state (which yields `_cur = 0` for every retracted
    * group, so the bag-replenish becomes a no-op — i.e. the pre-fix
    * over-delete behaviour). All in-tree callers pass `Some(meta.lastVersion)`
    * from [[org.openivm.spark.commands.MaterializedViewCommands]].
    */
  private def rewriteSimpleProjectionDataInsert(
      stmt: String,
      viewLogicalName: String,
      mvName: TableIdentifier,
      mvLocation: String,
      viewDeltaPath: String,
      mvVersionBeforeRefresh: Option[Long]
  ): Seq[String] = {
    val mv          = backtickMvName(mvName)
    val escapedPath = viewDeltaPath.replace("`", "``")
    val deltaRef    = s"delta.`$escapedPath`"

    // Extract user columns from the SIMPLE_PROJECTION apply statement. Older
    // openivm emitted FROM openivm_delta_<view> directly; current openivm first
    // materialises an openivm_net CTE and inserts from that net relation.
    val insertSelectPrefix = "(?is)\\bINSERT\\s+INTO\\s+openivm_data_" +
      java.util.regex.Pattern.quote(viewLogicalName) +
      "\\b\\s+SELECT\\s+(.*?)\\s*\\bFROM\\s+"
    val selectRes = Seq(
      (insertSelectPrefix + "(?:`?openivm_delta_" +
        java.util.regex.Pattern.quote(viewLogicalName) +
        "`?)").r,
      (insertSelectPrefix + "(?:`?openivm_net`?)\\b").r
    )

    val userCols: Seq[String] = selectRes.view.flatMap(_.findFirstMatchIn(stmt)).headOption match {
      case None => Nil
      case Some(m) =>
        m.group(1)
          .split(",")
          .map(_.trim)
          .map { col =>
            // Normalise DuckDB double-quoted identifiers: "name" -> `name`
            if (col.startsWith("\"") && col.endsWith("\""))
              s"`${col.substring(1, col.length - 1)}`"
            else
              s"`${col.replace("`", "``")}`"
          }
          .toSeq
    }

    if (userCols.isEmpty) return Nil

    val colList = userCols.mkString(", ")
    // Unique-per-MV temp view name. `viewLogicalName` is the short MV name
    // (`TableIdentifier.table`), normally a bare ident; backtick-escaped at
    // the use site to be safe.
    val netView           = s"openivm_sp_net_$viewLogicalName"
    val netViewBackticked = s"`${netView.replace("`", "``")}`"

    // NULL-safe value-equality (mirrors DuckDB's `IS NOT DISTINCT FROM`)
    val nullSafeEq = (lhs: String, rhs: String) =>
      userCols.map(c => s"$lhs.$c IS NOT DISTINCT FROM $rhs.$c").mkString(" AND ")

    // ─── Statement A: consolidate the view-delta into one net row per group ───
    val createNetView =
      s"""|CREATE OR REPLACE TEMPORARY VIEW $netViewBackticked AS
          |SELECT $colList,
          |       SUM(CAST(`openivm_multiplicity` AS BIGINT)) AS __openivm_net
          |FROM $deltaRef
          |GROUP BY $colList
          |HAVING SUM(CAST(`openivm_multiplicity` AS BIGINT)) != 0""".stripMargin

    // ─── Statement B: MERGE-DELETE all MV rows in any negative-net group ───
    // This deletes ALL matching copies (Delta MERGE semantics), which
    // over-deletes when MV has more value-equal copies than |_net|.
    // Statement C compensates by re-inserting the bag-correct count.
    val deleteMergeSql = markSimpleProjectionDeleteMerge(
      s"""|MERGE INTO $mv AS v
          |USING (
          |  SELECT $colList
          |  FROM $netViewBackticked
          |  WHERE __openivm_net < 0
          |) AS d
          |ON ${nullSafeEq("v", "d")}
          |WHEN MATCHED THEN DELETE""".stripMargin
    )

    // ─── Statement C: bag-correct INSERT ───
    // Combines:
    //   • positive-net groups → insert `_net` copies (no over-delete to undo).
    //   • negative-net groups → insert `max(0, _cur + _net)` copies, where
    //                            `_cur` is the **pre-DELETE** MV bag-count
    //                            read via Delta time travel.
    val escapedMvLocation = mvLocation.replace("`", "``")
    val mvSnapshotRef = mvVersionBeforeRefresh match {
      case Some(v) => s"delta.`$escapedMvLocation` VERSION AS OF $v"
      case None    => s"delta.`$escapedMvLocation`"
    }
    val mvUserCols  = userCols.map(c => s"v.$c").mkString(", ")
    val aliasMvCols = userCols.map(c => s"v.$c AS $c").mkString(", ")

    val insertSql =
      s"""|INSERT INTO $mv
          |SELECT $colList
          |FROM (
          |  SELECT ${userCols.map(c => s"n.$c AS $c").mkString(", ")},
          |    CASE
          |      WHEN n.__openivm_net > 0 THEN n.__openivm_net
          |      WHEN n.__openivm_net < 0
          |        THEN GREATEST(
          |               CAST(0 AS BIGINT),
          |               COALESCE(a.__openivm_cur, CAST(0 AS BIGINT)) + n.__openivm_net
          |             )
          |      ELSE CAST(0 AS BIGINT)
          |    END AS __openivm_to_insert
          |  FROM $netViewBackticked n
          |  LEFT JOIN (
          |    SELECT $aliasMvCols, COUNT(*) AS __openivm_cur
          |    FROM $mvSnapshotRef v
          |    JOIN $netViewBackticked n2
          |      ON n2.__openivm_net < 0 AND ${nullSafeEq("v", "n2")}
          |    GROUP BY $mvUserCols
          |  ) a ON ${nullSafeEq("n", "a")}
          |) __openivm_src
          |LATERAL VIEW EXPLODE(
          |  SEQUENCE(CAST(1 AS BIGINT), __openivm_src.__openivm_to_insert)
          |) __openivm_lv AS __openivm_idx
          |WHERE __openivm_src.__openivm_to_insert > 0""".stripMargin

    Seq(createNetView, deleteMergeSql, insertSql)
  }

  // ── SIMPLE_AGGREGATE Statements C/D/E rewrite (ScalarUpdate) ─────────────

  /** Rewrites SIMPLE_AGGREGATE statements that target `openivm_data_<view>`:
    *
    *   C (CTE form): `WITH openivm_delta AS (SELECT SUM(mult*col) AS d_col FROM openivm_delta_<view> WHERE ts) UPDATE openivm_data_<view> SET col = COALESCE(col,0) + COALESCE((SELECT d_col FROM openivm_delta),0)`
    *     → Spark SQL doesn't support `WITH` before `UPDATE`, so the CTE is inlined
    *       into scalar subqueries: `UPDATE <mv> SET col = COALESCE(col,0) + COALESCE((SELECT SUM(mult*col) FROM delta.<viewDeltaPath>),0)`
    *
    *   D (recompute form): `UPDATE openivm_data_<view> SET col = <expr>`  (no WHERE)
    *     → `UPDATE <mv> SET col = <expr>`
    *
    *   E (null-reset form): `UPDATE openivm_data_<view> SET col=NULL WHERE NOT EXISTS (SELECT 1 FROM memory.main.<src> LIMIT 1)`
    *     → Delta Lake doesn't support subqueries in the UPDATE WHERE clause, so this
    *       is rewritten as a MERGE that evaluates the source row-count in the USING clause:
    *       `MERGE INTO <mv> AS v USING (SELECT COUNT(*) AS _cnt FROM \`<src>\`) AS _chk ON TRUE WHEN MATCHED AND _chk._cnt = 0 THEN UPDATE SET col=NULL`
    */
  private def rewriteScalarUpdate(
      stmt: String,
      viewLogicalName: String,
      mvName: TableIdentifier,
      viewDeltaPath: String
  ): String = {
    val escapedPath = viewDeltaPath.replace("`", "``")
    val deltaRef    = s"delta.`$escapedPath`"
    val mvRef       = backtickMvName(mvName)

    // Replace openivm_data_<view> → actual MV name
    val dataViewRe = ("(?i)\\bopenivm_data_" +
      java.util.regex.Pattern.quote(viewLogicalName) + "\\b").r
    var s = dataViewRe.replaceAllIn(stmt, java.util.regex.Matcher.quoteReplacement(mvRef))

    // Rewrite memory.main.<source> for the null-reset NOT EXISTS subquery
    s = rewriteMemoryMainPrefix(s)

    if (s.trim.toUpperCase.startsWith("WITH")) {
      // CTE form (Statement C): Spark SQL doesn't support WITH before UPDATE; inline the CTE
      s = stripTimestampPredicate(s)
      val deltaViewRe = ("(?i)\\bopenivm_delta_" +
        java.util.regex.Pattern.quote(viewLogicalName) + "\\b").r
      s = deltaViewRe.replaceAllIn(s, java.util.regex.Matcher.quoteReplacement(deltaRef))
      s = inlineOpeniVmDeltaCte(s, deltaRef)
    } else if (s.toUpperCase.contains("WHERE NOT EXISTS")) {
      // Null-reset form (Statement E): Delta doesn't support subqueries in UPDATE WHERE;
      // rewrite as MERGE using the source table count in the USING clause.
      s = rewriteNullResetAsMerge(s, mvRef)
    }
    // Recompute form (Statement D) needs no special handling beyond name replacement.
    s
  }

  /** Rewrites `DELETE FROM openivm_data_<view>` → `DELETE FROM <mv>`.
    *
    * Emitted by openivm for:
    *   - non-additive aggregates (MIN/MAX) before the full-recompute INSERT
    *     (no WHERE clause, no timestamp predicate);
    *   - GROUP_RECOMPUTE statement 2 (WHERE EXISTS referencing
    *     `openivm_affected_<view>` temp object — no timestamp predicate);
    *   - AGGREGATE_GROUP + has_minmax: `DELETE FROM openivm_data_<view> AS
    *     openivm_tgt WHERE EXISTS (SELECT 1 FROM (SELECT DISTINCT <keys>
    *     FROM openivm_delta_<view>) AS openivm_aff WHERE …)` — inline affected-
    *     keys subquery, needs `openivm_delta_<view>` → `delta.<viewDeltaPath>`
    *     substitution AND the surrounding `DELETE … WHERE EXISTS` reshaped to
    *     a `MERGE … WHEN MATCHED THEN DELETE` because Delta Lake disallows
    *     subqueries in DELETE WHERE conditions
    *     (refresh_compiler.cpp:372-387, refresh_helpers.cpp:195-218);
    *   - WINDOW_PARTITION delete-by-partition (WHERE <part_col> IN (SELECT
    *     DISTINCT <part_col> FROM openivm_delta_<source> WHERE openivm_timestamp > '<ts>'::TIMESTAMP)
    *     — the timestamp predicate must be stripped because the Spark-side
    *     staging-delta temp view already restricts visible rows to the pending
    *     batch).
    *
    * `stripTimestampPredicate` is a safe no-op when the inner subquery has no
    * timestamp filter (MIN/MAX, AGGREGATE_GROUP+minmax, and GROUP_RECOMPUTE shapes).
    */
  private def rewriteScalarDeleteMv(
      stmt: String,
      viewLogicalName: String,
      mvName: TableIdentifier,
      viewDeltaPath: String
  ): Seq[String] = {
    val mvRef = backtickMvName(mvName)
    val dataViewRe = ("(?i)\\bopenivm_data_" +
      java.util.regex.Pattern.quote(viewLogicalName) + "\\b").r
    var s = dataViewRe.replaceAllIn(stmt, java.util.regex.Matcher.quoteReplacement(mvRef))
    s = rewriteMemoryMainPrefix(s)
    s = stripTimestampPredicate(s)
    // AGGREGATE_GROUP+minmax affected-keys subquery references the view delta
    // table inline (no separate openivm_affected_<view> temp object).  Repoint
    // those references at the per-refresh CTAS Delta path.  No-op for SIMPLE_
    // AGGREGATE MIN/MAX (no openivm_delta_<view> reference in the DELETE) and
    // for GROUP_RECOMPUTE (which uses the openivm_affected_<view> temp view).
    val escapedPath = viewDeltaPath.replace("`", "``")
    val deltaViewRe = ("(?i)\\bopenivm_delta_" +
      java.util.regex.Pattern.quote(viewLogicalName) + "\\b").r
    s = deltaViewRe.replaceAllIn(s, java.util.regex.Matcher.quoteReplacement(s"delta.`$escapedPath`"))
    // openivm's self-join SIMPLE_PROJECTION delete keys on the virtual `rowid`
    // column of `openivm_data_<v>` (a DuckDB-only pseudo-column). Spark Delta
    // has no rowid, so the rewrite cannot be executed. Drop the DELETE so the
    // CREATE-time apply probe demotes the MV to FULL_REFRESH via
    // `simple_projection_no_apply` instead of producing a runtime error.
    if (referencesRowid(s)) return Seq.empty
    // Delta Lake forbids DELETE subqueries and DELETE ... USING. Rewrite the
    // affected-key delete forms that openivm emits for outer-join partial
    // recompute into Delta-compatible MERGE ... WHEN MATCHED THEN DELETE.
    val usingRewritten = rewriteDeleteUsingAsMerge(s, mvRef)
    if (usingRewritten != s) return Seq(usingRewritten)
    val inRewritten = rewriteDeleteInAsMerge(s, mvRef)
    if (inRewritten.nonEmpty) return inRewritten
    Seq(rewriteDeleteExistsAsMerge(s, mvRef))
  }

  /** Rewrites the full-recompute `INSERT INTO openivm_data_<view> WITH scan_0 … SELECT … FROM memory.main.<src>`
    * into `INSERT INTO <mv> WITH scan_0 … SELECT … FROM \`<src>\``.
    *
    * Emitted by openivm for:
    *   - non-additive aggregates (MIN/MAX) — the MV is cleared by a preceding
    *     ScalarDeleteMv statement and then fully re-populated from the live source;
    *   - GROUP_RECOMPUTE statement 3 (`WHERE EXISTS … openivm_affected_<view>`);
    *   - AGGREGATE_GROUP + has_minmax: `INSERT INTO openivm_data_<view> SELECT *
    *     FROM (<view_query_sql>) openivm_recompute WHERE EXISTS (SELECT 1 FROM
    *     (SELECT DISTINCT <keys> FROM openivm_delta_<view>) AS openivm_aff
    *     WHERE …)` — inline affected-keys subquery, needs `openivm_delta_<view>`
    *     → `delta.<viewDeltaPath>` substitution
    *     (refresh_compiler.cpp:372-387, refresh_helpers.cpp:195-218);
    *   - WINDOW_PARTITION recompute INSERT (`SELECT * FROM (<view_body>) openivm_recompute
    *     WHERE <part_col> IN (SELECT DISTINCT <part_col> FROM openivm_delta_<source>
    *     WHERE openivm_timestamp > '<ts>'::TIMESTAMP)`) — the timestamp predicate
    *     must be stripped because the Spark-side staging-delta temp view already
    *     restricts visible rows to the pending batch.
    *
    * `stripTimestampPredicate` is a safe no-op for the MIN/MAX, AGGREGATE_GROUP+minmax,
    * and GROUP_RECOMPUTE shapes (their INSERT WHERE clauses reference temp objects
    * or inline subqueries, not delta tables with timestamps).
    */
  private def rewriteScalarFullRecomputeInsert(
      stmt: String,
      viewLogicalName: String,
      mvName: TableIdentifier,
      mvLocation: String,
      viewDeltaPath: String
  ): String = {
    val mvRef = backtickMvName(mvName)
    val dataViewRe = ("(?i)\\bopenivm_data_" +
      java.util.regex.Pattern.quote(viewLogicalName) + "\\b").r
    var s = dataViewRe.replaceAllIn(stmt, java.util.regex.Matcher.quoteReplacement(mvRef))
    s = rewriteMemoryMainPrefix(s)
    s = stripTimestampPredicate(s)
    // AGGREGATE_GROUP+minmax: inline affected-keys subquery references the
    // view delta table.  Repoint at the per-refresh CTAS Delta path.  No-op
    // for SIMPLE_AGGREGATE MIN/MAX and for the temp-table GROUP_RECOMPUTE form.
    val escapedPath = viewDeltaPath.replace("`", "``")
    val deltaViewRe = ("(?i)\\bopenivm_delta_" +
      java.util.regex.Pattern.quote(viewLogicalName) + "\\b").r
    s = deltaViewRe.replaceAllIn(s, java.util.regex.Matcher.quoteReplacement(s"delta.`$escapedPath`"))
    val escapedLocation = mvLocation.replace("`", "``")
    s = rewriteInsertSelectStarFromSubquery(s, mvRef, s"delta.`$escapedLocation`")
    s
  }

  private def rewriteInsertSelectStarFromSubquery(stmt: String, mvRef: String, writeTargetRef: String): String = {
    val prefixRe = ("(?is)\\bINSERT\\s+INTO\\s+" + java.util.regex.Pattern.quote(mvRef) +
      "\\s+SELECT\\s+\\*\\s+FROM\\s*\\(").r
    prefixRe.findFirstMatchIn(stmt) match {
      case None => stmt
      case Some(m) =>
        val openIdx  = m.end - 1
        val closeIdx = findMatchingCloseParen(stmt, openIdx)
        if (closeIdx < 0) return stmt
        val afterSubquery = stmt.substring(closeIdx + 1)
        val aliasRe       = """(?is)^\s+(\w+)\b(.*)$""".r
        aliasRe.findFirstMatchIn(afterSubquery) match {
          case None => stmt
          case Some(aliasMatch) =>
            val alias = aliasMatch.group(1)
            val rest  = aliasMatch.group(2)
            val inner = stmt.substring(openIdx + 1, closeIdx)
            finalSelectAliases(inner) match {
              case Nil => stmt
              case aliases =>
                val statementPrefix = stmt.substring(0, m.start).trim
                val projection      = aliases.map(c => s"$alias.$c").mkString(", ")
                val insertCols      = aliases.mkString(", ")
                val sourceSql =
                  if (statementPrefix.nonEmpty) {
                    s"""|(
                        |$statementPrefix
                        |SELECT $projection FROM ($inner) $alias$rest
                        |)""".stripMargin
                  } else {
                    s"""|(
                        |SELECT $projection FROM ($inner) $alias$rest
                        |)""".stripMargin
                  }
                val values = aliases.map(c => s"d.$c").mkString(", ")
                s"""|MERGE INTO $writeTargetRef AS v
                    |USING $sourceSql AS d
                    |ON false
                    |WHEN NOT MATCHED THEN INSERT ($insertCols) VALUES ($values)""".stripMargin
            }
        }
    }
  }

  private def finalSelectAliases(sql: String): Seq[String] = {
    var depth       = 0
    var lastSelect  = -1
    var i           = 0
    val upperLength = sql.length
    while (i < upperLength) {
      sql.charAt(i) match {
        case '\'' => i = consumeSqlSingleQuoted(sql, i)
        case '"'  => i = consumeSqlDoubleQuoted(sql, i)
        case '('  => depth += 1; i += 1
        case ')'  => depth -= 1; i += 1
        case _ if depth == 0 && startsWithSqlKeyword(sql, i, "SELECT") =>
          lastSelect = i
          i += "SELECT".length
        case _ => i += 1
      }
    }
    if (lastSelect < 0) Nil
    else {
      findTopLevelSqlKeyword(sql, lastSelect + "SELECT".length, sql.length, "FROM") match {
        case None => Nil
        case Some(fromIdx) =>
          splitSelectList(sql.substring(lastSelect + "SELECT".length, fromIdx)).flatMap(selectItemAlias(_))
      }
    }
  }

  private def selectItemAlias(item: String): Option[String] = {
    val trimmed = item.trim
    val asRe    = """(?is)^.+\s+AS\s+(`[^`]+`|\"[^\"]+\"|\w+)\s*$""".r
    val raw     = asRe.findFirstMatchIn(trimmed).map(_.group(1)).getOrElse(trimmed.split("\\.").last.trim)
    if (raw.isEmpty || raw.contains(" ") || raw.contains("(")) None else Some(normalizeColumnRef(raw))
  }

  private def findTopLevelSqlKeyword(sql: String, start: Int, endExclusive: Int, keyword: String): Option[Int] = {
    var depth = 0
    var i     = start
    while (i < endExclusive) {
      sql.charAt(i) match {
        case '\'' => i = consumeSqlSingleQuoted(sql, i).min(endExclusive)
        case '"'  => i = consumeSqlDoubleQuoted(sql, i).min(endExclusive)
        case '('  => depth += 1; i += 1
        case ')'  => depth -= 1; i += 1
        case _ if depth == 0 && startsWithSqlKeyword(sql, i, keyword) => return Some(i)
        case _                                                        => i += 1
      }
    }
    None
  }

  private def startsWithSqlKeyword(sql: String, start: Int, keyword: String): Boolean = {
    val end = start + keyword.length
    end <= sql.length &&
    sql.regionMatches(true, start, keyword, 0, keyword.length) &&
    (start == 0 || !isSqlIdentifierPart(sql.charAt(start - 1))) &&
    (end == sql.length || !isSqlIdentifierPart(sql.charAt(end)))
  }

  private def isSqlIdentifierPart(c: Char): Boolean = c.isLetterOrDigit || c == '_'

  private def consumeSqlSingleQuoted(sql: String, start: Int): Int = {
    var i = start + 1
    while (i < sql.length) {
      if (sql.charAt(i) == '\'' && i + 1 < sql.length && sql.charAt(i + 1) == '\'') i += 2
      else if (sql.charAt(i) == '\'') return i + 1
      else i += 1
    }
    sql.length
  }

  private def consumeSqlDoubleQuoted(sql: String, start: Int): Int = {
    var i = start + 1
    while (i < sql.length) {
      if (sql.charAt(i) == '"' && i + 1 < sql.length && sql.charAt(i + 1) == '"') i += 2
      else if (sql.charAt(i) == '"') return i + 1
      else i += 1
    }
    sql.length
  }

  /** Rewrites `DELETE FROM <mv> AS t USING <source> s WHERE <match>` into a
    * Delta-compatible delete MERGE. Handles an optional leading CTE by moving it
    * inside the MERGE source subquery.
    */
  private def rewriteDeleteUsingAsMerge(stmt: String, mvRef: String): String = {
    val deleteRe = ("(?is)^(.*?)\\bDELETE\\s+FROM\\s+" + java.util.regex.Pattern.quote(mvRef) +
      "(?:\\s+AS\\s+(\\w+))?\\s+USING\\s+(\\S+)\\s+(?:AS\\s+)?(\\w+)\\s+WHERE\\s+(.+?)\\s*;?\\s*$").r
    deleteRe.findFirstMatchIn(stmt) match {
      case None => stmt
      case Some(m) =>
        val ctePrefix = m.group(1).trim
        val tgtAlias  = Option(m.group(2)).getOrElse("v")
        val src       = m.group(3).trim
        val srcAlias  = m.group(4).trim
        val onCond    = m.group(5).trim
        // openivm's self-join SIMPLE_PROJECTION delete keys on the virtual
        // `rowid` column of `openivm_data_<v>`. Spark Delta tables have no
        // rowid, so rewriting that shape would emit a MERGE referencing a
        // non-existent column. Decline and let the CREATE-time apply probe
        // demote the MV to FULL_REFRESH via `simple_projection_no_apply`.
        if (referencesRowid(onCond)) stmt
        else {
          val usingSql =
            if (ctePrefix.nonEmpty) s"(\n$ctePrefix\nSELECT * FROM $src\n)"
            else src
          s"""|MERGE INTO $mvRef AS $tgtAlias
              |USING $usingSql AS $srcAlias
              |ON $onCond
              |WHEN MATCHED THEN DELETE""".stripMargin
        }
    }
  }

  private val rowidColumnRe = "(?i)(?<![A-Za-z0-9_])rowid(?![A-Za-z0-9_])".r

  private def referencesRowid(sql: String): Boolean =
    rowidColumnRe.findFirstIn(sql).isDefined

  /** Rewrites `DELETE FROM <mv> WHERE <col> IN (<subquery>) [OR ...]` into one
    * delete MERGE per top-level IN clause. Handles an optional leading CTE by
    * moving it inside each MERGE source subquery. Used for FULL OUTER projection
    * partial recompute over `openivm_left_key` / `openivm_right_key`.
    */
  private def rewriteDeleteInAsMerge(stmt: String, mvRef: String): Seq[String] = {
    val deleteRe = ("(?is)^(.*?)\\bDELETE\\s+FROM\\s+" + java.util.regex.Pattern.quote(mvRef) +
      "(?:\\s+AS\\s+(\\w+))?\\s+WHERE\\s+(.+?)\\s*;?\\s*$").r
    deleteRe.findFirstMatchIn(stmt) match {
      case None => Nil
      case Some(m) =>
        val ctePrefix = m.group(1).trim
        val tgtAlias  = Option(m.group(2)).getOrElse("v")
        val whereBody = m.group(3).trim
        splitTopLevelOr(whereBody).flatMap { clause =>
          inClauseToMergeWithOptionalCte(clause.trim, mvRef, tgtAlias, ctePrefix)
        }
    }
  }

  private def inClauseToMergeWithOptionalCte(
      clause: String,
      mvRef: String,
      tgtAlias: String,
      ctePrefix: String
  ): Option[String] = {
    val openIdx = clause.toUpperCase.indexOf(" IN ")
    if (openIdx < 0) return None
    val lhs  = clause.substring(0, openIdx).trim
    val rest = clause.substring(openIdx + 4).trim
    if (!rest.startsWith("(")) return None
    val close = findMatchingCloseParen(rest, 0)
    if (close < 0) return None
    val subq   = rest.substring(1, close).trim
    val rhsCol = selectedColumnAlias(subq).getOrElse(return None)
    // openivm's self-join SIMPLE_PROJECTION delete keys on the virtual `rowid`
    // column. Spark Delta has no rowid, so decline and let the CREATE-time
    // apply probe demote the MV to FULL_REFRESH via `simple_projection_no_apply`.
    if (referencesRowid(lhs) || referencesRowid(subq)) return None
    val source =
      if (ctePrefix.nonEmpty) s"(\n$ctePrefix\n$subq\n)"
      else s"($subq)"
    Some(
      s"""|MERGE INTO $mvRef AS $tgtAlias
          |USING $source AS d
          |ON $tgtAlias.$lhs IS NOT DISTINCT FROM d.$rhsCol
          |WHEN MATCHED THEN DELETE""".stripMargin
    )
  }

  private def selectedColumnAlias(selectSql: String): Option[String] = {
    val re = """(?is)^\s*SELECT\s+(?:DISTINCT\s+)?(.+?)\s+FROM\s+.+$""".r
    re.findFirstMatchIn(selectSql).flatMap { m =>
      val items = splitSelectList(m.group(1).trim)
      if (items.size != 1) None
      else {
        val item = items.head.trim
        val asRe = """(?is)^.+\s+AS\s+(`[^`]+`|\"[^\"]+\"|\w+)\s*$""".r
        val raw  = asRe.findFirstMatchIn(item).map(_.group(1)).getOrElse(item.split("\\.").last.trim)
        Some(normalizeColumnRef(raw))
      }
    }
  }

  private def normalizeColumnRef(col: String): String = {
    val trimmed = col.trim.stripPrefix("`").stripSuffix("`").stripPrefix("\"").stripSuffix("\"")
    s"`${trimmed.replace("`", "``")}`"
  }

  /** Rewrites the AGGREGATE_GROUP+minmax `DELETE FROM <mv> AS openivm_tgt
    * WHERE EXISTS (SELECT 1 FROM (<inner>) AS openivm_aff WHERE <match>)` form
    * into a Delta-compatible `MERGE INTO <mv> AS openivm_tgt USING (<inner>)
    * AS openivm_aff ON <match> WHEN MATCHED THEN DELETE`.
    *
    * Delta Lake's DELETE does not allow subqueries in the WHERE clause (it
    * raises DELTA_UNSUPPORTED_SUBQUERY), but MERGE accepts an arbitrary source
    * subquery, so reshaping is the only path that preserves the affected-keys
    * semantics.
    *
    * Returns the input unchanged if no such pattern is found (e.g. MIN/MAX
    * bare DELETE statements with no WHERE clause, or the GROUP_RECOMPUTE form
    * that already references a temp view rather than an inline subquery).
    *
    * Uses paren-aware extraction so nested subqueries inside the affected-keys
    * block (which the existing AGGREGATE_GROUP+minmax shape emits) are
    * captured correctly.
    */
  private def rewriteDeleteExistsAsMerge(stmt: String, mvRef: String): String = {
    val headerRe = ("(?is)\\bDELETE\\s+FROM\\s+" + java.util.regex.Pattern.quote(mvRef) +
      "(?:\\s+AS\\s+(\\w+))?\\s+WHERE\\s+EXISTS\\s*\\(").r
    headerRe.findFirstMatchIn(stmt) match {
      case None => stmt
      case Some(m) =>
        val tgtAlias    = Option(m.group(1)).getOrElse("v")
        val existsOpen  = m.end - 1
        val existsClose = findMatchingCloseParen(stmt, existsOpen)
        if (existsClose < 0) return stmt
        val existsBody = stmt.substring(existsOpen + 1, existsClose).trim
        val trailing   = stmt.substring(existsClose + 1).trim
        // The trailing text past the EXISTS must be just a terminator (`)` or
        // empty) — anything else means the surrounding context is more complex
        // than the inline affected-keys form and we bail out rather than
        // mangle the SQL.
        if (trailing.nonEmpty && trailing != ";") return stmt

        // EXISTS body forms (from refresh_helpers.cpp:195-218):
        //   (a) inline subquery — `SELECT 1 FROM (<sub>) AS <aff> WHERE <cond>`
        //   (b) temp object     — `SELECT 1 FROM <temp_obj> AS <aff> WHERE <cond>`
        val inlineRe = """(?is)^\s*SELECT\s+\S+\s+FROM\s+\(""".r
        inlineRe.findFirstMatchIn(existsBody) match {
          case Some(b) =>
            val subOpen  = b.end - 1
            val subClose = findMatchingCloseParen(existsBody, subOpen)
            if (subClose < 0) return stmt
            val subBody = existsBody.substring(subOpen + 1, subClose).trim
            val tail    = existsBody.substring(subClose + 1).trim
            val tailRe  = """(?is)^(?:AS\s+)?(\w+)\s+WHERE\s+(.+?)\s*$""".r
            tailRe
              .findFirstMatchIn(tail)
              .map { t =>
                val affAlias = t.group(1)
                val onCond   = t.group(2)
                s"""|MERGE INTO $mvRef AS $tgtAlias
                  |USING (
                  |$subBody
                  |) AS $affAlias
                  |ON $onCond
                  |WHEN MATCHED THEN DELETE""".stripMargin
              }
              .getOrElse(stmt)
          case None =>
            val tempRe = """(?is)^\s*SELECT\s+\S+\s+FROM\s+(\S+)\s+(?:AS\s+)?(\w+)\s+WHERE\s+(.+?)\s*$""".r
            tempRe
              .findFirstMatchIn(existsBody)
              .map { t =>
                val src      = t.group(1)
                val affAlias = t.group(2)
                val onCond   = t.group(3)
                s"""|MERGE INTO $mvRef AS $tgtAlias
                  |USING $src AS $affAlias
                  |ON $onCond
                  |WHEN MATCHED THEN DELETE""".stripMargin
              }
              .getOrElse(stmt)
        }
    }
  }

  // ── WINDOW_PARTITION (RefreshType 5) statement rewrites ───────────────────

  /** Returns `true` when `upperCaseStmt` contains an `IN (SELECT …)` clause
    * (top-level, with a top-level `SELECT`). Used by the classifier to
    * distinguish the WINDOW_PARTITION DELETE/INSERT shapes from the MIN/MAX
    * and GROUP_RECOMPUTE / AGGREGATE_GROUP+minmax shapes.
    */
  private def containsInSubquery(upperCaseStmt: String): Boolean = {
    val re = """\bIN\s*\(\s*SELECT\b""".r
    re.findFirstIn(upperCaseStmt).isDefined
  }

  /** Rewrites a WINDOW_PARTITION DELETE statement into one or more `MERGE INTO
    * <mv> AS v USING (<subquery>) AS d ON v.<col> <=> d.<col> WHEN MATCHED THEN
    * DELETE` statements, one per `IN (SELECT …)` clause in the WHERE.
    *
    * openivm emits the WINDOW_PARTITION DELETE as:
    * {{{
    *   DELETE FROM openivm_data_<v> WHERE
    *     <part_col_1> IN (SELECT DISTINCT <src_col_1> FROM openivm_delta_<src> WHERE openivm_timestamp > '<ts>'::TIMESTAMP)
    *     OR <part_col_2> IN (SELECT DISTINCT <src_col_2> FROM openivm_delta_<src> WHERE …)
    *     OR …
    * }}}
    * (One IN-clause per (partition column × source table) pair, OR-joined —
    * `refresh_compiler_aux.cpp:265-291`).
    *
    * Delta Lake does not support `IN (subquery)` in `DELETE` (see
    * `DeltaErrors.subqueryNotSupportedException`), so we split the OR-joined
    * IN-clauses and emit one MERGE per clause.  Running the MERGEs in
    * sequence is semantically equivalent: each MERGE deletes the rows
    * satisfying its clause, and the union of deletions matches the original
    * OR-joined predicate.
    *
    * The inner timestamp filter is stripped: the Spark staging delta temp
    * view already restricts visible rows to the pending refresh batch.
    *
    * @return one MERGE statement per IN-clause; an empty Seq if no IN-clause
    *         is found (callers should fall back to the original statement).
    */
  private def rewritePartitionScopedDelete(
      stmt: String,
      viewLogicalName: String,
      mvName: TableIdentifier,
      singleMergeEnabled: Boolean
  ): Seq[String] = {
    val mvRef = backtickMvName(mvName)
    val dataViewRe = ("(?i)\\bopenivm_data_" +
      java.util.regex.Pattern.quote(viewLogicalName) + "\\b").r
    var s = dataViewRe.replaceAllIn(stmt, java.util.regex.Matcher.quoteReplacement(mvRef))
    s = stripTimestampPredicate(s)

    // Extract WHERE clause body.
    val whereRe = ("(?is)^\\s*DELETE\\s+FROM\\s+" + java.util.regex.Pattern.quote(mvRef) +
      "\\s+WHERE\\s+(.+?)\\s*;?\\s*$").r
    val whereBody = whereRe.findFirstMatchIn(s).map(_.group(1).trim).getOrElse {
      return Seq(s)
    }

    val clauses = splitTopLevelOr(whereBody)
    if (singleMergeEnabled) {
      combinedPartitionDeleteMerge(clauses, mvRef)
        .map(Seq(_))
        .getOrElse(clauses.flatMap { clause =>
          inClauseToMerge(clause.trim, mvRef)
        })
    } else
      clauses.flatMap { clause =>
        val trimmed = clause.trim
        inClauseToMerge(trimmed, mvRef)
      }
  }

  private def combinedPartitionDeleteMerge(clauses: Seq[String], mvRef: String): Option[String] = {
    val parsed = clauses.flatMap(parseSingleColumnInClause)
    if (parsed.size != clauses.size || parsed.isEmpty) return None

    val targetCol = parsed.head.targetCol
    if (!parsed.forall(_.targetCol == targetCol)) return None

    val keyAlias = quoteIfNeeded(targetCol)
    val unionArms = parsed.zipWithIndex.map { case (p, idx) =>
      s"SELECT ${p.sourceExpr} AS $keyAlias FROM (${p.subquery}) openivm_key_src_$idx"
    }
    Some(
      s"""|MERGE INTO $mvRef AS v
          |USING (
          |  SELECT DISTINCT $keyAlias
          |  FROM (
          |    ${unionArms.mkString("\n    UNION ALL\n    ")}
          |  ) openivm_affected_keys
          |) AS d
          |ON v.${paddedSqlIdent(targetCol)} IS NOT DISTINCT FROM d.$keyAlias
          |WHEN MATCHED THEN DELETE""".stripMargin
    )
  }

  private case class ParsedInClause(targetCol: String, sourceExpr: String, subquery: String)

  private def parseSingleColumnInClause(clause: String): Option[ParsedInClause] = {
    val openIdx = clause.toUpperCase.indexOf(" IN ")
    if (openIdx < 0) return None
    val lhs = clause.substring(0, openIdx).trim
    if (lhs.startsWith("(") || lhs.contains(",")) return None
    val rest = clause.substring(openIdx + 4).trim
    if (!rest.startsWith("(")) return None
    val close = findMatchingCloseParen(rest, 0)
    if (close < 0) return None
    val subq     = rest.substring(1, close).trim
    val selectRe = """(?is)^\s*SELECT\s+(?:DISTINCT\s+)?(.+?)\s+FROM\s+.+$""".r
    selectRe.findFirstMatchIn(subq).flatMap { m =>
      val sourceExpr = stripProjectionAlias(m.group(1).trim)
      if (sourceExpr.contains(",")) None
      else Some(ParsedInClause(stripSqlIdentifier(lhs), sourceExpr, subq))
    }
  }

  private def stripProjectionAlias(expr: String): String =
    """(?is)^(.+?)\s+AS\s+(`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)\s*$""".r
      .findFirstMatchIn(expr)
      .map(_.group(1).trim)
      .getOrElse(expr)

  private def stripSqlIdentifier(ident: String): String =
    ident.trim.stripPrefix("`").stripSuffix("`")

  private def paddedSqlIdent(ident: String): String =
    quoteIfNeeded(stripSqlIdentifier(ident))

  private def quoteIfNeeded(ident: String): String = {
    val clean = stripSqlIdentifier(ident)
    if (clean.matches("[A-Za-z_][A-Za-z0-9_]*")) clean
    else s"`${clean.replace("`", "``")}`"
  }

  /** Convert a single `<col> IN (SELECT …)` clause to a `MERGE INTO <mv> AS v
    * USING (<subquery>) AS d ON v.<col> <=> d.<col> WHEN MATCHED THEN DELETE`
    * statement.  Returns None when `clause` doesn't match the expected shape
    * (in which case the caller should bail out).
    */
  private def inClauseToMerge(clause: String, mvRef: String): Option[String] = {
    val openIdx = clause.toUpperCase.indexOf(" IN ")
    if (openIdx < 0) return None
    val lhs  = clause.substring(0, openIdx).trim
    val rest = clause.substring(openIdx + 4).trim // skip " IN "
    if (!rest.startsWith("(")) return None
    val close = findMatchingCloseParen(rest, 0)
    if (close < 0) return None
    val subq = rest.substring(1, close).trim
    // Strip surrounding parens on lhs (composite tuple form) if present —
    // although openivm's non-DuckLake path only emits single-column IN clauses.
    val lhsCols = if (lhs.startsWith("(") && lhs.endsWith(")")) {
      lhs.stripPrefix("(").stripSuffix(")").split(",").map(_.trim)
    } else {
      Array(lhs)
    }
    val onCond = lhsCols
      .map(c => s"v.$c IS NOT DISTINCT FROM d.$c")
      .mkString(" AND ")
    Some(
      s"""|MERGE INTO $mvRef AS v
          |USING ($subq) AS d
          |ON $onCond
          |WHEN MATCHED THEN DELETE""".stripMargin
    )
  }

  /** Split a SQL boolean expression on top-level `OR` (whitespace-delimited,
    * case-insensitive), respecting nesting of `(` `)` and single-quoted
    * string literals (with `''` as the escape for an embedded quote).
    *
    * Used to break the OR-joined IN-clauses in a WINDOW_PARTITION DELETE
    * predicate into independent per-clause units.
    */
  private def splitTopLevelOr(expr: String): Seq[String] = {
    val pieces = scala.collection.mutable.ArrayBuffer[String]()
    val sb     = new StringBuilder
    var depth  = 0
    var i      = 0
    while (i < expr.length) {
      val c = expr(i)
      c match {
        case '\'' =>
          sb += c
          i += 1
          var done = false
          while (i < expr.length && !done) {
            val sc = expr(i)
            sb += sc
            i += 1
            if (sc == '\'') {
              if (i < expr.length && expr(i) == '\'') {
                sb += expr(i)
                i += 1
              } else {
                done = true
              }
            }
          }
        case '(' =>
          depth += 1
          sb += c
          i += 1
        case ')' =>
          depth -= 1
          sb += c
          i += 1
        case _
            if depth == 0 &&
              i + 4 <= expr.length &&
              (expr(i) == 'O' || expr(i) == 'o') &&
              (expr(i + 1) == 'R' || expr(i + 1) == 'r') &&
              (i == 0 || expr(i - 1).isWhitespace) &&
              expr(i + 2).isWhitespace =>
          // Found a top-level OR (preceded by whitespace, followed by whitespace, depth 0).
          pieces += sb.toString
          sb.clear()
          i += 3 // skip "OR "
        case _ =>
          sb += c
          i += 1
      }
    }
    pieces += sb.toString
    pieces.toSeq.map(_.trim).filter(_.nonEmpty)
  }

  /** Rewrites the WINDOW_PARTITION INSERT: substitute `openivm_data_<v>` →
    * backticked MV name, `memory.main.<src>` → `` `<src>` `` for source-table
    * references, and strip the inner `openivm_timestamp > '<ts>'::TIMESTAMP`
    * predicate.
    *
    * Spark/Delta `INSERT INTO <table> SELECT … WHERE <col> IN (subquery)` is
    * supported (the IN-subquery restriction only applies to DELETE/UPDATE),
    * so the OR-joined IN-clauses are kept verbatim.
    */
  private def rewritePartitionScopedInsert(
      stmt: String,
      viewLogicalName: String,
      mvName: TableIdentifier
  ): String = {
    val mvRef = backtickMvName(mvName)
    val dataViewRe = ("(?i)\\bopenivm_data_" +
      java.util.regex.Pattern.quote(viewLogicalName) + "\\b").r
    var s = dataViewRe.replaceAllIn(stmt, java.util.regex.Matcher.quoteReplacement(mvRef))
    s = rewriteMemoryMainPrefix(s)
    s = stripTimestampPredicate(s)
    s
  }

  private def rewriteCreateOrReplaceTempTableAsView(stmt: String): String =
    """(?i)CREATE\s+OR\s+REPLACE\s+TEMP\s+TABLE""".r
      .replaceFirstIn(stmt, "CREATE OR REPLACE TEMPORARY VIEW")

  /** Rewrite a WINDOW running-suffix `openivm_run_*` TEMP TABLE create.
    *
    * openivm emits these as materialised `CREATE OR REPLACE TEMP TABLE`s — they
    * are per-partition SNAPSHOTS (bounds/fast/fallback read the MV data table;
    * state reads the MV + run_fast) taken BEFORE the program mutates the MV
    * (the fallback DELETE+INSERT and the fast INSERT). A naive rewrite to a
    * lazy Spark `TEMPORARY VIEW` re-evaluates the snapshot AFTER the MV is
    * mutated, so bounds/fallback/state go stale (the backdated partition loses
    * its recomputed rows; the fast cascade reads post-insert state). We
    * therefore emit the view AND an eager `CACHE TABLE` so the snapshot is
    * frozen at creation time, matching openivm's materialised-temp-table
    * semantics. The trailing `DROP VIEW` (RunningWindowTempDrop) auto-uncaches.
    */
  private def rewriteRunningWindowTempCreate(
      stmt: String,
      viewLogicalName: String,
      mvName: TableIdentifier,
      mvLocation: String,
      mvVersionBeforeRefresh: Option[Long]
  ): Seq[String] = {
    var s = rewriteCreateOrReplaceTempTableAsView(stmt)
    s = rewriteRunningWindowSnapshotRefs(s, viewLogicalName, mvName, mvLocation, mvVersionBeforeRefresh)
    val nameRe =
      "(?is)CREATE\\s+OR\\s+REPLACE\\s+TEMPORARY\\s+VIEW\\s+`?([A-Za-z0-9_]+)`?\\s+AS".r
    nameRe.findFirstMatchIn(s) match {
      case Some(m) => Seq(s, s"CACHE TABLE `${m.group(1)}`")
      case None    => Seq(s)
    }
  }

  private def rewriteRunningWindowFastInsert(
      stmt: String,
      viewLogicalName: String,
      mvName: TableIdentifier
  ): String =
    rewriteRunningWindowSqlRefs(stmt, viewLogicalName, mvName)

  /** Rewrite the WINDOW running-suffix fast-path cascade delta INSERT.
    *
    * openivm emits (for a cascade-source cumulative window):
    * {{{
    *   INSERT INTO openivm_delta_<view>
    *   SELECT <running-adjusted cols>, CAST(1 AS INTEGER), CURRENT_TIMESTAMP
    *   FROM   openivm_delta_<src> d
    *   JOIN   openivm_run_fast_<view>  fk ON …
    *   LEFT JOIN openivm_run_state_<view> s ON …
    *   WHERE  d.openivm_multiplicity > 0 AND openivm_timestamp > '…'
    * }}}
    *
    * The fallback cascade (`openivm_old`/`openivm_new` signed-multiset, emitted
    * earlier as a [[StatementKind.ViewDeltaInsert]]) CTAS-creates the
    * `viewDeltaPath`; this statement APPENDS the fast suffix rows to it. The
    * body's `openivm_data_<view>` / `memory.main.` / EXCLUDE / timestamp refs
    * are rewritten with the same helper as the fast MV insert so the two stay
    * consistent.
    */
  private def rewriteRunningWindowCascadeInsert(
      stmt: String,
      viewLogicalName: String,
      mvName: TableIdentifier,
      viewDeltaPath: String
  ): String = {
    val escapedPath = viewDeltaPath.replace("`", "``")
    val insertTargetRe = ("(?i)INSERT\\s+INTO\\s+`?openivm_delta_" +
      java.util.regex.Pattern.quote(viewLogicalName) + "`?").r
    val retargeted = insertTargetRe.replaceFirstIn(
      stmt,
      java.util.regex.Matcher.quoteReplacement(s"INSERT INTO delta.`$escapedPath`")
    )
    rewriteRunningWindowSqlRefs(retargeted, viewLogicalName, mvName)
  }

  /** Shared reference rewrites for the fast MV insert + fast cascade append:
    * the `openivm_data_<view>` occurrence is the writable INSERT TARGET (fast
    * insert) or absent (cascade), so it maps to the live MV identifier.
    */
  private def rewriteRunningWindowSqlRefs(
      stmt: String,
      viewLogicalName: String,
      mvName: TableIdentifier
  ): String = {
    val dataViewRe = ("(?i)\\bopenivm_data_" +
      java.util.regex.Pattern.quote(viewLogicalName) + "\\b").r
    var s = dataViewRe.replaceAllIn(stmt, java.util.regex.Matcher.quoteReplacement(backtickMvName(mvName)))
    s = stripTimestampPredicate(s)
    s = rewriteMemoryMainPrefix(s)
    s = rewriteExcludeAsExcept(s)
    s
  }

  /** Reference rewrites for the running-window `bounds`/`state` snapshot
    * TEMP-TABLE creates. Their `openivm_data_<view>` occurrences are READS of
    * the MV that must snapshot the PRE-refresh version (the program mutates the
    * MV mid-refresh via the fallback DELETE+INSERT and the fast INSERT), so
    * they pin to `delta.<location> VERSION AS OF <pre-refresh-version>`. Falls
    * back to the live identifier when no version is known.
    */
  private def rewriteRunningWindowSnapshotRefs(
      stmt: String,
      viewLogicalName: String,
      mvName: TableIdentifier,
      mvLocation: String,
      mvVersionBeforeRefresh: Option[Long]
  ): String = {
    val mvRef = mvVersionBeforeRefresh match {
      case Some(version) => s"delta.`${mvLocation.replace("`", "``")}` VERSION AS OF $version"
      case None          => backtickMvName(mvName)
    }
    val dataViewRe = ("(?i)\\bopenivm_data_" +
      java.util.regex.Pattern.quote(viewLogicalName) + "\\b").r
    var s = dataViewRe.replaceAllIn(stmt, java.util.regex.Matcher.quoteReplacement(mvRef))
    s = stripTimestampPredicate(s)
    s = rewriteMemoryMainPrefix(s)
    s = rewriteExcludeAsExcept(s)
    s
  }

  private def rewriteRunningWindowTempDrop(stmt: String, viewLogicalName: String): String = {
    val re = ("(?i)^\\s*DROP\\s+TABLE\\s+IF\\s+EXISTS\\s+(`?openivm_run_[A-Za-z0-9_]+_" +
      java.util.regex.Pattern.quote(viewLogicalName) + "`?)\\s*;?\\s*$").r
    stmt match {
      case re(name) => s"DROP VIEW IF EXISTS `${name.stripPrefix("`").stripSuffix("`")}`"
      case _        => stmt.replaceFirst("(?i)DROP\\s+TABLE", "DROP VIEW")
    }
  }

  /** Rewrites the pragma-gated `openivm_old_<view>` snapshot create into a
    * TEMPORARY VIEW over the MV's pre-refresh Delta version.
    */
  private def rewriteOldSnapshotCreate(
      stmt: String,
      viewLogicalName: String,
      mvLocation: String,
      mvVersionBeforeRefresh: Option[Long]
  ): String = {
    val escapedLocation = mvLocation.replace("`", "``")
    val snapshotTable = mvVersionBeforeRefresh match {
      case Some(version) => s"delta.`$escapedLocation` VERSION AS OF $version"
      case None          => s"delta.`$escapedLocation`"
    }
    val dataViewRe = ("(?i)\\bopenivm_data_" +
      java.util.regex.Pattern.quote(viewLogicalName) +
      "\\b").r

    var s = rewriteCreateOrReplaceTempTableAsView(stmt)
    s = stripTimestampPredicate(s)
    s = dataViewRe.replaceAllIn(s, java.util.regex.Matcher.quoteReplacement(snapshotTable))
    s
  }

  /** Rewrites the current-diff full-query snapshot into a Spark TEMPORARY VIEW. */
  private def rewriteCurrentSnapshotCreate(stmt: String): String = {
    var s = rewriteCreateOrReplaceTempTableAsView(stmt)
    s = stripTimestampPredicate(s)
    s = rewriteMemoryMainPrefix(s)
    s = rewriteExcludeAsExcept(s)
    s
  }

  /** Rewrites the pragma-gated `openivm_new_<view>` snapshot create into a
    * Spark TEMPORARY VIEW. The query itself stays live because it only depends
    * on stable source staging / base tables during the current refresh.
    */
  private def rewriteNewSnapshotCreate(stmt: String): String = {
    var s = rewriteCreateOrReplaceTempTableAsView(stmt)
    s = stripTimestampPredicate(s)
    s = rewriteMemoryMainPrefix(s)
    s = rewriteExcludeAsExcept(s)
    s
  }

  /** Rewrites `INSERT INTO openivm_data_<view> SELECT * FROM openivm_new_<view>`
    * to target the actual MV identifier.
    */
  private def rewriteSnapshotDataInsert(
      stmt: String,
      viewLogicalName: String,
      mvName: TableIdentifier
  ): String = {
    val insertTargetRe = ("(?i)\\bINSERT\\s+INTO\\s+openivm_data_" +
      java.util.regex.Pattern.quote(viewLogicalName) + "\\b").r
    insertTargetRe.replaceFirstIn(
      stmt,
      java.util.regex.Matcher.quoteReplacement(s"INSERT INTO ${backtickMvName(mvName)}")
    )
  }

  private def rewriteCurrentSnapshotDrop(viewLogicalName: String): String =
    s"DROP VIEW IF EXISTS `openivm_current_$viewLogicalName`"

  /** Rewrites `DROP TABLE IF EXISTS openivm_old_<view>` / `openivm_new_<view>`
    * to `DROP VIEW IF EXISTS …` matching the TEMPORARY VIEW materialisation.
    */
  private def rewriteSnapshotDrop(stmt: String, viewLogicalName: String): String = {
    val upper = stmt.toUpperCase
    if (upper.contains(s"OPENIVM_OLD_${viewLogicalName.toUpperCase}"))
      s"DROP VIEW IF EXISTS `openivm_old_$viewLogicalName`"
    else
      s"DROP VIEW IF EXISTS `openivm_new_$viewLogicalName`"
  }

  // ── GROUP_RECOMPUTE (RefreshType 6) statement rewrites ────────────────────

  /** Rewrites the GROUP_RECOMPUTE Statement B into a Spark-executable
    * `CREATE OR REPLACE TEMPORARY VIEW` of the affected group keys.
    *
    * Input (DuckDB-target):
    * {{{
    *   CREATE OR REPLACE TEMP TABLE openivm_affected_<view> AS
    *     SELECT DISTINCT <keys> FROM (
    *       WITH scan_0 (…) AS (
    *         SELECT … FROM (SELECT * EXCLUDE (openivm_multiplicity, openivm_timestamp)
    *                        FROM openivm_delta_<src>
    *                        WHERE openivm_timestamp >= '<ts>'::TIMESTAMP)
    *       ), …
    *       SELECT … FROM projection_N
    *     ) openivm_src_<i>_<occ>
    *     UNION
    *     SELECT DISTINCT <keys> FROM (… memory.main.<src> …) openivm_src_<j>_<occ>;
    * }}}
    *
    * Output (Spark-executable):
    * {{{
    *   CREATE OR REPLACE TEMPORARY VIEW openivm_affected_<view> AS
    *     SELECT DISTINCT <keys> FROM (
    *       WITH scan_0 (…) AS (
    *         SELECT … FROM (SELECT * EXCEPT (openivm_multiplicity, openivm_timestamp)
    *                        FROM `openivm_delta_<src>`)
    *       ), …
    *       SELECT … FROM projection_N
    *     ) openivm_src_<i>_<occ>
    *     UNION …
    * }}}
    *
    * Transformations applied (in order):
    *   1. `CREATE OR REPLACE TEMP TABLE` → `CREATE OR REPLACE TEMPORARY VIEW`
    *   2. Strip `openivm_timestamp >= '…'::TIMESTAMP` predicates (Spark-side
    *      `openivm_delta_<src>` TEMP VIEWs already restrict visible rows to the
    *      pending batch, and the timestamp value references openivm's own
    *      bookkeeping which is not present on the Spark side).
    *   3. `memory.main.<src>` → `` `<src>` ``
    *   4. `SELECT * EXCLUDE (…)` → `SELECT * EXCEPT (…)`  (Spark 3.4+ syntax).
    */
  private def rewriteGroupRecomputeAffectedCreate(
      stmt: String,
      viewLogicalName: String,
      mvLocation: String,
      mvVersionBeforeRefresh: Option[Long]
  ): String = {
    val escapedLocation = mvLocation.replace("`", "``")
    val snapshotTable = mvVersionBeforeRefresh match {
      case Some(version) => s"delta.`$escapedLocation` VERSION AS OF $version"
      case None          => s"delta.`$escapedLocation`"
    }
    val dataViewRe = ("(?i)\\bopenivm_data_" +
      java.util.regex.Pattern.quote(viewLogicalName) +
      "\\b").r

    var s = rewriteCreateOrReplaceTempTableAsView(stmt)
    s = stripTimestampPredicate(s)
    s = rewriteMemoryMainPrefix(s)
    s = rewriteExcludeAsExcept(s)
    s = dataViewRe.replaceAllIn(s, java.util.regex.Matcher.quoteReplacement(snapshotTable))
    s
  }

  /** Rewrites the GROUP_RECOMPUTE Statement E into a Spark
    * `DROP VIEW IF EXISTS` matching the TEMPORARY VIEW created by Statement B.
    *
    * Rebuilds the statement from `viewLogicalName` to avoid fragile pattern
    * preservation (openivm sometimes emits the identifier unquoted, sometimes
    * with DuckDB double-quotes).
    */
  private def rewriteGroupRecomputeAffectedDrop(
      stmt: String,
      viewLogicalName: String
  ): String = {
    val _            = stmt
    val affectedName = s"openivm_affected_$viewLogicalName"
    s"DROP VIEW IF EXISTS `$affectedName`"
  }

  /** Rewrites `SELECT * EXCLUDE (col1, col2)` (DuckDB) by stripping the
    * `EXCLUDE (...)` clause entirely.
    *
    * Spark 3.5 does not support either `SELECT * EXCLUDE` (DuckDB) or
    * `SELECT * EXCEPT (...)` (Databricks / Spark 4.0+) column-exclusion
    * syntax. In the openivm-emitted GROUP_RECOMPUTE program the inner
    * subquery `(SELECT * EXCLUDE (openivm_multiplicity, openivm_timestamp)
    * FROM openivm_delta_<src>)` is always wrapped by an outer SELECT that
    * lists the user-facing columns by name, so stripping the EXCLUDE clause
    * just lets the hidden `openivm_multiplicity` / `openivm_timestamp`
    * columns flow through to the outer SELECT, which then ignores them.
    *
    * Scoped to `EXCLUDE` immediately preceded by `*` and followed by a
    * parenthesised column list — unrelated occurrences of `EXCLUDE` (e.g.
    * inside identifiers or string literals) are left alone.
    */
  private def rewriteExcludeAsExcept(sql: String): String = {
    val re = """(?is)(\*\s*)EXCLUDE\s*\([^)]*\)""".r
    re.replaceAllIn(sql, m => java.util.regex.Matcher.quoteReplacement(m.group(1)))
  }

  /** Expand DuckDB-style `SELECT * EXCEPT (col1, col2) FROM <openivm_delta_short>`
    * into an explicit column list `SELECT <c_a>, <c_b>, … FROM …`.
    *
    * Spark 3.5 does NOT support `SELECT * EXCEPT (col, …)` for column exclusion
    * — the SQL parser interprets `EXCEPT` as the set operation and raises
    * `PARSE_SYNTAX_ERROR` on the first column inside the parenthesised list.
    * openivm emits this syntax inside `scan_<i>` CTEs of GROUP_RECOMPUTE
    * affected-keys queries (and other multi-source refresh programs) to strip
    * `openivm_multiplicity` / `openivm_timestamp` before joining the delta with
    * other relations — see `openivm/src/rules/join.cpp` and the staging delta
    * temp view shape in [[StagingDeltaView.buildSourceDeltaViewSql]].
    *
    * `sourceSchemas` maps each source's short table name to its user-facing
    * column list (NOT including bookkeeping columns).  For a match against
    * `openivm_delta_<short>` we emit the source columns minus the EXCEPT list.
    * Unknown source tables, or absence of a matching schema, leave the
    * statement unchanged so the failing parser path surfaces a clear error
    * during refresh rather than silently dropping the clause.
    */
  private def expandSelectStarExcept(sql: String, sourceSchemas: Map[String, Seq[String]]): String = {
    if (sourceSchemas.isEmpty) return sql
    // Match `SELECT * EXCEPT (a, b, …) FROM <table>` where the table is one of
    // the openivm delta temp views.  Allow optional whitespace, backticks, and
    // an optional table alias after FROM.  The trailing `)` / token boundary
    // is captured so we can splice cleanly.
    val re =
      """(?is)SELECT\s+\*\s+EXCEPT\s*\(([^)]+)\)\s+FROM\s+`?(openivm_delta_[A-Za-z0-9_]+)`?""".r
    re.replaceAllIn(
      sql,
      mm => {
        val excludedCols = mm
          .group(1)
          .split(",")
          .map { c =>
            c.trim.stripPrefix("`").stripSuffix("`").stripPrefix("\"").stripSuffix("\"").toLowerCase
          }
          .toSet
        val tableName = mm.group(2)
        val shortName = tableName.stripPrefix("openivm_delta_")
        sourceSchemas.get(shortName) match {
          case None => java.util.regex.Matcher.quoteReplacement(mm.matched)
          case Some(cols) =>
            val kept = cols.filterNot(c => excludedCols.contains(c.toLowerCase))
            val projection =
              if (kept.isEmpty) "NULL"
              else kept.map(c => s"`${c.replace("`", "``")}`").mkString(", ")
            java.util.regex.Matcher.quoteReplacement(s"SELECT $projection FROM `$tableName`")
        }
      }
    )
  }

  /** Rewrite the null-reset UPDATE as a MERGE that counts the source table rows.
    *
    * Input:  `UPDATE <mv> SET col=NULL WHERE NOT EXISTS (SELECT 1 FROM \`src\` LIMIT 1)`
    * Output: `MERGE INTO <mv> AS v USING (SELECT COUNT(*) AS _cnt FROM \`src\`) AS _chk ON TRUE WHEN MATCHED AND _chk._cnt = 0 THEN UPDATE SET col=NULL`
    */
  private def rewriteNullResetAsMerge(stmt: String, mvRef: String): String = {
    val re = ("(?is)UPDATE\\s+" + java.util.regex.Pattern.quote(mvRef) +
      "\\s+SET\\s+(.+?)\\s+WHERE\\s+NOT\\s+EXISTS\\s*" +
      "\\(\\s*SELECT\\s+1\\s+FROM\\s+(\\S+)\\s+LIMIT\\s+1\\s*\\)\\s*$").r
    re.findFirstMatchIn(stmt) match {
      case None => stmt
      case Some(m) =>
        val setClause = m.group(1).trim
        val srcRef    = m.group(2).trim
        s"""|MERGE INTO $mvRef AS v
            |USING (SELECT COUNT(*) AS _cnt FROM $srcRef) AS _chk
            |ON TRUE
            |WHEN MATCHED AND _chk._cnt = 0 THEN UPDATE SET $setClause""".stripMargin
    }
  }

  /** Inline the `WITH openivm_delta AS (<cte>) UPDATE` pattern into a plain UPDATE.
    *
    * Finds `WITH openivm_delta AS (` in `stmt`, paren-matches the CTE body, builds
    * an alias→expression map from the CTE's SELECT list, then replaces every
    * `(SELECT <alias> FROM openivm_delta)` scalar-subquery reference in the UPDATE
    * with `(SELECT <expression> FROM <deltaRef>)`.  Returns only the `UPDATE` part.
    */
  private def inlineOpeniVmDeltaCte(stmt: String, deltaRef: String): String = {
    val cteMarkerRe = "(?i)WITH\\s+openivm_delta\\s+AS\\s*\\(".r
    cteMarkerRe.findFirstMatchIn(stmt) match {
      case None => stmt
      case Some(m) =>
        val openParenIdx  = m.end - 1
        val closeParenIdx = findMatchingCloseParen(stmt, openParenIdx)
        if (closeParenIdx < 0) return stmt
        val cteBody    = stmt.substring(openParenIdx + 1, closeParenIdx).trim
        val updatePart = stmt.substring(closeParenIdx + 1).trim
        val aliasMap   = parseCteSelectAliases(cteBody)
        val refRe      = """(?i)\(\s*SELECT\s+(\w+)\s+FROM\s+openivm_delta\s*\)""".r
        refRe.replaceAllIn(
          updatePart,
          mm => {
            val alias = mm.group(1).toUpperCase
            aliasMap.get(alias) match {
              case None => mm.matched
              case Some(expr) =>
                java.util.regex.Matcher.quoteReplacement(s"(SELECT $expr FROM $deltaRef)")
            }
          }
        )
    }
  }

  /** Find the index of the `)` that closes the `(` at `openIdx`, respecting
    * nesting and single-quoted string literals (with `''` as escape).
    * Returns -1 if not found.
    */
  private def findMatchingCloseParen(s: String, openIdx: Int): Int = {
    var depth = 0
    var i     = openIdx
    while (i < s.length) {
      s(i) match {
        case '\'' =>
          i += 1
          var done = false
          while (i < s.length && !done) {
            if (s(i) == '\'' && i + 1 < s.length && s(i + 1) == '\'') {
              i += 2
            } else if (s(i) == '\'') {
              done = true
              i += 1
            } else {
              i += 1
            }
          }
        case '(' =>
          depth += 1
          i += 1
        case ')' =>
          depth -= 1
          if (depth == 0) return i
          i += 1
        case _ =>
          i += 1
      }
    }
    -1
  }

  /** Extract alias → expression pairs from the SELECT list of a CTE body.
    *
    * Example: `SELECT SUM(openivm_multiplicity * total) AS d_total FROM delta.\`path\``
    * → Map("D_TOTAL" → "SUM(openivm_multiplicity * total)")
    */
  private def parseCteSelectAliases(cteBody: String): Map[String, String] = {
    val upper       = cteBody.toUpperCase.trim
    val selectStart = if (upper.startsWith("SELECT")) 6 else return Map.empty

    var depth   = 0
    var fromIdx = -1
    var i       = selectStart
    while (i < upper.length && fromIdx < 0) {
      upper(i) match {
        case '(' => depth += 1; i += 1
        case ')' => depth -= 1; i += 1
        case 'F'
            if depth == 0 && i + 4 <= upper.length &&
              upper.substring(i, i + 4) == "FROM" &&
              (i + 4 >= upper.length || upper(i + 4).isWhitespace) =>
          fromIdx = i
        case _ => i += 1
      }
    }

    val selectList =
      if (fromIdx > 0) cteBody.substring(selectStart, fromIdx).trim
      else cteBody.substring(selectStart).trim

    splitSelectList(selectList).flatMap { item =>
      val trimmed = item.trim
      val asRe    = """(?i)\s+AS\s+(\w+)\s*$""".r
      asRe.findFirstMatchIn(trimmed).map { m =>
        m.group(1).toUpperCase -> trimmed.substring(0, m.start).trim
      }
    }.toMap
  }

  /** Split a SELECT list on commas at nesting depth 0. */
  private def splitSelectList(selectList: String): Seq[String] = {
    val items = scala.collection.mutable.ArrayBuffer[String]()
    val sb    = new StringBuilder
    var depth = 0
    for (c <- selectList) {
      c match {
        case '(' => depth += 1; sb += c
        case ')' => depth -= 1; sb += c
        case ',' if depth == 0 =>
          items += sb.toString
          sb.clear()
        case _ => sb += c
      }
    }
    items += sb.toString
    items.toSeq
  }

  /** Rename duplicate CTE column aliases by appending a counter suffix.
    *
    * DuckDB allows duplicate column alias names in CTE column lists (the first
    * definition wins when both are referenced by name); Spark SQL rejects them.
    * openivm produces duplicates in the scan CTE for COUNT(*) aggregates, e.g.:
    *   `scan_0 (t3_openivm_multiplicity, t3_openivm_multiplicity) AS (SELECT openivm_multiplicity, openivm_multiplicity …)`
    * The renamed copies are never referenced by downstream CTEs, so renaming
    * them is safe.
    */
  private def deduplicateCteColumnAliases(sql: String): String = {
    val cteColListRe = ("""(\w+)\s*\(([^)]+)\)\s+AS\s+\(""").r
    cteColListRe.replaceAllIn(
      sql,
      mm => {
        val cteName = mm.group(1)
        val rawCols = mm.group(2).split(",").map(_.trim)
        val seen    = scala.collection.mutable.Map[String, Int]()
        val newCols = rawCols.map { col =>
          val count = seen.getOrElse(col, 0)
          seen(col) = count + 1
          if (count == 0) col else s"${col}_$count"
        }
        if (newCols sameElements rawCols) mm.matched
        else java.util.regex.Matcher.quoteReplacement(s"$cteName (${newCols.mkString(", ")}) AS (")
      }
    )
  }

  /** Returns `true` if the compiled openivm SQL contains at least one
    * [[StatementKind.ViewDeltaInsert]] that reads from real staging data
    * (i.e. is NOT a bare `SELECT … WHERE false` empty-placeholder).
    *
    * openivm emits a bare `INSERT INTO openivm_delta_<view> … SELECT NULL … WHERE false`
    * for multi-source JOINs where it cannot compute incremental deltas.  Callers
    * that detect this case may choose to fall back to a full recompute instead of
    * executing a refresh program that would be a no-op.
    *
    * @param compiledSql       Full multi-statement refresh SQL as emitted by openivm.
    * @param viewLogicalName   The bare view name (the `<view>` in `openivm_delta_<view>`).
    */
  def hasRealDelta(compiledSql: String, viewLogicalName: String): Boolean = {
    val stmts      = splitStatements(compiledSql).map(_.trim).filter(_.nonEmpty)
    val deltaTbl   = s"openivm_delta_$viewLogicalName".toUpperCase
    val compactTbl = s"openivm_old_compact_$viewLogicalName".toUpperCase
    val deltaStmts = stmts.filter { stmt =>
      stmt.toUpperCase.contains(s"INSERT INTO $deltaTbl")
    }
    // Filter out openivm-internal bookkeeping INSERTs that don't carry actual
    // delta semantics:
    //   - AGGREGATE_GROUP retract/add companions (`force_view_delta_cascade=true`):
    //     SELECT body references `openivm_delta_<view>` in the FROM clause
    //     (self-join). The main delta-query CTAS reads from a CTE alias.
    //   - compact_delta_view post-processing (emitted when has_downstream=true):
    //     `INSERT INTO openivm_delta_<view> SELECT ... FROM openivm_old_compact_<view>`.
    //     This collapses sign-cancelling rows in the duck-side delta — irrelevant
    //     for the Spark-side view-delta path (which is per-refresh scratch).
    val realDeltaStmts = deltaStmts.filterNot { stmt =>
      val upper     = stmt.toUpperCase
      val insertIdx = upper.indexOf(s"INSERT INTO $deltaTbl")
      val tail      = if (insertIdx >= 0) upper.substring(insertIdx + s"INSERT INTO $deltaTbl".length) else upper
      tail.contains(deltaTbl) || tail.contains(compactTbl)
    }
    // A placeholder ends with `SELECT … WHERE false` (case-insensitive); a real
    // delta ends with `SELECT * FROM <lastCte>` preceded by a CTE block.
    realDeltaStmts.nonEmpty && !realDeltaStmts.forall(_.toUpperCase.contains("WHERE FALSE"))
  }

  /** Split SQL on `;` boundaries, respecting single-quoted string literals
    * (with `''` as the escape for an embedded quote).
    *
    * Does not attempt to handle block comments or double-dollar-quoted
    * strings — openivm-emitted SQL never uses either.
    */
  def splitStatements(sql: String): Seq[String] = {
    val stmts = scala.collection.mutable.ArrayBuffer[String]()
    val sb    = new StringBuilder
    var i     = 0
    while (i < sql.length) {
      val c = sql(i)
      if (c == '\'') {
        sb += c
        i += 1
        var closed = false
        while (i < sql.length && !closed) {
          val sc = sql(i)
          sb += sc
          i += 1
          if (sc == '\'') {
            if (i < sql.length && sql(i) == '\'') {
              sb += sql(i)
              i += 1
            } else {
              closed = true
            }
          }
        }
      } else if (c == ';') {
        stmts += sb.toString
        sb.clear()
        i += 1
      } else {
        sb += c
        i += 1
      }
    }
    val tail = sb.toString.trim
    if (tail.nonEmpty) stmts += tail
    stmts.toSeq
  }

  // ── Helpers ──────────────────────────────────────────────────────────────

  private def backtickMvName(id: TableIdentifier): String = {
    val parts = id.catalog.toSeq ++ id.database.toSeq ++ Seq(id.table)
    parts.map(p => s"`${p.replace("`", "``")}`").mkString(".")
  }

  /** Fix MERGE alias references: when a MERGE INTO targets a multi-part MV
    * name with an alias (e.g. `MERGE INTO \`db\`.\`table\` AS v USING ...`),
    * Spark/Delta requires the ON / WHEN clauses to use the alias (`v.col`)
    * rather than the fully-qualified name (`\`db\`.\`table\`.col`).
    *
    * Runs as a top-level pass on every rewritten statement — after all
    * per-statement rewriters and before the dialect postProcess — so it
    * catches cases regardless of which rewriter produced the MERGE.
    */
  private def fixMergeAliasRefs(stmt: String, mvName: TableIdentifier): String = {
    val mvSqlName = backtickMvName(mvName)
    val mvDot     = mvSqlName + "."

    // Quick exit: no MV-qualified column references to fix.
    if (!stmt.contains(mvDot)) return stmt

    // Detect: MERGE INTO <mv> [AS] <alias> <whitespace> USING
    val mergeRe = ("(?is)MERGE\\s+INTO\\s+" +
      java.util.regex.Pattern.quote(mvSqlName) +
      "\\s+(?:AS\\s+)?\"?(\\w+)\"?\\s+USING\\b").r

    mergeRe.findFirstMatchIn(stmt) match {
      case Some(m) =>
        val alias     = m.group(1)
        val bodyStart = m.end - "USING".length // keep USING in the body
        val body      = stmt.substring(bodyStart)
        val fixedBody = body.replace(mvDot, alias + ".")
        stmt.substring(0, bodyStart) + fixedBody
      case None => stmt
    }
  }

  /** Dedupe the USING source of a DELETE-only MERGE whose ON clause uses
    * `IS NOT DISTINCT FROM` predicates.
    *
    * Why: OpenIVM emits MERGE statements like
    *   {{{
    *     MERGE INTO openivm_data_<v> AS v
    *     USING openivm_delta_<v> AS _d
    *     ON _d.openivm_left_key IS NOT DISTINCT FROM v.openivm_left_key
    *     WHEN MATCHED THEN DELETE
    *   }}}
    * to retract MV rows whose hidden join-key column matches any key in the
    * view-delta. The predicate is intentionally NULL-safe — openivm packs all
    * LEFT/FULL OUTER dangling rows under a NULL key, and they must all be
    * recomputed when the right-side delta touches the NULL group (see
    * .temp/openivm/docs/operators/full-outer-join.md:61).
    *
    * The problem is **duplicate keys in the source**: if the view-delta has
    * many rows sharing the same key value (e.g. 200 NULL-keyed rows for the
    * LEFT-dangling group, or a hot non-NULL key duplicated many times), the
    * MERGE join with the MV's same-keyed rows produces an N×M intermediate
    * — a Cartesian explosion that overflows Spark's broadcast cap (8 GiB) and
    * fails with `Cannot broadcast the table that is larger than 8.0 GiB`.
    *
    * The fix is to wrap the USING source with a `SELECT DISTINCT <key_cols>
    * FROM (<orig_source>)` projection. This:
    *   1. Preserves the NULL-safe `IS NOT DISTINCT FROM` semantic openivm
    *      requires (NULL still matches NULL — once).
    *   2. Removes the duplicate-key amplification (one source row per
    *      affected key).
    *   3. Gives Spark accurate row-count statistics for the MERGE join
    *      planning.
    *
    * Triggers only when:
    *   - statement is a MERGE INTO the rewritten MV target
    *   - the only WHEN clause is `WHEN MATCHED THEN DELETE` (no
    *     INSERT/UPDATE branches that would reference non-key source columns)
    *   - the ON clause contains at least one `IS NOT DISTINCT FROM`
    *
    * Safe no-op otherwise.
    */
  private def deduplicateNullSafeMergeSource(stmt: String, mvName: TableIdentifier): String = {
    val mvSqlName = backtickMvName(mvName)

    // Detect MERGE INTO <mv> [AS] <alias> USING <source> [AS] <usingAlias> ON <pred> WHEN <when-body>
    // Use paren-aware parsing for the source so subquery `(…)` forms are
    // captured correctly.
    val headerRe = ("(?is)\\bMERGE\\s+INTO\\s+" +
      java.util.regex.Pattern.quote(mvSqlName) +
      "(?:\\s+AS)?\\s+\"?(\\w+)\"?\\s+USING\\s+").r

    headerRe.findFirstMatchIn(stmt) match {
      case None => stmt
      case Some(m) =>
        val afterUsing = m.end
        // Find the source token: either `(<paren-balanced>)` or a non-whitespace token.
        val srcEnd =
          if (afterUsing < stmt.length && stmt(afterUsing) == '(') {
            val close = findMatchingCloseParen(stmt, afterUsing)
            if (close < 0) return stmt
            close + 1
          } else {
            // Match up to whitespace, but respect backticked identifiers like delta.`<path>`
            var i          = afterUsing
            var inBacktick = false
            while (i < stmt.length && (inBacktick || !Character.isWhitespace(stmt(i)))) {
              if (stmt(i) == '`') inBacktick = !inBacktick
              i += 1
            }
            i
          }
        val source = stmt.substring(afterUsing, srcEnd)

        // Parse "[AS] <alias> ON <on-predicate> WHEN <when-body>"
        val tailRe = """(?is)^\s*(?:AS\s+)?(\w+)\s+ON\s+(.+?)\s+WHEN\s+""".r
        val tailMatch = tailRe.findFirstMatchIn(stmt.substring(srcEnd)) match {
          case Some(tm) => tm
          case None     => return stmt
        }
        val usingAlias     = tailMatch.group(1)
        val onPredicate    = tailMatch.group(2)
        val onPredicateEnd = srcEnd + tailMatch.end(2)
        val whenBodyStart  = srcEnd + tailMatch.end - "WHEN ".length
        val whenBody       = stmt.substring(whenBodyStart)

        // Gate 1: NULL-safe predicate is the trigger we care about.
        if (!onPredicate.toUpperCase.contains("IS NOT DISTINCT FROM")) return stmt

        // Gate 2: only DELETE WHEN clauses (no INSERT/UPDATE that would need
        // non-key source columns).  Look at the WHEN-body up to the next
        // semicolon (or end-of-statement); reject if it contains INSERT or
        // UPDATE.
        val whenBodyUpper = whenBody.toUpperCase
        if (whenBodyUpper.contains("INSERT") || whenBodyUpper.contains("UPDATE")) return stmt

        // Extract the columns referenced via the USING alias in the ON clause.
        // Match `<alias>.<col>` where col is either an unquoted identifier or
        // a backticked identifier.
        val aliasColRe = ("(?i)\\b" + java.util.regex.Pattern.quote(usingAlias) +
          "\\s*\\.\\s*(`[^`]+`|\\w+)").r
        val keyCols = aliasColRe.findAllMatchIn(onPredicate).map(_.group(1)).toList.distinct
        if (keyCols.isEmpty) return stmt

        // Build the deduped source.  Wrap the original source in a derived
        // table that selects DISTINCT on just the key columns referenced by
        // the ON clause.  The original source may be a bare table reference
        // (`delta.\`<path>\``), a backticked identifier, or a parenthesized
        // subquery — `SELECT DISTINCT … FROM <source>` works for all three.
        // (For a paren-wrapped subquery, the wrapping parens are preserved by
        // `source` substring.)
        val dedupedSource = s"(SELECT DISTINCT ${keyCols.mkString(", ")} FROM $source AS _openivm_dedup_src)"

        // Rebuild the statement: original head up to USING, dedupedSource,
        // " AS <usingAlias> ON ", onPredicate, trailing.
        val head     = stmt.substring(0, afterUsing)
        val trailing = stmt.substring(onPredicateEnd)
        s"$head$dedupedSource AS $usingAlias ON $onPredicate$trailing"
    }
  }

  /** Rewrites a recompute-INSERT MERGE whose source body filters via
    * `WHERE EXISTS (SELECT 1 FROM <ref> _d WHERE _d.<col> IS NOT DISTINCT FROM
    * <outer>.<col> [AND …])` into a `LEFT SEMI JOIN (SELECT DISTINCT <cols>
    * FROM <ref>) _openivm_ak ON <outer>.<col> <=> _openivm_ak.<col>` shape.
    *
    * Why this rewrite exists
    * -----------------------
    * openivm's SIMPLE_PROJECTION (and AGGREGATE_GROUP+has_minmax) recompute
    * INSERT shape is, post-rewrite:
    * {{{
    *   MERGE INTO delta.`<mvLocation>` AS v
    *   USING ( SELECT openivm_lj.<cols>, openivm_lj.openivm_left_key
    *           FROM (<full openivm view body, CTE-chained>) openivm_lj
    *           WHERE EXISTS (SELECT 1 FROM delta.`<viewDeltaPath>` _d
    *                         WHERE _d.openivm_left_key IS NOT DISTINCT FROM
    *                               openivm_lj.openivm_left_key)
    *         ) AS d
    *   ON false
    *   WHEN NOT MATCHED THEN INSERT (...) VALUES (d....)
    * }}}
    *
    * When the view body has joins that range-expand (e.g. SCD2 timestamp
    * ranges) Catalyst cannot push the correlated `WHERE EXISTS … IS NOT
    * DISTINCT FROM` filter through the CTE chain into the source scans, so
    * it materialises the FULL view-body join first and then filters. The
    * resulting intermediate can overflow Spark's hard-coded 8 GiB
    * BroadcastExchangeExec cap (observed: 57.6 GiB at SF=10 for
    * `gold.fact_market_history`'s SCD2 daily-market × dim-security range
    * join with only ~2.4k affected keys in the delta).
    *
    * Rewriting to `LEFT SEMI JOIN (SELECT DISTINCT …)` engages Catalyst's
    * `PushDownLeftSemiAntiJoin` rule, which DOES push the semi-join through
    * the view body's joins, pruning the SCD2 range join to only affected
    * rows.  `<=>` (EqualNullSafe) preserves the NULL-safe matching of
    * `IS NOT DISTINCT FROM` and is recognised as an equi-join key by
    * `ExtractEquiJoinKeys` (no broadcast-cap escalation).  `SELECT DISTINCT`
    * gives Catalyst accurate row-count statistics for the build side.
    *
    * Cardinality preservation
    * ------------------------
    * `LEFT SEMI JOIN (SELECT DISTINCT k FROM X) ON o.k <=> i.k` is
    * row-by-row equivalent to `WHERE EXISTS (SELECT 1 FROM X WHERE x.k IS
    * NOT DISTINCT FROM o.k)`:
    *
    *   - outer-side duplicates are preserved (semi-join is left-side bounded);
    *   - inner-side duplicates do not amplify outer rows (semi-join, not inner);
    *   - NULL outer keys still match NULL inner keys exactly once (`<=>`).
    *
    * The `DISTINCT` is technically redundant under LEFT SEMI semantics but
    * is kept for build-side stat accuracy and to keep the equivalence proof
    * trivial if the semi-join were ever lowered to an inner-join form by a
    * downstream optimisation.
    *
    * Strict gating (see `tryParseExistsKeyPredicates` for predicate shape):
    *   - statement is `MERGE INTO … USING (…) AS <alias> ON FALSE WHEN NOT
    *     MATCHED THEN INSERT` (DELETE / UPDATE / non-`ON FALSE` merges are
    *     out of scope — they already have their own dedupe path);
    *   - source body's outermost WHERE clause is **exactly** one `EXISTS
    *     (SELECT 1 FROM <ref> _d WHERE <preds>)` (no other conjuncts);
    *   - every predicate inside the EXISTS body is `_d.<col> IS NOT
    *     DISTINCT FROM <outer_alias>.<col>` joined by AND (no other ops, no
    *     OR, no functions, no constants);
    *   - `<ref>` is a single source ref (table or `delta.`<path>``, optional
    *     `AS _d` / bare `_d`), no joins inside the EXISTS subquery.
    *
    * Safe no-op otherwise.  Idempotent: the rewrite removes the `WHERE
    * EXISTS … IS NOT DISTINCT FROM` shape, so a second pass cannot re-fire.
    */
  private def rewriteRecomputeWhereExistsAsAffectedKeysJoin(stmt: String): String = {
    // Detect a MERGE statement whose USING body we may need to rewrite.
    // Match `MERGE INTO <target> [AS] <alias> USING (` paren-aware.
    val mergeHeaderRe = "(?is)\\bMERGE\\s+INTO\\s+".r
    val mergeHeader   = mergeHeaderRe.findFirstMatchIn(stmt).getOrElse(return stmt)

    // Skip the target (could be `delta.`<path>`` or backticked multi-part).
    // Walk until we hit `USING\s*(` at depth=0 from the MERGE token.
    val mergeHeaderEnd = mergeHeader.end
    val usingOpenIdx   = findMergeUsingOpenParen(stmt, mergeHeaderEnd).getOrElse(return stmt)
    val closeIdx       = findMatchingCloseParen(stmt, usingOpenIdx)
    if (closeIdx < 0) return stmt

    // Tail must be `AS <alias> ON false WHEN NOT MATCHED ...` — explicitly
    // require `ON false` so we never strip an active match predicate.
    val tail    = stmt.substring(closeIdx + 1)
    val tailRe  = "(?is)^\\s*(?:AS\\s+)?\\w+\\s+ON\\s+(?:FALSE|\\(\\s*FALSE\\s*\\))\\s+WHEN\\s+NOT\\s+MATCHED\\b".r
    val matched = tailRe.findFirstMatchIn(tail)
    if (matched.isEmpty) return stmt

    val source          = stmt.substring(usingOpenIdx + 1, closeIdx)
    val rewrittenSource = rewriteSourceWhereExistsAsSemiJoin(source).getOrElse(return stmt)
    stmt.substring(0, usingOpenIdx + 1) + rewrittenSource + stmt.substring(closeIdx)
  }

  /** Locate the `(` of `... USING (` starting at `from`, paren-aware. Skips
    * over any nested parens that might appear in the MERGE target (none today,
    * but `delta.`<path>`` could contain `(`-shaped chars in some FS paths).
    * Returns the index of the `(` or None if no USING is found at depth 0.
    */
  private def findMergeUsingOpenParen(stmt: String, from: Int): Option[Int] = {
    var depth = 0
    var i     = from
    while (i < stmt.length) {
      stmt.charAt(i) match {
        case '\'' => i = consumeSqlSingleQuoted(stmt, i)
        case '`'  =>
          // Skip backticked identifier
          i += 1
          while (i < stmt.length && stmt.charAt(i) != '`') i += 1
          if (i < stmt.length) i += 1
        case '('                                                       => depth += 1; i += 1
        case ')'                                                       => depth -= 1; i += 1
        case _ if depth == 0 && startsWithSqlKeyword(stmt, i, "USING") =>
          // Find the next `(` at this depth, skipping whitespace.
          var j = i + "USING".length
          while (j < stmt.length && Character.isWhitespace(stmt.charAt(j))) j += 1
          if (j < stmt.length && stmt.charAt(j) == '(') return Some(j)
          // Not a parenthesised USING source — out of scope.
          return None
        case _ => i += 1
      }
    }
    None
  }

  /** Locate and rewrite an outermost (depth=0 within `source`) `WHERE EXISTS
    * (SELECT 1 FROM <ref> _d WHERE <key_preds>)` clause into a `LEFT SEMI
    * JOIN (SELECT DISTINCT <cols> FROM <ref>) _openivm_ak ON …` clause
    * attached to the immediately-preceding FROM-clause tail.
    *
    * Returns None on any deviation from the strict shape (so the caller no-ops).
    */
  private def rewriteSourceWhereExistsAsSemiJoin(source: String): Option[String] = {
    // Find the WHERE keyword at depth 0 inside the source body.
    val whereStart = findTopLevelSqlKeyword(source, 0, source.length, "WHERE").getOrElse(return None)

    // The clause must start with `WHERE` followed by `EXISTS (`. Nothing else
    // is allowed (no conjuncts before or after EXISTS).
    val afterWhere = skipWhitespace(source, whereStart + "WHERE".length)
    if (!startsWithSqlKeyword(source, afterWhere, "EXISTS")) return None
    val afterExists = skipWhitespace(source, afterWhere + "EXISTS".length)
    if (afterExists >= source.length || source.charAt(afterExists) != '(') return None

    val existsClose = findMatchingCloseParen(source, afterExists)
    if (existsClose < 0) return None

    // After the EXISTS subquery's `)`, only whitespace is allowed (no
    // trailing `AND …` / `OR …` / `GROUP BY …` / `LIMIT …` / etc.).
    val afterExistsClose = skipWhitespace(source, existsClose + 1)
    if (afterExistsClose != source.length) return None

    val existsBody = source.substring(afterExists + 1, existsClose)
    val parsed     = tryParseExistsKeyPredicates(existsBody).getOrElse(return None)

    // Build the LEFT SEMI JOIN form.
    val deltaAlias   = "_openivm_ak"
    val distinctCols = parsed.predicates.map(_.innerCol).distinct
    val distinctList = distinctCols.mkString(", ")
    val joinPredicate = parsed.predicates
      .map(p => s"${p.outerAlias}.${p.outerCol} <=> $deltaAlias.${p.innerCol}")
      .mkString(" AND ")
    val semiJoinClause =
      s"LEFT SEMI JOIN (SELECT DISTINCT $distinctList FROM ${parsed.fromRef}) $deltaAlias ON $joinPredicate"

    // Splice: replace `[whitespace] WHERE EXISTS (…)` with `[space]<semiJoin>`.
    // Use a single space separator so the previous FROM-clause tail (e.g. an
    // alias) gets a clean break before `LEFT SEMI JOIN`.
    val head = source.substring(0, whereStart).stripTrailing()
    Some(s"$head $semiJoinClause")
  }

  /** A single null-safe key predicate from an `EXISTS` WHERE clause.
    * E.g. `_d.openivm_left_key IS NOT DISTINCT FROM openivm_lj.openivm_left_key`.
    */
  private case class ExistsKeyPredicate(
      innerAlias: String,
      innerCol: String,
      outerAlias: String,
      outerCol: String
  )

  /** Parsed shape of `SELECT 1 FROM <ref> [AS] <innerAlias> WHERE <p1> AND <p2> …`
    * where every `<pn>` is `<innerAlias>.<col> IS NOT DISTINCT FROM
    * <outerAlias>.<col>` (or the symmetric reversed form).
    */
  private case class ParsedExistsBody(
      fromRef: String,
      innerAlias: String,
      predicates: Seq[ExistsKeyPredicate]
  )

  private def tryParseExistsKeyPredicates(body: String): Option[ParsedExistsBody] = {
    // Must start with `SELECT 1 FROM <ref> [AS] <alias> WHERE <preds>`.
    val headerRe = "(?is)^\\s*SELECT\\s+1\\s+FROM\\s+".r
    val header   = headerRe.findFirstMatchIn(body).getOrElse(return None)
    val refStart = header.end

    // Tokenise the FROM ref: a single source until whitespace, but respect
    // backticked identifiers and `delta.`<path>`` forms which embed
    // back-ticked path segments that can contain `:`, `/`, etc.
    val refEnd = scanSqlTableRef(body, refStart)
    if (refEnd <= refStart) return None
    val fromRef = body.substring(refStart, refEnd)

    // After ref: optional `AS`, then required alias word.
    var i = skipWhitespace(body, refEnd)
    if (startsWithSqlKeyword(body, i, "AS")) {
      i = skipWhitespace(body, i + "AS".length)
    }
    val aliasStart = i
    while (i < body.length && isSqlIdentifierPart(body.charAt(i))) i += 1
    if (i == aliasStart) return None
    val innerAlias = body.substring(aliasStart, i)

    // Then required `WHERE`.
    i = skipWhitespace(body, i)
    if (!startsWithSqlKeyword(body, i, "WHERE")) return None
    i = skipWhitespace(body, i + "WHERE".length)

    val predsText = body.substring(i).trim
    val preds     = splitTopLevelAnd(predsText)
    if (preds.isEmpty) return None

    val parsedPreds = preds.flatMap { p =>
      parseIsNotDistinctFrom(p, innerAlias)
    }
    if (parsedPreds.length != preds.length) return None
    Some(ParsedExistsBody(fromRef, innerAlias, parsedPreds))
  }

  /** Parse `<a>.<col> IS NOT DISTINCT FROM <b>.<col>` (or its reverse).
    * Returns the predicate with the inner side identified by `innerAlias`.
    * Rejects anything else.
    */
  private def parseIsNotDistinctFrom(text: String, innerAlias: String): Option[ExistsKeyPredicate] = {
    val re =
      "(?is)^\\s*(\\w+|`[^`]+`)\\s*\\.\\s*(\\w+|`[^`]+`)\\s+IS\\s+NOT\\s+DISTINCT\\s+FROM\\s+(\\w+|`[^`]+`)\\s*\\.\\s*(\\w+|`[^`]+`)\\s*$".r
    re.findFirstMatchIn(text).flatMap { m =>
      val lhsAlias = stripBackticks(m.group(1))
      val lhsCol   = m.group(2)
      val rhsAlias = stripBackticks(m.group(3))
      val rhsCol   = m.group(4)
      if (lhsAlias == innerAlias && rhsAlias != innerAlias) {
        Some(ExistsKeyPredicate(lhsAlias, lhsCol, rhsAlias, rhsCol))
      } else if (rhsAlias == innerAlias && lhsAlias != innerAlias) {
        Some(ExistsKeyPredicate(rhsAlias, rhsCol, lhsAlias, lhsCol))
      } else None
    }
  }

  private def stripBackticks(s: String): String =
    if (s.startsWith("`") && s.endsWith("`")) s.substring(1, s.length - 1).replace("``", "`")
    else s

  /** Split a SQL predicate text on top-level `AND` (depth=0, case-insensitive,
    * word-boundary aware). Empty input yields Nil.
    */
  private def splitTopLevelAnd(text: String): Seq[String] = {
    val parts = scala.collection.mutable.ArrayBuffer.empty[String]
    var depth = 0
    var i     = 0
    var start = 0
    while (i < text.length) {
      text.charAt(i) match {
        case '\'' => i = consumeSqlSingleQuoted(text, i)
        case '`' =>
          i += 1
          while (i < text.length && text.charAt(i) != '`') i += 1
          if (i < text.length) i += 1
        case '(' => depth += 1; i += 1
        case ')' => depth -= 1; i += 1
        case _ if depth == 0 && startsWithSqlKeyword(text, i, "AND") =>
          parts += text.substring(start, i).trim
          i += "AND".length
          start = i
        case _ => i += 1
      }
    }
    val tail = text.substring(start).trim
    if (tail.nonEmpty) parts += tail
    parts.toSeq.filter(_.nonEmpty)
  }

  /** Scan a single SQL table reference starting at `start`. Handles
    * dotted identifiers, backticked segments (`delta.`<path>``), and stops
    * at the first un-backticked whitespace (so a following alias / keyword
    * is not consumed). Returns the end index (exclusive).
    */
  private def scanSqlTableRef(text: String, start: Int): Int = {
    var i          = start
    var inBacktick = false
    while (i < text.length) {
      val c = text.charAt(i)
      if (c == '`') {
        inBacktick = !inBacktick
        i += 1
      } else if (!inBacktick && Character.isWhitespace(c)) {
        return i
      } else {
        i += 1
      }
    }
    i
  }

  private def skipWhitespace(text: String, from: Int): Int = {
    var i = from
    while (i < text.length && Character.isWhitespace(text.charAt(i))) i += 1
    i
  }

  private def scd2RangeJoins(sql: String): Seq[Scd2RangeJoin] = {
    val rangeRe =
      ("""(?is)((?:CAST\s*\(\s*)?([A-Za-z_]\w*)\s*\.\s*(`?[A-Za-z_]\w*`?)""" +
        """(?:\s+AS\s+(?:TIMESTAMP|DATE)\s*\))?)\s+BETWEEN\s+""" +
        """([A-Za-z_]\w*)\s*\.\s*(`?(?:effective_timestamp|effective_ts|eff)`?)\s+AND\s+""" +
        """\4\s*\.\s*(`?(?:end_timestamp|end_ts|endts)`?)""").r

    rangeRe
      .findAllMatchIn(sql)
      .map { m =>
        Scd2RangeJoin(
          probeAlias = m.group(2),
          probeExpr = m.group(1),
          dimAlias = m.group(4),
          effectiveExpr = s"${m.group(4)}.${m.group(5)}",
          endExpr = s"${m.group(4)}.${m.group(6)}"
        )
      }
      .toVector
      .distinct
  }

  private def injectScd2OverlapPredicates(sql: String, joins: Seq[Scd2RangeJoin]): String = {
    val relationByAlias = relationsByAlias(sql)
    joins.zipWithIndex.foldLeft(sql) { case (current, (join, idx)) =>
      relationByAlias.get(join.probeAlias).filter(isSourceDeltaRelation) match {
        case None => current
        case Some(probeRelation) =>
          val subAlias       = s"__openivm_scd2_probe_$idx"
          val probeExprInSub = qualifyAlias(join.probeExpr, join.probeAlias, subAlias)
          val overlap =
            s" /*__openivm_scd2_range_accel__*/ AND ${join.effectiveExpr} <= " +
              s"(SELECT MAX($probeExprInSub) FROM $probeRelation AS $subAlias) AND ${join.endExpr} >= " +
              s"(SELECT MIN($probeExprInSub) FROM $probeRelation AS $subAlias)"
          val needle = java.util.regex.Pattern.quote(join.probeExpr) +
            "\\s+BETWEEN\\s+" +
            java.util.regex.Pattern.quote(join.effectiveExpr) +
            "\\s+AND\\s+" +
            java.util.regex.Pattern.quote(join.endExpr)
          val rangeRe = ("(?is)" + needle).r
          rangeRe.findFirstMatchIn(current) match {
            case None => current
            case Some(m) =>
              current.substring(0, m.end) + overlap + current.substring(m.end)
          }
      }
    }
  }

  private def qualifyAlias(expr: String, fromAlias: String, toAlias: String): String = {
    val aliasDot = ("(?i)\\b" + java.util.regex.Pattern.quote(fromAlias) + "\\s*\\.").r
    aliasDot.replaceAllIn(expr, java.util.regex.Matcher.quoteReplacement(toAlias + "."))
  }

  private def isSourceDeltaRelation(relation: String): Boolean =
    normalizeSqlIdentifier(relation).split("\\.").lastOption.exists(_.startsWith("openivm_delta_"))

  private def relationsByAlias(sql: String): Map[String, String] =
    selectKeywordOffsets(sql)
      .flatMap { selectIdx =>
        val block = sql.substring(selectIdx, selectBlockEnd(sql, selectIdx))
        relationRefsInBlock(block)
      }
      .map(ref => ref.alias -> ref.relation)
      .toMap

  private def relationRefsInBlock(block: String): Seq[SqlRelationRef] = {
    val refs = scala.collection.mutable.ArrayBuffer[SqlRelationRef]()
    scanSql(block) { (idx, _, depth) =>
      if (depth == 0 && (isKeywordAt(block, idx, "FROM") || isKeywordAt(block, idx, "JOIN"))) {
        val keywordEnd = idx + (if (isKeywordAt(block, idx, "FROM")) "FROM".length else "JOIN".length)
        val refStart   = skipWhitespace(block, keywordEnd)
        if (refStart < block.length && block.charAt(refStart) != '(') {
          val refEnd   = scanSqlTableRef(block, refStart)
          val relation = block.substring(refStart, refEnd).replaceAll("\\s+", "")
          val short    = normalizeSqlIdentifier(relation).split("\\.").lastOption.getOrElse(relation)
          var aliasAt  = skipWhitespace(block, refEnd)
          if (isKeywordAt(block, aliasAt, "AS")) aliasAt = skipWhitespace(block, aliasAt + "AS".length)
          val aliasEnd = scanBareToken(block, aliasAt)
          val alias =
            if (aliasEnd > aliasAt) block.substring(aliasAt, aliasEnd)
            else short
          if (!sqlClauseKeywords.contains(alias.toUpperCase)) refs += SqlRelationRef(relation, alias)
        }
      }
    }
    refs.toVector
  }

  private def normalizeSqlIdentifier(value: String): String =
    value
      .split("\\.")
      .map(_.trim.stripPrefix("`").stripSuffix("`"))
      .filter(_.nonEmpty)
      .mkString(".")
      .toLowerCase

  private val sqlClauseKeywords: Set[String] =
    Set(
      "ON",
      "WHERE",
      "JOIN",
      "LEFT",
      "RIGHT",
      "FULL",
      "INNER",
      "OUTER",
      "CROSS",
      "GROUP",
      "ORDER",
      "HAVING",
      "LIMIT",
      "UNION",
      "WHEN",
      "USING"
    )

  private def injectBroadcastHintsForAliases(sql: String, aliases: Set[String]): String = {
    val cleanAliases = aliases.filter(_.matches("[A-Za-z_]\\w*"))
    if (cleanAliases.isEmpty) sql
    else {
      val edits = selectKeywordOffsets(sql).flatMap { selectIdx =>
        val blockEnd     = selectBlockEnd(sql, selectIdx)
        val block        = sql.substring(selectIdx, blockEnd)
        val blockAliases = relationRefsInBlock(block).map(_.alias).filter(cleanAliases.contains).distinct.sorted
        if (blockAliases.isEmpty) None
        else Some(selectIdx + "SELECT".length -> s" /*+ BROADCAST(${blockAliases.mkString(", ")}) */")
      }
      if (edits.isEmpty) sql
      else {
        val out  = new StringBuilder(sql)
        var bias = 0
        edits.foreach { case (idx, hint) =>
          out.insert(idx + bias, hint)
          bias += hint.length
        }
        out.toString()
      }
    }
  }

  private def broadcastAliasesInBlock(block: String, broadcastNames: Set[String]): Seq[String] = {
    val aliases = scala.collection.mutable.ArrayBuffer[String]()
    scanSql(block) { (idx, _, depth) =>
      if (depth == 0 && (isKeywordAt(block, idx, "FROM") || isKeywordAt(block, idx, "JOIN"))) {
        val keywordEnd = idx + (if (isKeywordAt(block, idx, "FROM")) "FROM".length else "JOIN".length)
        val refStart   = skipWhitespace(block, keywordEnd)
        if (refStart < block.length && block.charAt(refStart) != '(') {
          val refEnd   = scanSqlTableRef(block, refStart)
          val relation = normalizeSqlIdentifier(block.substring(refStart, refEnd).replaceAll("\\s+", ""))
          val short    = relation.split("\\.").lastOption.getOrElse(relation)
          var aliasAt  = skipWhitespace(block, refEnd)
          if (isKeywordAt(block, aliasAt, "AS")) aliasAt = skipWhitespace(block, aliasAt + "AS".length)
          val aliasEnd = scanBareToken(block, aliasAt)
          val alias =
            if (aliasEnd > aliasAt) block.substring(aliasAt, aliasEnd)
            else short
          if (
            (broadcastNames.contains(relation) || broadcastNames.contains(short)) &&
            !sqlClauseKeywords.contains(alias.toUpperCase)
          ) aliases += alias
        }
      }
    }
    aliases.toVector.distinct.sortBy(_.toLowerCase)
  }

  private def scanBareToken(text: String, start: Int): Int = {
    var i = start
    while (i < text.length && (Character.isLetterOrDigit(text.charAt(i)) || text.charAt(i) == '_')) i += 1
    i
  }

  private def isKeywordAt(sql: String, idx: Int, keyword: String): Boolean =
    idx >= 0 &&
      idx + keyword.length <= sql.length &&
      sql.regionMatches(true, idx, keyword, 0, keyword.length) &&
      isWordBoundary(sql, idx - 1) &&
      isWordBoundary(sql, idx + keyword.length)

  private def selectKeywordOffsets(sql: String): Seq[Int] = {
    val offsets = scala.collection.mutable.ArrayBuffer[Int]()
    scanSql(sql) { (idx, c, _) =>
      if (
        (c == 'S' || c == 's') &&
        sql.regionMatches(true, idx, "SELECT", 0, "SELECT".length) &&
        isWordBoundary(sql, idx - 1) &&
        isWordBoundary(sql, idx + "SELECT".length)
      ) offsets += idx
    }
    offsets.toSeq
  }

  private def selectBlockEnd(sql: String, selectIdx: Int): Int = {
    val depths     = depthBeforeOffsets(sql, Set(selectIdx))
    val startDepth = depths.getOrElse(selectIdx, 0)
    var end        = sql.length
    scanSql(sql, selectIdx + "SELECT".length) { (idx, c, depthBefore) =>
      if (end == sql.length && c == ')' && depthBefore <= startDepth) end = idx
    }
    end
  }

  private def depthBeforeOffsets(sql: String, offsets: Set[Int]): Map[Int, Int] = {
    val found = scala.collection.mutable.Map[Int, Int]()
    scanSql(sql) { (idx, _, depthBefore) =>
      if (offsets.contains(idx)) found += idx -> depthBefore
    }
    found.toMap
  }

  private def scanSql(sql: String, start: Int = 0)(f: (Int, Char, Int) => Unit): Unit = {
    var depth = 0
    var i     = 0
    while (i < sql.length) {
      val c = sql.charAt(i)
      if (i >= start) f(i, c, depth)
      c match {
        case '\'' =>
          i += 1
          var done = false
          while (i < sql.length && !done) {
            if (sql.charAt(i) == '\'' && i + 1 < sql.length && sql.charAt(i + 1) == '\'') i += 2
            else if (sql.charAt(i) == '\'') {
              done = true
              i += 1
            } else i += 1
          }
        case '`' =>
          i += 1
          while (i < sql.length && sql.charAt(i) != '`') i += 1
          if (i < sql.length) i += 1
        case '-' if i + 1 < sql.length && sql.charAt(i + 1) == '-' =>
          i += 2
          while (i < sql.length && sql.charAt(i) != '\n') i += 1
        case '/' if i + 1 < sql.length && sql.charAt(i + 1) == '*' =>
          i += 2
          while (i + 1 < sql.length && !(sql.charAt(i) == '*' && sql.charAt(i + 1) == '/')) i += 1
          if (i + 1 < sql.length) i += 2
        case '(' =>
          depth += 1
          i += 1
        case ')' =>
          depth = math.max(0, depth - 1)
          i += 1
        case _ =>
          i += 1
      }
    }
  }
}
