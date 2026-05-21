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
            Seq(rewriteViewDeltaInsert(stmt, viewLogicalName, viewDeltaPath))
          case StatementKind.ViewDeltaCompanion =>
            Seq(rewriteViewDeltaCompanion(stmt, viewLogicalName, mvName, viewDeltaPath))
          case StatementKind.MvMerge =>
            Seq(rewriteMvMerge(stmt, viewLogicalName, mvName, viewDeltaPath))
          case StatementKind.SimpleProjectionDataInsert =>
            rewriteSimpleProjectionDataInsert(stmt, viewLogicalName, mvName, viewDeltaPath)
          case StatementKind.ScalarUpdate =>
            Seq(rewriteScalarUpdate(stmt, viewLogicalName, mvName, viewDeltaPath))
          case StatementKind.ScalarDeleteMv =>
            Seq(rewriteScalarDeleteMv(stmt, viewLogicalName, mvName, viewDeltaPath))
          case StatementKind.ScalarFullRecomputeInsert =>
            Seq(rewriteScalarFullRecomputeInsert(stmt, viewLogicalName, mvName, viewDeltaPath))
          case StatementKind.PartitionScopedDelete =>
            rewritePartitionScopedDelete(stmt, viewLogicalName, mvName)
          case StatementKind.PartitionScopedInsert =>
            Seq(rewritePartitionScopedInsert(stmt, viewLogicalName, mvName))
          case StatementKind.GroupRecomputeAffectedCreate =>
            Seq(rewriteGroupRecomputeAffectedCreate(stmt, viewLogicalName))
          case StatementKind.GroupRecomputeAffectedDrop =>
            Seq(rewriteGroupRecomputeAffectedDrop(stmt, viewLogicalName))
          case StatementKind.OldSnapshotCreate =>
            Seq(rewriteOldSnapshotCreate(stmt, viewLogicalName, mvLocation, mvVersionBeforeRefresh))
          case StatementKind.NewSnapshotCreate =>
            Seq(rewriteNewSnapshotCreate(stmt))
          case StatementKind.SnapshotDataInsert =>
            Seq(rewriteSnapshotDataInsert(stmt, viewLogicalName, mvName))
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
      RewrittenRefresh(withAliasFixup.map(postProcess))
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
      * emits when `openivm_force_view_delta_cascade=true` (or when a downstream
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

    /** Recompute-cascade snapshot of the PRE-refresh MV rows.
      *
      * When openivm `4471f4e929fd3b21ac55ea0c47249d4716853c98`
      * (`openivm_emit_cascade_delta_for_recompute=true`) is enabled, both
      * WINDOW_PARTITION and GROUP_RECOMPUTE emit
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
    case object PartitionScopedInsert extends StatementKind
    case object Cleanup               extends StatementKind
    case object Unknown               extends StatementKind
  }

  private def classify(stmt: String, viewLogicalName: String): StatementKind = {
    val upper            = stmt.toUpperCase.trim
    val affectedKeysName = s"OPENIVM_AFFECTED_${viewLogicalName.toUpperCase}"
    val oldSnapshotName  = s"OPENIVM_OLD_${viewLogicalName.toUpperCase}"
    val newSnapshotName  = s"OPENIVM_NEW_${viewLogicalName.toUpperCase}"
    val compactName      = s"OPENIVM_OLD_COMPACT_${viewLogicalName.toUpperCase}"
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
    } else if (upper.startsWith(s"CREATE OR REPLACE TEMP TABLE $affectedKeysName")) {
      // GROUP_RECOMPUTE Statement B: TEMP TABLE materialising affected group keys.
      StatementKind.GroupRecomputeAffectedCreate
    } else if (upper.startsWith(s"CREATE OR REPLACE TEMP TABLE $oldSnapshotName")) {
      StatementKind.OldSnapshotCreate
    } else if (upper.startsWith(s"CREATE OR REPLACE TEMP TABLE $newSnapshotName")) {
      StatementKind.NewSnapshotCreate
    } else if (upper.startsWith(s"DROP TABLE IF EXISTS $affectedKeysName")) {
      // GROUP_RECOMPUTE Statement E: cleanup of the affected-keys scratch object.
      StatementKind.GroupRecomputeAffectedDrop
    } else if (
      upper.startsWith(s"DROP TABLE IF EXISTS $oldSnapshotName") ||
      upper.startsWith(s"DROP TABLE IF EXISTS $newSnapshotName")
    ) {
      StatementKind.SnapshotDrop
    } else if (upper.contains(s"INSERT INTO OPENIVM_DELTA_${viewLogicalName.toUpperCase}")) {
      // Distinguish the AGGREGATE_GROUP retract companion (refresh_sql.cpp:620,
      // emitted when `openivm_force_view_delta_cascade=true`) from the main
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
      upper.contains(s"INSERT INTO OPENIVM_DATA_${viewLogicalName.toUpperCase}") &&
      upper.contains(s"FROM $newSnapshotName")
    ) {
      StatementKind.SnapshotDataInsert
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
      viewDeltaPath: String
  ): String = {
    var s = stmt
    s = deduplicateCteColumnAliases(s)
    s = stripTimestampPredicate(s)
    s = rewriteMemoryMainPrefix(s)
    s = rewriteInsertToCtas(s, viewLogicalName, viewDeltaPath)
    s = rewriteInsertNoColumnListToCtas(s, viewLogicalName, viewDeltaPath)
    s
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
    // emitted by openivm when `openivm_force_view_delta_cascade=true`).
    val qcol = "(?:\\w+\\.)?openivm_timestamp"
    // Case 1: standalone `WHERE openivm_timestamp OP '...'::TIMESTAMP`
    val standalone = ("(?i)\\s+WHERE\\s+" + qcol + "\\s*(?:>=|>|<=|<|=)\\s*'[^']*'::\\s*TIMESTAMP").r
    // Case 2: trailing `AND openivm_timestamp OP '...'::TIMESTAMP`
    val trailingAnd = ("(?i)\\s+AND\\s+" + qcol + "\\s*(?:>=|>|<=|<|=)\\s*'[^']*'::\\s*TIMESTAMP").r
    // Case 3: leading `openivm_timestamp OP '...'::TIMESTAMP AND `
    val leadingAnd = ("(?i)\\b" + qcol + "\\s*(?:>=|>|<=|<|=)\\s*'[^']*'::\\s*TIMESTAMP\\s+AND\\s+").r

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
    * (emitted by openivm when `openivm_force_view_delta_cascade=true`):
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

  // ── Statement C (SIMPLE_PROJECTION): INSERT + retraction MERGE ───────────

  /** Rewrites the SIMPLE_PROJECTION data-INSERT statement into two Spark
    * statements:
    *
    *   1. INSERT — asserts rows with positive multiplicity into the MV.
    *   2. MERGE (DELETE-only) — retracts rows with negative multiplicity from
    *      the MV using value-equality `IS NOT DISTINCT FROM` conditions.
    *
    * The openivm-emitted Statement C has the form:
    * {{{
    *   INSERT INTO openivm_data_<view> SELECT <user_cols>
    *   FROM openivm_delta_<view>, generate_series(1, openivm_multiplicity::BIGINT)
    *   WHERE openivm_timestamp > '<ts>'::TIMESTAMP AND openivm_multiplicity > 0
    * }}}
    *
    * After rewrite:
    * {{{
    *   INSERT INTO <mv> SELECT <user_cols> FROM delta.<viewDeltaPath> WHERE `openivm_multiplicity` > 0
    *
    *   MERGE INTO <mv> AS v
    *   USING (SELECT <user_cols> FROM delta.<viewDeltaPath> WHERE `openivm_multiplicity` < 0) AS d
    *   ON v.<c1> IS NOT DISTINCT FROM d.<c1> AND ...
    *   WHEN MATCHED THEN DELETE
    * }}}
    *
    * OpenIVM limitation: this MERGE uses value-equality which is non-deterministic
    * when the MV contains duplicate rows (Delta MERGE will delete ALL matching
    * copies). SIMPLE_PROJECTION MVs over sources with duplicate rows are not
    * fully supported in this MVP. See RESEARCH.md §12 risk 8.
    */
  private def rewriteSimpleProjectionDataInsert(
      stmt: String,
      viewLogicalName: String,
      mvName: TableIdentifier,
      viewDeltaPath: String
  ): Seq[String] = {
    val mv          = backtickMvName(mvName)
    val escapedPath = viewDeltaPath.replace("`", "``")
    val deltaRef    = s"delta.`$escapedPath`"

    // Extract user columns from "INSERT INTO openivm_data_<view> SELECT <cols> FROM openivm_delta_<view>"
    val selectRe = ("(?is)\\bINSERT\\s+INTO\\s+openivm_data_" +
      java.util.regex.Pattern.quote(viewLogicalName) +
      "\\b\\s+SELECT\\s+(.*?)\\s*\\bFROM\\s+(?:`?openivm_delta_" +
      java.util.regex.Pattern.quote(viewLogicalName) +
      "`?)").r

    val userCols: Seq[String] = selectRe.findFirstMatchIn(stmt) match {
      case None => Nil
      case Some(m) =>
        m.group(1)
          .split(",")
          .map(_.trim)
          .map { col =>
            // Normalise DuckDB double-quoted identifiers: "name" → `name`
            if (col.startsWith("\"") && col.endsWith("\""))
              s"`${col.substring(1, col.length - 1)}`"
            else
              s"`${col.replace("`", "``")}`"
          }
          .toSeq
    }

    if (userCols.isEmpty) return Nil

    val colList = userCols.mkString(", ")

    // 1. INSERT: assert positive-multiplicity rows into the MV
    val insertSql =
      s"""|INSERT INTO $mv
          |SELECT $colList
          |FROM $deltaRef
          |WHERE `openivm_multiplicity` > 0""".stripMargin

    // 2. MERGE (DELETE-only): retract negative-multiplicity rows from the MV
    val onCondition = userCols
      .map(c => s"v.$c IS NOT DISTINCT FROM d.$c")
      .mkString(" AND ")
    val deleteMergeSql = markSimpleProjectionDeleteMerge(
      s"""|MERGE INTO $mv AS v
          |USING (
          |  SELECT $colList
          |  FROM $deltaRef
          |  WHERE `openivm_multiplicity` < 0
          |) AS d
          |ON $onCondition
          |WHEN MATCHED THEN DELETE""".stripMargin
    )

    Seq(insertSql, deleteMergeSql)
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
  ): String = {
    val mvRef = backtickMvName(mvName)
    val dataViewRe = ("(?i)\\bopenivm_data_" +
      java.util.regex.Pattern.quote(viewLogicalName) + "\\b").r
    var s = dataViewRe.replaceAllIn(stmt, java.util.regex.Matcher.quoteReplacement(mvRef))
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
    // Delta Lake forbids subqueries (including EXISTS) inside DELETE WHERE
    // conditions, so rewrite the affected-keys form into a MERGE…WHEN MATCHED
    // THEN DELETE.  No-op when the DELETE has no EXISTS WHERE clause.
    s = rewriteDeleteExistsAsMerge(s, mvRef)
    s
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
    s
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
      mvName: TableIdentifier
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

    splitTopLevelOr(whereBody).flatMap { clause =>
      val trimmed = clause.trim
      inClauseToMerge(trimmed, mvRef)
    }
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
      viewLogicalName: String
  ): String = {
    val _ = viewLogicalName // captured by the SQL itself; reserved for future use
    var s = rewriteCreateOrReplaceTempTableAsView(stmt)
    s = stripTimestampPredicate(s)
    s = rewriteMemoryMainPrefix(s)
    s = rewriteExcludeAsExcept(s)
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
    //   - AGGREGATE_GROUP retract/add companions (`openivm_force_view_delta_cascade=true`):
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
}
