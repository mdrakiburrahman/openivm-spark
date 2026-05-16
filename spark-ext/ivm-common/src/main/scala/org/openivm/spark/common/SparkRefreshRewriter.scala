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

  /** Rewrite the openivm-emitted multi-statement refresh SQL into Spark-
    * executable statements.
    *
    * @param compiledSql      Full multi-statement refresh program as emitted
    *                         by openivm (after dialect translation has
    *                         already happened upstream — though [[postProcess]]
    *                         offers a final hook).
    * @param mvName           Fully-qualified MV identifier (e.g. `db.v`).
    * @param mvLocation       Reserved for future use; the MV's Delta path is
    *                         not directly referenced by any rewritten
    *                         statement (statement C uses the table identifier).
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
    */
  def rewrite(
      compiledSql: String,
      mvName: TableIdentifier,
      mvLocation: String,
      viewLogicalName: String,
      sourceTempViews: Map[String, String],
      viewDeltaPath: String,
      postProcess: String => String = identity
  ): RewrittenRefresh = {
    val _ = (mvLocation, sourceTempViews) // reserved for future passes

    val stmts = splitStatements(compiledSql).map(_.trim).filter(_.nonEmpty)

    val rewritten: Seq[String] = stmts.flatMap { stmt =>
      classify(stmt, viewLogicalName) match {
        case StatementKind.InProgressFlag | StatementKind.Cleanup => Nil
        case StatementKind.ViewDeltaInsert =>
          Seq(rewriteViewDeltaInsert(stmt, viewLogicalName, viewDeltaPath))
        case StatementKind.MvMerge =>
          Seq(rewriteMvMerge(stmt, viewLogicalName, mvName, viewDeltaPath))
        case StatementKind.SimpleProjectionDataInsert =>
          rewriteSimpleProjectionDataInsert(stmt, viewLogicalName, mvName, viewDeltaPath)
        case StatementKind.ScalarUpdate =>
          Seq(rewriteScalarUpdate(stmt, viewLogicalName, mvName, viewDeltaPath))
        case StatementKind.ScalarDeleteMv =>
          Seq(rewriteScalarDeleteMv(stmt, viewLogicalName, mvName))
        case StatementKind.ScalarFullRecomputeInsert =>
          Seq(rewriteScalarFullRecomputeInsert(stmt, viewLogicalName, mvName))
        case StatementKind.Unknown => Nil
      }
    }

    RewrittenRefresh(rewritten.map(postProcess))
  }

  // ── Statement classification ─────────────────────────────────────────────

  private sealed trait StatementKind
  private object StatementKind {
    case object InProgressFlag            extends StatementKind
    case object ViewDeltaInsert           extends StatementKind
    case object MvMerge                   extends StatementKind
    /** SIMPLE_PROJECTION Statement C: `INSERT INTO openivm_data_<view> SELECT … FROM openivm_delta_<view>, generate_series(…)` */
    case object SimpleProjectionDataInsert extends StatementKind
    /** SIMPLE_AGGREGATE Statements C/D/E: any `UPDATE openivm_data_<view> SET …` form,
      * including the CTE-prefixed incremental-sum update, the hidden-column recompute,
      * and the null-reset for an empty source. */
    case object ScalarUpdate              extends StatementKind
    /** SIMPLE_AGGREGATE full-recompute for non-additive aggregates (MIN/MAX):
      * `DELETE FROM openivm_data_<view>` — clears the MV before re-insert. */
    case object ScalarDeleteMv            extends StatementKind
    /** SIMPLE_AGGREGATE full-recompute for non-additive aggregates (MIN/MAX):
      * `INSERT INTO openivm_data_<view> WITH scan_0 … SELECT … FROM memory.main.<src>`
      * — re-inserts by querying the live source table (not the delta). */
    case object ScalarFullRecomputeInsert extends StatementKind
    case object Cleanup                   extends StatementKind
    case object Unknown                   extends StatementKind
  }

  private def classify(stmt: String, viewLogicalName: String): StatementKind = {
    val upper = stmt.toUpperCase.trim
    if (upper.startsWith("UPDATE OPENIVM_VIEWS")) {
      StatementKind.InProgressFlag
    } else if (upper.contains(s"INSERT INTO OPENIVM_DELTA_${viewLogicalName.toUpperCase}")) {
      StatementKind.ViewDeltaInsert
    } else if (upper.contains(s"MERGE INTO OPENIVM_DATA_${viewLogicalName.toUpperCase}")) {
      StatementKind.MvMerge
    } else if (upper.contains(s"INSERT INTO OPENIVM_DATA_${viewLogicalName.toUpperCase}") &&
               upper.contains(s"OPENIVM_DELTA_${viewLogicalName.toUpperCase}")) {
      StatementKind.SimpleProjectionDataInsert
    } else if (upper.contains(s"INSERT INTO OPENIVM_DATA_${viewLogicalName.toUpperCase}")) {
      // Full-recompute INSERT for non-additive aggregates (MIN/MAX): reads from live source
      StatementKind.ScalarFullRecomputeInsert
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
      viewDeltaPath: String
  ): String = {
    var s = stmt
    s = deduplicateCteColumnAliases(s)
    s = stripTimestampPredicate(s)
    s = rewriteMemoryMainPrefix(s)
    s = rewriteInsertToCtas(s, viewLogicalName, viewDeltaPath)
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
    // Case 1: standalone `WHERE openivm_timestamp OP '...'::TIMESTAMP`
    val standalone = """(?i)\s+WHERE\s+openivm_timestamp\s*(?:>=|>|<=|<|=)\s*'[^']*'::\s*TIMESTAMP""".r
    // Case 2: trailing `AND openivm_timestamp OP '...'::TIMESTAMP`
    val trailingAnd = """(?i)\s+AND\s+openivm_timestamp\s*(?:>=|>|<=|<|=)\s*'[^']*'::\s*TIMESTAMP""".r
    // Case 3: leading `openivm_timestamp OP '...'::TIMESTAMP AND `
    val leadingAnd = """(?i)\bopenivm_timestamp\s*(?:>=|>|<=|<|=)\s*'[^']*'::\s*TIMESTAMP\s+AND\s+""".r

    leadingAnd.replaceAllIn(
      trailingAnd.replaceAllIn(
        standalone.replaceAllIn(sql, ""),
        ""
      ),
      ""
    )
  }

  /** Replace `memory.main.<identifier>` → `` `<identifier>` ``.
    *
    * openivm always emits the DuckDB catalog prefix `memory.main.` when the
    * SPARK target dialect is selected; on the Spark side these reference our
    * temp views and tables directly.
    */
  private def rewriteMemoryMainPrefix(sql: String): String = {
    val re = """memory\.main\.(\w+)""".r
    re.replaceAllIn(sql, m => s"`${m.group(1)}`")
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
    s = dataViewRe.replaceAllIn(s, java.util.regex.Matcher.quoteReplacement(mvSqlName))

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
        m.group(1).split(",").map(_.trim).map { col =>
          // Normalise DuckDB double-quoted identifiers: "name" → `name`
          if (col.startsWith("\"") && col.endsWith("\""))
            s"`${col.substring(1, col.length - 1)}`"
          else
            s"`${col.replace("`", "``")}`"
        }.toSeq
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
    val deleteMergeSql =
      s"""|MERGE INTO $mv AS v
          |USING (
          |  SELECT $colList
          |  FROM $deltaRef
          |  WHERE `openivm_multiplicity` < 0
          |) AS d
          |ON $onCondition
          |WHEN MATCHED THEN DELETE""".stripMargin

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
    * Emitted by openivm for non-additive aggregates (MIN/MAX) before the full-recompute INSERT.
    */
  private def rewriteScalarDeleteMv(
      stmt: String,
      viewLogicalName: String,
      mvName: TableIdentifier
  ): String = {
    val mvRef = backtickMvName(mvName)
    val dataViewRe = ("(?i)\\bopenivm_data_" +
      java.util.regex.Pattern.quote(viewLogicalName) + "\\b").r
    dataViewRe.replaceAllIn(stmt, java.util.regex.Matcher.quoteReplacement(mvRef))
  }

  /** Rewrites the full-recompute `INSERT INTO openivm_data_<view> WITH scan_0 … SELECT … FROM memory.main.<src>`
    * into `INSERT INTO <mv> WITH scan_0 … SELECT … FROM \`<src>\``.
    *
    * Emitted by openivm for non-additive aggregates (MIN/MAX) — the MV is cleared by a preceding
    * ScalarDeleteMv statement and then fully re-populated from the live source table.
    */
  private def rewriteScalarFullRecomputeInsert(
      stmt: String,
      viewLogicalName: String,
      mvName: TableIdentifier
  ): String = {
    val mvRef = backtickMvName(mvName)
    val dataViewRe = ("(?i)\\bopenivm_data_" +
      java.util.regex.Pattern.quote(viewLogicalName) + "\\b").r
    var s = dataViewRe.replaceAllIn(stmt, java.util.regex.Matcher.quoteReplacement(mvRef))
    s = rewriteMemoryMainPrefix(s)
    s
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
        refRe.replaceAllIn(updatePart, mm => {
          val alias = mm.group(1).toUpperCase
          aliasMap.get(alias) match {
            case None       => mm.matched
            case Some(expr) =>
              java.util.regex.Matcher.quoteReplacement(s"(SELECT $expr FROM $deltaRef)")
          }
        })
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
    val upper = cteBody.toUpperCase.trim
    val selectStart = if (upper.startsWith("SELECT")) 6 else return Map.empty

    var depth   = 0
    var fromIdx = -1
    var i       = selectStart
    while (i < upper.length && fromIdx < 0) {
      upper(i) match {
        case '(' => depth += 1; i += 1
        case ')' => depth -= 1; i += 1
        case 'F' if depth == 0 && i + 4 <= upper.length &&
            upper.substring(i, i + 4) == "FROM" &&
            (i + 4 >= upper.length || upper(i + 4).isWhitespace) =>
          fromIdx = i
        case _ => i += 1
      }
    }

    val selectList =
      if (fromIdx > 0) cteBody.substring(selectStart, fromIdx).trim
      else             cteBody.substring(selectStart).trim

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
    cteColListRe.replaceAllIn(sql, mm => {
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
    })
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
    val stmts = splitStatements(compiledSql).map(_.trim).filter(_.nonEmpty)
    val deltaStmts = stmts.filter { stmt =>
      stmt.toUpperCase.contains(s"INSERT INTO OPENIVM_DELTA_${viewLogicalName.toUpperCase}")
    }
    // A placeholder ends with `SELECT … WHERE false` (case-insensitive); a real
    // delta ends with `SELECT * FROM <lastCte>` preceded by a CTE block.
    deltaStmts.nonEmpty && !deltaStmts.forall(_.toUpperCase.contains("WHERE FALSE"))
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
}
