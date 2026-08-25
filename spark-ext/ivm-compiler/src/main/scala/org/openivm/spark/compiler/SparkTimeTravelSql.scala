package org.openivm.spark.compiler

import java.util.Locale

import org.apache.spark.sql.catalyst.analysis.{RelationTimeTravel, UnresolvedRelation}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/** Splits Spark/Delta snapshot pins (`… VERSION AS OF <v>` / `… TIMESTAMP AS OF
  * <ts>`, a.k.a. Spark's `temporalClause`) out of a materialized-view body.
  *
  * ## Why this exists
  *
  * A snapshot pin is a Spark/Delta STORAGE concern: it selects which committed
  * version of a Delta table a relation reads. It carries no information the
  * OpenIVM/LPTS compiler can use — the compile bridge registers schema-only,
  * row-less DuckDB tables ([[OpenIvmCompiler.compile]]), so there is no
  * snapshot history on the DuckDB side to pin to. DuckDB rejects Spark's
  * spelling outright at parse time:
  *
  * {{{
  * Parser Error: syntax error at or near "as"
  * LINE 1: … FROM billing_meter_dim VERSION AS OF 366 GROUP BY region;
  *                                          ^
  * }}}
  *
  * and its own DuckLake spelling (`AT (VERSION => 366)`) parses but then fails
  * to bind (`Binder Error: Catalog type does not support time travel`) against
  * the bridge's plain in-memory tables. Either way the whole view is demoted
  * `COMPILE_FAILED -> FULL_REFRESH`, so every refresh — including a refresh
  * with an empty delta — re-executes the entire view body.
  *
  * The pin is therefore stripped from the compile-bridge COPY of the body only.
  * It is NOT stripped from anything Spark executes:
  *   - `MvMetadata.querySql` keeps the user's SQL verbatim, so the FULL_REFRESH
  *     `INSERT OVERWRITE` path stays pinned;
  *   - [[OpenIvmCompiler.parseInitialLoadSql]] re-applies the pin to the
  *     initial-load SELECT openivm emits, so the CTAS at CREATE loads the
  *     pinned snapshot rather than live data;
  *   - `SparkRefreshRewriter` re-applies the pin to every live-source read in
  *     the compiled refresh program (`memory.main.<source>`), so a mixed
  *     pinned/live join still reads the pinned side at its pinned version.
  *
  * ## Correctness of the split
  *
  * The clause is located with a Spark-dialect, quote/comment-aware scanner —
  * this object deliberately does NOT re-implement a SQL parser. Every split is
  * then VERIFIED against Spark's own parser ([[CatalystSqlParser]]):
  *
  *   1. the de-pinned SQL must parse,
  *   2. it must contain no remaining `RelationTimeTravel` node,
  *   3. its multiset of relation identifiers must be identical to the original's
  *      (source-table identity is preserved — nothing is renamed or dropped).
  *
  * If any check fails the ORIGINAL SQL is returned unchanged, so the compile
  * bridge fails loudly exactly as it does today rather than silently compiling
  * something that is not what the user wrote.
  */
object SparkTimeTravelSql {

  /** One snapshot pin: the relation reference exactly as the user wrote it
    * (`billing_meter_dim`, `arc_sql_db_bi.billing_meter_dim`, `` `db`.`t` ``)
    * plus the temporal clause text (`VERSION AS OF 366`).
    */
  final case class SnapshotPin(tableRef: String, clause: String) {

    /** Last segment of [[tableRef]], unquoted and lower-cased. */
    def shortName: String = SparkTimeTravelSql.identifierSegments(tableRef).last

    /** Unquoted, lower-cased segments of [[tableRef]]. */
    def segments: Seq[String] = SparkTimeTravelSql.identifierSegments(tableRef)
  }

  /** Result of [[split]]: the de-pinned SQL plus every pin removed from it.
    * `pins` is empty exactly when `sql` is the untouched input.
    */
  final case class Split(sql: String, pins: Seq[SnapshotPin])

  /** Cheap pre-filter so the scanner + parser round-trip only runs for bodies
    * that plausibly contain a temporal clause. Matches inside string literals
    * too — that is fine, it only gates the precise pass below.
    */
  private val TemporalClauseGuard =
    """(?i)\b(?:SYSTEM_VERSION|VERSION|SYSTEM_TIME|TIMESTAMP)\s+AS\s+OF\b""".r

  private val VersionKeywords   = Seq("SYSTEM_VERSION", "VERSION")
  private val TimestampKeywords = Seq("SYSTEM_TIME", "TIMESTAMP")

  /** Keywords that can never start a temporal-clause VALUE. Guards against
    * mis-reading a query like `SELECT timestamp AS of FROM t` (where `of` is a
    * column alias) as a pin.
    */
  private val ValueStopKeywords: Set[String] =
    Set(
      "from",
      "where",
      "group",
      "order",
      "having",
      "limit",
      "join",
      "inner",
      "outer",
      "left",
      "right",
      "full",
      "cross",
      "semi",
      "anti",
      "on",
      "using",
      "union",
      "intersect",
      "except",
      "select",
      "with",
      "as",
      "and",
      "or",
      "when",
      "then",
      "else",
      "end",
      "window",
      "qualify",
      "cluster",
      "distribute",
      "sort",
      "lateral",
      "natural",
      "tablesample",
      "pivot",
      "unpivot"
    )

  /** True when `sql` plausibly carries a Spark snapshot pin. */
  def hasSnapshotPin(sql: String): Boolean =
    sql != null && sql.nonEmpty && TemporalClauseGuard.findFirstIn(sql).isDefined && split(sql).pins.nonEmpty

  /** True when `sql` pins a source to a snapshot in a shape [[split]] refuses
    * to lift out — the same source read at two different versions, or pinned in
    * one place and read live in another (including through a CTE that shadows
    * the pinned name).
    *
    * OpenIVM re-applies a pin per SOURCE, so it cannot honor those shapes:
    * whichever single clause it picked would freeze or unfreeze the other read.
    * Historically DuckDB's parser caught them for us — the un-split body still
    * carried `VERSION AS OF`, so the compile aborted and the view fell back to
    * FULL_REFRESH, which re-executes the user's pinned body verbatim and is
    * therefore correct.
    *
    * An LPTS front-end that ACCEPTS Spark's `temporalClause` removes that
    * accident: the compile would succeed, no pin would be registered, and the
    * incremental program would silently read live rows for a frozen relation.
    * The compile bridge refuses these bodies itself so the fallback does not
    * depend on a downstream parser rejecting them.
    *
    * Bodies `CatalystSqlParser` cannot parse are NOT refused here: without a
    * parse there is no evidence of a real pin, so they are passed through and
    * DuckDB decides, exactly as before.
    */
  def hasUnsupportedSnapshotPin(sql: String): Boolean =
    sql != null && sql.nonEmpty &&
      TemporalClauseGuard.findFirstIn(sql).isDefined &&
      split(sql).pins.isEmpty &&
      parsePlan(sql).exists(plan => timeTravelCount(plan) > 0)

  /** Split every snapshot pin out of `sql`.
    *
    * Returns `Split(sql, Nil)` unchanged when there is no pin, when the scanner
    * cannot recognise the clause shape, when the same source is read at more
    * than one version (or both pinned and live), or when the parser cross-check
    * rejects the rewrite. Those bodies are reported by
    * [[hasUnsupportedSnapshotPin]] and refused by the compile bridge, because
    * re-applying one clause to every read of that source would silently produce
    * wrong rows; `COMPILE_FAILED -> FULL_REFRESH` re-executes the pinned body
    * verbatim and stays correct.
    */
  def split(sql: String): Split = {
    if (sql == null || sql.isEmpty) return Split(sql, Nil)
    if (TemporalClauseGuard.findFirstIn(sql).isEmpty) return Split(sql, Nil)
    val scanned = scan(sql)
    if (scanned.pins.isEmpty) return Split(sql, Nil)
    if (!pinsAreUnambiguous(scanned.pins)) return Split(sql, Nil)
    if (!verifiesAgainstSparkParser(sql, scanned.sql, scanned.pins)) Split(sql, Nil) else scanned
  }

  /** False when one source carries two different pins (a cross-version
    * self-join): the Spark side re-applies a pin per SOURCE, so there is no
    * single version to freeze that relation at.
    */
  private def pinsAreUnambiguous(pins: Seq[SnapshotPin]): Boolean =
    pins.groupBy(_.segments).forall { case (_, group) => group.map(_.clause).distinct.size == 1 }

  /** De-pinned copy of `sql` for the DuckDB compile bridge. */
  def stripSnapshotPins(sql: String): String = split(sql).sql

  /** Resolve the pins in `sql` against a view's tracked source tables.
    *
    * A pin written as a bare short name matches the qualified source with the
    * same trailing segment (the `db.table` vs `table` sharp edge documented in
    * `MaterializedViewCommands.postRefreshCleanup`); a pin written with a
    * qualifier must match those trailing segments exactly.
    *
    * @return qualified source table name → temporal clause text
    */
  def pinsByQualifiedSource(sql: String, qualifiedSources: Seq[String]): Map[String, String] = {
    val pins = split(sql).pins
    if (pins.isEmpty) return Map.empty
    qualifiedSources.flatMap { qualified =>
      val qualSegments = identifierSegments(qualified)
      pins
        .find(pin => qualSegments.endsWith(pin.segments))
        .map(pin => qualified -> pin.clause)
    }.toMap
  }

  /** Same as [[pinsByQualifiedSource]] but keyed by the short (last-segment)
    * table name — the key space `SparkRefreshRewriter` uses when it expands
    * openivm's `memory.main.<source>` references.
    */
  def pinsByShortSource(sql: String, qualifiedSources: Seq[String]): Map[String, String] =
    pinsByQualifiedSource(sql, qualifiedSources).map { case (qualified, clause) =>
      identifierSegments(qualified).last -> clause
    }

  /** Unquoted, lower-cased dot-separated segments of a (possibly backtick- or
    * double-quote-quoted) table reference.
    */
  private[compiler] def identifierSegments(tableRef: String): Seq[String] = {
    val segments = scala.collection.mutable.ArrayBuffer.empty[String]
    val current  = new StringBuilder
    var i        = 0
    var quote    = '\u0000'
    while (i < tableRef.length) {
      val c = tableRef.charAt(i)
      if (quote != '\u0000') {
        if (c == quote) {
          if (i + 1 < tableRef.length && tableRef.charAt(i + 1) == quote) { current += c; i += 2 }
          else { quote = '\u0000'; i += 1 }
        } else { current += c; i += 1 }
      } else if (c == '`' || c == '"') { quote = c; i += 1 }
      else if (c == '.') { segments += current.toString; current.setLength(0); i += 1 }
      else { current += c; i += 1 }
    }
    segments += current.toString
    segments.map(_.trim.toLowerCase(Locale.ROOT)).toVector
  }

  // ── Scanner ────────────────────────────────────────────────────────────────

  /** Locate and elide every temporal clause outside string literals, quoted
    * identifiers and comments. Pure text surgery — validated by [[split]].
    */
  private def scan(sql: String): Split = {
    val out  = new StringBuilder(sql.length)
    val pins = scala.collection.mutable.ArrayBuffer.empty[SnapshotPin]
    var i    = 0
    while (i < sql.length) {
      val protectedEnd = consumeProtectedRegion(sql, i)
      if (protectedEnd > i) {
        out.append(sql.substring(i, protectedEnd))
        i = protectedEnd
      } else {
        parseTemporalClause(sql, i) match {
          case Some((clauseEnd, clause)) =>
            // `out` holds everything already emitted, so the relation reference
            // this clause pins is the trailing identifier chain of `out`.
            trailingTableRef(out) match {
              case Some(tableRef) =>
                pins += SnapshotPin(tableRef, clause)
                elideClause(out, sql, clauseEnd)
                i = clauseEnd
              case None =>
                out.append(sql.charAt(i))
                i += 1
            }
          case None =>
            out.append(sql.charAt(i))
            i += 1
        }
      }
    }
    if (pins.isEmpty) Split(sql, Nil) else Split(out.toString, pins.toVector)
  }

  /** Drop the elided clause together with the inline whitespace that preceded
    * it, re-inserting a single separator only when the next token needs one.
    * A trailing newline is never removed: it may terminate a line comment.
    */
  private def elideClause(out: StringBuilder, sql: String, clauseEnd: Int): Unit = {
    var trimmed = out.length
    while (trimmed > 0 && isInlineWhitespace(out.charAt(trimmed - 1))) trimmed -= 1
    out.setLength(trimmed)
    val endsWithNewline = trimmed > 0 && (out.charAt(trimmed - 1) == '\n' || out.charAt(trimmed - 1) == '\r')
    val next            = if (clauseEnd < sql.length) sql.charAt(clauseEnd) else ' '
    if (!endsWithNewline && !next.isWhitespace && next != ')' && next != ',' && next != ';') out.append(' ')
  }

  private def isInlineWhitespace(c: Char): Boolean = c.isWhitespace && c != '\n' && c != '\r'

  /** End index (exclusive) of a string literal / quoted identifier / comment
    * starting at `i`, or `i` itself when `i` does not start one.
    */
  private def consumeProtectedRegion(sql: String, i: Int): Int = sql.charAt(i) match {
    case '\'' => SparkFunctionShimSql.consumeSparkSingleQuoted(sql, i)
    case '"'  => consumeSparkDoubleQuoted(sql, i)
    case '`'  => consumeBacktickQuoted(sql, i)
    case '-' if i + 1 < sql.length && sql.charAt(i + 1) == '-' =>
      var j = i + 2
      while (j < sql.length && sql.charAt(j) != '\n' && sql.charAt(j) != '\r') j += 1
      j
    case '/' if i + 1 < sql.length && sql.charAt(i + 1) == '*' =>
      var j = i + 2
      while (j + 1 < sql.length && !(sql.charAt(j) == '*' && sql.charAt(j + 1) == '/')) j += 1
      if (j + 1 < sql.length) j + 2 else sql.length
    case _ => i
  }

  private def consumeSparkDoubleQuoted(sql: String, start: Int): Int = {
    var i = start + 1
    while (i < sql.length) {
      sql.charAt(i) match {
        case '\\'                                                  => i += 2
        case '"' if i + 1 < sql.length && sql.charAt(i + 1) == '"' => i += 2
        case '"'                                                   => return i + 1
        case _                                                     => i += 1
      }
    }
    sql.length
  }

  private def consumeBacktickQuoted(sql: String, start: Int): Int = {
    var i = start + 1
    while (i < sql.length) {
      if (sql.charAt(i) == '`') {
        if (i + 1 < sql.length && sql.charAt(i + 1) == '`') i += 2 else return i + 1
      } else i += 1
    }
    sql.length
  }

  /** Match Spark's `temporalClause` at `start`.
    *
    * {{{
    * temporalClause
    *   : FOR? (SYSTEM_VERSION | VERSION)  AS OF version=(INTEGER_VALUE | STRING)
    *   | FOR? (SYSTEM_TIME | TIMESTAMP)   AS OF timestamp=valueExpression
    * }}}
    *
    * @return `(endExclusive, clauseText)` of the whole clause including a
    *         leading `FOR`, or `None`.
    */
  private def parseTemporalClause(sql: String, start: Int): Option[(Int, String)] = {
    if (!isIdentifierStart(sql.charAt(start)) || !hasLeftIdentifierBoundary(sql, start)) return None

    var pos = start
    if (isKeywordAt(sql, pos, "FOR")) pos = skipTrivia(sql, pos + "FOR".length)

    val kindKeyword =
      (VersionKeywords ++ TimestampKeywords).find(isKeywordAt(sql, pos, _)).getOrElse(return None)
    // `SYSTEM_VERSION`/`SYSTEM_TIME` must win over their `VERSION`/`TIME`
    // suffixes; `isKeywordAt` already enforces identifier boundaries, so the
    // first hit in the ordered list is the full keyword.
    pos = skipTrivia(sql, pos + kindKeyword.length)
    if (!isKeywordAt(sql, pos, "AS")) return None
    pos = skipTrivia(sql, pos + "AS".length)
    if (!isKeywordAt(sql, pos, "OF")) return None
    pos = skipTrivia(sql, pos + "OF".length)

    val valueEnd =
      if (VersionKeywords.contains(kindKeyword)) parseVersionValueEnd(sql, pos)
      else parseTimestampValueEnd(sql, pos)

    valueEnd.map(end => end -> sql.substring(start, end).trim.replaceAll("\\s+", " "))
  }

  /** `INTEGER_VALUE | STRING` — the only two forms Spark's grammar allows after
    * `VERSION AS OF`.
    */
  private def parseVersionValueEnd(sql: String, start: Int): Option[Int] = {
    if (start >= sql.length) return None
    val c = sql.charAt(start)
    if (c == '\'') Some(SparkFunctionShimSql.consumeSparkSingleQuoted(sql, start))
    else if (c.isDigit || ((c == '+' || c == '-') && start + 1 < sql.length && sql.charAt(start + 1).isDigit)) {
      var i = start + 1
      while (i < sql.length && sql.charAt(i).isDigit) i += 1
      Some(i)
    } else None
  }

  /** `valueExpression` after `TIMESTAMP AS OF`, restricted to the shapes a
    * scanner can bound safely: a string literal, a number, or an identifier
    * chain with an optional balanced argument list (`current_timestamp()`,
    * `date_sub(current_date(), 1)`). Anything else yields `None`, which leaves
    * the SQL untouched.
    */
  private def parseTimestampValueEnd(sql: String, start: Int): Option[Int] = {
    if (start >= sql.length) return None
    val c = sql.charAt(start)
    if (c == '\'') return Some(SparkFunctionShimSql.consumeSparkSingleQuoted(sql, start))
    if (c.isDigit) {
      var i = start + 1
      while (i < sql.length && (sql.charAt(i).isDigit || sql.charAt(i) == '.')) i += 1
      return Some(i)
    }
    if (!isIdentifierStart(c)) return None
    var i = start
    while (i < sql.length && (isIdentifierChar(sql.charAt(i)) || sql.charAt(i) == '.')) i += 1
    val word = sql.substring(start, i).toLowerCase(Locale.ROOT)
    if (ValueStopKeywords.contains(word)) return None
    val afterName = skipTrivia(sql, i)
    if (afterName < sql.length && sql.charAt(afterName) == '(') findMatchingCloseParen(sql, afterName).map(_ + 1)
    else Some(i)
  }

  /** Trailing dot-separated identifier chain already emitted into `out` — the
    * relation the temporal clause pins. `None` when the preceding token is not
    * a plausible relation reference (e.g. a comma, an operator, or a SQL
    * keyword), which makes the caller leave the text untouched.
    */
  private def trailingTableRef(out: StringBuilder): Option[String] = {
    var end = out.length
    while (end > 0 && out.charAt(end - 1).isWhitespace) end -= 1
    if (end == 0) return None
    var start = end
    var ok    = true
    while (ok && start > 0) {
      val c = out.charAt(start - 1)
      if (c == '`') {
        var open = start - 2
        while (open >= 0 && out.charAt(open) != '`') open -= 1
        if (open < 0) { ok = false }
        else start = open
      } else if (isIdentifierChar(c) || c == '.') start -= 1
      else ok = false
    }
    if (start >= end) return None
    val ref = out.substring(start, end)
    if (ref.startsWith(".") || ref.endsWith(".")) return None
    val segments = identifierSegments(ref)
    if (segments.exists(_.isEmpty)) return None
    if (ValueStopKeywords.contains(segments.last) || segments.last.forall(_.isDigit)) return None
    Some(ref)
  }

  private def findMatchingCloseParen(sql: String, openParen: Int): Option[Int] = {
    var depth = 0
    var i     = openParen
    while (i < sql.length) {
      val protectedEnd = consumeProtectedRegion(sql, i)
      if (protectedEnd > i) i = protectedEnd
      else {
        sql.charAt(i) match {
          case '(' => depth += 1; i += 1
          case ')' =>
            depth -= 1
            if (depth == 0) return Some(i)
            i += 1
          case _ => i += 1
        }
      }
    }
    None
  }

  private def skipTrivia(sql: String, start: Int): Int = {
    var i    = start
    var more = true
    while (more && i < sql.length) {
      if (sql.charAt(i).isWhitespace) i += 1
      else {
        val end = i match {
          case _ if sql.charAt(i) == '-' && i + 1 < sql.length && sql.charAt(i + 1) == '-' =>
            consumeProtectedRegion(sql, i)
          case _ if sql.charAt(i) == '/' && i + 1 < sql.length && sql.charAt(i + 1) == '*' =>
            consumeProtectedRegion(sql, i)
          case _ => i
        }
        if (end > i) i = end else more = false
      }
    }
    i
  }

  private def isKeywordAt(sql: String, start: Int, keyword: String): Boolean =
    start >= 0 && start + keyword.length <= sql.length &&
      sql.regionMatches(true, start, keyword, 0, keyword.length) &&
      hasLeftIdentifierBoundary(sql, start) &&
      hasRightIdentifierBoundary(sql, start + keyword.length)

  private def isIdentifierStart(c: Char): Boolean = c.isLetter || c == '_'

  private def isIdentifierChar(c: Char): Boolean = c.isLetterOrDigit || c == '_'

  private def hasLeftIdentifierBoundary(sql: String, idx: Int): Boolean =
    idx <= 0 || (!isIdentifierChar(sql.charAt(idx - 1)) && sql.charAt(idx - 1) != '.')

  private def hasRightIdentifierBoundary(sql: String, idx: Int): Boolean =
    idx >= sql.length || !isIdentifierChar(sql.charAt(idx))

  // ── Spark-parser cross-check ───────────────────────────────────────────────

  /** The de-pinned SQL must parse, carry no residual time-travel node, and
    * reference exactly the same relations as the original. A source that is
    * pinned somewhere and read live elsewhere is rejected: the Spark side
    * re-pins per source, which would freeze the live read too.
    */
  private def verifiesAgainstSparkParser(original: String, depinned: String, pins: Seq[SnapshotPin]): Boolean =
    (parsePlan(original), parsePlan(depinned)) match {
      case (Some(before), Some(after)) =>
        timeTravelCount(before) > 0 &&
        timeTravelCount(after) == 0 &&
        relationIdentifiers(before) == relationIdentifiers(after) &&
        !readsPinnedSourceLive(before, pins)
      case _ => false
    }

  /** True when a relation reference outside any `RelationTimeTravel` resolves
    * to one of the pinned sources. CTE names are compared too, so a CTE that
    * shadows a pinned source name conservatively refuses the split.
    */
  private def readsPinnedSourceLive(plan: LogicalPlan, pins: Seq[SnapshotPin]): Boolean = {
    val pinnedSegments  = pins.map(_.segments).distinct
    val nodes           = collectPlans(plan)
    val pinnedRelations = nodes.collect { case tt: RelationTimeTravel => tt.relation }
    nodes
      .collect { case r: UnresolvedRelation if !pinnedRelations.exists(_ eq r) => r }
      .exists { relation =>
        val id = relation.multipartIdentifier.map(_.toLowerCase(Locale.ROOT))
        pinnedSegments.exists(segments => id.endsWith(segments) || segments.endsWith(id))
      }
  }

  private def parsePlan(sql: String): Option[LogicalPlan] =
    try Some(CatalystSqlParser.parsePlan(sql))
    catch { case _: Throwable => None }

  private def timeTravelCount(plan: LogicalPlan): Int =
    collectPlans(plan).count(_.isInstanceOf[RelationTimeTravel])

  private def relationIdentifiers(plan: LogicalPlan): Seq[Seq[String]] =
    collectPlans(plan)
      .collect { case r: UnresolvedRelation => r.multipartIdentifier.map(_.toLowerCase(Locale.ROOT)) }
      .sortBy(_.mkString("."))

  /** Every plan node in `plan`, including nodes Spark's own `collect` does not
    * reach: the relation wrapped by a `RelationTimeTravel` (an
    * `UnresolvedLeafNode`, so its relation is not a plan child) and
    * `innerChildren` such as the CTE definitions of an unresolved `WITH` and the
    * plans of subquery expressions.
    */
  private def collectPlans(plan: LogicalPlan): Seq[LogicalPlan] = {
    val out = scala.collection.mutable.ArrayBuffer.empty[LogicalPlan]
    def visit(p: LogicalPlan): Unit = {
      out += p
      val pinned = p match {
        case tt: RelationTimeTravel => Seq(tt.relation)
        case _                      => Seq.empty[LogicalPlan]
      }
      val inner = p.innerChildren.collect { case lp: LogicalPlan => lp }
      (p.children ++ inner ++ pinned).foreach(visit)
    }
    visit(plan)
    out.toVector
  }
}
