package org.openivm.spark.compiler

/** Post-processor for the lpts/openivm-emitted refresh SQL. Applies
  * dialect-leak fixes for tokens openivm emits outside the lpts pipeline
  * (e.g. `generate_series`, `::TIMESTAMP` casts, interval literals).
  *
  * Pure function — no Spark or DuckDB dependencies; operates on strings.
  * Idempotent — applying twice yields the same output.
  */
object LptsSparkDialect {

  // ── Compiled regex patterns (object-level to avoid recompilation per call) ──

  /** `now()::timestamp` — must be rewritten before the generic `::TYPE` pass
    * so that `now()` is never wrapped in a redundant `CAST(... AS TIMESTAMP)`.
    */
  private val NowTsRe = """(?i)now\s*\(\s*\)\s*::\s*timestamp""".r

  /** `"identifier"` — DuckDB double-quoted identifiers. Rewritten to Spark
    * backtick-quoted identifiers (`` `identifier` ``).
    * Only matches if the content looks like a SQL identifier (letters, digits,
    * underscores). Skipped inside single-quoted string literals.
    */
  private val DblQuoteIdentRe = """"([a-zA-Z_][a-zA-Z0-9_]*)"""".r

  /** `generate_series(` — word-boundary anchored so partial matches
    * (e.g. `my_generate_series(`) are not rewritten.
    */
  private val GenSeriesRe = """(?i)\bgenerate_series\s*\(""".r

  /** `<expr>::TYPE[(...)]` on the placeholder-substituted string.
    *
    * Expression alternatives (group 1):
    *   - qualified identifier: `foo`, `foo.bar`, `foo.bar.baz`
    *   - number: `123`, `3.14`
    *   - string-literal placeholder: `__STRLIT_N__`
    *
    * Type (group 2): one or more uppercase letters optionally followed by
    * `(digits[, digits])`, e.g. `TIMESTAMP`, `DECIMAL(10,2)`.
    */
  private val CastRe =
    """((?:[a-zA-Z_][a-zA-Z0-9_.]*|[0-9]+(?:\.[0-9]+)?|__STRLIT_[0-9]+__))::([A-Z]+(?:\([0-9]+(?:,\s*[0-9]+)?\))?)""".r

  /** Matches a closing paren immediately followed by `::TYPE`.
    *
    * Used by [[rewriteParenthesisedCasts]] to find function-call postfix-cast
    * patterns such as `COALESCE(a, b)::DOUBLE` that [[CastRe]] cannot match
    * because the expression before `::` is not a simple identifier or number.
    */
  private val CloseParenCastRe =
    """\)::([A-Z]+(?:\([0-9]+(?:,\s*[0-9]+)?\))?)""".r

  /** `INTERVAL 'N unit'` — strips the surrounding quotes and uppercases the
    * unit keyword to match Spark's `INTERVAL N UNIT` syntax.
    */
  private val IntervalRe = """(?i)INTERVAL\s+'(\d+)\s+(\w+)'""".r

  /** `count_star()` — DuckDB's count-star spelling. Rewritten to Spark's
    * standard `COUNT(*)` form.
    */
  private val CountStarRe = """(?i)\bcount_star\s*\(\s*\)""".r

  /** `error(<args>)` — DuckDB function used by lpts when emitting assertion-style
    * checks (e.g. divide-by-zero guards in scalar-subquery rewrites).  Spark has
    * no `error` builtin; the equivalent is `raise_error(<args>)`.
    *
    * We anchor on a word-boundary + opening-paren so partial matches
    * (e.g. `division_error(`) are not rewritten.
    */
  private val ErrorFnRe = """(?i)\berror\s*\(""".r

  // ── Public API ───────────────────────────────────────────────────────────────

  /** Run all post-processing passes in order.
    *
    * Pass order matters:
    *   - [[rewriteNowTimestamp]] must precede [[rewritePostfixCasts]] so that
    *     `now()::timestamp` becomes `current_timestamp()` rather than
    *     `CAST(now() AS timestamp)`.
    *   - [[rewriteDoubleQuotedIdentifiers]] runs after all other passes so it
    *     normalises DuckDB double-quoted column names produced by openivm
    *     (e.g. `AS "name"`) to Spark backtick-quoted identifiers.
    */
  def translate(sql: String): String =
    rewriteDoubleQuotedIdentifiers(
      rewriteErrorFn(
        rewriteCountStar(
          rewriteIntervalLiterals(
            rewriteGenerateSeries(
              rewriteBareVarcharCast(
                rewritePostfixCasts(
                  rewriteNowTimestamp(sql)
                )
              )
            )
          )
        )
      )
    )

  // ── Individual passes ────────────────────────────────────────────────────────

  /** Rewrites `now()::timestamp` (any case) to `current_timestamp()`.
    * Must run before [[rewritePostfixCasts]].
    */
  private[compiler] def rewriteNowTimestamp(sql: String): String =
    NowTsRe.replaceAllIn(sql, "current_timestamp()")

  /** Rewrites `generate_series(a, b[, c])` to `sequence(a, b[, c])`. */
  private[compiler] def rewriteGenerateSeries(sql: String): String =
    GenSeriesRe.replaceAllIn(sql, "sequence(")

  /** Rewrites DuckDB postfix casts (`<expr>::TYPE`) to Spark
    * `CAST(<expr> AS TYPE)`, skipping occurrences that are inside
    * single-quoted SQL string literals.
    *
    * Implementation: replace every string literal with a numbered
    * placeholder `__STRLIT_N__`, apply the regex, then restore the literals.
    */
  private[compiler] def rewritePostfixCasts(sql: String): String = {
    // ── Step 1: extract string literals → placeholders ──────────────────────
    val literals = scala.collection.mutable.ArrayBuffer[String]()
    val withPlaceholders: String = {
      val sb = new StringBuilder
      var i  = 0
      while (i < sql.length) {
        if (sql(i) == '\'') {
          val start = i
          i += 1
          // Scan to the end of the SQL string literal.
          // In SQL, '' is an escaped single quote inside a string.
          var closed = false
          while (i < sql.length && !closed) {
            if (sql(i) == '\'') {
              i += 1
              if (i >= sql.length || sql(i) != '\'') {
                closed = true // closing quote; i is now one past it
              } else {
                i += 1 // '' escape — skip both chars
              }
            } else {
              i += 1
            }
          }
          val literal = sql.substring(start, i)
          val ph      = s"__STRLIT_${literals.size}__"
          literals += literal
          sb ++= ph
        } else {
          sb += sql(i)
          i += 1
        }
      }
      sb.toString
    }

    // ── Step 2: rewrite ::TYPE on the placeholder-substituted SQL ───────────
    // Normalise DuckDB type names that Spark cannot parse bare:
    //   VARCHAR (without length) → STRING
    //   CHAR / TEXT              → STRING
    val rewritten = CastRe.replaceAllIn(
      withPlaceholders,
      m => {
        val sparkType = m.group(2).toUpperCase match {
          case "VARCHAR" | "CHAR" | "TEXT" => "STRING"
          case t                           => t
        }
        s"CAST(${m.group(1)} AS $sparkType)"
      }
    )

    // ── Step 2b: rewrite func(...)::TYPE (parenthesised postfix casts) ───────
    // OpenIVM AGGREGATE_GROUP MERGE emits patterns like:
    //   COALESCE(v.sum + d.sum, v.sum, d.sum)::DOUBLE
    // which CastRe above cannot match because the expression before '::' ends
    // with ')' rather than a simple identifier or number literal.
    val rewrittenWithParens = rewriteParenthesisedCasts(rewritten)

    // ── Step 3: restore string literal placeholders ──────────────────────────
    literals.zipWithIndex.foldLeft(rewrittenWithParens) { case (s, (literal, idx)) =>
      s.replace(s"__STRLIT_${idx}__", literal)
    }
  }

  /** Iteratively rewrites `func(args)::TYPE` patterns to `CAST(func(args) AS TYPE)`.
    *
    * Called by [[rewritePostfixCasts]] after [[CastRe]] to handle postfix casts
    * on function-call expressions (e.g. `COALESCE(a, b)::DOUBLE`).  Runs on
    * the placeholder-substituted string so string literal contents are opaque.
    *
    * The outer loop repeats until no more `)::TYPE` patterns remain so that
    * nested casts like `CAST(func(...)::DOUBLE AS DOUBLE)` (after an earlier
    * pass rewrote an inner fragment) are also handled.
    */
  private[compiler] def rewriteParenthesisedCasts(sql: String): String = {
    var s       = sql
    var changed = true
    while (changed) {
      changed = false
      CloseParenCastRe.findFirstMatchIn(s) match {
        case None => ()
        case Some(m) =>
          val closeParenIdx = m.start // index of ')'
          val castType      = m.group(1)
          // Scan backward to find the matching open paren.
          var depth        = 1
          var j            = closeParenIdx - 1
          var openParenIdx = -1
          while (j >= 0 && openParenIdx < 0) {
            s(j) match {
              case ')' => depth += 1
              case '(' =>
                depth -= 1
                if (depth == 0) openParenIdx = j
              case _ => ()
            }
            if (openParenIdx < 0) j -= 1
          }
          if (openParenIdx >= 0) {
            // Extend backward over any preceding identifier characters (function name / qualifier).
            var exprStart = openParenIdx
            while (
              exprStart > 0 && {
                val c = s(exprStart - 1)
                c.isLetterOrDigit || c == '_' || c == '.'
              }
            ) exprStart -= 1
            val sparkType = castType.toUpperCase match {
              case "VARCHAR" | "CHAR" | "TEXT" => "STRING"
              case t                           => t
            }
            val prefix = s.substring(0, exprStart)
            val expr   = s.substring(exprStart, closeParenIdx + 1) // up to and including ')'
            val after  = s.substring(m.end)                        // after ::TYPE
            s = s"${prefix}CAST($expr AS $sparkType)$after"
            changed = true
          }
      }
    }
    s
  }

  /** Rewrites `INTERVAL 'N unit'` to `INTERVAL N UNIT`, stripping the
    * surrounding quotes and uppercasing the unit keyword to match Spark's
    * `INTERVAL <N> <UNIT>` syntax.
    */
  private[compiler] def rewriteIntervalLiterals(sql: String): String =
    IntervalRe.replaceAllIn(sql, m => s"INTERVAL ${m.group(1)} ${m.group(2).toUpperCase}")

  /** Rewrites DuckDB's `count_star()` to standard SQL `COUNT(*)`. */
  private[compiler] def rewriteCountStar(sql: String): String =
    CountStarRe.replaceAllIn(sql, "COUNT(*)")

  /** Rewrites bare `error(...)` to Spark's `raise_error(...)`.
    *
    * Skipped inside SQL string literals so that a user-supplied `'error('` in a
    * text column is preserved verbatim. The transform uses the same
    * placeholder-based literal-protection scheme as [[rewritePostfixCasts]].
    */
  private[compiler] def rewriteErrorFn(sql: String): String = {
    val literals = scala.collection.mutable.ArrayBuffer[String]()
    val withPlaceholders: String = {
      val sb = new StringBuilder
      var i  = 0
      while (i < sql.length) {
        if (sql(i) == '\'') {
          val start = i
          i += 1
          var closed = false
          while (i < sql.length && !closed) {
            if (sql(i) == '\'') {
              i += 1
              if (i >= sql.length || sql(i) != '\'') closed = true else i += 1
            } else i += 1
          }
          val literal = sql.substring(start, i)
          val idx     = literals.size
          literals += literal
          sb.append(s"__STRLIT_${idx}__")
        } else {
          sb.append(sql(i))
          i += 1
        }
      }
      sb.toString
    }
    val rewritten = ErrorFnRe.replaceAllIn(withPlaceholders, "raise_error(")
    val restoreRe = """__STRLIT_(\d+)__""".r
    restoreRe.replaceAllIn(rewritten, m => java.util.regex.Matcher.quoteReplacement(literals(m.group(1).toInt)))
  }

  /** Rewrites bare `CAST(<expr> AS VARCHAR)` (no length) to `CAST(<expr> AS STRING)`.
    *
    * DuckDB and openivm/lpts emit `VARCHAR` without a length in some contexts
    * (e.g. `COALESCE(col, CAST('<orphan>' AS VARCHAR))` for FULL OUTER JOIN's
    * NULL-placeholder values).  Spark 3.5 requires every `VARCHAR` to carry an
    * explicit length parameter — `VARCHAR` alone raises `DATATYPE_MISSING_SIZE`.
    * Spark's `STRING` type is the closest semantic match (variable-length text
    * with no encoding limit), so we map bare `VARCHAR` / `CHAR` / `TEXT` to it.
    *
    * Preserves `CAST(x AS VARCHAR(n))` and `CAST(x AS CHAR(n))` (which Spark
    * supports) by matching only when the type identifier is NOT followed by an
    * open-paren length spec.
    */
  private[compiler] def rewriteBareVarcharCast(sql: String): String = {
    val re = """(?i)\bAS\s+(VARCHAR|CHAR|TEXT)\b(?!\s*\()""".r
    re.replaceAllIn(sql, _ => "AS STRING")
  }

  /** Rewrites DuckDB double-quoted identifiers (e.g. `"name"`) to Spark
    * backtick-quoted identifiers (e.g. `` `name` ``).  Only matches tokens
    * that contain valid SQL identifier characters; skips occurrences that
    * appear inside single-quoted SQL string literals.
    *
    * This handles openivm/lpts output where DuckDB reserved words used as
    * column names (like `name`, `user`, `value`) are double-quoted.
    */
  private[compiler] def rewriteDoubleQuotedIdentifiers(sql: String): String = {
    // Step 1: replace single-quoted string literals with numbered placeholders
    val literals = scala.collection.mutable.ArrayBuffer[String]()
    val withPlaceholders: String = {
      val sb = new StringBuilder
      var i  = 0
      while (i < sql.length) {
        if (sql(i) == '\'') {
          val start = i
          i += 1
          var closed = false
          while (i < sql.length && !closed) {
            if (sql(i) == '\'') {
              i += 1
              if (i >= sql.length || sql(i) != '\'') closed = true
              else i += 1
            } else i += 1
          }
          val ph = s"__STRLIT_${literals.size}__"
          literals += sql.substring(start, i)
          sb ++= ph
        } else {
          sb += sql(i)
          i += 1
        }
      }
      sb.toString
    }

    // Step 2: rewrite `"identifier"` → `` `identifier` `` on placeholder string
    val rewritten = DblQuoteIdentRe.replaceAllIn(
      withPlaceholders,
      m => java.util.regex.Matcher.quoteReplacement(s"`${m.group(1)}`")
    )

    // Step 3: restore placeholders
    literals.zipWithIndex.foldLeft(rewritten) { case (s, (literal, idx)) =>
      s.replace(s"__STRLIT_${idx}__", literal)
    }
  }
}
