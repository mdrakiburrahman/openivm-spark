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

  /** `to_timestamp(CAST('<literal>' AS DOUBLE))` — openivm's DuckDB binder
    * resolves the user's `to_timestamp('<date-string>')` to the only matching
    * DuckDB signature `to_timestamp(DOUBLE)` (UNIX epoch seconds), inserting
    * a `CAST(<string-literal> AS DOUBLE)` around the original arg. In Spark,
    * `CAST('9999-12-31 23:59:59.999' AS DOUBLE)` evaluates to NULL because
    * the string isn't a valid number, and `to_timestamp(NULL)` returns NULL.
    *
    * Restore Spark semantics by stripping the spurious `CAST(... AS DOUBLE)`
    * wrapper when its argument is a single-quoted string literal.
    *
    * Matches:
    *   - `to_timestamp(CAST('...' AS DOUBLE))`              → `to_timestamp('...')`
    *   - `to_timestamp(CAST(__STRLIT_N__ AS DOUBLE))`       (placeholder form)
    *
    * Numeric / column arguments (real UNIX epochs) are intentionally NOT
    * matched so genuine `to_timestamp(<double>)` calls remain untouched.
    */
  private val ToTimestampDoubleCastRe =
    """(?is)\bto_timestamp\s*\(\s*CAST\s*\(\s*('(?:''|[^'])*')\s*AS\s*DOUBLE\s*\)\s*\)""".r

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
            rewriteToTemporalUnit(
              rewriteGenerateSeries(
                rewriteBareHugeIntCast(
                  rewriteBareVarcharCast(
                    rewritePostfixCasts(
                      rewriteStructExtract(
                        rewriteTimestampWithTimeZone(
                          rewriteSparkFunctionInlinings(
                            rewriteToTimestampDoubleCast(
                              rewriteNowTimestamp(sql)
                            )
                          )
                        )
                      )
                    )
                  )
                )
              )
            )
          )
        )
      )
    )

  /** Removes the spurious `CAST(<string-literal> AS DOUBLE)` wrapper that
    * openivm inserts around `to_timestamp('<date-string>')` arguments due to
    * a DuckDB function-binder collision. See [[ToTimestampDoubleCastRe]]
    * for details. Idempotent; numeric/column args are untouched.
    */
  private[compiler] def rewriteToTimestampDoubleCast(sql: String): String =
    ToTimestampDoubleCastRe.replaceAllIn(
      sql,
      m => java.util.regex.Matcher.quoteReplacement(s"to_timestamp(${m.group(1)})")
    )

  /** Reverse the inlining of Spark-function shim macros registered by
    * [[org.openivm.spark.compiler.OpenIvmCompiler.sparkFunctionShimsPrologue]].
    *
    * openivm serializes the macro's expansion — NOT the original Spark
    * function name — into the emitted refresh SQL. Spark therefore sees the
    * inlined DuckDB-side body unless we translate it back here.
    *
    * Current back-translations:
    *   - `regexp_matches(s, p)`               -> `regexp_like(s, p)`
    *   - `CAST(strptime(s, fmt) AS DATE)`     -> `to_date(s, fmt)`
    *   - `strptime(s, fmt)`                   -> `to_timestamp(s, fmt)`
    *   - `strftime(d, fmt)`                   -> `date_format(d, fmt)`
    *
    * The 2-arg date/time rewrites use the shared quote/comment-aware scanner in
    * [[SparkFunctionShimSql]] so only full call-shapes are rewritten, never
    * text inside string literals or comments.
    */
  private[compiler] def rewriteSparkFunctionInlinings(sql: String): String = {
    // regexp_matches(s, p) → regexp_like(s, p)
    // Spark has regexp_like; not regexp_matches. The shim macro body
    // (`regexp_matches(s, p)`) gets inlined by openivm's LPTS serializer
    // — undo that here so Spark's analyzer binds against the built-in.
    val regexpMatches = """(?i)\bregexp_matches\s*\(""".r
    SparkFunctionShimSql.rewriteInlinedTwoArgDateFns(
      regexpMatches.replaceAllIn(sql, "regexp_like(")
    )
  }

  private def normalizeSparkTypeName(rawType: String): String =
    rawType.toUpperCase match {
      case "VARCHAR" | "CHAR" | "TEXT" => "STRING"
      case "HUGEINT" | "UHUGEINT"      => "BIGINT"
      case t                           => t
    }

  /** Rewrites DuckDB's `to_<unit>(N)` interval constructors to Spark's
    * `INTERVAL N <UNIT>` syntax. openivm emits these forms when a user view
    * body uses `INTERVAL N <unit>` and the LPTS pipeline serializes the
    * bound AST in DuckDB's preferred form (e.g. `INTERVAL 1 MILLISECOND`
    * round-trips as `to_milliseconds(CAST(1 AS DOUBLE))`).
    *
    * Supported units (matching DuckDB's interval-helper functions):
    *   - to_milliseconds(N)  → INTERVAL N MILLISECOND
    *   - to_seconds(N)       → INTERVAL N SECOND
    *   - to_minutes(N)       → INTERVAL N MINUTE
    *   - to_hours(N)         → INTERVAL N HOUR
    *   - to_days(N)          → INTERVAL N DAY
    *   - to_months(N)        → INTERVAL N MONTH
    *   - to_years(N)         → INTERVAL N YEAR
    *
    * The argument can be any expression with balanced parentheses (e.g.
    * `1`, `CAST(1 AS DOUBLE)`, `<column>`). Spark's `INTERVAL <expr> <UNIT>`
    * accepts any numeric expression as long as the value can be evaluated to
    * a numeric constant — for column refs, the user should use multiplication
    * `<col> * INTERVAL 1 MILLISECOND` instead, but the LPTS-emitted form
    * never uses column args here, just CAST'd literals.
    */
  private[compiler] def rewriteToTemporalUnit(sql: String): String = {
    val unitMap = Seq(
      "milliseconds" -> "MILLISECOND",
      "seconds"      -> "SECOND",
      "minutes"      -> "MINUTE",
      "hours"        -> "HOUR",
      "days"         -> "DAY",
      "months"       -> "MONTH",
      "years"        -> "YEAR"
    )
    // Helper: extract the balanced-paren argument starting at `start` (which
    // should be the opening `(` index). Returns the argument substring and
    // the index just past the closing `)`, or (None, -1) if unbalanced.
    def extractBalancedArg(s: String, openIdx: Int): (Option[String], Int) = {
      if (openIdx >= s.length || s.charAt(openIdx) != '(') return (None, -1)
      var depth    = 1
      var i        = openIdx + 1
      val argStart = i
      while (i < s.length && depth > 0) {
        s.charAt(i) match {
          case '(' => depth += 1
          case ')' => depth -= 1
          case _   => ()
        }
        if (depth > 0) i += 1
      }
      if (depth != 0) (None, -1)
      else (Some(s.substring(argStart, i)), i + 1)
    }
    var s = sql
    for ((fn, unit) <- unitMap) {
      val callRe  = ("(?i)\\bto_" + fn + "\\s*(?=\\()").r
      var changed = true
      while (changed) {
        changed = false
        callRe.findFirstMatchIn(s) match {
          case None =>
            ()
          case Some(m) =>
            val (argOpt, endIdx) = extractBalancedArg(s, m.end)
            argOpt match {
              case Some(arg) =>
                val before = s.substring(0, m.start)
                val after  = s.substring(endIdx)
                // Use Spark's `<expr> * INTERVAL 1 <UNIT>` form because Spark
                // 3.5's `INTERVAL <expr> <UNIT>` requires the expression to be
                // a literal — openivm/lpts emits `to_milliseconds(CAST(1 AS
                // DOUBLE))` for `INTERVAL 1 MILLISECOND`, so the arg is rarely
                // a bare literal.
                val argTrim = arg.trim
                s = s"${before}(($argTrim) * INTERVAL 1 $unit)$after"
                changed = true
              case None => ()
            }
        }
      }
    }
    s
  }

  // ── Individual passes ────────────────────────────────────────────────────────

  /** Rewrites DuckDB's `TIMESTAMP WITH TIME ZONE` (and `WITHOUT`) to Spark's
    * `TIMESTAMP`. Spark 3.5 does not accept the SQL-standard `WITH TIME ZONE`
    * suffix in CAST or column-type contexts.
    *
    * openivm emits `CAST(<expr> AS TIMESTAMP WITH TIME ZONE)` when the source
    * column is a tz-aware timestamp (TPC-DI `_ActionTS` for example).
    *
    * Both variants collapse to the same Spark type because Spark's
    * `TIMESTAMP` is timezone-aware (TIMESTAMP_LTZ); `TIMESTAMP_NTZ` is
    * available since Spark 3.4 but openivm-emitted SQL never targets it.
    */
  private[compiler] def rewriteTimestampWithTimeZone(sql: String): String = {
    val withTz    = """(?i)\bTIMESTAMP\s+WITH\s+TIME\s+ZONE\b""".r
    val withoutTz = """(?i)\bTIMESTAMP\s+WITHOUT\s+TIME\s+ZONE\b""".r
    withTz.replaceAllIn(
      withoutTz.replaceAllIn(sql, java.util.regex.Matcher.quoteReplacement("TIMESTAMP")),
      java.util.regex.Matcher.quoteReplacement("TIMESTAMP")
    )
  }

  /** Rewrites DuckDB's `struct_extract(<struct_expr>, '<field>')` →
    * Spark's dot-notation field access `<struct_expr>.<field>`.
    *
    * openivm emits `struct_extract` when the source-table schema contains
    * STRUCT columns and the user query accesses a nested field. Spark's
    * SQL parser does not recognise `struct_extract`, so the openivm-emitted
    * initial-load and refresh SQL fails with PARSE_SYNTAX_ERROR otherwise.
    *
    * The rewrite handles arbitrarily-nested calls (e.g.
    * `struct_extract(struct_extract(s, 'a'), 'b')` →  `s.a.b`) by iterating
    * the innermost-first regex until no more matches remain.
    *
    * Field names go through Spark backtick quoting if they contain anything
    * other than a leading letter/underscore + alphanumerics/underscores,
    * so DuckDB names like `'_c_id'` (leading underscore — legal in Spark
    * but Spark's parser sometimes balks at sequences like `x._c_id`
    * depending on tokenisation; backticks are always safe).
    */
  private[compiler] def rewriteStructExtract(sql: String): String = {
    // Match struct_extract(<expr>, '<field>') where <expr> contains NO
    // unmatched parens (i.e. is the innermost call). The negated character
    // class for the first argument allows literal-paren-free expressions:
    // identifiers, dotted names, backtick-wrapped names, and dotted chains
    // of struct_extract output (after a prior pass).
    //
    // We do not match across a struct_extract( ... ( ... ) ... ) — the
    // inner struct_extract is rewritten first, then the outer call is
    // exposed and rewritten on the next pass.
    val re = """(?i)struct_extract\s*\(\s*([^(),]+?)\s*,\s*'([^']*)'\s*\)""".r

    def needsBackticks(name: String): Boolean = {
      // Conservative: any character outside [A-Za-z0-9_] (including leading
      // digit) forces quoting. Even a leading underscore is fine without
      // quotes in Spark, but matching DuckDB's exact case-sensitive name
      // requires backticks if the source schema name was case-mixed.
      name.isEmpty || !name.matches("[A-Za-z_][A-Za-z0-9_]*")
    }

    var s          = sql
    var prev       = ""
    var iterations = 0
    while (s != prev && iterations < 64) {
      prev = s
      s = re.replaceAllIn(
        s,
        m => {
          val expr  = m.group(1).trim
          val field = m.group(2)
          val rendered =
            if (needsBackticks(field)) s".`${field.replace("`", "``")}`"
            else s".$field"
          java.util.regex.Matcher.quoteReplacement(expr + rendered)
        }
      )
      iterations += 1
    }
    s
  }

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
    //   HUGEINT / UHUGEINT       → BIGINT
    val rewritten = CastRe.replaceAllIn(
      withPlaceholders,
      m => s"CAST(${m.group(1)} AS ${normalizeSparkTypeName(m.group(2))})"
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
            val sparkType = normalizeSparkTypeName(castType)
            val prefix    = s.substring(0, exprStart)
            val expr      = s.substring(exprStart, closeParenIdx + 1) // up to and including ')'
            val after     = s.substring(m.end)                        // after ::TYPE
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

  /** Rewrites bare `CAST(<expr> AS HUGEINT|UHUGEINT)` to `CAST(<expr> AS BIGINT)`.
    *
    * DuckDB promotes some integer aggregates (e.g. `SUM(BIGINT)`) to 128-bit
    * `HUGEINT` / `UHUGEINT` in the compiled SQL. Spark 3.5 does not recognize
    * those type names, but the matching Spark aggregate/result type is BIGINT.
    */
  private[compiler] def rewriteBareHugeIntCast(sql: String): String = {
    val re = """(?i)\bAS\s+(UHUGEINT|HUGEINT)\b""".r
    re.replaceAllIn(sql, _ => "AS BIGINT")
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
