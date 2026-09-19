package org.openivm.spark.compiler

import java.util.Locale

/** Quote/comment-aware SQL scanner shared by the compile-bridge pre-pass and
  * the LPTS post-pass for Spark function shims.
  *
  * The scanner treats single-quoted string literals (with `''` escapes),
  * double-quoted identifiers (with `""` escapes), `--` line comments, and
  * slash-star block comments as opaque. Function-call matching and comma
  * counting only happen outside those regions.
  */
private[compiler] object SparkFunctionShimSql {

  private[compiler] val MakeIntervalMarker: String  = "__openivm_spark_make_interval__"
  private[compiler] val GetJsonObjectMarker: String = "__openivm_spark_get_json_object__"
  private[compiler] val MarkerArgSeparator: String  = "|"

  private final case class FunctionCall(
      openParen: Int,
      closeParen: Int,
      topLevelCommaCount: Int
  ) {
    def args(sql: String): String = sql.substring(openParen + 1, closeParen)
  }

  private final case class RenameRule(
      replacementByTopLevelCommaCount: Map[Int, String]
  ) {
    def replacementFor(call: FunctionCall): Option[String] =
      replacementByTopLevelCommaCount.get(call.topLevelCommaCount)
  }

  private val OneArgToDateLiteral      = "'%Y-%m-%d'"
  private val OneArgToTimestampLiteral = "'%Y-%m-%d %H:%M:%S'"

  private val sparkFunctionRenameRules: Map[String, RenameRule] = Map(
    "to_date" -> RenameRule(
      Map(
        0 -> "__sparkfn_to_date_1arg",
        1 -> "__sparkfn_to_date"
      )
    ),
    "to_timestamp" -> RenameRule(
      Map(
        0 -> "__sparkfn_to_timestamp_1arg",
        1 -> "__sparkfn_to_timestamp"
      )
    ),
    "date_format" -> RenameRule(
      Map(
        1 -> "__sparkfn_date_format"
      )
    ),
    "make_interval" -> RenameRule(
      Map(
        6 -> "__sparkfn_make_interval"
      )
    ),
    "get_json_object" -> RenameRule(
      Map(
        1 -> "__sparkfn_get_json_object"
      )
    ),
    "last_value" -> RenameRule(
      Map(
        1 -> "__sparkfn_last_value"
      )
    ),
    "first_value" -> RenameRule(
      Map(
        1 -> "__sparkfn_first_value"
      )
    ),
    "current_date" -> RenameRule(
      Map(
        0 -> "__sparkfn_current_date"
      )
    ),
    "current_timestamp" -> RenameRule(
      Map(
        0 -> "__sparkfn_current_timestamp"
      )
    )
  )

  /** Pre-pass for the compile bridge: rename Spark function spellings that
    * DuckDB would otherwise parse or bind incompatibly to collision-free
    * `__sparkfn_*` spellings before the SQL reaches DuckDB.
    *
    * Only the function NAME is rewritten for generic shim calls; argument text is
    * preserved verbatim. The Spark-only literal-boolean `last_value(expr,
    * ignoreNulls)` / `first_value(expr, ignoreNulls)` spellings are instead
    * translated to DuckDB's native window modifier so ignore-null semantics
    * survive planning. Current coverage:
    *   - 1-arg / 2-arg `to_date(...)`
    *   - 1-arg / 2-arg `to_timestamp(...)`
    *   - 2-arg `date_format(...)`
    *   - 7-arg `make_interval(...)`
    *   - 2-arg `get_json_object(...)`
    *   - 2-arg `last_value(expr, true|false)`
    *   - 2-arg `first_value(expr, true|false)`
    *   - 0-arg `current_date()`
    *   - 0-arg `current_timestamp()`
    */
  def renameSparkFunctionShimCalls(sql: String): String =
    rewriteOutsideProtected(sql) { i =>
      if (!isIdentifierStart(sql.charAt(i)) || !hasLeftIdentifierBoundary(sql, i)) None
      else {
        val identEnd = readIdentifierEnd(sql, i)
        val lower    = sql.substring(i, identEnd).toLowerCase(Locale.ROOT)
        parseSparkWindowLiteralBoolRewrite(sql, i, identEnd, "last_value")
          .orElse(parseSparkWindowLiteralBoolRewrite(sql, i, identEnd, "first_value"))
          .orElse {
            sparkFunctionRenameRules.get(lower).flatMap { rule =>
              parseFunctionCall(sql, identEnd)
                .flatMap(call => rule.replacementFor(call).map(replacement => identEnd -> replacement))
            }
          }
      }
    }

  /** Post-pass for the LPTS serializer: reverse the inlined DuckDB macro bodies
    * back to Spark's original shim spellings.
    *
    * Rewrites:
    *   - `CAST(CASE WHEN s IS NOT NULL THEN NULL WHEN s IS NULL THEN NULL END AS DATE)` -> `to_date(s)`
    *   - `CAST(strptime(s, '%Y-%m-%d') AS DATE)`                                         -> `to_date(s)`
    *   - `CAST(strptime(s, fmt) AS DATE)`                                                 -> `to_date(s, fmt)`
    *   - `CAST(CASE WHEN s IS NOT NULL THEN NULL WHEN s IS NULL THEN NULL END AS TIMESTAMP)` -> `to_timestamp(s)`
    *   - `strptime(s, '%Y-%m-%d %H:%M:%S')`                                               -> `to_timestamp(s)`
    *   - `strptime(s, fmt)`                                                               -> `to_timestamp(s, fmt)`
    *   - `strftime(d, fmt)`                                                               -> `date_format(d, fmt)`
    *   - the `__sparkfn_make_interval` marker expansion                                    -> `make_interval(...)`
    *   - the `__sparkfn_get_json_object` marker expansion                                  -> `get_json_object(...)`
    *   - `last_value(expr IGNORE NULLS) OVER (...)`                                      -> `last_value(expr, true) OVER (...)`
    *   - `last(expr) OVER (...)`                                                          -> `last_value(expr) OVER (...)`
    *   - `first_value(expr IGNORE NULLS) OVER (...)`                                     -> `first_value(expr, true) OVER (...)`
    *   - `first(expr) OVER (...)`                                                         -> `first_value(expr) OVER (...)`
    *   - `CAST(get_current_timestamp() AS TIMESTAMP)`                                     -> `current_timestamp()`
    *   - `CAST(CAST(get_current_timestamp() AS TIMESTAMP) AS DATE)`                       -> `current_date()`
    *
    * The 1-arg date/time rewrites trigger when the inlined body matches either
    * the legacy fixed-format `strptime` shim or the current polymorphic
    * `CASE WHEN expr IS [NOT] NULL THEN NULL ...` shim registered by
    * [[OpenIvmCompiler.sparkFunctionShimsPrologue]]. Nested shim expansions are
    * rewritten recursively so expressions like
    * `strftime(CAST(strptime(x, f) AS DATE), g)` become
    * `date_format(to_date(x, f), g)`.
    */
  def rewriteInlinedSparkShimCalls(sql: String): String =
    rewriteOutsideProtected(sql) { i =>
      parseMakeIntervalShim(sql, i)
        .orElse(parseGetJsonObjectShim(sql, i))
        .orElse(parseCastTemporalShim(sql, i))
        .orElse(parseFunctionRewrite(sql, i, "strptime", "to_timestamp", Some(OneArgToTimestampLiteral)))
        .orElse(parseFunctionRewrite(sql, i, "strftime", "date_format"))
        .orElse(parseWindowIgnoreNullsRewrite(sql, i, "last_value"))
        .orElse(parseWindowIgnoreNullsRewrite(sql, i, "first_value"))
        .orElse(parseWindowFunctionNameRewrite(sql, i, "last", "last_value"))
        .orElse(parseWindowFunctionNameRewrite(sql, i, "first", "first_value"))
    }

  /** Rewrites DuckDB's native positional 2-arg `trim(<str>, <chars>)` call
    * (bare, or backtick-quoted as `` `trim`(<str>, <chars>) `` once
    * [[LptsSparkDialect.rewriteDoubleQuotedIdentifiers]] has already turned
    * DuckDB's own `"trim"(...)` spelling into a Spark-legal identifier) into
    * Spark's unambiguous ANSI `TRIM(<chars> FROM <str>)` form.
    *
    * DuckDB's `trim(string, characters)` builtin takes the source string
    * first and the trim-character set second. Spark's parser instead
    * resolves a *positional* 2-arg `trim(a, b)` call to `TRIM(a FROM b)` --
    * the OPPOSITE argument order -- so re-parsing DuckDB's serialized call
    * verbatim under Spark silently swaps the operands with no parse error:
    * `` `trim`(<longExpr>, '_') `` becomes "trim every character that
    * occurs in `<longExpr>` off the two-character string `'_'`", which
    * erases it to an empty string for every row (this is how the nested
    * `normalize_os_name` REPLACE/TRIM fragment produced empty strings after
    * a CREATE/refresh round-trip, even though its literal escaping was
    * already correct).
    *
    * The rewritten ANSI `TRIM(... FROM ...)` form is not itself a 2-arg
    * positional call (no top-level comma), so re-running this pass over its
    * own output is a no-op.
    *
    * Only the 2-arg positional shape is rewritten -- 1-arg `trim(s)`
    * (whitespace trim) and the native `TRIM([BOTH|LEADING|TRAILING] chars
    * FROM str)` spelling (zero top-level commas) already match between the
    * two dialects and are left untouched.
    */
  def rewriteTrimTwoArgToAnsiFrom(sql: String): String =
    rewriteOutsideProtected(sql) { i =>
      parseTrimIdentifier(sql, i).flatMap { nameEnd =>
        parseFunctionCall(sql, nameEnd)
          .filter(_.topLevelCommaCount == 1)
          .flatMap { call =>
            splitTopLevelArgs(sql, call).collect { case Seq((s1, e1), (s2, e2)) =>
              val strArg   = sql.substring(s1, e1).trim
              val charsArg = sql.substring(s2, e2).trim
              call.closeParen + 1 -> s"TRIM($charsArg FROM $strArg)"
            }
          }
      }
    }

  /** Matches the identifier `trim` at `start`, either bare or
    * backtick-quoted (e.g. `` `trim` ``), returning the index immediately
    * after the name (including the closing backtick, if quoted) provided a
    * legal identifier boundary follows. Case-insensitive, matching Spark's
    * identifier resolution.
    */
  private def parseTrimIdentifier(sql: String, start: Int): Option[Int] =
    if (sql.charAt(start) == '`') {
      val nameStart = start + 1
      val nameEnd   = nameStart + 4
      if (nameEnd < sql.length && sql.regionMatches(true, nameStart, "trim", 0, 4) && sql.charAt(nameEnd) == '`')
        Some(nameEnd + 1)
      else None
    } else if (isIdentifierStart(sql.charAt(start)) && hasLeftIdentifierBoundary(sql, start)) {
      val nameEnd = start + 4
      if (
        nameEnd <= sql.length && sql
          .regionMatches(true, start, "trim", 0, 4) && hasRightIdentifierBoundary(sql, nameEnd)
      )
        Some(nameEnd)
      else None
    } else None

  /** Pre-pass step that translates Spark single-quoted string literals into
    * DuckDB-compatible syntax before any other rewriting happens.
    *
    * Spark string literals accept both the SQL-standard doubled-quote escape
    * (`''`) AND backslash escapes (`\\`, `\'`, `\n`, ... — see
    * https://spark.apache.org/docs/latest/sql-ref-literals.html). DuckDB has
    * no backslash-escape convention at all: a bare `\` is an ordinary
    * character and only `''` represents an embedded quote. Left unrewritten,
    * a Spark literal like `'\''` (one embedded quote character) is rejected
    * outright by DuckDB's parser (`Parser Error: syntax error at or near
    * "\"`), and `'\\'` (one embedded backslash character) is silently
    * misread as containing TWO backslash characters — no exception, wrong
    * value. This decodes each literal's Spark-escaped value and re-encodes
    * it using DuckDB's doubled-quote convention so both failure modes are
    * fixed (e.g. the nested `REPLACE(..., '\\', '_'), ..., '\'', '_')`
    * fragment in the dbt `normalize_os_name` macro).
    *
    * Recognized Spark escapes (per the Spark SQL literal grammar):
    *   `\0 \b \n \r \t \Z \\ \' \"` -> the corresponding literal character
    *   `\%` `\_`                    -> preserved verbatim (Spark keeps the
    *                                   backslash for LIKE-pattern escapes)
    *   `\<any other char>`          -> that character, backslash dropped
    *   `''`                        -> a literal quote (already valid in both
    *                                   dialects; copied through unchanged)
    *
    * Comments and double-quoted identifiers are left untouched. Only
    * genuinely Spark-dialect source SQL should be passed to this function —
    * SQL already emitted by DuckDB (e.g. the "sql" payload from
    * `openivm_compile_with_facts`, processed by
    * [[rewriteInlinedSparkShimCalls]]) has no backslash-escape convention and
    * must NOT be re-scanned here.
    */
  def translateSparkStringLiteralEscapes(sql: String): String = {
    val out = new StringBuilder(sql.length)
    var i   = 0
    while (i < sql.length) {
      if (startsLineComment(sql, i)) {
        val end = consumeLineComment(sql, i)
        out ++= sql.substring(i, end)
        i = end
      } else if (startsBlockComment(sql, i)) {
        val end = consumeBlockComment(sql, i)
        out ++= sql.substring(i, end)
        i = end
      } else if (sql.charAt(i) == '"') {
        val end = consumeDoubleQuoted(sql, i)
        out ++= sql.substring(i, end)
        i = end
      } else if (sql.charAt(i) == '\'') {
        val (literal, end) = reencodeSparkSingleQuotedLiteral(sql, i)
        out ++= literal
        i = end
      } else {
        out += sql.charAt(i)
        i += 1
      }
    }
    out.toString
  }

  /** Decodes a single Spark single-quoted literal starting at `sql(start)`
    * (which must be `'`) and re-encodes it using DuckDB's doubled-quote-only
    * convention. Returns the re-encoded literal (including surrounding
    * quotes) and the index just past the literal's closing quote.
    */
  private def reencodeSparkSingleQuotedLiteral(sql: String, start: Int): (String, Int) = {
    val body = new StringBuilder
    var i    = start + 1
    var done = false
    while (i < sql.length && !done) {
      val ch = sql.charAt(i)
      if (ch == '\\' && i + 1 < sql.length) {
        body ++= decodeSparkEscapeForDuckdb(sql.charAt(i + 1))
        i += 2
      } else if (ch == '\'') {
        if (i + 1 < sql.length && sql.charAt(i + 1) == '\'') {
          body ++= "''"
          i += 2
        } else {
          done = true
          i += 1
        }
      } else {
        body += ch
        i += 1
      }
    }
    ("'" + body.toString + "'", i)
  }

  /** Decodes one Spark backslash-escaped character (the character following
    * `\`) and re-encodes the result for DuckDB's single-quoted literal
    * syntax (only `'` needs doubling; every other character is literal).
    */
  private def decodeSparkEscapeForDuckdb(escaped: Char): String = escaped match {
    case '0'   => "\u0000"
    case 'b'   => "\b"
    case 'n'   => "\n"
    case 'r'   => "\r"
    case 't'   => "\t"
    case 'Z'   => "\u001A"
    case '\\'  => "\\"
    case '\''  => "''"
    case '"'   => "\""
    case '%'   => "\\%"
    case '_'   => "\\_"
    case other => other.toString
  }

  /** Spark-dialect single-quoted literal boundary scanner: unlike
    * [[consumeSingleQuoted]] (DuckDB dialect — doubled-quote only), this
    * additionally recognizes Spark's backslash-escape convention so a
    * two-character escape pair (`\` + any character, including `\'` and
    * `\\`) is never mistaken for the closing quote. Used to keep the outer
    * identifier scan in [[OpenIvmCompiler.stripSparkBacktickIdentifiers]]
    * correctly positioned when it must skip over — but not otherwise touch —
    * a Spark-dialect string literal.
    */
  private[compiler] def consumeSparkSingleQuoted(sql: String, start: Int): Int = {
    var i = start + 1
    while (i < sql.length) {
      val ch = sql.charAt(i)
      if (ch == '\\' && i + 1 < sql.length) {
        i += 2
      } else if (ch == '\'') {
        if (i + 1 < sql.length && sql.charAt(i + 1) == '\'') i += 2
        else return i + 1
      } else {
        i += 1
      }
    }
    sql.length
  }

  /** Matches Spark's literal-boolean `<functionName>(expr, true|false)`
    * spelling (only ever meaningful for `last_value` / `first_value`) and
    * rewrites it to DuckDB's native `IGNORE NULLS` window-function modifier.
    */
  private def parseSparkWindowLiteralBoolRewrite(
      sql: String,
      start: Int,
      identEnd: Int,
      functionName: String
  ): Option[(Int, String)] = {
    if (
      identEnd - start != functionName.length || !sql.regionMatches(true, start, functionName, 0, functionName.length)
    ) None
    else {
      parseFunctionCall(sql, identEnd)
        .filter(_.topLevelCommaCount == 1)
        .flatMap { call =>
          splitTopLevelArgs(sql, call).flatMap {
            case Seq(exprRange, boolRange) =>
              parseBooleanLiteralArg(sql, boolRange).map { ignoreNulls =>
                val expr = sql.substring(exprRange._1, exprRange._2).trim
                val replacement =
                  if (ignoreNulls) s"$functionName($expr IGNORE NULLS)"
                  else s"$functionName($expr)"
                call.closeParen + 1 -> replacement
              }
            case _ => None
          }
        }
    }
  }

  private def parseWindowIgnoreNullsRewrite(sql: String, start: Int, functionName: String): Option[(Int, String)] =
    parseFunctionCallAt(sql, start, functionName)
      .filter(_.topLevelCommaCount == 0)
      .filter(call => hasOverClause(sql, call.closeParen + 1))
      .flatMap { call =>
        findTopLevelKeyword(sql, call.openParen + 1, call.closeParen, "IGNORE").flatMap { ignoreStart =>
          val nullsStart = skipTriviaForward(sql, ignoreStart + "IGNORE".length, call.closeParen)
          val expr       = sql.substring(call.openParen + 1, ignoreStart).trim
          if (expr.isEmpty || !isKeywordAt(sql, nullsStart, "NULLS")) None
          else if (!isTriviaOnly(sql, nullsStart + "NULLS".length, call.closeParen)) None
          else Some(call.closeParen + 1 -> s"$functionName(${rewriteInlinedSparkShimCalls(expr)}, true)")
        }
      }

  private def parseMakeIntervalShim(sql: String, start: Int): Option[(Int, String)] =
    parseFunctionCallAt(sql, start, "coalesce")
      .filter(_.topLevelCommaCount == 1)
      .flatMap { call =>
        splitTopLevelArgs(sql, call).flatMap {
          case Seq(markerRange, _) =>
            parseMakeIntervalMarkerArgs(sql, markerRange).map { args =>
              call.closeParen + 1 -> s"make_interval(${args.mkString(", ")})"
            }
          case _ => None
        }
      }

  private def parseMakeIntervalMarkerArgs(sql: String, range: (Int, Int)): Option[Seq[String]] =
    parseSingleFunctionCallInRange(sql, range, "to_seconds")
      .filter(_.topLevelCommaCount == 0)
      .flatMap { secondsCall =>
        parseSingleFunctionCallInRange(
          sql,
          (secondsCall.openParen + 1, secondsCall.closeParen),
          "try_cast"
        )
      }
      .flatMap { tryCastCall =>
        val bodyStart = tryCastCall.openParen + 1
        val bodyEnd   = tryCastCall.closeParen
        findTopLevelKeyword(sql, bodyStart, bodyEnd, "AS").flatMap { asStart =>
          val typeStart = skipTriviaForward(sql, asStart + "AS".length, bodyEnd)
          if (typeStart >= bodyEnd) None
          else {
            val typeEnd = readIdentifierEnd(sql, typeStart)
            if (
              !sql.substring(typeStart, typeEnd).equalsIgnoreCase("DOUBLE") ||
              !isTriviaOnly(sql, typeEnd, bodyEnd)
            ) None
            else {
              parseSingleFunctionCallInRange(sql, (bodyStart, asStart), "concat")
                .filter(_.topLevelCommaCount == 13)
                .flatMap(splitTopLevelArgs(sql, _))
                .filter { args =>
                  args.size == 14 &&
                  argEqualsSingleQuotedLiteral(sql, args.head, s"'$MakeIntervalMarker'") &&
                  (2 until 13 by 2).forall { idx =>
                    argEqualsSingleQuotedLiteral(sql, args(idx), s"'$MarkerArgSeparator'")
                  }
                }
                .map { args =>
                  (1 until 14 by 2).map(idx => restoreMakeIntervalArg(sql, args(idx)))
                }
            }
          }
        }
      }

  private def parseGetJsonObjectShim(sql: String, start: Int): Option[(Int, String)] =
    parseFunctionCallAt(sql, start, "concat")
      .filter(_.topLevelCommaCount == 3)
      .flatMap { call =>
        splitTopLevelArgs(sql, call).flatMap {
          case Seq(marker, jsonText, separator, path)
              if argEqualsSingleQuotedLiteral(sql, marker, s"'$GetJsonObjectMarker'") &&
                argEqualsSingleQuotedLiteral(sql, separator, s"'$MarkerArgSeparator'") =>
            val jsonArg = restoreSerializedStringArg(sql, jsonText)
            val pathArg = restoreSerializedStringArg(sql, path)
            Some(call.closeParen + 1 -> s"get_json_object($jsonArg, $pathArg)")
          case _ => None
        }
      }

  private def parseSingleFunctionCallInRange(
      sql: String,
      range: (Int, Int),
      functionName: String
  ): Option[FunctionCall] = {
    val (start, endExclusive) = range
    val callStart             = skipTriviaForward(sql, start, endExclusive)
    if (callStart >= endExclusive) None
    else {
      parseFunctionCallAt(sql, callStart, functionName)
        .filter(call => call.closeParen < endExclusive && isTriviaOnly(sql, call.closeParen + 1, endExclusive))
    }
  }

  private def restoreMakeIntervalArg(sql: String, range: (Int, Int)): String = {
    val restored = restoreSerializedStringArg(sql, range)
    if (
      restored.length >= 2 &&
      restored.head == '\'' &&
      restored.last == '\'' &&
      scala.util.Try(BigDecimal(restored.substring(1, restored.length - 1))).isSuccess
    ) restored.substring(1, restored.length - 1)
    else restored
  }

  private def restoreSerializedStringArg(sql: String, range: (Int, Int)): String = {
    val (start, endExclusive) = range
    val argStart              = skipTriviaForward(sql, start, endExclusive)
    if (argStart >= endExclusive) ""
    else {
      parseFunctionCallAt(sql, argStart, "cast")
        .filter(call => call.closeParen < endExclusive && isTriviaOnly(sql, call.closeParen + 1, endExclusive))
        .flatMap { castCall =>
          val bodyStart = castCall.openParen + 1
          val bodyEnd   = castCall.closeParen
          findTopLevelKeyword(sql, bodyStart, bodyEnd, "AS").flatMap { asStart =>
            val typeStart = skipTriviaForward(sql, asStart + "AS".length, bodyEnd)
            if (typeStart >= bodyEnd) None
            else {
              val typeEnd = readIdentifierEnd(sql, typeStart)
              val isStringCast =
                sql.substring(typeStart, typeEnd).equalsIgnoreCase("STRING") ||
                  sql.substring(typeStart, typeEnd).equalsIgnoreCase("VARCHAR")
              if (!isStringCast || !isTriviaOnly(sql, typeEnd, bodyEnd)) None
              else Some(rewriteInlinedSparkShimCalls(sql.substring(bodyStart, asStart).trim))
            }
          }
        }
        .getOrElse(rewriteInlinedSparkShimCalls(sql.substring(argStart, endExclusive).trim))
    }
  }

  private def parseCastTemporalShim(sql: String, start: Int): Option[(Int, String)] =
    parseFunctionCallAt(sql, start, "cast").flatMap { castCall =>
      val bodyStart = castCall.openParen + 1
      val bodyEnd   = castCall.closeParen
      findTopLevelKeyword(sql, bodyStart, bodyEnd, "AS").flatMap { asStart =>
        val exprStart = skipTriviaForward(sql, bodyStart, bodyEnd)
        val typeStart = skipTriviaForward(sql, asStart + 2, bodyEnd)
        if (exprStart >= bodyEnd || typeStart >= bodyEnd) None
        else {
          val typeEnd = readIdentifierEnd(sql, typeStart)
          if (typeEnd <= typeStart || !isTriviaOnly(sql, typeEnd, bodyEnd)) None
          else {
            val replacement =
              sql.substring(typeStart, typeEnd).toUpperCase(Locale.ROOT) match {
                case "DATE" =>
                  parseCurrentDateShim(sql, exprStart, asStart)
                    .orElse(rewriteCastTemporalBody(sql, exprStart, asStart, "to_date", Some(OneArgToDateLiteral)))
                case "TIMESTAMP" =>
                  parseCurrentTimestampShim(sql, exprStart, asStart)
                    .orElse(
                      rewriteCastTemporalBody(sql, exprStart, asStart, "to_timestamp", Some(OneArgToTimestampLiteral))
                    )
                case _ =>
                  None
              }
            replacement.map(rewritten => (castCall.closeParen + 1, rewritten))
          }
        }
      }
    }

  /** Matches `CAST(get_current_timestamp() AS TIMESTAMP)` — or the newer
    * native spelling `CAST(current_timestamp() AS TIMESTAMP)` emitted by some
    * OpenIVM/LPTS pins — and restores Spark's `current_timestamp()` spelling.
    */
  private def parseCurrentTimestampShim(sql: String, exprStart: Int, exprEndExclusive: Int): Option[String] =
    if (isCurrentTimestampLikeCall(sql, exprStart, exprEndExclusive)) Some("current_timestamp()") else None

  /** Matches `CAST(CAST(get_current_timestamp() AS TIMESTAMP) AS DATE)` — or
    * the newer native spelling with `current_timestamp()` already inlined —
    * and restores Spark's `current_date()` spelling.
    *
    * The inner `CAST(get_current_timestamp() AS TIMESTAMP)` is itself a
    * [[parseCastTemporalShim]] match, so the cheap `CAST(` prefix guard below
    * avoids wastefully recursing into ordinary `to_date` shim shapes (which
    * never start with a nested `CAST`), and the recursive
    * [[rewriteInlinedSparkShimCalls]] call normalizes the inner expression
    * before comparing — it always terminates after exactly one extra level
    * since the inner text bottoms out at the direct-equality check in
    * [[parseCurrentTimestampShim]].
    */
  private def parseCurrentDateShim(sql: String, exprStart: Int, exprEndExclusive: Int): Option[String] = {
    val trimmedExpr = sql.substring(exprStart, exprEndExclusive).trim
    if (!trimmedExpr.regionMatches(true, 0, "CAST(", 0, "CAST(".length)) None
    else if (rewriteInlinedSparkShimCalls(trimmedExpr).equalsIgnoreCase("current_timestamp()")) Some("current_date()")
    else None
  }

  private def isCurrentTimestampLikeCall(sql: String, start: Int, endExclusive: Int): Boolean = {
    val expr = sql.substring(start, endExclusive).trim
    expr.equalsIgnoreCase("get_current_timestamp()") || expr.equalsIgnoreCase("current_timestamp()")
  }

  private def rewriteCastTemporalBody(
      sql: String,
      exprStart: Int,
      exprEndExclusive: Int,
      sparkName: String,
      oneArgLiteral: Option[String]
  ): Option[String] =
    parseFunctionCallAt(sql, exprStart, "strptime")
      .filter(call => isTriviaOnly(sql, call.closeParen + 1, exprEndExclusive) && call.topLevelCommaCount == 1)
      .flatMap(call => rewriteSparkDateCall(sql, call, sparkName, oneArgLiteral))
      .orElse(
        parseCaseNullShimArg(sql, exprStart, exprEndExclusive).map { expr =>
          val rewrittenExpr = rewriteInlinedSparkShimCalls(expr)
          s"$sparkName($rewrittenExpr)"
        }
      )

  private def parseFunctionRewrite(
      sql: String,
      start: Int,
      duckdbName: String,
      sparkName: String,
      oneArgLiteral: Option[String] = None
  ): Option[(Int, String)] =
    parseFunctionCallAt(sql, start, duckdbName)
      .filter(_.topLevelCommaCount == 1)
      .flatMap { call =>
        rewriteSparkDateCall(sql, call, sparkName, oneArgLiteral)
          .map(replacement => (call.closeParen + 1, replacement))
      }

  private def parseWindowFunctionNameRewrite(
      sql: String,
      start: Int,
      duckdbName: String,
      sparkName: String
  ): Option[(Int, String)] =
    if (!isIdentifierStart(sql.charAt(start)) || !hasLeftIdentifierBoundary(sql, start)) None
    else {
      val nameEnd = start + duckdbName.length
      if (nameEnd > sql.length || !sql.regionMatches(true, start, duckdbName, 0, duckdbName.length)) None
      else if (!hasRightIdentifierBoundary(sql, nameEnd)) None
      else {
        parseFunctionCall(sql, nameEnd)
          .filter(_.topLevelCommaCount == 0)
          .filter(call => hasOverClause(sql, call.closeParen + 1))
          .map(_ => nameEnd -> sparkName)
      }
    }

  private def rewriteSparkDateCall(
      sql: String,
      call: FunctionCall,
      sparkName: String,
      oneArgLiteral: Option[String]
  ): Option[String] =
    splitTopLevelArgs(sql, call).map { argRanges =>
      oneArgLiteral
        .filter(literal => argRanges.size == 2 && argEqualsSingleQuotedLiteral(sql, argRanges(1), literal))
        .map { _ =>
          val expr = rewriteInlinedSparkShimCalls(sql.substring(argRanges.head._1, argRanges.head._2))
          s"$sparkName($expr)"
        }
        .getOrElse {
          val args = rewriteInlinedSparkShimCalls(call.args(sql))
          s"$sparkName($args)"
        }
    }

  private def parseCaseNullShimArg(sql: String, start: Int, endExclusive: Int): Option[String] = {
    val caseStart = skipTriviaForward(sql, start, endExclusive)
    if (!isKeywordAt(sql, caseStart, "CASE")) None
    else {
      val when1Start = skipTriviaForward(sql, caseStart + "CASE".length, endExclusive)
      if (!isKeywordAt(sql, when1Start, "WHEN")) None
      else {
        val cond1Start = skipTriviaForward(sql, when1Start + "WHEN".length, endExclusive)
        findTopLevelKeywordRespectingCase(sql, cond1Start, endExclusive, "THEN").flatMap { then1Start =>
          parseIsNullConditionExpr(sql, cond1Start, then1Start, expectsNot = true).flatMap { expr1 =>
            val then1ValueStart = skipTriviaForward(sql, then1Start + "THEN".length, endExclusive)
            findTopLevelKeywordRespectingCase(sql, then1ValueStart, endExclusive, "WHEN").flatMap { when2Start =>
              if (!isBareNull(sql, then1ValueStart, when2Start)) None
              else {
                val cond2Start = skipTriviaForward(sql, when2Start + "WHEN".length, endExclusive)
                findTopLevelKeywordRespectingCase(sql, cond2Start, endExclusive, "THEN").flatMap { then2Start =>
                  parseIsNullConditionExpr(sql, cond2Start, then2Start, expectsNot = false).flatMap { expr2 =>
                    if (expr1 != expr2) None
                    else {
                      val then2ValueStart = skipTriviaForward(sql, then2Start + "THEN".length, endExclusive)
                      val elseStart = findTopLevelKeywordRespectingCase(sql, then2ValueStart, endExclusive, "ELSE")
                      val endStart  = findTopLevelKeywordRespectingCase(sql, then2ValueStart, endExclusive, "END")
                      endStart.flatMap { endPos =>
                        val tailOk = elseStart match {
                          case Some(elsePos) if elsePos < endPos =>
                            isBareNull(sql, then2ValueStart, elsePos) &&
                            isBareNull(sql, elsePos + "ELSE".length, endPos) &&
                            isTriviaOnly(sql, endPos + "END".length, endExclusive)
                          case _ =>
                            isBareNull(sql, then2ValueStart, endPos) &&
                            isTriviaOnly(sql, endPos + "END".length, endExclusive)
                        }
                        if (tailOk) Some(expr1) else None
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  private def parseIsNullConditionExpr(
      sql: String,
      start: Int,
      endExclusive: Int,
      expectsNot: Boolean
  ): Option[String] =
    findTopLevelKeywordRespectingCase(sql, start, endExclusive, "IS").flatMap { isStart =>
      val expr = sql.substring(skipTriviaForward(sql, start, isStart), isStart).trim
      if (expr.isEmpty) None
      else {
        val afterIs = skipTriviaForward(sql, isStart + "IS".length, endExclusive)
        val nullStart =
          if (expectsNot) {
            if (!isKeywordAt(sql, afterIs, "NOT")) -1
            else skipTriviaForward(sql, afterIs + "NOT".length, endExclusive)
          } else afterIs
        if (nullStart < 0 || !isKeywordAt(sql, nullStart, "NULL")) None
        else if (!isTriviaOnly(sql, nullStart + "NULL".length, endExclusive)) None
        else Some(expr)
      }
    }

  private def isBareNull(sql: String, start: Int, endExclusive: Int): Boolean = {
    val nullStart = skipTriviaForward(sql, start, endExclusive)
    isKeywordAt(sql, nullStart, "NULL") && isTriviaOnly(sql, nullStart + "NULL".length, endExclusive)
  }

  private def hasOverClause(sql: String, start: Int): Boolean = {
    val overStart = skipTriviaForward(sql, start, sql.length)
    overStart < sql.length &&
    startsWithKeyword(sql, overStart, "OVER") &&
    hasLeftIdentifierBoundary(sql, overStart) &&
    hasRightIdentifierBoundary(sql, overStart + "OVER".length)
  }

  private def splitTopLevelArgs(sql: String, call: FunctionCall): Option[Seq[(Int, Int)]] = {
    val args     = scala.collection.mutable.ArrayBuffer.empty[(Int, Int)]
    var depth    = 1
    var argStart = call.openParen + 1
    var i        = argStart
    while (i < call.closeParen) {
      if (startsLineComment(sql, i)) {
        i = consumeLineComment(sql, i).min(call.closeParen)
      } else if (startsBlockComment(sql, i)) {
        i = consumeBlockComment(sql, i).min(call.closeParen)
      } else {
        sql.charAt(i) match {
          case '\'' => i = consumeSingleQuoted(sql, i).min(call.closeParen)
          case '"'  => i = consumeDoubleQuoted(sql, i).min(call.closeParen)
          case '(' =>
            depth += 1
            i += 1
          case ')' =>
            depth -= 1
            i += 1
          case ',' if depth == 1 =>
            args += ((argStart, i))
            argStart = i + 1
            i += 1
          case _ =>
            i += 1
        }
      }
    }
    if (depth != 1) None
    else {
      args += ((argStart, call.closeParen))
      Some(args.toSeq)
    }
  }

  private def parseBooleanLiteralArg(sql: String, argRange: (Int, Int)): Option[Boolean] = {
    val (start, endExclusive) = argRange
    val literalStart          = skipTriviaForward(sql, start, endExclusive)
    if (isKeywordAt(sql, literalStart, "TRUE") && isTriviaOnly(sql, literalStart + "TRUE".length, endExclusive)) {
      Some(true)
    } else if (
      isKeywordAt(sql, literalStart, "FALSE") && isTriviaOnly(
        sql,
        literalStart + "FALSE".length,
        endExclusive
      )
    ) {
      Some(false)
    } else None
  }

  private def argEqualsSingleQuotedLiteral(
      sql: String,
      argRange: (Int, Int),
      expectedLiteral: String
  ): Boolean = {
    val (start, endExclusive) = argRange
    val literalStart          = skipTriviaForward(sql, start, endExclusive)
    if (literalStart >= endExclusive || sql.charAt(literalStart) != '\'') false
    else {
      val literalEnd = consumeSingleQuoted(sql, literalStart).min(endExclusive)
      literalEnd <= endExclusive &&
      sql.substring(literalStart, literalEnd) == expectedLiteral &&
      isTriviaOnly(sql, literalEnd, endExclusive)
    }
  }

  private def rewriteOutsideProtected(sql: String)(matcher: Int => Option[(Int, String)]): String = {
    val out = new StringBuilder(sql.length)
    var i   = 0
    while (i < sql.length) {
      val next =
        if (startsLineComment(sql, i)) Some(consumeLineComment(sql, i))
        else if (startsBlockComment(sql, i)) Some(consumeBlockComment(sql, i))
        else if (sql.charAt(i) == '\'') Some(consumeSingleQuoted(sql, i))
        else if (sql.charAt(i) == '"') Some(consumeDoubleQuoted(sql, i))
        else None

      next match {
        case Some(end) =>
          out.append(sql.substring(i, end))
          i = end
        case None =>
          matcher(i) match {
            case Some((endExclusive, replacement)) =>
              out.append(replacement)
              i = endExclusive
            case None =>
              out.append(sql.charAt(i))
              i += 1
          }
      }
    }
    out.toString
  }

  private def parseFunctionCallAt(sql: String, start: Int, expectedName: String): Option[FunctionCall] = {
    if (!isIdentifierStart(sql.charAt(start)) || !hasLeftIdentifierBoundary(sql, start)) return None
    val nameEnd = start + expectedName.length
    if (nameEnd > sql.length || !sql.regionMatches(true, start, expectedName, 0, expectedName.length)) return None
    if (!hasRightIdentifierBoundary(sql, nameEnd)) return None
    parseFunctionCall(sql, nameEnd)
  }

  private def parseFunctionCall(sql: String, nameEndExclusive: Int): Option[FunctionCall] = {
    val openParen = skipTriviaForward(sql, nameEndExclusive, sql.length)
    if (openParen >= sql.length || sql.charAt(openParen) != '(') return None

    var depth              = 1
    var i                  = openParen + 1
    var topLevelCommaCount = 0
    while (i < sql.length) {
      if (startsLineComment(sql, i)) {
        i = consumeLineComment(sql, i)
      } else if (startsBlockComment(sql, i)) {
        i = consumeBlockComment(sql, i)
      } else {
        sql.charAt(i) match {
          case '\'' => i = consumeSingleQuoted(sql, i)
          case '"'  => i = consumeDoubleQuoted(sql, i)
          case '(' =>
            depth += 1
            i += 1
          case ')' =>
            depth -= 1
            if (depth == 0) {
              return Some(FunctionCall(openParen, i, topLevelCommaCount))
            }
            i += 1
          case ',' if depth == 1 =>
            topLevelCommaCount += 1
            i += 1
          case _ =>
            i += 1
        }
      }
    }
    None
  }

  private def findTopLevelKeyword(sql: String, start: Int, endExclusive: Int, keyword: String): Option[Int] = {
    var depth = 0
    var i     = start
    while (i < endExclusive) {
      if (startsLineComment(sql, i)) {
        i = consumeLineComment(sql, i).min(endExclusive)
      } else if (startsBlockComment(sql, i)) {
        i = consumeBlockComment(sql, i).min(endExclusive)
      } else {
        sql.charAt(i) match {
          case '\'' =>
            i = consumeSingleQuoted(sql, i).min(endExclusive)
          case '"' =>
            i = consumeDoubleQuoted(sql, i).min(endExclusive)
          case '(' =>
            depth += 1
            i += 1
          case ')' =>
            depth -= 1
            i += 1
          case _
              if depth == 0 && startsWithKeyword(sql, i, keyword) && hasLeftIdentifierBoundary(sql, i) &&
                hasRightIdentifierBoundary(sql, i + keyword.length) =>
            return Some(i)
          case _ =>
            i += 1
        }
      }
    }
    None
  }

  private def findTopLevelKeywordRespectingCase(
      sql: String,
      start: Int,
      endExclusive: Int,
      keyword: String
  ): Option[Int] = {
    var parenDepth      = 0
    var nestedCaseDepth = 0
    var i               = start
    while (i < endExclusive) {
      if (startsLineComment(sql, i)) {
        i = consumeLineComment(sql, i).min(endExclusive)
      } else if (startsBlockComment(sql, i)) {
        i = consumeBlockComment(sql, i).min(endExclusive)
      } else {
        sql.charAt(i) match {
          case '\'' =>
            i = consumeSingleQuoted(sql, i).min(endExclusive)
          case '"' =>
            i = consumeDoubleQuoted(sql, i).min(endExclusive)
          case '(' =>
            parenDepth += 1
            i += 1
          case ')' =>
            parenDepth -= 1
            i += 1
          case _ if parenDepth == 0 && isKeywordAt(sql, i, "CASE") =>
            nestedCaseDepth += 1
            i += "CASE".length
          case _ if parenDepth == 0 && isKeywordAt(sql, i, "END") =>
            if (nestedCaseDepth == 0 && keyword.equalsIgnoreCase("END")) return Some(i)
            if (nestedCaseDepth > 0) nestedCaseDepth -= 1
            i += "END".length
          case _ if parenDepth == 0 && nestedCaseDepth == 0 && isKeywordAt(sql, i, keyword) =>
            return Some(i)
          case _ =>
            i += 1
        }
      }
    }
    None
  }

  private def isTriviaOnly(sql: String, start: Int, endExclusive: Int): Boolean = {
    var i = start
    while (i < endExclusive) {
      if (sql.charAt(i).isWhitespace) {
        i += 1
      } else if (startsLineComment(sql, i)) {
        i = consumeLineComment(sql, i).min(endExclusive)
      } else if (startsBlockComment(sql, i)) {
        i = consumeBlockComment(sql, i).min(endExclusive)
      } else {
        return false
      }
    }
    true
  }

  private def skipTriviaForward(sql: String, start: Int, endExclusive: Int): Int = {
    var i       = start
    var changed = true
    while (i < endExclusive && changed) {
      changed = false
      while (i < endExclusive && sql.charAt(i).isWhitespace) {
        i += 1
        changed = true
      }
      if (startsLineComment(sql, i)) {
        i = consumeLineComment(sql, i).min(endExclusive)
        changed = true
      } else if (startsBlockComment(sql, i)) {
        i = consumeBlockComment(sql, i).min(endExclusive)
        changed = true
      }
    }
    i
  }

  private def startsWithKeyword(sql: String, start: Int, keyword: String): Boolean =
    start + keyword.length <= sql.length && sql.regionMatches(true, start, keyword, 0, keyword.length)

  private def isKeywordAt(sql: String, start: Int, keyword: String): Boolean =
    start >= 0 && start + keyword.length <= sql.length &&
      startsWithKeyword(sql, start, keyword) &&
      hasLeftIdentifierBoundary(sql, start) &&
      hasRightIdentifierBoundary(sql, start + keyword.length)

  private def readIdentifierEnd(sql: String, start: Int): Int = {
    var i = start + 1
    while (i < sql.length && isIdentifierChar(sql.charAt(i))) i += 1
    i
  }

  private def isIdentifierStart(c: Char): Boolean = c.isLetter || c == '_'

  private def isIdentifierChar(c: Char): Boolean = c.isLetterOrDigit || c == '_'

  private def hasLeftIdentifierBoundary(sql: String, idx: Int): Boolean =
    idx <= 0 || !isIdentifierChar(sql.charAt(idx - 1))

  private def hasRightIdentifierBoundary(sql: String, idx: Int): Boolean =
    idx >= sql.length || !isIdentifierChar(sql.charAt(idx))

  private def startsLineComment(sql: String, idx: Int): Boolean =
    idx + 1 < sql.length && sql.charAt(idx) == '-' && sql.charAt(idx + 1) == '-'

  private def startsBlockComment(sql: String, idx: Int): Boolean =
    idx + 1 < sql.length && sql.charAt(idx) == '/' && sql.charAt(idx + 1) == '*'

  private def consumeSingleQuoted(sql: String, start: Int): Int = {
    var i = start + 1
    while (i < sql.length) {
      if (sql.charAt(i) == '\'') {
        if (i + 1 < sql.length && sql.charAt(i + 1) == '\'') i += 2
        else return i + 1
      } else {
        i += 1
      }
    }
    sql.length
  }

  private def consumeDoubleQuoted(sql: String, start: Int): Int = {
    var i = start + 1
    while (i < sql.length) {
      if (sql.charAt(i) == '"') {
        if (i + 1 < sql.length && sql.charAt(i + 1) == '"') i += 2
        else return i + 1
      } else {
        i += 1
      }
    }
    sql.length
  }

  private def consumeLineComment(sql: String, start: Int): Int = {
    var i = start + 2
    while (i < sql.length && sql.charAt(i) != '\n' && sql.charAt(i) != '\r') i += 1
    i
  }

  private def consumeBlockComment(sql: String, start: Int): Int = {
    var i = start + 2
    while (i + 1 < sql.length && !(sql.charAt(i) == '*' && sql.charAt(i + 1) == '/')) i += 1
    if (i + 1 < sql.length) i + 2 else sql.length
  }
}
