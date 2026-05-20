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
    "last_value" -> RenameRule(
      Map(
        1 -> "__sparkfn_last_value"
      )
    )
  )

  /** Pre-pass for the compile bridge: rename Spark function spellings that
    * DuckDB would otherwise parse or bind incompatibly to collision-free
    * `__sparkfn_*` spellings before the SQL reaches DuckDB.
    *
    * Only the function NAME is rewritten; argument text is preserved verbatim.
    * Current coverage:
    *   - 1-arg / 2-arg `to_date(...)`
    *   - 1-arg / 2-arg `to_timestamp(...)`
    *   - 2-arg `date_format(...)`
    *   - 2-arg `last_value(expr, ignoreNulls)`
    */
  def renameSparkFunctionShimCalls(sql: String): String =
    rewriteOutsideProtected(sql) { i =>
      if (!isIdentifierStart(sql.charAt(i)) || !hasLeftIdentifierBoundary(sql, i)) None
      else {
        val identEnd = readIdentifierEnd(sql, i)
        val lower    = sql.substring(i, identEnd).toLowerCase(Locale.ROOT)
        sparkFunctionRenameRules.get(lower).flatMap { rule =>
          parseFunctionCall(sql, identEnd)
            .flatMap(call => rule.replacementFor(call).map(replacement => identEnd -> replacement))
        }
      }
    }

  /** Post-pass for the LPTS serializer: reverse the inlined DuckDB macro bodies
    * back to Spark's original shim spellings.
    *
    * Rewrites:
    *   - `CAST(strptime(s, '%Y-%m-%d') AS DATE)` -> `to_date(s)`
    *   - `CAST(strptime(s, fmt) AS DATE)`        -> `to_date(s, fmt)`
    *   - `strptime(s, '%Y-%m-%d %H:%M:%S')`      -> `to_timestamp(s)`
    *   - `strptime(s, fmt)`                      -> `to_timestamp(s, fmt)`
    *   - `strftime(d, fmt)`                      -> `date_format(d, fmt)`
    *   - `last(expr) OVER (...)`                 -> `last_value(expr) OVER (...)`
    *
    * The 1-arg date/time rewrites only trigger when the format literal matches
    * the exact shim body registered by
    * [[OpenIvmCompiler.sparkFunctionShimsPrologue]]. Nested shim expansions are
    * rewritten recursively so expressions like
    * `strftime(CAST(strptime(x, f) AS DATE), g)` become
    * `date_format(to_date(x, f), g)`.
    */
  def rewriteInlinedSparkShimCalls(sql: String): String =
    rewriteOutsideProtected(sql) { i =>
      parseCastStrptimeAsDate(sql, i)
        .orElse(parseFunctionRewrite(sql, i, "strptime", "to_timestamp", Some(OneArgToTimestampLiteral)))
        .orElse(parseFunctionRewrite(sql, i, "strftime", "date_format"))
        .orElse(parseWindowFunctionNameRewrite(sql, i, "last", "last_value"))
    }

  private def parseCastStrptimeAsDate(sql: String, start: Int): Option[(Int, String)] =
    parseFunctionCallAt(sql, start, "cast").flatMap { castCall =>
      val bodyStart = castCall.openParen + 1
      val bodyEnd   = castCall.closeParen
      findTopLevelKeyword(sql, bodyStart, bodyEnd, "AS").flatMap { asStart =>
        val exprStart = skipTriviaForward(sql, bodyStart, bodyEnd)
        val typeStart = skipTriviaForward(sql, asStart + 2, bodyEnd)
        if (exprStart >= bodyEnd || typeStart >= bodyEnd) None
        else {
          parseFunctionCallAt(sql, exprStart, "strptime").flatMap { strptimeCall =>
            val typeEnd = readIdentifierEnd(sql, typeStart)
            val typeOk =
              typeEnd > typeStart &&
                sql.substring(typeStart, typeEnd).equalsIgnoreCase("DATE") &&
                isTriviaOnly(sql, strptimeCall.closeParen + 1, asStart) &&
                isTriviaOnly(sql, typeEnd, bodyEnd) &&
                strptimeCall.topLevelCommaCount == 1
            if (!typeOk) None
            else {
              rewriteSparkDateCall(sql, strptimeCall, "to_date", Some(OneArgToDateLiteral))
                .map(replacement => (castCall.closeParen + 1, replacement))
            }
          }
        }
      }
    }

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
