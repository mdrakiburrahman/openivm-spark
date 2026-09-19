/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.openivm.spark.parser

import java.util.Locale

import org.apache.spark.sql.catalyst.analysis.{
  MultiAlias,
  UnresolvedAlias,
  UnresolvedRelation,
  UnresolvedSubqueryColumnAliases
}
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression, SubqueryExpression}
import org.apache.spark.sql.catalyst.parser.{ParseException, ParserInterface, ParserUtils}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias, UnresolvedWith}
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.catalyst.util.IntervalUtils
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.CalendarInterval

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

/**
 * Adapts Spark 4.1's relation-local STREAM / WITH / WATERMARK syntax to Spark 3.5
 * without crossing the extension's shaded ANTLR boundary.
 *
 * The complete query is still parsed by Spark. This scanner only replaces each
 * extension-owned fragment with a collision-free relation/alias marker, then
 * binds that exact marker occurrence back to the corresponding Catalyst node.
 */
private[parser] object StreamingQuerySql {

  private sealed trait TokenKind
  private case object WordToken        extends TokenKind
  private case object BacktickToken    extends TokenKind
  private case object StringToken      extends TokenKind
  private case object DoubleQuoteToken extends TokenKind
  private case object NumberToken      extends TokenKind
  private case object SymbolToken      extends TokenKind

  private final case class SqlToken(kind: TokenKind, text: String, start: Int, end: Int) {
    def keyword(value: String): Boolean =
      kind == WordToken && text.equalsIgnoreCase(value)

    def symbol(value: String): Boolean =
      kind == SymbolToken && text == value

    def identifier: Boolean = kind == WordToken || kind == BacktickToken
  }

  private final case class TextEdit(start: Int, end: Int, replacement: String)

  private final case class SourceBinding(marker: String, name: Seq[String], options: Map[String, String])

  private final case class AliasSpec(name: String, columns: Seq[String])

  private final case class WatermarkBinding(
      marker: String,
      eventTime: NamedExpression,
      delay: CalendarInterval,
      alias: Option[AliasSpec]
  )

  private final case class ParsedProperties(values: Map[String, String], nextToken: Int)

  private final case class ParsedAlias(value: Option[AliasSpec], nextToken: Int)

  private val IntervalUnits = Set(
    "NANOSECOND",
    "NANOSECONDS",
    "MICROSECOND",
    "MICROSECONDS",
    "MILLISECOND",
    "MILLISECONDS",
    "SECOND",
    "SECONDS",
    "MINUTE",
    "MINUTES",
    "HOUR",
    "HOURS",
    "DAY",
    "DAYS",
    "WEEK",
    "WEEKS",
    "MONTH",
    "MONTHS",
    "YEAR",
    "YEARS"
  )

  private val RelationBoundaryKeywords = Set(
    "ANTI",
    "CLUSTER",
    "CROSS",
    "DISTRIBUTE",
    "EXCEPT",
    "FETCH",
    "FULL",
    "GROUP",
    "HAVING",
    "INNER",
    "INTERSECT",
    "JOIN",
    "LATERAL",
    "LEFT",
    "LIMIT",
    "MINUS",
    "OFFSET",
    "ON",
    "ORDER",
    "PIVOT",
    "QUALIFY",
    "RIGHT",
    "SEMI",
    "SORT",
    "TABLESAMPLE",
    "UNION",
    "UNPIVOT",
    "USING",
    "WATERMARK",
    "WHERE",
    "WINDOW",
    "WITH"
  )

  private val RelationContextStarts = Set("FROM", "JOIN", "LATERAL")
  private val RelationContextStops = Set(
    "EXCEPT",
    "GROUP",
    "HAVING",
    "INTERSECT",
    "LIMIT",
    "ON",
    "ORDER",
    "QUALIFY",
    "SELECT",
    "UNION",
    "WHERE",
    "WINDOW"
  )

  def parse(queryText: String, delegate: ParserInterface): LogicalPlan = {
    val tokens            = tokenize(queryText)
    val tokenDepths       = depths(tokens)
    val existingNames     = tokens.iterator.filter(_.identifier).map(identifierText).map(lower).toSet
    val allocatedNames    = mutable.HashSet.empty[String]
    val sourceBindings    = mutable.ArrayBuffer.empty[SourceBinding]
    val watermarkBindings = mutable.ArrayBuffer.empty[WatermarkBinding]
    val edits             = mutable.ArrayBuffer.empty[TextEdit]

    tokens.indices.foreach { index =>
      val token = tokens(index)
      if (token.keyword("STREAM") && isStreamRelationPosition(tokens, tokenDepths, index)) {
        parseStreamRelation(queryText, tokens, index, delegate, existingNames, allocatedNames)
          .foreach { case (binding, edit) =>
            sourceBindings += binding
            edits += edit
          }
      }
    }

    tokens.indices.foreach { index =>
      val token = tokens(index)
      if (
        token.keyword("WATERMARK") &&
        isInRelationClause(tokens, tokenDepths, index)
      ) {
        val (binding, edit) =
          parseWatermark(queryText, tokens, tokenDepths, index, delegate, existingNames, allocatedNames)
        watermarkBindings += binding
        edits += edit
      }
    }

    ensureNonOverlapping(queryText, edits.toSeq)
    val rewritten = applyEdits(queryText, edits.toSeq)
    val parsed    = delegate.parseQuery(rewritten)
    bindMarkers(queryText, parsed, sourceBindings.toSeq, watermarkBindings.toSeq)
  }

  private[parser] def isStreamingStatement(sqlText: String): Boolean =
    try {
      val tokens = tokenize(sqlText)
      startsWith(tokens, "CREATE", "STREAMING", "TABLE") ||
      startsWith(tokens, "SHOW", "STREAMING", "TABLES") ||
      startsWith(tokens, "ALTER", "STREAMING", "TABLE") ||
      startsWith(tokens, "DROP", "STREAMING", "TABLE")
    } catch {
      case _: ParseException => false
    }

  private[parser] def parseStringLiteral(literalText: String, sqlText: String): String =
    try {
      if (
        literalText.length < 2 ||
        literalText.head != '\'' ||
        literalText.last != '\''
      ) {
        parseError(sqlText, s"Expected a single-quoted string literal but found: $literalText")
      }
      val normalized = new java.lang.StringBuilder("'")
      val body       = literalText.substring(1, literalText.length - 1)
      var index      = 0
      while (index < body.length) {
        if (
          body.charAt(index) == '\'' &&
          index + 1 < body.length &&
          body.charAt(index + 1) == '\''
        ) {
          normalized.append("\\'")
          index += 2
        } else {
          normalized.append(body.charAt(index))
          index += 1
        }
      }
      normalized.append('\'')
      ParserUtils.unescapeSQLString(normalized.toString())
    } catch {
      case e: ParseException => throw e
      case NonFatal(e) =>
        parseError(sqlText, s"Invalid string literal $literalText: ${e.getMessage}")
    }

  private def parseStreamRelation(
      sqlText: String,
      tokens: IndexedSeq[SqlToken],
      streamIndex: Int,
      delegate: ParserInterface,
      existingNames: Set[String],
      allocatedNames: mutable.Set[String]
  ): Option[(SourceBinding, TextEdit)] = {
    var next = streamIndex + 1
    if (next >= tokens.length) {
      parseError(sqlText, "STREAM must be followed by a multipart table identifier")
    }

    val parenthesized = tokens(next).symbol("(")
    if (parenthesized) next += 1

    val nameStart      = next
    val (_, afterName) = parseMultipartIdentifier(sqlText, tokens, next)
    val identifierSql  = sqlText.substring(tokens(nameStart).start, tokens(afterName - 1).end)
    val name           = delegate.parseMultipartIdentifier(identifierSql)
    next = afterName

    if (parenthesized) {
      if (next >= tokens.length || !tokens(next).symbol(")")) {
        parseError(sqlText, "STREAM(<table>) requires a closing parenthesis")
      }
      next += 1
    }

    val options =
      if (next < tokens.length && tokens(next).keyword("WITH")) {
        val parsed = parseProperties(sqlText, tokens, next + 1, "stream reader WITH")
        next = parsed.nextToken
        rejectContradictoryOffsets(sqlText, parsed.values)
        parsed.values
      } else {
        Map.empty[String, String]
      }

    val marker = freshMarker("s", existingNames, allocatedNames)
    Some(
      SourceBinding(marker, name, options) ->
        TextEdit(tokens(streamIndex).start, tokens(next - 1).end, quoted(marker))
    )
  }

  private def parseWatermark(
      sqlText: String,
      tokens: IndexedSeq[SqlToken],
      tokenDepths: Array[Int],
      watermarkIndex: Int,
      delegate: ParserInterface,
      existingNames: Set[String],
      allocatedNames: mutable.Set[String]
  ): (WatermarkBinding, TextEdit) = {
    val clauseDepth = tokenDepths(watermarkIndex)
    val delayIndex = findDelayOf(tokens, tokenDepths, watermarkIndex + 1, clauseDepth).getOrElse {
      parseError(sqlText, "WATERMARK requires DELAY OF INTERVAL")
    }
    if (delayIndex == watermarkIndex + 1) {
      parseError(sqlText, "WATERMARK requires an event-time expression")
    }

    val ofIndex       = delayIndex + 1
    val intervalIndex = delayIndex + 2
    if (
      intervalIndex >= tokens.length ||
      !tokens(ofIndex).keyword("OF") ||
      !tokens(intervalIndex).keyword("INTERVAL")
    ) {
      parseError(sqlText, "WATERMARK requires DELAY OF INTERVAL")
    }

    val expressionText =
      sqlText.substring(tokens(watermarkIndex + 1).start, tokens(delayIndex - 1).end)
    val eventTime = toNamedExpression(sqlText, expressionText, delegate)

    val intervalEnd  = parseIntervalEnd(sqlText, tokens, intervalIndex)
    val intervalText = sqlText.substring(tokens(intervalIndex).start, tokens(intervalEnd - 1).end)
    val delay =
      try {
        val parsed = IntervalUtils.fromIntervalString(intervalText)
        if (IntervalUtils.isNegative(parsed)) {
          parseError(sqlText, s"Watermark delay must not be negative: $intervalText")
        }
        parsed
      } catch {
        case e: ParseException => throw e
        case NonFatal(e) =>
          parseError(sqlText, s"Invalid watermark interval $intervalText: ${e.getMessage}")
      }

    val parsedAlias = parseAlias(sqlText, tokens, intervalEnd)
    val marker      = freshMarker("w", existingNames, allocatedNames)
    val editEnd =
      if (parsedAlias.nextToken > intervalEnd) tokens(parsedAlias.nextToken - 1).end
      else tokens(intervalEnd - 1).end

    WatermarkBinding(marker, eventTime, delay, parsedAlias.value) ->
      TextEdit(tokens(watermarkIndex).start, editEnd, s"AS ${quoted(marker)}")
  }

  private def findDelayOf(
      tokens: IndexedSeq[SqlToken],
      tokenDepths: Array[Int],
      start: Int,
      clauseDepth: Int
  ): Option[Int] = {
    var index = start
    while (index + 2 < tokens.length) {
      if (
        tokenDepths(index) == clauseDepth &&
        tokens(index).keyword("DELAY") &&
        tokens(index + 1).keyword("OF") &&
        tokens(index + 2).keyword("INTERVAL")
      ) {
        return Some(index)
      }
      if (tokenDepths(index) < clauseDepth) return None
      index += 1
    }
    None
  }

  private def parseIntervalEnd(sqlText: String, tokens: IndexedSeq[SqlToken], intervalIndex: Int): Int = {
    var next = intervalIndex + 1

    def parseValue(): Unit = {
      if (
        next < tokens.length &&
        (tokens(next).symbol("+") || tokens(next).symbol("-"))
      ) {
        next += 1
      }
      if (
        next >= tokens.length ||
        (tokens(next).kind != NumberToken && tokens(next).kind != StringToken)
      ) {
        parseError(sqlText, "INTERVAL requires a numeric or string value")
      }
      next += 1
    }

    def parseUnit(): Unit = {
      if (
        next >= tokens.length ||
        tokens(next).kind != WordToken ||
        !IntervalUnits.contains(tokens(next).text.toUpperCase(Locale.ROOT))
      ) {
        parseError(sqlText, "INTERVAL requires a supported time unit")
      }
      next += 1
    }

    parseValue()
    parseUnit()

    if (next < tokens.length && tokens(next).keyword("TO")) {
      next += 1
      parseUnit()
    } else {
      while (startsIntervalValue(tokens, next)) {
        parseValue()
        parseUnit()
      }
    }
    next
  }

  private def startsIntervalValue(tokens: IndexedSeq[SqlToken], index: Int): Boolean =
    index < tokens.length && (
      tokens(index).kind == NumberToken ||
        tokens(index).kind == StringToken ||
        tokens(index).symbol("+") ||
        tokens(index).symbol("-")
    )

  private def parseAlias(sqlText: String, tokens: IndexedSeq[SqlToken], start: Int): ParsedAlias = {
    if (start >= tokens.length) return ParsedAlias(None, start)

    val explicit = tokens(start).keyword("AS")
    val aliasIndex =
      if (explicit) start + 1
      else if (isImplicitAlias(tokens(start))) start
      else return ParsedAlias(None, start)

    if (aliasIndex >= tokens.length || !tokens(aliasIndex).identifier) {
      parseError(sqlText, "Relation alias requires an identifier")
    }

    val aliasName = identifierText(tokens(aliasIndex))
    var next      = aliasIndex + 1
    val columns =
      if (next < tokens.length && tokens(next).symbol("(")) {
        val (names, afterColumns) = parseIdentifierList(sqlText, tokens, next)
        next = afterColumns
        names
      } else {
        Seq.empty[String]
      }
    ParsedAlias(Some(AliasSpec(aliasName, columns)), next)
  }

  private def isImplicitAlias(token: SqlToken): Boolean =
    token.kind == BacktickToken ||
      (token.kind == WordToken &&
        !RelationBoundaryKeywords.contains(token.text.toUpperCase(Locale.ROOT)) &&
        !token.keyword("AS"))

  private def parseIdentifierList(sqlText: String, tokens: IndexedSeq[SqlToken], openIndex: Int): (Seq[String], Int) = {
    val names = mutable.ArrayBuffer.empty[String]
    var next  = openIndex + 1
    var done  = false

    while (!done) {
      if (next >= tokens.length || !tokens(next).identifier) {
        parseError(sqlText, "Column alias list requires an identifier")
      }
      names += identifierText(tokens(next))
      next += 1
      if (next >= tokens.length) {
        parseError(sqlText, "Column alias list requires a closing parenthesis")
      } else if (tokens(next).symbol(",")) {
        next += 1
      } else if (tokens(next).symbol(")")) {
        next += 1
        done = true
      } else {
        parseError(sqlText, s"Unexpected token in column alias list: ${tokens(next).text}")
      }
    }
    names.toSeq -> next
  }

  private def parseProperties(
      sqlText: String,
      tokens: IndexedSeq[SqlToken],
      openIndex: Int,
      clauseName: String
  ): ParsedProperties = {
    if (openIndex >= tokens.length || !tokens(openIndex).symbol("(")) {
      parseError(sqlText, s"$clauseName requires a parenthesized option list")
    }

    val entries = mutable.ArrayBuffer.empty[(String, String)]
    var next    = openIndex + 1
    var done    = false

    if (next < tokens.length && tokens(next).symbol(")")) {
      parseError(sqlText, s"$clauseName requires at least one option")
    }

    while (!done) {
      val (key, afterKey) = parsePropertyKey(sqlText, tokens, next)
      next = afterKey
      if (
        next >= tokens.length ||
        (!tokens(next).symbol("=") && !tokens(next).symbol("=="))
      ) {
        parseError(sqlText, s"$clauseName option '$key' requires '='")
      }
      next += 1
      if (next >= tokens.length) {
        parseError(sqlText, s"$clauseName option '$key' requires a value")
      }
      val valueToken = tokens(next)
      val value =
        valueToken.kind match {
          case StringToken =>
            parseStringLiteral(valueToken.text, sqlText)
          case WordToken | NumberToken =>
            valueToken.text
          case _ =>
            parseError(sqlText, s"$clauseName option '$key' has an invalid value")
        }
      entries += key -> value
      next += 1

      if (next >= tokens.length) {
        parseError(sqlText, s"$clauseName requires a closing parenthesis")
      } else if (tokens(next).symbol(",")) {
        next += 1
      } else if (tokens(next).symbol(")")) {
        next += 1
        done = true
      } else {
        parseError(sqlText, s"Unexpected token in $clauseName: ${tokens(next).text}")
      }
    }

    val duplicate = entries
      .groupBy { case (key, _) => lower(key) }
      .collectFirst { case (_, values) if values.size > 1 => values.head._1 }
    duplicate.foreach { key =>
      parseError(sqlText, s"$clauseName names option '$key' more than once")
    }
    ParsedProperties(entries.toMap, next)
  }

  private def parsePropertyKey(sqlText: String, tokens: IndexedSeq[SqlToken], start: Int): (String, Int) = {
    if (start >= tokens.length) {
      parseError(sqlText, "Option list requires a key")
    }
    if (tokens(start).kind == StringToken) {
      return parseStringLiteral(tokens(start).text, sqlText) -> (start + 1)
    }

    val (parts, next) = parseMultipartIdentifier(sqlText, tokens, start)
    parts.mkString(".") -> next
  }

  private def rejectContradictoryOffsets(sqlText: String, options: Map[String, String]): Unit = {
    val keys = options.keysIterator.map(lower).toSet
    if (keys.contains("startingversion") && keys.contains("startingtimestamp")) {
      parseError(
        sqlText,
        "stream reader WITH cannot specify both startingVersion and startingTimestamp"
      )
    }
  }

  private def parseMultipartIdentifier(
      sqlText: String,
      tokens: IndexedSeq[SqlToken],
      start: Int
  ): (Seq[String], Int) = {
    if (start >= tokens.length || !tokens(start).identifier) {
      parseError(sqlText, "Expected a multipart identifier")
    }

    val parts = mutable.ArrayBuffer(identifierText(tokens(start)))
    var next  = start + 1
    while (next < tokens.length && tokens(next).symbol(".")) {
      if (next + 1 >= tokens.length || !tokens(next + 1).identifier) {
        parseError(sqlText, "Multipart identifier cannot end with '.'")
      }
      parts += identifierText(tokens(next + 1))
      next += 2
    }
    parts.toSeq -> next
  }

  private def toNamedExpression(sqlText: String, expressionText: String, delegate: ParserInterface): NamedExpression =
    delegate.parseExpression(expressionText) match {
      case _: MultiAlias =>
        parseError(sqlText, "WATERMARK does not support multiple expression aliases")
      case named: NamedExpression => named
      case expression: Expression => UnresolvedAlias(expression)
    }

  private def bindMarkers(
      sqlText: String,
      plan: LogicalPlan,
      sources: Seq[SourceBinding],
      watermarks: Seq[WatermarkBinding]
  ): LogicalPlan = {
    val sourcesByMarker    = sources.map(binding => binding.marker -> binding).toMap
    val watermarksByMarker = watermarks.map(binding => binding.marker -> binding).toMap
    val usedSources        = mutable.HashMap.empty[String, Int].withDefaultValue(0)
    val usedWatermarks     = mutable.HashMap.empty[String, Int].withDefaultValue(0)

    def bindCurrent(current: LogicalPlan): LogicalPlan = current match {
      case relation: UnresolvedRelation
          if relation.multipartIdentifier.size == 1 &&
            sourcesByMarker.contains(relation.multipartIdentifier.head) =>
        val marker  = relation.multipartIdentifier.head
        val binding = sourcesByMarker(marker)
        usedSources.update(marker, usedSources(marker) + 1)
        val replacement = UnresolvedRelation(
          binding.name,
          new CaseInsensitiveStringMap(binding.options.asJava),
          isStreaming = true
        )
        replacement.copyTagsFrom(relation)
        replacement

      case alias @ SubqueryAlias(identifier, child)
          if identifier.qualifier.isEmpty && watermarksByMarker.contains(identifier.name) =>
        val marker  = identifier.name
        val binding = watermarksByMarker(marker)
        usedWatermarks.update(marker, usedWatermarks(marker) + 1)
        val aliasedChild = binding.alias match {
          case Some(aliasSpec) =>
            val withColumnAliases =
              if (aliasSpec.columns.nonEmpty)
                UnresolvedSubqueryColumnAliases(aliasSpec.columns, child)
              else
                child
            SubqueryAlias(aliasSpec.name, withColumnAliases)
          case None =>
            child
        }
        val replacement =
          UnresolvedStreamingWatermark(binding.eventTime, binding.delay, aliasedChild)
        replacement.copyTagsFrom(alias)
        replacement

      case other =>
        other
    }

    def bindPlan(current: LogicalPlan): LogicalPlan = {
      val withChildren = current.mapChildren(bindPlan)
      val withCtes = withChildren match {
        case unresolved: UnresolvedWith =>
          val rebound = unresolved.copy(
            cteRelations = unresolved.cteRelations.map { case (name, alias) =>
              name -> bindPlan(alias).asInstanceOf[SubqueryAlias]
            }
          )
          rebound.copyTagsFrom(unresolved)
          rebound
        case other =>
          other
      }
      val withSubqueries = withCtes.transformExpressionsUp { case expression: SubqueryExpression =>
        expression.withNewPlan(bindPlan(expression.plan))
      }
      bindCurrent(withSubqueries)
    }

    val bound = bindPlan(plan)
    verifyBindings(sqlText, "stream relation", sourcesByMarker.keySet, usedSources.toMap)
    verifyBindings(sqlText, "watermark", watermarksByMarker.keySet, usedWatermarks.toMap)
    bound
  }

  private def verifyBindings(
      sqlText: String,
      bindingType: String,
      expected: Set[String],
      actual: Map[String, Int]
  ): Unit =
    expected.foreach { marker =>
      val count = actual.getOrElse(marker, 0)
      if (count != 1) {
        parseError(
          sqlText,
          s"Internal $bindingType marker '$marker' resolved $count times instead of exactly once"
        )
      }
    }

  private def isStreamRelationPosition(tokens: IndexedSeq[SqlToken], tokenDepths: Array[Int], index: Int): Boolean = {
    if (index == 0) return false
    val previous = tokens(index - 1)
    if (RelationContextStarts.exists(previous.keyword)) return true
    if (previous.symbol(",")) return isInRelationClause(tokens, tokenDepths, index)
    if (previous.symbol("(")) return openingParenthesisStartsRelation(tokens, index - 1)
    false
  }

  private def isInRelationClause(tokens: IndexedSeq[SqlToken], tokenDepths: Array[Int], index: Int): Boolean = {
    val targetDepth = tokenDepths(index)
    var current     = index - 1
    while (current >= 0) {
      if (tokenDepths(current) < targetDepth) return false
      if (tokenDepths(current) == targetDepth && tokens(current).kind == WordToken) {
        val keyword = tokens(current).text.toUpperCase(Locale.ROOT)
        if (RelationContextStarts.contains(keyword)) return true
        if (RelationContextStops.contains(keyword)) return false
      }
      current -= 1
    }
    false
  }

  private def openingParenthesisStartsRelation(tokens: IndexedSeq[SqlToken], openIndex: Int): Boolean = {
    var current = openIndex - 1
    while (current >= 0 && tokens(current).symbol("(")) current -= 1
    current >= 0 && (
      RelationContextStarts.exists(tokens(current).keyword) ||
        tokens(current).symbol(",")
    )
  }

  private def depths(tokens: IndexedSeq[SqlToken]): Array[Int] = {
    val result = Array.ofDim[Int](tokens.length)
    var depth  = 0
    tokens.indices.foreach { index =>
      if (tokens(index).symbol(")")) depth -= 1
      result(index) = depth
      if (tokens(index).symbol("(")) depth += 1
    }
    result
  }

  private def freshMarker(category: String, existingNames: Set[String], allocatedNames: mutable.Set[String]): String = {
    var index     = allocatedNames.size
    var candidate = s"__openivm_${category}_$index"
    while (existingNames.contains(lower(candidate)) || allocatedNames.contains(lower(candidate))) {
      index += 1
      candidate = s"__openivm_${category}_$index"
    }
    allocatedNames += lower(candidate)
    candidate
  }

  private def ensureNonOverlapping(sqlText: String, edits: Seq[TextEdit]): Unit = {
    val sorted = edits.sortBy(_.start)
    sorted.sliding(2).foreach {
      case Seq(left, right) if left.end > right.start =>
        parseError(sqlText, "Overlapping STREAM/WATERMARK clauses are not valid")
      case _ =>
    }
  }

  private def applyEdits(sqlText: String, edits: Seq[TextEdit]): String = {
    val result = new java.lang.StringBuilder(sqlText)
    edits.sortBy(_.start).reverseIterator.foreach { edit =>
      result.replace(edit.start, edit.end, edit.replacement)
    }
    result.toString()
  }

  private def startsWith(tokens: IndexedSeq[SqlToken], keywords: String*): Boolean =
    tokens.length >= keywords.length &&
      keywords.indices.forall(index => tokens(index).keyword(keywords(index)))

  private def identifierText(token: SqlToken): String =
    token.kind match {
      case BacktickToken =>
        token.text.substring(1, token.text.length - 1).replace("``", "`")
      case _ =>
        token.text
    }

  private def quoted(identifier: String): String =
    s"`${identifier.replace("`", "``")}`"

  private def lower(value: String): String =
    value.toLowerCase(Locale.ROOT)

  private def tokenize(sqlText: String): IndexedSeq[SqlToken] = {
    val tokens = mutable.ArrayBuffer.empty[SqlToken]
    var index  = 0

    while (index < sqlText.length) {
      val ch = sqlText.charAt(index)
      if (Character.isWhitespace(ch)) {
        index += 1
      } else if (ch == '-' && index + 1 < sqlText.length && sqlText.charAt(index + 1) == '-') {
        index += 2
        while (index < sqlText.length && sqlText.charAt(index) != '\n') index += 1
      } else if (ch == '/' && index + 1 < sqlText.length && sqlText.charAt(index + 1) == '*') {
        index = skipBlockComment(sqlText, index)
      } else if (ch == '\'') {
        val end = scanQuoted(sqlText, index, '\'', doubledEscape = true, backslashEscape = true)
        tokens += SqlToken(StringToken, sqlText.substring(index, end), index, end)
        index = end
      } else if (ch == '`') {
        val end = scanQuoted(sqlText, index, '`', doubledEscape = true, backslashEscape = false)
        tokens += SqlToken(BacktickToken, sqlText.substring(index, end), index, end)
        index = end
      } else if (ch == '"') {
        val end = scanQuoted(sqlText, index, '"', doubledEscape = true, backslashEscape = true)
        tokens += SqlToken(DoubleQuoteToken, sqlText.substring(index, end), index, end)
        index = end
      } else if (isWordStart(ch)) {
        val start = index
        index += 1
        while (index < sqlText.length && isWordPart(sqlText.charAt(index))) index += 1
        tokens += SqlToken(WordToken, sqlText.substring(start, index), start, index)
      } else if (
        Character.isDigit(ch) ||
        (ch == '.' && index + 1 < sqlText.length && Character.isDigit(sqlText.charAt(index + 1)))
      ) {
        val start = index
        index = scanNumber(sqlText, index)
        tokens += SqlToken(NumberToken, sqlText.substring(start, index), start, index)
      } else {
        val start = index
        index += 1
        if (index < sqlText.length) {
          val pair = sqlText.substring(start, index + 1)
          if (Set("==", ">=", "<=", "<>", "!=", "<=>", "=>").contains(pair)) index += 1
        }
        tokens += SqlToken(SymbolToken, sqlText.substring(start, index), start, index)
      }
    }
    tokens.toIndexedSeq
  }

  private def skipBlockComment(sqlText: String, start: Int): Int = {
    var index = start + 2
    var depth = 1
    while (index < sqlText.length && depth > 0) {
      if (
        index + 1 < sqlText.length &&
        sqlText.charAt(index) == '/' &&
        sqlText.charAt(index + 1) == '*'
      ) {
        depth += 1
        index += 2
      } else if (
        index + 1 < sqlText.length &&
        sqlText.charAt(index) == '*' &&
        sqlText.charAt(index + 1) == '/'
      ) {
        depth -= 1
        index += 2
      } else {
        index += 1
      }
    }
    if (depth != 0) parseError(sqlText, "Unterminated block comment")
    index
  }

  private def scanQuoted(
      sqlText: String,
      start: Int,
      quote: Char,
      doubledEscape: Boolean,
      backslashEscape: Boolean
  ): Int = {
    var index = start + 1
    while (index < sqlText.length) {
      val ch = sqlText.charAt(index)
      if (backslashEscape && ch == '\\' && index + 1 < sqlText.length) {
        index += 2
      } else if (
        ch == quote &&
        doubledEscape &&
        index + 1 < sqlText.length &&
        sqlText.charAt(index + 1) == quote
      ) {
        index += 2
      } else if (ch == quote) {
        return index + 1
      } else {
        index += 1
      }
    }
    parseError(sqlText, s"Unterminated quoted token beginning with '$quote'")
  }

  private def scanNumber(sqlText: String, start: Int): Int = {
    var index      = start
    var seenDot    = false
    var seenExp    = false
    var expectSign = false
    while (index < sqlText.length) {
      val ch = sqlText.charAt(index)
      if (Character.isDigit(ch)) {
        index += 1
        expectSign = false
      } else if (ch == '.' && !seenDot && !seenExp) {
        seenDot = true
        index += 1
      } else if ((ch == 'e' || ch == 'E') && !seenExp) {
        seenExp = true
        expectSign = true
        index += 1
      } else if ((ch == '+' || ch == '-') && expectSign) {
        expectSign = false
        index += 1
      } else {
        return index
      }
    }
    index
  }

  private def isWordStart(ch: Char): Boolean =
    Character.isLetter(ch) || ch == '_'

  private def isWordPart(ch: Char): Boolean =
    Character.isLetterOrDigit(ch) || ch == '_' || ch == '$'

  private def parseError(sqlText: String, message: String): Nothing =
    throw new ParseException(Some(sqlText), message, Origin(), Origin())
}
