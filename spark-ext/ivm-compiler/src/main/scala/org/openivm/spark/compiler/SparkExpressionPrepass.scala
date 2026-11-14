package org.openivm.spark.compiler

import java.nio.charset.StandardCharsets
import java.util.Locale

import scala.collection.mutable

import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  CaseWhen,
  Cast,
  CreateNamedStruct,
  Expression,
  Explode,
  If,
  Literal,
  NamePlaceholder,
  RLike,
  SubqueryExpression
}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.{NaturalJoin, UsingJoin}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, Project, UnresolvedWith}
import org.apache.spark.sql.types.{ArrayType, DateType, StringType, TimestampType}
import org.openivm.spark.compiler.SparkExpressionMarker._

private[spark] object SparkExpressionPrepass {
  val RestoredInitialLoadProperty = "_ivm_spark_expression_initial_load_sql"
  final case class Prepared(sql: String, markers: SparkExpressionMarker)
  private final case class Span(start: Int, end: Int)
  private final case class Edit(span: Span, render: Seq[String] => String)

  private val resultTypes: Map[String, ResultType] = Map(
    "regexp_replace" -> Text,
    "regexp_extract" -> Text,
    "regexp_like"    -> Boolean,
    "sha2"           -> Text,
    "to_json"        -> Text,
    "lower"          -> Text,
    "split"          -> TextArray,
    "size"           -> Integer,
    "unix_timestamp" -> Long,
    "unix_micros"    -> Long,
    "raise_error"    -> Text
  )

  private val allowedFunctions = resultTypes.keySet ++ Set(
    "named_struct",
    "map",
    "trim",
    "ltrim",
    "rtrim",
    "substring",
    "substr",
    "rpad",
    "lpad",
    "replace",
    "date_format",
    "concat",
    "concat_ws",
    "coalesce",
    "nullif",
    "upper",
    "element_at"
  )
  private val allowedNodes = Set(
    "Literal",
    "Cast",
    "RLike",
    "Like",
    "CaseWhen",
    "If",
    "And",
    "Or",
    "Not",
    "IsNull",
    "IsNotNull",
    "EqualTo",
    "EqualNullSafe",
    "GreaterThan",
    "GreaterThanOrEqual",
    "LessThan",
    "LessThanOrEqual",
    "Add",
    "Subtract",
    "Multiply",
    "Divide",
    "Remainder",
    "UnaryMinus",
    "UnaryPositive",
    "In",
    "InSet"
  )

  def prepare(
      sql: String,
      caseSensitive: Boolean,
      stableToken: Option[String] = None,
      analyzedPlan: => Option[LogicalPlan] = None
  ): Prepared = {
    val markers    = new SparkExpressionMarker(opaque = true, stableToken = stableToken)
    val targets    = mutable.ArrayBuffer.empty[(Expression, ResultType, Int)]
    val generators = mutable.ArrayBuffer.empty[(UnresolvedFunction, UnresolvedFunction)]
    lazy val dateGenerators: Set[(Int, Int)] = {
      val spans   = mutable.Set.empty[(Int, Int)]
      val visited = new java.util.IdentityHashMap[LogicalPlan, java.lang.Boolean]()
      def collectExpression(expr: Expression): Unit = {
        expr match {
          case subquery: SubqueryExpression => collectPlan(subquery.plan)
          case explode: Explode if (explode.child.dataType match {
                case ArrayType(DateType, _) => true
                case _                      => false
              }) =>
            for (start <- explode.origin.startIndex; stop <- explode.origin.stopIndex) spans += ((start, stop))
          case _ =>
        }
        expr.children.foreach(collectExpression)
      }
      def collectPlan(plan: LogicalPlan): Unit = {
        if (!visited.containsKey(plan)) {
          visited.put(plan, java.lang.Boolean.TRUE)
          plan.expressions.foreach(collectExpression)
          plan.children.foreach(collectPlan)
          plan.innerChildren.collect { case child: LogicalPlan => child }.foreach(collectPlan)
        }
      }
      analyzedPlan.foreach(collectPlan)
      spans.toSet
    }
    lazy val coercedCases: Map[(Int, Int), ResultType] = {
      val cases                = mutable.Map.empty[(Int, Int), ResultType]
      val visitedAnalyzedPlans = new java.util.IdentityHashMap[LogicalPlan, java.lang.Boolean]()

      def analyzedExpressions(expr: Expression): Unit = {
        expr match {
          case subquery: SubqueryExpression => analyzedPlans(subquery.plan)
          case branch: CaseWhen =>
            val values = branch.branches.map(_._2) ++ branch.elseValue.toSeq
            val inputTypes = values.map {
              case cast: Cast => cast.child.dataType
              case value      => value.dataType
            }.toSet
            val result = branch.dataType match {
              case StringType    => Some(Text)
              case TimestampType => Some(Timestamp)
              case _             => None
            }
            for {
              start <- branch.origin.startIndex
              stop  <- branch.origin.stopIndex
              kind  <- result
              if inputTypes.contains(StringType) && inputTypes.contains(TimestampType)
            } {
              val span = (start, stop)
              cases.get(span).foreach { existing =>
                if (existing != kind) throw new IllegalArgumentException("Conflicting analyzed CASE result types")
              }
              cases.update(span, kind)
            }
          case _ =>
        }
        expr.children.foreach(analyzedExpressions)
      }
      def analyzedPlans(plan: LogicalPlan): Unit = {
        if (!visitedAnalyzedPlans.containsKey(plan)) {
          visitedAnalyzedPlans.put(plan, java.lang.Boolean.TRUE)
          plan.expressions.foreach(analyzedExpressions)
          plan.children.foreach(analyzedPlans)
          plan.innerChildren.collect { case child: LogicalPlan => child }.foreach(analyzedPlans)
        }
      }
      analyzedPlan.foreach(analyzedPlans)
      cases.toMap
    }

    val parsed = CatalystSqlParser.parsePlan(sql)

    def normalized(name: String): String = if (caseSensitive) name else name.toLowerCase(Locale.ROOT)
    val keyRefs                          = mutable.Set.empty[String]
    val otherRefs                        = mutable.Set.empty[String]
    val aliasCounts                      = mutable.Map.empty[String, Int].withDefaultValue(0)
    val starOwners                       = mutable.ArrayBuffer.empty[Set[String]]
    var hasStar                          = false
    var unsupportedPlan                  = false
    val scannedPlans                     = new java.util.IdentityHashMap[LogicalPlan, java.lang.Boolean]()

    def outputPlan(plan: LogicalPlan): LogicalPlan = plan match {
      case withCtes: UnresolvedWith => outputPlan(withCtes.child)
      case other                    => other
    }
    val rootOutput = outputPlan(parsed)

    def scanExpression(expr: Expression, allowMaxBy: Boolean = false): Unit = expr match {
      case _: UnresolvedStar => hasStar = true
      case other
          if (other.getClass.getSimpleName.startsWith("Unresolved") &&
            other.getClass.getSimpleName.contains("Star")) || other.getClass.getSimpleName == "UnresolvedRegex" =>
        hasStar = true
      case attr: UnresolvedAttribute =>
        attr.nameParts.foreach(part => otherRefs += normalized(part))
      case alias: Alias =>
        val name = normalized(alias.name)
        aliasCounts.update(name, aliasCounts(name) + 1)
        scanExpression(alias.child, allowMaxBy)
      case function: UnresolvedFunction
          if allowMaxBy && function.nameParts.size == 1 && function.nameParts.head.equalsIgnoreCase("max_by") &&
            function.arguments.size == 2 && !function.isDistinct && function.filter.isEmpty && !function.ignoreNulls =>
        scanExpression(function.arguments.head)
        scanOrdering(function.arguments(1))
      case subquery: SubqueryExpression =>
        unsupportedPlan = true
        scanPlan(subquery.plan)
        subquery.children.foreach(child => scanExpression(child))
      case other => other.children.foreach(child => scanExpression(child, allowMaxBy))
    }

    // Only direct or nested-struct MAX_BY keys are safe; arbitrary expressions may compare structs natively.
    def scanOrdering(expr: Expression): Unit = expr match {
      case attr: UnresolvedAttribute if attr.nameParts.size == 1 =>
        keyRefs += normalized(attr.nameParts.head)
      case struct: CreateNamedStruct =>
        struct.valExprs.foreach {
          case alias: Alias => scanOrdering(alias.child)
          case value        => scanOrdering(value)
        }
      case function: UnresolvedFunction
          if function.nameParts.size == 1 && function.nameParts.head.equalsIgnoreCase("named_struct") &&
            function.arguments.size % 2 == 0 =>
        function.arguments.grouped(2).foreach { pair =>
          scanExpression(pair.head)
          scanOrdering(pair(1))
        }
      case other => scanExpression(other)
    }

    def scanPlan(plan: LogicalPlan): Unit =
      if (!scannedPlans.containsKey(plan)) {
        scannedPlans.put(plan, java.lang.Boolean.TRUE)
        plan match {
          case project: Project =>
            val structAliases = project.projectList.flatMap {
              case alias: Alias
                  if alias.child.isInstanceOf[CreateNamedStruct] ||
                    (alias.child match {
                      case f: UnresolvedFunction =>
                        f.nameParts.size == 1 && f.nameParts.head.equalsIgnoreCase("named_struct")
                      case _ => false
                    }) =>
                val span = expressionSpan(sql, alias.child)
                if (SparkFunctionShimSql.namedStructTypeSeed(sql.substring(span.start, span.end)).isDefined)
                  Some(normalized(alias.name))
                else None
              case _ => None
            }.toSet
            project.projectList.foreach {
              case _: UnresolvedStar if project eq rootOutput => hasStar = true
              case _: UnresolvedStar                          => starOwners += structAliases
              case attr: UnresolvedAttribute if (project ne rootOutput) && attr.nameParts.size == 1 =>
              case alias: Alias if project eq rootOutput =>
                otherRefs += normalized(alias.name)
                scanExpression(alias)
              case expr => scanExpression(expr)
            }
          case join: Join =>
            join.joinType match {
              case _: NaturalJoin | _: UsingJoin => unsupportedPlan = true
              case _                             =>
            }
            join.expressions.foreach(expr => scanExpression(expr))
          case other =>
            if (
              !Set("UnresolvedWith", "SubqueryAlias", "UnresolvedRelation", "Aggregate", "Filter", "OneRowRelation")
                .contains(other.getClass.getSimpleName)
            ) unsupportedPlan = true
            val isAggregate = other.getClass.getSimpleName == "Aggregate"
            other.expressions.foreach(expr => scanExpression(expr, allowMaxBy = isAggregate))
        }
        plan.children.foreach(scanPlan)
        plan.innerChildren.collect { case child: LogicalPlan => child }.foreach(scanPlan)
      }

    scanPlan(parsed)
    val safeKeyAliases =
      if (hasStar || unsupportedPlan) Set.empty[String]
      else keyRefs.diff(otherRefs).filter(name => aliasCounts(name) == 1 && starOwners.forall(_.contains(name))).toSet

    def canTemplate(expr: Expression): Boolean = expr match {
      case _: UnresolvedAttribute => true
      case struct: CreateNamedStruct =>
        struct.valExprs.forall {
          case alias: Alias => canTemplate(alias.child)
          case other        => canTemplate(other)
        }
      case f: UnresolvedFunction =>
        f.nameParts.size == 1 && allowedFunctions.contains(f.nameParts.head.toLowerCase(Locale.ROOT)) &&
        !f.isDistinct && f.filter.isEmpty && !f.ignoreNulls && f.children.forall(canTemplate)
      case other => allowedNodes.contains(other.getClass.getSimpleName) && other.children.forall(canTemplate)
    }

    def target(expr: Expression): Option[ResultType] = expr match {
      case _: RLike => Some(Boolean)
      case f: UnresolvedFunction if f.nameParts.size == 1 =>
        val name = f.nameParts.head.toLowerCase(Locale.ROOT)
        if (name == "element_at") {
          f.arguments.headOption match {
            case Some(a: UnresolvedFunction) if a.nameParts.size == 1 && a.nameParts.head.equalsIgnoreCase("split") =>
              Some(Text)
            case _ => None
          }
        } else resultTypes.get(name)
      case _ => None
    }

    var nextScope    = 0
    val visitedPlans = new java.util.IdentityHashMap[LogicalPlan, java.lang.Boolean]()
    def expressions(expr: Expression, scope: Int): Unit = expr match {
      case alias: Alias if safeKeyAliases.contains(normalized(alias.name)) && canTemplate(alias.child) =>
        val span = expressionSpan(sql, alias.child)
        SparkFunctionShimSql.namedStructTypeSeed(sql.substring(span.start, span.end)) match {
          case Some(seed) => targets += ((alias.child, Struct(seed), scope))
          case None       => expressions(alias.child, scope)
        }
      case subquery: SubqueryExpression =>
        plans(subquery.plan)
        subquery.children.foreach(child => expressions(child, scope))
      case explode: UnresolvedFunction
          if explode.nameParts.size == 1 && explode.nameParts.head.equalsIgnoreCase("explode") &&
            explode.arguments.size == 1 && !explode.isDistinct && explode.filter.isEmpty && !explode.ignoreNulls &&
            explode.origin.startIndex.isDefined && explode.origin.stopIndex.isDefined &&
            dateGenerators.contains((explode.origin.startIndex.get, explode.origin.stopIndex.get)) =>
        explode.arguments.head match {
          case sequence: UnresolvedFunction
              if sequence.nameParts.size == 1 && sequence.nameParts.head.equalsIgnoreCase("sequence") &&
                sequence.arguments.size == 3 && sequence.arguments
                  .take(2)
                  .forall(_.isInstanceOf[UnresolvedAttribute]) &&
                !sequence.isDistinct && sequence.filter.isEmpty && !sequence.ignoreNulls &&
                sql
                  .substring(
                    expressionSpan(sql, sequence.arguments(2)).start,
                    expressionSpan(sql, sequence.arguments(2)).end
                  )
                  .trim
                  .matches("(?i)INTERVAL\\s*'1'\\s+DAY") =>
            generators += ((explode, sequence))
          case _ => explode.children.foreach(child => expressions(child, scope))
        }
      case branch: CaseWhen
          if branch.branches.size == 1 && branch.branches.head._2.isInstanceOf[UnresolvedAttribute] &&
            branch.elseValue.exists(_.isInstanceOf[UnresolvedAttribute]) &&
            branch.origin.startIndex.isDefined && branch.origin.stopIndex.isDefined &&
            coercedCases.contains((branch.origin.startIndex.get, branch.origin.stopIndex.get)) =>
        targets += ((branch, coercedCases((branch.origin.startIndex.get, branch.origin.stopIndex.get)), scope))
      case branch: CaseWhen =>
        var reachable = true
        branch.branches.foreach { case (condition, value) =>
          if (reachable) {
            expressions(condition, scope)
            condition match {
              case literal: Literal if literal.value == false || literal.value == null =>
              case literal: Literal if literal.value == true =>
                expressions(value, scope)
                reachable = false
              case _ => expressions(value, scope)
            }
          }
        }
        if (reachable) branch.elseValue.foreach(child => expressions(child, scope))
      case branch: If =>
        expressions(branch.predicate, scope)
        branch.predicate match {
          case literal: Literal if literal.value == false || literal.value == null =>
            expressions(branch.falseValue, scope)
          case literal: Literal if literal.value == true => expressions(branch.trueValue, scope)
          case _ =>
            expressions(branch.trueValue, scope)
            expressions(branch.falseValue, scope)
        }
      case branch: UnresolvedFunction
          if branch.nameParts.size == 1 && branch.nameParts.head.equalsIgnoreCase("if") &&
            branch.arguments.size == 3 && !branch.isDistinct && branch.filter.isEmpty && !branch.ignoreNulls =>
        expressions(branch.arguments.head, scope)
        branch.arguments.head match {
          case literal: Literal if literal.value == false || literal.value == null =>
            expressions(branch.arguments(2), scope)
          case literal: Literal if literal.value == true => expressions(branch.arguments(1), scope)
          case _ =>
            branch.arguments.tail.foreach(child => expressions(child, scope))
        }
      case _ =>
        target(expr) match {
          case Some(result) => targets += ((expr, result, scope))
          case None         => expr.children.foreach(child => expressions(child, scope))
        }
    }

    def plans(plan: LogicalPlan): Unit = {
      if (!visitedPlans.containsKey(plan)) {
        visitedPlans.put(plan, java.lang.Boolean.TRUE)
        val scope = nextScope
        nextScope += 1
        plan.expressions.foreach(expr => expressions(expr, scope))
        plan.children.foreach(plans)
        plan.innerChildren.collect { case child: LogicalPlan => child }.foreach(plans)
      }
    }

    plans(parsed)
    val uniqueTargets = mutable.LinkedHashMap.empty[Span, (Expression, ResultType, Int)]
    targets.foreach { case (expr, result, scope) =>
      val span = expressionSpan(sql, expr)
      uniqueTargets.get(span) match {
        case Some((previous, previousResult, previousScope))
            if previousScope == scope && previousResult == result && expr.semanticEquals(previous) =>
        case Some(_) => throw new IllegalArgumentException("Conflicting Spark expressions at the same source span")
        case None    => uniqueTargets.put(span, (expr, result, scope))
      }
    }
    val encodedExpressions = mutable.Map.empty[(Int, String, ResultType, Vector[String]), (Expression, String)]
    val replacements = uniqueTargets.toSeq
      .map { case (span, (expr, result, scope)) =>
        val arguments = mutable.ArrayBuffer.empty[String]
        val body      = template(sql, expr, span, arguments, caseSensitive)
        val inputs    = if (arguments.isEmpty) Seq("0") else arguments.toSeq
        val key       = (scope, sql.substring(span.start, span.end), result, inputs.toVector)
        val encoded = encodedExpressions.get(key) match {
          case Some((previous, existing)) if expr.semanticEquals(previous) => existing
          case _ =>
            val created = markers.encode(inputs, result)(args => body(args))
            encodedExpressions.update(key, expr -> created)
            created
        }
        span -> encoded
      }
    val generatorEdits = generators.distinct.map { case (explode, sequence) =>
      val bounds = sequence.arguments.take(2).map { arg =>
        val span      = expressionSpan(sql, arg)
        val arguments = mutable.ArrayBuffer.empty[String]
        val body      = template(sql, arg, span, arguments, caseSensitive)
        markers.encode(arguments.toSeq, DateBound)(body)
      }
      expressionSpan(sql, explode) ->
        s"CAST(UNNEST(GENERATE_SERIES(${bounds.mkString(", ")}, to_days(1))) AS DATE)"
    }
    val edits  = (replacements ++ generatorEdits).sortBy(_._1.start)
    var cursor = 0
    val output = new StringBuilder
    edits.foreach { case (span, encoded) =>
      if (span.start < cursor) throw new IllegalArgumentException("Overlapping Spark expression spans")
      output ++= sql.substring(cursor, span.start)
      output ++= encoded
      cursor = span.end
    }
    output ++= sql.substring(cursor)
    Prepared(output.toString, markers)
  }

  private def expressionSpan(sql: String, expr: Expression): Span = {
    val origin = originSpan(sql, expr)
    expr match {
      case like: RLike =>
        val left = expressionSpan(sql, like.left)
        if (
          left.end > origin.start || sql.substring(left.end, origin.start).trim.nonEmpty ||
          !sql.substring(origin.start, origin.end).trim.toUpperCase(Locale.ROOT).startsWith("RLIKE")
        )
          throw new IllegalArgumentException("Unsupported RLIKE source span")
        Span(left.start, origin.end)
      case _ =>
        val childSpans = expr.children.map(child => expressionSpan(sql, child))
        Span((childSpans.map(_.start) :+ origin.start).min, (childSpans.map(_.end) :+ origin.end).max)
    }
  }

  private def originSpan(sql: String, expr: Expression): Span = {
    val start = expr.origin.startIndex.getOrElse(throw new IllegalArgumentException("Missing expression source span"))
    val end = expr.origin.stopIndex.getOrElse(throw new IllegalArgumentException("Missing expression source span")) + 1
    if (start < 0 || end <= start || end > sql.length)
      throw new IllegalArgumentException("Invalid expression source span")
    Span(start, end)
  }

  private def template(
      sql: String,
      expr: Expression,
      span: Span,
      arguments: mutable.ArrayBuffer[String],
      caseSensitive: Boolean
  ): Seq[String] => String = {
    val edits = mutable.ArrayBuffer.empty[Edit]
    def visit(node: Expression): Unit = node match {
      case attr: UnresolvedAttribute =>
        val location = originSpan(sql, attr)
        val value    = sql.substring(location.start, location.end)
        CatalystSqlParser.parseExpression(value) match {
          case parsed: UnresolvedAttribute if parsed.nameParts == attr.nameParts =>
          case _ => throw new IllegalArgumentException("Invalid Spark attribute source span")
        }
        val index = arguments.size
        arguments += value
        edits += Edit(
          location,
          args => {
            val rewritten = args(index)
            if (caseSensitive) {
              CatalystSqlParser.parseExpression(rewritten) match {
                case parsed: UnresolvedAttribute =>
                  val suffixLength = math.min(parsed.nameParts.size, attr.nameParts.size)
                  if (parsed.nameParts.takeRight(suffixLength) != attr.nameParts.takeRight(suffixLength))
                    throw new IllegalStateException("Native compiler changed a case-sensitive Spark attribute")
                case _ =>
                  throw new IllegalStateException("Native compiler changed a case-sensitive Spark attribute expression")
              }
            }
            rewritten
          }
        )
      case struct: CreateNamedStruct =>
        val location = originSpan(sql, struct)
        val names = struct.nameExprs.zip(struct.valExprs).map { case (name, value) =>
          val fieldName = (name, value) match {
            case (lit: Literal, _) if lit.value != null => lit.value.toString
            case (NamePlaceholder, alias: Alias)        => alias.name
            case _ => throw new IllegalArgumentException("Struct field name is not a literal or explicit alias")
          }
          val hex = fieldName.getBytes(StandardCharsets.UTF_8).map(b => f"${b & 0xff}%02x").mkString
          s"CAST(X'$hex' AS STRING)"
        }
        val values = struct.valExprs.map { value =>
          val inner = value match {
            case alias: Alias => alias.child
            case other        => other
          }
          val valueSpan = expressionSpan(sql, inner)
          template(sql, inner, valueSpan, arguments, caseSensitive)
        }
        edits += Edit(
          location,
          args =>
            s"named_struct(${names.zip(values).flatMap { case (name, value) => Seq(name, value(args)) }.mkString(", ")})"
        )
      case f: UnresolvedFunction =>
        if (
          f.nameParts.size != 1 || !allowedFunctions.contains(f.nameParts.head.toLowerCase(Locale.ROOT)) ||
          f.isDistinct || f.filter.nonEmpty || f.ignoreNulls
        )
          throw new IllegalArgumentException("Unsupported function inside Spark expression marker")
        f.children.foreach(visit)
      case other =>
        val name = other.getClass.getSimpleName
        if (!allowedNodes.contains(name))
          throw new IllegalArgumentException(s"Unsupported Spark expression inside marker: $name")
        other.children.foreach(visit)
    }
    visit(expr)
    val ordered = edits.sortBy(_.span.start)
    var cursor  = span.start
    ordered.foreach { edit =>
      if (edit.span.start < cursor || edit.span.end > span.end)
        throw new IllegalArgumentException("Overlapping or invalid Spark expression argument spans")
      cursor = edit.span.end
    }
    args => {
      val result = new StringBuilder
      var offset = span.start
      ordered.foreach { edit =>
        result ++= sql.substring(offset, edit.span.start)
        result ++= edit.render(args)
        offset = edit.span.end
      }
      result ++= sql.substring(offset, span.end)
      result.toString
    }
  }
}
