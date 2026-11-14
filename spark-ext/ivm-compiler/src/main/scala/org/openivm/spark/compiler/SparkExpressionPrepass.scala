package org.openivm.spark.compiler

import java.nio.charset.StandardCharsets
import java.util.Locale

import scala.collection.mutable

import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction}
import org.apache.spark.sql.catalyst.expressions.{CreateNamedStruct, Expression, Literal, RLike, SubqueryExpression}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.openivm.spark.compiler.SparkExpressionMarker._

private[compiler] object SparkExpressionPrepass {
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
    "size"           -> Integer
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

  def prepare(sql: String): Prepared = {
    val markers = new SparkExpressionMarker(opaque = true)
    val targets = mutable.ArrayBuffer.empty[(Expression, ResultType)]

    def target(expr: Expression): Option[ResultType] = expr match {
      case _: RLike => Some(Boolean)
      case f: UnresolvedFunction if f.nameParts.size == 1 =>
        val name = f.nameParts.head.toLowerCase(Locale.ROOT)
        if (name == "element_at") {
          f.arguments.headOption match {
            case Some(a: UnresolvedFunction) if a.nameParts.size == 1 && a.nameParts.head.equalsIgnoreCase("split") =>
              Some(Text)
            case _ => throw new IllegalArgumentException("Cannot infer element_at result type")
          }
        } else resultTypes.get(name)
      case _ => None
    }

    def expressions(expr: Expression): Unit = expr match {
      case subquery: SubqueryExpression =>
        plans(subquery.plan)
        subquery.children.foreach(expressions)
      case _ =>
        target(expr) match {
          case Some(result) => targets += expr -> result
          case None         => expr.children.foreach(expressions)
        }
    }

    def plans(plan: LogicalPlan): Unit = {
      plan.expressions.foreach(expressions)
      plan.children.foreach(plans)
      plan.innerChildren.collect { case child: LogicalPlan => child }.foreach(plans)
    }

    plans(CatalystSqlParser.parsePlan(sql))
    val replacements = targets
      .map { case (expr, result) =>
        val span      = expressionSpan(sql, expr)
        val arguments = mutable.ArrayBuffer.empty[String]
        val body      = template(sql, expr, span, arguments)
        val inputs    = if (arguments.isEmpty) Seq("0") else arguments.toSeq
        val encoded   = markers.encode(inputs, result)(args => body(args))
        span -> encoded
      }
      .sortBy(_._1.start)
    var cursor = 0
    val output = new StringBuilder
    replacements.foreach { case (span, encoded) =>
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
      arguments: mutable.ArrayBuffer[String]
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
        edits += Edit(location, args => args(index))
      case struct: CreateNamedStruct =>
        val location = originSpan(sql, struct)
        val names = struct.nameExprs.map {
          case lit: Literal if lit.value != null =>
            val hex = lit.value.toString.getBytes(StandardCharsets.UTF_8).map(b => f"${b & 0xff}%02x").mkString
            s"CAST(X'$hex' AS STRING)"
          case _ => throw new IllegalArgumentException("Struct field name is not a literal")
        }
        val values = struct.valExprs.map { value =>
          val valueSpan = expressionSpan(sql, value)
          template(sql, value, valueSpan, arguments)
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
