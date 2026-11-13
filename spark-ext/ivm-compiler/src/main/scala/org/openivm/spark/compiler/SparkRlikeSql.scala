package org.openivm.spark.compiler

import java.lang.reflect.{InvocationTargetException, Method}

import org.apache.spark.sql.catalyst.parser.CatalystSqlParser

import scala.collection.mutable.ArrayBuffer

/** Compiler-copy normalization; never replace the stored Spark model SQL. */
private[compiler] object SparkRlikeSql {
  private case class Edit(start: Int, end: Int, text: String)

  // Spark owns lexer configuration, error handling and the private[parser]
  // UpperCaseCharStream. Reflect only at this boundary: referencing ANTLR types
  // here would link our shaded ANTLR runtime to Spark's unshaded parser classes.
  // The bytecode parse(String, Function1) entry point exists on both pinned
  // Spark versions, though Scala declares it protected.
  private val RlikeCandidate = "(?i)\\b(?:rlike|regexp)\\b".r
  private lazy val parseMethod = CatalystSqlParser.getClass.getMethods
    .find { method =>
      method.getName == "parse" && method.getParameterTypes.toSeq.map(_.getName) ==
        Seq("java.lang.String", "scala.Function1")
    }
    .getOrElse(throw new IllegalStateException("Unsupported Spark parser: missing parse(String, Function1)"))

  private def invoke(method: Method, target: AnyRef, args: AnyRef*): AnyRef =
    try method.invoke(target, args: _*)
    catch {
      // In particular, preserve ParseCancellationException for Spark's SLL -> LL retry.
      case e: InvocationTargetException => throw e.getCause
    }

  private def call(target: AnyRef, name: String): AnyRef =
    invoke(target.getClass.getMethod(name), target)

  private def number(target: AnyRef, name: String): Int =
    call(target, name).asInstanceOf[java.lang.Integer].intValue()

  private def field(target: AnyRef, name: String): AnyRef = target.getClass.getField(name).get(target)

  /** Parse untouched Spark SQL before quote removal and literal-escape translation.
    * regexp_like uses the existing compiler macro (DuckDB regexp_matches/search),
    * and is restored to Spark regexp_like before execution. No anchoring is added.
    */
  def normalize(sql: String): String = {
    if (RlikeCandidate.findFirstIn(sql).isEmpty) return sql
    val callback: AnyRef => String = parser => {
      val tree      = call(parser, "singleStatement")
      val rlikeType = parser.getClass.getField("RLIKE").getInt(null)
      val notType   = parser.getClass.getField("NOT").getInt(null)
      val edits     = ArrayBuffer.empty[Edit]

      // ANTLR indexes Unicode code points; String.substring indexes UTF-16.
      def index(offset: Int): Int = sql.offsetByCodePoints(0, offset)
      def visit(node: AnyRef): Unit = {
        if (node.getClass.getSimpleName == "PredicatedContext") {
          val predicate = call(node, "predicate")
          val kind      = if (predicate == null) null else field(predicate, "kind")
          if (kind != null && number(kind, "getType") == rlikeType) {
            val first    = call(predicate, "getStart")
            val negative = number(first, "getType") == notType
            require(
              negative || number(first, "getTokenIndex") == number(kind, "getTokenIndex"),
              "Unsupported negation before RLIKE"
            )
            val left  = call(node, "valueExpression")
            val right = field(predicate, "pattern")
            val start = index(number(call(left, "getStart"), "getStartIndex"))
            val end   = index(number(call(right, "getStop"), "getStopIndex") + 1)
            edits += Edit(start, start, if (negative) "(NOT regexp_like(" else "(regexp_like(")
            edits += Edit(index(number(kind, "getStartIndex")), index(number(kind, "getStopIndex") + 1), ",")
            if (negative)
              edits += Edit(index(number(first, "getStartIndex")), index(number(first, "getStopIndex") + 1), "")
            edits += Edit(end, end, "))")
          }
        }
        val count = number(node, "getChildCount")
        if (count > 0) {
          val childMethod = node.getClass.getMethod("getChild", java.lang.Integer.TYPE)
          var i           = 0
          while (i < count) {
            visit(invoke(childMethod, node, Int.box(i)))
            i += 1
          }
        }
      }
      visit(tree)
      edits.sortBy(edit => -edit.start).foldLeft(sql) { (text, edit) =>
        text.substring(0, edit.start) + edit.text + text.substring(edit.end)
      }
    }
    invoke(parseMethod, CatalystSqlParser, sql, callback).asInstanceOf[String]
  }

  /** Replacement for the existing unsafe regexp_matches name-only regex pass. */
  def restore(sql: String): String = {
    val name                    = "regexp_matches"
    def ident(c: Char): Boolean = c.isLetterOrDigit || c == '_' || c == '$'
    SparkFunctionShimSql.rewriteOutsideProtected(sql, protectBackticks = true) { i =>
      val end      = i + name.length
      val boundary = i == 0 || (!ident(sql.charAt(i - 1)) && sql.charAt(i - 1) != '.')
      if (
        !boundary || end > sql.length || !sql.regionMatches(true, i, name, 0, name.length) ||
        (end < sql.length && ident(sql.charAt(end)))
      ) None
      else {
        var next = end
        while (next < sql.length && sql.charAt(next).isWhitespace) next += 1
        if (next < sql.length && sql.charAt(next) == '(') Some(end -> "regexp_like") else None
      }
    }
  }
}
