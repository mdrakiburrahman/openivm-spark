package org.openivm.spark.compiler

import java.util.UUID
import java.nio.charset.StandardCharsets

import scala.collection.mutable

/** Per-compilation placeholders for expressions evaluated by Spark, not DuckDB.
  *
  * This helper only encodes and decodes expressions; callers must preserve them
  * throughout native planning and reject any compilation that loses a marker.
  */
private[spark] final class SparkExpressionMarker(opaque: Boolean = false, stableToken: Option[String] = None) {
  import SparkExpressionMarker._

  require(stableToken.forall(_.matches("[0-9a-f]{64}")), "Invalid stable Spark expression marker token")
  private val prefix  = "__openivm_expr_" + stableToken.getOrElse(UUID.randomUUID().toString.replace("-", "")) + "_"
  private val markers = mutable.Map.empty[String, Marker]
  private val seen    = mutable.Set.empty[String]

  def encode(args: Seq[String], result: ResultType)(render: Seq[String] => String): String = {
    require(args.nonEmpty, "A marker must carry at least one argument")
    require(!opaque || !args.exists(hasStructConstructor), "Opaque marker struct arguments must be flattened")
    val id = prefix + markers.size + "__"
    markers.put(id, Marker(args.size, result, render))
    if (opaque) {
      val seed = result match {
        case Text      => "CAST(NULL AS STRING)"
        case Integer   => "CAST(NULL AS INTEGER)"
        case Long      => "CAST(NULL AS BIGINT)"
        case Boolean   => "CAST(NULL AS BOOLEAN)"
        case TextArray => "string_split('openivm_type', ',')"
        case Struct(seed) => seed
      }
      s"__openivm_spark_expression('$id', $seed, ${args.mkString(", ")})"
    } else {
      val payload = s"concat('$id', CAST(row(${args.mkString(", ")}) AS VARCHAR))"
      result match {
        case Text      => payload
        case Integer   => s"CAST($payload AS INTEGER)"
        case Long      => s"CAST($payload AS BIGINT)"
        case Boolean   => s"CAST($payload AS BOOLEAN)"
        case TextArray => s"string_split($payload, '${id}separator')"
        case _: Struct => throw new IllegalArgumentException("Struct markers require opaque mode")
      }
    }
  }

  /** Restore markers in native-emitted SQL. Call on every emitted fragment
    * before asserting completion with [[requireAllSeen]].
    */
  def restore(sql: String): String = {
    val out = SparkFunctionShimSql.rewriteOutsideProtected(sql, protectBackticks = true) { i =>
      parseCall(sql, i, "__openivm_spark_expression")
        .flatMap { call =>
          if (call.args.size < 3) None
          else {
            val idLiteral = call.args.head.trim
            val id =
              if (idLiteral.length >= 2 && idLiteral.head == '\'' && idLiteral.last == '\'')
                idLiteral.substring(1, idLiteral.length - 1)
              else ""
            markers.get(id).flatMap { marker =>
              val seed = call.args(1).replaceAll("\\s+", "").toUpperCase(java.util.Locale.ROOT)
              val allowed = marker.result match {
                case Text    => Set("CAST(NULLASVARCHAR)", "CAST(NULLASSTRING)").contains(seed)
                case Integer => Set("CAST(NULLASINTEGER)", "CAST(NULLASINT)").contains(seed)
                case Long    => Set("CAST(NULLASBIGINT)").contains(seed)
                case Boolean => Set("CAST(NULLASBOOLEAN)").contains(seed)
                case TextArray =>
                  Set(
                    "STRING_SPLIT('OPENIVM_TYPE',',')",
                    "SPLIT('OPENIVM_TYPE',',')",
                    "ARRAY('OPENIVM_TYPE')",
                    "LIST_VALUE('OPENIVM_TYPE')"
                  ).contains(seed)
                case _: Struct =>
                  (seed.startsWith("{") && seed.endsWith("}")) ||
                  seed.startsWith("STRUCT_PACK(") || seed.startsWith("NAMED_STRUCT(")
              }
              if (!allowed) None
              else Some(call.end -> renderMarker(id, call.args.drop(2)))
            }
          }
        }
        .orElse {
          parseCall(sql, i, "string_split").flatMap { call =>
            if (call.args.size != 2) None
            else
              parsePayload(call.args.head).flatMap { case (id, args) =>
                val marker = markers(id)
                if (marker.result != TextArray || call.args(1).trim != s"'${id}separator'") None
                else Some(call.end -> renderMarker(id, args))
              }
          }
        }
        .orElse {
          parseCall(sql, i, "concat").flatMap { call =>
            parsePayload(sql.substring(i, call.end)).flatMap { case (id, args) =>
              if (markers(id).result == TextArray) None
              else Some(call.end -> renderMarker(id, args))
            }
          }
        }
    }
    if (out.contains(prefix))
      throw new IllegalStateException("Unresolved Spark expression marker in native SQL")
    out
  }

  def requireAllSeen(): Unit = {
    val missing = markers.keySet.diff(seen)
    if (missing.nonEmpty)
      throw new IllegalStateException(s"Native compiler lost ${missing.size} Spark expression marker(s)")
  }

  def resetSeen(): Unit = seen.clear()

  private def renderMarker(id: String, args: Seq[String]): String = {
    val marker = markers(id)
    if (args.size != marker.arity)
      throw new IllegalStateException("Native compiler changed a Spark expression marker's arity")
    seen += id
    marker.render(args.map(arg => restoreStructPacks(restore(arg))))
  }

  private def parsePayload(text: String): Option[(String, Seq[String])] = {
    val trimmed = text.trim
    parseCall(trimmed, 0, "concat").filter(_.end == trimmed.length).flatMap { concat =>
      if (concat.args.size != 2) None
      else {
        val literal = concat.args.head.trim
        val id =
          if (literal.length >= 2 && literal.head == '\'' && literal.last == '\'')
            literal.substring(1, literal.length - 1)
          else ""
        if (!markers.contains(id)) None
        else {
          val cast = concat.args(1).trim
          parseCall(cast, 0, "cast").filter(_.end == cast.length).flatMap { parsed =>
            if (parsed.args.size != 1) None
            else {
              val inner = parsed.args.head.trim
              parseCall(inner, 0, "row").flatMap { row =>
                val suffix = inner.substring(row.end).trim
                if (!suffix.equalsIgnoreCase("AS STRING") && !suffix.equalsIgnoreCase("AS VARCHAR")) None
                else Some(id -> row.args)
              }
            }
          }
        }
      }
    }
  }
}

private[compiler] object SparkExpressionMarker {
  sealed trait ResultType
  case object Text      extends ResultType
  case object Integer   extends ResultType
  case object Long      extends ResultType
  case object Boolean   extends ResultType
  case object TextArray extends ResultType
  final case class Struct(seed: String) extends ResultType

  private final case class Marker(arity: Int, result: ResultType, render: Seq[String] => String)
  private final case class Call(end: Int, args: Seq[String])

  private def hasStructConstructor(sql: String): Boolean = {
    var found = false
    SparkFunctionShimSql.rewriteOutsideProtected(sql, protectBackticks = true) { i =>
      if (Seq("struct_pack", "named_struct", "struct", "row").exists(name => parseCall(sql, i, name).isDefined))
        found = true
      None
    }
    found
  }

  private[compiler] def restoreStructPacks(sql: String): String =
    SparkFunctionShimSql.rewriteOutsideProtected(sql, protectBackticks = true) { i =>
      parseCall(sql, i, "struct_pack").map { call =>
        val fields = call.args.flatMap { arg =>
          val field = arg.trim
          val split =
            if (field.startsWith("\"") || field.startsWith("`")) {
              val quote  = field.head
              var pos    = 1
              var closed = false
              while (pos < field.length && !closed) {
                if (field.charAt(pos) == quote) {
                  pos += 1
                  if (pos < field.length && field.charAt(pos) == quote) pos += 1
                  else closed = true
                } else pos += 1
              }
              if (!closed) throw new IllegalStateException("Unterminated struct field name in native SQL")
              pos
            } else {
              val pos = field.indexOf(":=")
              if (pos < 0) throw new IllegalStateException("Missing struct field assignment in native SQL")
              pos
            }
          val identifier = field.substring(0, split).trim
          val tail       = field.substring(split).trim
          if (!tail.startsWith(":="))
            throw new IllegalStateException("Missing struct field assignment in native SQL")
          val name = if (identifier.startsWith("\"") || identifier.startsWith("`")) {
            val quote = identifier.head
            if (identifier.length < 2 || identifier.last != quote)
              throw new IllegalStateException("Invalid struct field name in native SQL")
            identifier.substring(1, identifier.length - 1).replace(s"$quote$quote", quote.toString)
          } else {
            if (identifier.isEmpty || !identifier.forall(c => c.isLetterOrDigit || c == '_' || c == '$'))
              throw new IllegalStateException("Invalid struct field name in native SQL")
            identifier
          }
          val value = tail.substring(2).trim
          if (value.isEmpty) throw new IllegalStateException("Missing struct field value in native SQL")
          val hex = name.getBytes(StandardCharsets.UTF_8).map(b => f"${b & 0xff}%02x").mkString
          Seq(s"CAST(X'$hex' AS STRING)", restoreStructPacks(value))
        }
        call.end -> s"named_struct(${fields.mkString(", ")})"
      }
    }

  /** Parses one function call without interpreting quoted SQL or comments. */
  private def parseCall(sql: String, start: Int, name: String): Option[Call] = {
    if (start > 0 && (sql.charAt(start - 1).isLetterOrDigit || sql.charAt(start - 1) == '_')) return None
    val nameEnd = start + name.length
    if (nameEnd > sql.length || !sql.regionMatches(true, start, name, 0, name.length)) return None
    var i = nameEnd
    while (i < sql.length && sql.charAt(i).isWhitespace) i += 1
    if (i >= sql.length || sql.charAt(i) != '(') return None
    i += 1
    var argStart = i
    var depth    = 1
    var brackets = 0
    var braces   = 0
    val args     = mutable.ArrayBuffer.empty[String]
    while (i < sql.length) {
      val c = sql.charAt(i)
      if (c == '\'' || c == '"' || c == '`') {
        val quote = c
        i += 1
        var closed = false
        while (i < sql.length && !closed) {
          if (sql.charAt(i) == quote) {
            i += 1
            if (i < sql.length && sql.charAt(i) == quote) i += 1
            else closed = true
          } else i += 1
        }
        if (!closed) return None
      } else if (i + 1 < sql.length && sql.startsWith("--", i)) {
        i = sql.indexOf('\n', i + 2) match {
          case -1  => sql.length
          case end => end + 1
        }
      } else if (i + 1 < sql.length && sql.startsWith("/*", i)) {
        val end = sql.indexOf("*/", i + 2)
        if (end < 0) return None
        i = end + 2
      } else {
        c match {
          case '(' => depth += 1
          case ')' =>
            depth -= 1
            if (depth == 0) {
              val arg = sql.substring(argStart, i).trim
              if (arg.nonEmpty || args.nonEmpty) args += arg
              return Some(Call(i + 1, args.toSeq))
            }
          case '[' => brackets += 1
          case ']' => brackets -= 1
          case '{' => braces += 1
          case '}' => braces -= 1
          case ',' if depth == 1 && brackets == 0 && braces == 0 =>
            args += sql.substring(argStart, i).trim
            argStart = i + 1
          case _ =>
        }
        i += 1
      }
    }
    None
  }
}
