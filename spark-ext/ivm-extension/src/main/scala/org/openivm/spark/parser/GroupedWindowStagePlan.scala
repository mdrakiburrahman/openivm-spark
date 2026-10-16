package org.openivm.spark.parser

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import java.util.Locale
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.analysis.{
  UnresolvedAlias,
  UnresolvedAttribute,
  UnresolvedFunction,
  UnresolvedHaving,
  UnresolvedRelation,
  UnresolvedStar
}
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, Literal, NamedExpression, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.{Cross, Inner}
import org.apache.spark.sql.catalyst.plans.logical._
import org.openivm.spark.common.SparkRefreshRewriter
import org.openivm.spark.compiler.LptsSparkDialect

/** Typed pre-window stage. Spark owns parsing/expressions/CTE scope;
  * the existing native compiler still owns the signed joined-input program.
  * No Python, SQL text slicing, new catalog table or source-data action.
  */
private[spark] object GroupedWindowStagePlan {
  final case class Channel(input: String, output: String)
  private val Input = "__openivm_pre_window_input"
  private val State = "__openivm_pre_window_state"
  private val Aggregates = Set(
    "count",
    "count_star",
    "sum",
    "avg",
    "min",
    "max",
    "stddev",
    "stddev_samp",
    "stddev_pop",
    "variance",
    "var_samp",
    "var_pop"
  )
  private def quoted(name: String): String                = "`" + name.replace("`", "``") + "`"
  private def functionName(f: UnresolvedFunction): String = f.nameParts.last.toLowerCase(Locale.ROOT)

  /** Bind external relations before analysis, including nested CTE definitions
    * and correlated subqueries. Only preceding nonrecursive CTEs are visible in
    * each definition. No unknown name may fall through to the live catalog.
    * Native memory.main aliases must be supplied explicitly by the caller.
    */
  def bindReadPlan(spark: SparkSession, plan: LogicalPlan, frames: Map[String, DataFrame]): DataFrame = {
    val normalize: String => String =
      if (spark.sessionState.conf.caseSensitiveAnalysis) identity[String]
      else (s: String) => s.toLowerCase(Locale.ROOT)
    val entries = frames.toVector.map { case (name, df) =>
      spark.sessionState.sqlParser.parseMultipartIdentifier(name).map(normalize).toVector -> df.queryExecution.analyzed
    }
    require(entries.map(_._1).distinct.size == entries.size, "ambiguous stage source binding")
    val inputs = entries.toMap
    def bind(p: LogicalPlan, scope: Set[String]): LogicalPlan = p match {
      case w: UnresolvedWith =>
        var visible = scope
        var local   = Set.empty[String]
        val rebound = SparkParserCompat.rebindCtes(
          w,
          {
            case alias: SubqueryAlias =>
              val name = normalize(alias.identifier.name)
              require(!local.contains(name), "duplicate stage CTE name")
              val definition = bind(alias, visible)
              local += name; visible += name
              definition
            case _ => throw new IllegalArgumentException("unsupported stage CTE binding")
          }
        )
        rebound.withNewChildren(rebound.children.map(child => bind(child, visible)))
      case r: UnresolvedRelation =>
        require(!r.isStreaming, "streaming stage source is unsupported")
        val key = r.multipartIdentifier.map(normalize).toVector
        if (key.size == 1 && scope.contains(key.head)) r
        else
          inputs
            .get(key)
            .map(child => SubqueryAlias(r.multipartIdentifier, child))
            .getOrElse(
              throw new IllegalArgumentException("unbound stage relation " + r.multipartIdentifier.mkString("."))
            )
      case other =>
        other.mapChildren(child => bind(child, scope)).transformExpressionsDown { case s: SubqueryExpression =>
          s.withNewPlan(bind(s.plan, scope))
        }
    }
    org.apache.spark.sql.openivm.StreamingDatasetAccess.ofRows(spark, bind(plan, Set.empty))
  }

  final class InputDelta private[GroupedWindowStagePlan] (
      spark: SparkSession,
      plan: LogicalPlan,
      columns: Vector[String],
      val identity: String
  ) {
    def apply(frames: Map[String, DataFrame]): DataFrame = bindReadPlan(spark, plan, frames).toDF(columns: _*)
  }

  /** Extract the native compiler's one signed joined-input query, retaining its
    * WITH definitions and explicit result columns. No native DML is executed.
    */
  def inputDelta(spark: SparkSession, sql: String, view: String): InputDelta = {
    val target = "openivm_delta_" + view
    def namesTarget(insert: InsertIntoStatement): Boolean = insert.table match {
      case r: UnresolvedRelation =>
        val names = r.multipartIdentifier.map(_.toLowerCase(Locale.ROOT))
        names == Seq(target.toLowerCase(Locale.ROOT)) ||
        names == Seq("memory", "main", target.toLowerCase(Locale.ROOT))
      case _ => false
    }
    val statements = SparkRefreshRewriter
      .splitStatements(sql)
      .map(_.trim)
      .filter(s => Seq("WITH", "INSERT").exists(s.toUpperCase(Locale.ROOT).startsWith))
      .map(s => SparkRefreshRewriter.stripTimestampPredicate(LptsSparkDialect.translate(s)))
    val candidates = statements.flatMap { s =>
      val plan = spark.sessionState.sqlParser.parsePlan(s)
      plan.collect { case i: InsertIntoStatement if namesTarget(i) => (s, plan, i) }
    }
    require(candidates.size == 1, "one native signed-input contribution required")
    val (text, plan, insert) = candidates.head
    val columns              = insert.userSpecifiedCols.toVector
    require(
      columns.nonEmpty && columns.distinct.size == columns.size &&
        columns.count(_ == "openivm_multiplicity") == 1,
      "explicit signed-input columns required"
    )
    val read   = plan.transformUp { case i: InsertIntoStatement => i.query }
    val digest = MessageDigest.getInstance("SHA-256").digest(text.getBytes(UTF_8)).map(b => f"${b & 255}%02x").mkString
    new InputDelta(spark, read, columns, digest)
  }
  private def raw(e: NamedExpression): (Expression, String) = e match {
    case a: Alias => a.child -> a.name
    case a: UnresolvedAlias =>
      a.child match {
        case u: UnresolvedAttribute => u -> u.nameParts.last
        case _                      => throw new IllegalArgumentException("stage output needs an explicit alias")
      }
    case u: UnresolvedAttribute => u -> u.nameParts.last
    case _                      => throw new IllegalArgumentException("unsupported stage output " + e.nodeName)
  }
  private def grouped(alias: SubqueryAlias): Option[(Aggregate, Option[Expression])] = alias.child match {
    case a: Aggregate if a.groupingExpressions.nonEmpty => Some(a -> None)
    case h: UnresolvedHaving =>
      h.child match {
        case a: Aggregate if a.groupingExpressions.nonEmpty => Some(a -> Some(h.havingCondition))
        case _                                              => None
      }
    case _ => None
  }

  final class Contract private[GroupedWindowStagePlan] (
      spark: SparkSession,
      val inputSql: String,
      val groupColumns: Vector[String],
      val channels: Vector[Channel],
      val countName: String,
      val identity: String,
      inputPlan: LogicalPlan,
      summaryPlan: LogicalPlan,
      ordinaryPlan: LogicalPlan,
      postPlan: LogicalPlan,
      declared: Vector[Vector[String]],
      bindings: Map[Vector[String], Vector[String]],
      normalize: String => String
  ) {
    private def execute(plan: LogicalPlan, frames: Map[String, DataFrame]): DataFrame = {
      val entries = frames.toVector.map { case (name, df) =>
        spark.sessionState.sqlParser.parseMultipartIdentifier(name).map(normalize).toVector -> df
      }
      require(entries.map(_._1).distinct.size == entries.size, "ambiguous stage source binding")
      val supplied = entries.toMap
      val aliases  = bindings.flatMap { case (alias, source) => supplied.get(source).map(alias -> _) }
      bindReadPlan(spark, plan, (supplied ++ aliases).map { case (name, df) => name.map(quoted).mkString(".") -> df })
    }
    private def requireSources(frames: Map[String, DataFrame]): Unit = {
      val keys =
        frames.keys.map(n => spark.sessionState.sqlParser.parseMultipartIdentifier(n).map(normalize).toVector).toSet
      require(keys == declared.toSet, "missing or unexpected stage source binding")
    }
    def input(frames: Map[String, DataFrame]): DataFrame = {
      requireSources(frames)
      val df = execute(inputPlan, frames)
      require(
        df.queryExecution.analyzed.collect { case p if !p.expressions.forall(_.deterministic) => p }.isEmpty,
        "nondeterministic stage input"
      )
      df
    }
    def summary(frames: Map[String, DataFrame]): DataFrame = { requireSources(frames); execute(summaryPlan, frames) }
    def ordinary(input: DataFrame): DataFrame              = execute(ordinaryPlan, Map(Input -> input))
    def post(frames: Map[String, DataFrame], summary: DataFrame): DataFrame = {
      requireSources(frames); execute(postPlan, frames + (State -> summary))
    }
  }

  def extract(
      spark: SparkSession,
      query: String,
      sources: Vector[String],
      sourceAliases: Map[String, String] = Map.empty
  ): Contract = {
    val normalize: String => String =
      if (spark.sessionState.conf.caseSensitiveAnalysis) identity[String]
      else (s: String) => s.toLowerCase(Locale.ROOT)
    val declared = sources
      .map(n => spark.sessionState.sqlParser.parseMultipartIdentifier(n).map(normalize).toVector)
      .sortBy(_.mkString("\u0000"))
    require(declared.nonEmpty && declared.distinct.size == declared.size, "unique declared sources required")
    def identifier(name: String) = spark.sessionState.sqlParser.parseMultipartIdentifier(name).map(normalize).toVector
    val aliasesToSources = sourceAliases.toVector.map { case (alias, source) =>
      identifier(alias) -> identifier(source)
    }
    require(aliasesToSources.forall(p => declared.contains(p._2)), "alias must name a declared source")
    val bindingEntries = declared.map(k => k -> k) ++ aliasesToSources
    require(bindingEntries.groupBy(_._1).values.forall(_.map(_._2).distinct.size == 1), "ambiguous source alias")
    val bindings = bindingEntries.toMap
    val root = spark.sessionState.sqlParser.parsePlan(query) match {
      case w: UnresolvedWith => w
      case _                 => throw new IllegalArgumentException("one top-level CTE graph required")
    }
    val aliases = scala.collection.mutable.ArrayBuffer.empty[SubqueryAlias]
    SparkParserCompat.rebindCtes(root, p => { aliases += p.asInstanceOf[SubqueryAlias]; p })
    def nestedWith(p: LogicalPlan): Boolean = p match {
      case _: UnresolvedWith => true
      case _ =>
        p.children.exists(nestedWith) || p.expressions.exists(_.exists {
          case s: SubqueryExpression => nestedWith(s.plan)
          case _                     => false
        })
    }
    require(!nestedWith(root.child) && aliases.forall(a => !nestedWith(a.child)), "nested CTE scope is unsupported")
    val cteNames = aliases.map(a => normalize(a.identifier.name)).toSet
    require(cteNames.size == aliases.size, "duplicate stage CTE name")
    val reserved = Set(normalize(Input), normalize(State))
    require(
      (cteNames intersect bindings.keys.map(_.last).toSet).isEmpty &&
        (reserved intersect (cteNames ++ bindings.keys.map(_.last))).isEmpty,
      "stage source/CTE name shadowing"
    )
    val candidates = aliases.flatMap(a => grouped(a).map(g => (a, g._1, g._2)))
    require(candidates.size == 1, "exactly one grouped CTE required")
    val (selected, aggregate, having) = candidates.head
    val outputs                       = aggregate.aggregateExpressions.toVector.map(raw)
    require(outputs.map(o => normalize(o._2)).distinct.size == outputs.size, "duplicate grouped output name")
    val projected = scala.collection.mutable.LinkedHashMap.empty[String, (Expression, String)]
    val groups = aggregate.groupingExpressions.toVector.map { e =>
      val name = outputs
        .find(_._1.sql == e.sql)
        .map(_._2)
        .getOrElse(throw new IllegalArgumentException("group key must be exported"))
      projected.update(e.sql, e -> name); name
    }
    require(groups.distinct.size == groups.size, "duplicate grouped identity")
    def project(e: Expression, prefix: String): String = projected.get(e.sql).map(_._2).getOrElse {
      var i    = 0; while (projected.valuesIterator.exists(p => normalize(p._2) == normalize(prefix + i))) i += 1
      val name = prefix + i; projected.update(e.sql, e -> name); name
    }
    val channels = scala.collection.mutable.ArrayBuffer.empty[Channel]
    val ordinary = scala.collection.mutable.ArrayBuffer.empty[NamedExpression]
    val exported = outputs.map { case (e, n) => e.sql -> n }.toMap
    val countNames = outputs.collect {
      case (f: UnresolvedFunction, n)
          if !f.isDistinct && f.filter.isEmpty &&
            functionName(f) == "count" && f.arguments.size == 1 && (f.arguments.head match {
              case _: UnresolvedStar => true
              case l: Literal        => l.value != null
              case _                 => false
            }) =>
        n
    }
    require(countNames.size == 1, "one exported COUNT(*) required for exact group cardinality")
    def rewrite(e: Expression): Expression = e.transformDown {
      case x if aggregate.groupingExpressions.exists(_.sql == x.sql) => UnresolvedAttribute(Seq(projected(x.sql)._2))
      case f: UnresolvedFunction if Aggregates.contains(functionName(f)) =>
        require(!f.isDistinct && f.filter.isEmpty && !f.ignoreNulls, "unsupported ordinary aggregate modifier")
        val args = f.arguments.map {
          case star: UnresolvedStar                     => star
          case l: Literal if functionName(f) == "count" => l
          case arg                                      => UnresolvedAttribute(Seq(project(arg, "rd_measure_")))
        }
        f.copy(arguments = args)
    }
    outputs.foreach {
      case (f: UnresolvedFunction, name) if f.isDistinct =>
        require(
          functionName(f) == "count" && f.arguments.size == 1 && f.filter.isEmpty && !f.ignoreNulls,
          "unsupported DISTINCT stage expression"
        )
        channels += Channel(project(f.arguments.head, "rd_distinct_"), name)
      case (e, name) => ordinary += Alias(rewrite(e), name)()
    }
    require(channels.nonEmpty, "DISTINCT stage required")
    val available = projected.valuesIterator.map(_._2).toSet
    ordinary.foreach(e =>
      require(
        e.collect { case a: UnresolvedAttribute => a }
          .forall(a => a.nameParts.size == 1 && available.contains(a.nameParts.head)),
        "unprojected ordinary source expression"
      )
    )
    val inputColumns = projected.values.toVector.map { case (e, name) => Alias(e, name)(): NamedExpression }
    val inputPlan    = Project(inputColumns, aggregate.child)
    val ordinaryPlan =
      Aggregate(groups.map(n => UnresolvedAttribute(Seq(n))), ordinary.toVector, UnresolvedRelation(Seq(Input)))
    def relation(p: LogicalPlan): String = p match {
      case r: UnresolvedRelation =>
        val key = r.multipartIdentifier.map(normalize).toVector
        require(bindings.contains(key), "input must read declared physical sources, not another CTE")
        r.multipartIdentifier.map(quoted).mkString(".")
      case a: SubqueryAlias => relation(a.child) + " AS " + quoted(a.identifier.name)
      case j: Join if j.hint == JoinHint.NONE && (j.joinType == Inner || j.joinType == Cross) =>
        "(" + relation(j.left) + (if (j.joinType == Inner) " INNER JOIN " else " CROSS JOIN ") + relation(j.right) +
          j.condition.map(e => " ON (" + e.sql + ")").getOrElse("") + ")"
      case other => throw new IllegalArgumentException("unsupported stage source graph " + other.nodeName)
    }
    def from(p: LogicalPlan): (String, Vector[Expression]) = p match {
      case f: Filter => val (body, filters) = from(f.child); body -> (filters :+ f.condition)
      case other     => relation(other) -> Vector.empty
    }
    val (body, filters) = from(aggregate.child)
    val inputSql = "SELECT " + inputColumns.map(_.sql).mkString(", ") + " FROM " + body +
      (if (filters.isEmpty) "" else filters.map(e => "(" + e.sql + ")").mkString(" WHERE ", " AND ", ""))
    val postChild = {
      val visible = Project(outputs.map(o => UnresolvedAttribute(Seq(o._2))), UnresolvedRelation(Seq(State)))
      having
        .map { h =>
          val predicate = h.transformDown {
            case e if exported.contains(e.sql) => UnresolvedAttribute(Seq(exported(e.sql)))
            case e if aggregate.groupingExpressions.exists(_.sql == e.sql) =>
              UnresolvedAttribute(Seq(projected(e.sql)._2))
          }
          require(
            !predicate.exists {
              case f: UnresolvedFunction => Aggregates.contains(functionName(f))
              case _                     => false
            },
            "HAVING aggregate must be exported"
          )
          Filter(predicate, visible)
        }
        .getOrElse(visible)
    }
    val post = SparkParserCompat.rebindCtes(
      root,
      {
        case a: SubqueryAlias if normalize(a.identifier.name) == normalize(selected.identifier.name) =>
          a.copy(child = postChild)
        case p => p
      }
    )
    val digest = MessageDigest
      .getInstance("SHA-256")
      .digest(
        ("spark-stage-v1\n" + query + "\n" + inputSql + "\n" +
          declared.map(_.map(quoted).mkString(".")).mkString("\n") + "\n" +
          bindings.toVector
            .sortBy(_._1.mkString("\u0000"))
            .map { case (a, s) =>
              a.map(quoted).mkString(".") + "=>" + s.map(quoted).mkString(".")
            }
            .mkString("\n")).getBytes(UTF_8)
      )
      .map(b => f"${b & 255}%02x")
      .mkString
    new Contract(
      spark,
      inputSql,
      groups,
      channels.toVector,
      countNames.head,
      digest,
      inputPlan,
      aggregate,
      ordinaryPlan,
      post,
      declared,
      bindings,
      normalize
    )
  }
}
