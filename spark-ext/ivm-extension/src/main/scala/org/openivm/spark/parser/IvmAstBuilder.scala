package org.openivm.spark.parser

import org.antlr.v4.runtime.misc.Interval
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.Origin
import org.openivm.spark.commands.CreateMaterializedViewCommand
import org.openivm.spark.commands.DropMaterializedViewCommand
import org.openivm.spark.commands.RefreshMaterializedViewCommand
import org.openivm.spark.commands.ShowQueryLogCommand
import org.openivm.spark.commands.ShowRefreshProfileCommand
import org.openivm.spark.parser.gen.IvmSqlBaseBaseVisitor
import org.openivm.spark.parser.gen.IvmSqlBaseParser

import scala.collection.JavaConverters._

/**
 * Visitor that builds typed LogicalPlan nodes from the ANTLR-generated parse tree
 * produced by [[IvmSqlBaseParser]].
 *
 * The `queryBody` sub-parse is delegated back to the Spark session's own parser so
 * that every SELECT construct Spark 3.5 understands is automatically supported.
 */
private[parser] class IvmAstBuilder(session: SparkSession) extends IvmSqlBaseBaseVisitor[AnyRef] {

  // -------------------------------------------------------------------------
  // Top-level statements
  // -------------------------------------------------------------------------

  override def visitCreateMaterializedView(
      ctx: IvmSqlBaseParser.CreateMaterializedViewContext
  ): AnyRef = {
    val name        = toTableIdentifier(ctx.multipartIdentifier(0))
    val ifNotExists = ctx.IF() != null
    val provider =
      if (ctx.USING() != null) Some(identifierText(ctx.tableProvider)) else None
    val properties =
      if (ctx.tableProperties() != null) buildProperties(ctx.tableProperties())
      else Map.empty[String, String]
    val queryText = extractQueryBody(ctx.queryBody())
    val queryPlan = session.sessionState.sqlParser.parsePlan(queryText)
    CreateMaterializedViewCommand(name, queryPlan, properties, ifNotExists, provider, queryText)
  }

  override def visitRefreshMaterializedView(
      ctx: IvmSqlBaseParser.RefreshMaterializedViewContext
  ): AnyRef =
    RefreshMaterializedViewCommand(toTableIdentifier(ctx.multipartIdentifier()))

  override def visitDropMaterializedView(
      ctx: IvmSqlBaseParser.DropMaterializedViewContext
  ): AnyRef = {
    val name     = toTableIdentifier(ctx.multipartIdentifier())
    val ifExists = ctx.IF() != null
    DropMaterializedViewCommand(name, ifExists)
  }

  override def visitShowOpenivmRefreshProfile(
      ctx: IvmSqlBaseParser.ShowOpenivmRefreshProfileContext
  ): AnyRef =
    ShowRefreshProfileCommand()

  override def visitShowOpenivmQueryLog(
      ctx: IvmSqlBaseParser.ShowOpenivmQueryLogContext
  ): AnyRef =
    ShowQueryLogCommand()

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  /** Convert a multipart identifier context into a Spark [[TableIdentifier]]. */
  private def toTableIdentifier(
      ctx: IvmSqlBaseParser.MultipartIdentifierContext
  ): TableIdentifier = {
    val parts = ctx.identifier().asScala.map(identifierText).toList
    parts match {
      case t :: Nil              => TableIdentifier(t)
      case db :: t :: Nil        => TableIdentifier(t, Some(db))
      case cat :: db :: t :: Nil => TableIdentifier(t, Some(db), Some(cat))
      case _ =>
        throw new ParseException(
          Some(ctx.getText),
          s"Identifier has too many parts: ${ctx.getText}",
          Origin(),
          Origin()
        )
    }
  }

  /**
   * Strip back-tick quoting from an identifier part and unescape `` `` `` pairs.
   * Non-backtick identifiers and keywords (e.g. `nonReserved`) are returned as-is.
   */
  private def identifierText(ctx: IvmSqlBaseParser.IdentifierContext): String = {
    val text = ctx.getText
    if (text.startsWith("`") && text.endsWith("`"))
      text.substring(1, text.length - 1).replace("``", "`")
    else
      text
  }

  /**
   * Extract the raw source text for the `queryBody` rule using character offsets
   * from the original input stream.  This preserves all whitespace and Spark-syntax
   * constructs that the IVM grammar does not model.
   */
  private def extractQueryBody(ctx: IvmSqlBaseParser.QueryBodyContext): String = {
    val startIdx = ctx.start.getStartIndex
    val stopIdx  = ctx.stop.getStopIndex
    ctx.start.getInputStream.getText(new Interval(startIdx, stopIdx))
  }

  /** Build a [[Map]] from a TBLPROPERTIES context, unquoting STRING keys/values. */
  private def buildProperties(
      ctx: IvmSqlBaseParser.TablePropertiesContext
  ): Map[String, String] =
    ctx
      .tableProperty()
      .asScala
      .map { prop =>
        unquoteStringLiteral(prop.key.getText) ->
          unquoteStringLiteral(prop.value.getText)
      }
      .toMap

  /**
   * Unquote a SQL single-quoted string literal.
   * For non-string-literal tokens (INTEGER_VALUE, DECIMAL_VALUE, BOOLEAN_VALUE) the
   * raw text is returned unchanged.
   */
  private def unquoteStringLiteral(s: String): String =
    if (s.startsWith("'") && s.endsWith("'"))
      s.substring(1, s.length - 1).replace("\\'", "'").replace("''", "'")
    else
      s
}

/** Companion — exposes the entry-point used by [[IvmParser]]. */
private[parser] object IvmAstBuilder {

  def buildPlan(
      session: SparkSession,
      sqlText: String,
      tree: IvmSqlBaseParser.IvmStatementContext
  ): LogicalPlan = {
    val builder = new IvmAstBuilder(session)
    builder.visit(tree) match {
      case plan: LogicalPlan => plan
      case other =>
        throw new ParseException(
          Some(sqlText),
          s"Expected a LogicalPlan from IvmAstBuilder but got: ${other.getClass}",
          Origin(),
          Origin()
        )
    }
  }
}
