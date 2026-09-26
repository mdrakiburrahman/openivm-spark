package org.openivm.spark.parser

import org.antlr.v4.runtime.misc.Interval
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.openivm.spark.commands.CreateStreamingTableCommand
import org.openivm.spark.commands.CreateMaterializedViewCommand
import org.openivm.spark.commands.DropMaterializedViewCommand
import org.openivm.spark.commands.DropStreamingTableCommand
import org.openivm.spark.commands.ExplainCreateMaterializedViewCommand
import org.openivm.spark.commands.AdvanceMaterializedViewSourceVersionsCommand
import org.openivm.spark.commands.RefreshMaterializedViewCommand
import org.openivm.spark.commands.ShowMaterializedViewRefreshSqlCommand
import org.openivm.spark.commands.ShowQueryLogCommand
import org.openivm.spark.commands.ShowRefreshProfileCommand
import org.openivm.spark.commands.ShowStreamingTablesCommand
import org.openivm.spark.commands.StopStreamingTableCommand
import org.openivm.spark.parser.gen.IvmSqlBaseBaseVisitor
import org.openivm.spark.parser.gen.IvmSqlBaseParser
import org.openivm.spark.streaming.StreamingTableSpec

import scala.jdk.CollectionConverters._

/**
 * Visitor that builds typed LogicalPlan nodes from the ANTLR-generated parse tree
 * produced by [[IvmSqlBaseParser]].
 *
 * The `queryBody` sub-parse is delegated back to the wrapped Spark parser so that
 * every SELECT construct Spark 3.5 understands is automatically supported.
 */
private[parser] class IvmAstBuilder(session: SparkSession, delegate: ParserInterface, sqlText: String)
    extends IvmSqlBaseBaseVisitor[AnyRef] {

  // -------------------------------------------------------------------------
  // Top-level statements
  // -------------------------------------------------------------------------

  override def visitIvmStatement(
      ctx: IvmSqlBaseParser.IvmStatementContext
  ): AnyRef =
    visit(ctx.getChild(0))

  override def visitCreateMaterializedView(
      ctx: IvmSqlBaseParser.CreateMaterializedViewContext
  ): AnyRef = buildCreateCommand(ctx)

  override def visitExplainCreateMaterializedView(
      ctx: IvmSqlBaseParser.ExplainCreateMaterializedViewContext
  ): AnyRef = {
    val create = ctx.createMaterializedView()
    ExplainCreateMaterializedViewCommand(
      toTableIdentifier(create.multipartIdentifier()),
      extractQueryBody(create.queryBody()),
      clusterColumns(create)
    )
  }

  override def visitShowRefreshSql(
      ctx: IvmSqlBaseParser.ShowRefreshSqlContext
  ): AnyRef = {
    val create = ctx.createMaterializedView()
    ShowMaterializedViewRefreshSqlCommand(
      toTableIdentifier(create.multipartIdentifier()),
      extractQueryBody(create.queryBody()),
      clusterColumns(create)
    )
  }

  override def visitRefreshMaterializedView(
      ctx: IvmSqlBaseParser.RefreshMaterializedViewContext
  ): AnyRef =
    RefreshMaterializedViewCommand(toTableIdentifier(ctx.multipartIdentifier()))

  override def visitAdvanceMaterializedViewSourceVersions(
      ctx: IvmSqlBaseParser.AdvanceMaterializedViewSourceVersionsContext
  ): AnyRef = {
    val entries = ctx
      .sourceVersionEntry()
      .asScala
      .map { entry =>
        multipartColumnName(entry.multipartIdentifier()) -> entry.INTEGER_VALUE().getText.toLong
      }
      .toVector
    val duplicate = entries
      .groupBy(_._1.toLowerCase(java.util.Locale.ROOT))
      .collectFirst { case (_, values) if values.size > 1 => values.head._1 }
    duplicate.foreach { source =>
      throw SparkParserCompat.parseException(
        ctx.getText,
        s"Source version map names '$source' more than once"
      )
    }
    AdvanceMaterializedViewSourceVersionsCommand(
      toTableIdentifier(ctx.multipartIdentifier()),
      entries.toMap
    )
  }

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

  override def visitCreateStreamingTable(
      ctx: IvmSqlBaseParser.CreateStreamingTableContext
  ): AnyRef = {
    val clauses = ctx.streamingTableClause().asScala.toSeq

    val provider = singleClause(
      clauses.filter(_.USING() != null).map(clause => identifierText(clause.identifier())),
      "USING"
    )
    val location = singleClause(
      clauses
        .filter(_.LOCATION() != null)
        .map(clause => StreamingQuerySql.parseStringLiteral(clause.STRING().getText, sqlText)),
      "LOCATION"
    )
    val partitionColumns = singleClause(
      clauses
        .filter(_.PARTITIONED() != null)
        .map(_.multipartIdentifier().asScala.map(multipartColumnName).toSeq),
      "PARTITIONED BY"
    ).getOrElse(Seq.empty)
    val clusterColumns = singleClause(
      clauses
        .filter(_.clusterByClause() != null)
        .map(
          _.clusterByClause()
            .multipartIdentifier()
            .asScala
            .map(multipartIdentifierParts)
            .toSeq
        ),
      "CLUSTER BY"
    ).getOrElse(Seq.empty)
    val tableProperties = singleClause(
      clauses
        .filter(_.TBLPROPERTIES() != null)
        .map(clause => buildUniqueProperties(clause.tableProperties(), "TBLPROPERTIES")),
      "TBLPROPERTIES"
    ).getOrElse(Map.empty)
    val options = singleClause(
      clauses
        .filter(_.OPTIONS() != null)
        .map(clause => buildUniqueProperties(clause.tableProperties(), "OPTIONS")),
      "OPTIONS"
    ).getOrElse(Map.empty)

    val queryText = extractQueryBody(ctx.queryBody())
    val queryPlan = StreamingQuerySql.parse(queryText, delegate)
    CreateStreamingTableCommand(
      StreamingTableSpec(
        name = multipartIdentifierParts(ctx.multipartIdentifier()),
        queryText = queryText,
        query = queryPlan,
        provider = provider,
        location = location,
        partitionColumns = partitionColumns,
        tableProperties = tableProperties,
        options = options,
        ifNotExists = ctx.IF() != null,
        clusterColumns = clusterColumns
      )
    )
  }

  override def visitShowStreamingTables(
      ctx: IvmSqlBaseParser.ShowStreamingTablesContext
  ): AnyRef =
    ShowStreamingTablesCommand(
      Option(ctx.multipartIdentifier()).map(multipartIdentifierParts)
    )

  override def visitStopStreamingTable(
      ctx: IvmSqlBaseParser.StopStreamingTableContext
  ): AnyRef =
    StopStreamingTableCommand(multipartIdentifierParts(ctx.multipartIdentifier()))

  override def visitDropStreamingTable(
      ctx: IvmSqlBaseParser.DropStreamingTableContext
  ): AnyRef =
    DropStreamingTableCommand(
      multipartIdentifierParts(ctx.multipartIdentifier()),
      ifExists = ctx.IF() != null
    )

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  /** Build a [[CreateMaterializedViewCommand]] from a create-MV parse context.
    * Shared by the plain CREATE visitor and the EXPLAIN / SHOW REFRESH SQL
    * visitors that wrap the same `createMaterializedView` production.
    */
  private def buildCreateCommand(
      ctx: IvmSqlBaseParser.CreateMaterializedViewContext
  ): CreateMaterializedViewCommand = {
    val name        = toTableIdentifier(ctx.multipartIdentifier())
    val ifNotExists = ctx.IF() != null
    val provider =
      if (ctx.USING() != null) Some(identifierText(ctx.tableProvider)) else None
    val properties =
      if (ctx.tableProperties() != null) buildProperties(ctx.tableProperties())
      else Map.empty[String, String]
    val queryText = extractQueryBody(ctx.queryBody())
    val queryPlan = session.sessionState.sqlParser.parsePlan(queryText)
    CreateMaterializedViewCommand(
      name,
      queryPlan,
      properties,
      ifNotExists,
      provider,
      queryText,
      clusterColumns(ctx)
    )
  }

  /** Extract the `CLUSTER BY (...)` column names (declaration order), or empty
    * when the DDL had no `CLUSTER BY` clause.
    */
  private def clusterColumns(
      ctx: IvmSqlBaseParser.CreateMaterializedViewContext
  ): Seq[String] = {
    val clause = ctx.clusterByClause()
    if (clause == null) Seq.empty
    else clause.multipartIdentifier().asScala.map(multipartColumnName).toList
  }

  /** Reconstruct a (possibly dotted) column reference from a multipart id. */
  private def multipartColumnName(
      ctx: IvmSqlBaseParser.MultipartIdentifierContext
  ): String =
    ctx.identifier().asScala.map(identifierText).mkString(".")

  private def multipartIdentifierParts(
      ctx: IvmSqlBaseParser.MultipartIdentifierContext
  ): Seq[String] =
    ctx.identifier().asScala.map(identifierText).toSeq

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
        throw SparkParserCompat.parseException(
          ctx.getText,
          s"Identifier has too many parts: ${ctx.getText}"
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

  private def buildUniqueProperties(
      ctx: IvmSqlBaseParser.TablePropertiesContext,
      clauseName: String
  ): Map[String, String] = {
    val entries = ctx
      .tableProperty()
      .asScala
      .map { property =>
        if (property.EQ() == null) {
          parseError(s"$clauseName option '${property.key.getText}' requires '='")
        }
        val key =
          if (property.key.STRING() != null)
            StreamingQuerySql.parseStringLiteral(property.key.getText, sqlText)
          else
            property.key.identifier().asScala.map(identifierText).mkString(".")
        val value =
          if (property.value.STRING() != null)
            StreamingQuerySql.parseStringLiteral(property.value.getText, sqlText)
          else
            property.value.getText
        key -> value
      }
      .toSeq

    val duplicate = entries
      .groupBy { case (key, _) => key.toLowerCase(java.util.Locale.ROOT) }
      .collectFirst { case (_, values) if values.size > 1 => values.head._1 }
    duplicate.foreach { key =>
      parseError(s"$clauseName names option '$key' more than once")
    }
    entries.toMap
  }

  private def singleClause[T](values: Seq[T], clauseName: String): Option[T] =
    values match {
      case Seq()      => None
      case Seq(value) => Some(value)
      case _          => parseError(s"CREATE STREAMING TABLE may specify $clauseName only once")
    }

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

  private def parseError(message: String): Nothing =
    throw SparkParserCompat.parseException(sqlText, message)
}

/** Companion — exposes the entry-point used by [[IvmParser]]. */
private[parser] object IvmAstBuilder {

  def buildPlan(
      session: SparkSession,
      delegate: ParserInterface,
      sqlText: String,
      tree: IvmSqlBaseParser.IvmStatementContext
  ): LogicalPlan = {
    val builder = new IvmAstBuilder(session, delegate, sqlText)
    builder.visit(tree) match {
      case plan: LogicalPlan => plan
      case other =>
        throw SparkParserCompat.parseException(
          sqlText,
          s"Expected a LogicalPlan from IvmAstBuilder but got: ${other.getClass}"
        )
    }
  }
}
