package org.openivm.spark.parser

import java.util.regex.Pattern

import org.antlr.v4.runtime.BaseErrorListener
import org.antlr.v4.runtime.CharStreams
import org.antlr.v4.runtime.CommonTokenStream
import org.antlr.v4.runtime.RecognitionException
import org.antlr.v4.runtime.Recognizer
import org.antlr.v4.runtime.Token
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructType
import org.openivm.spark.common.FeatureGate
import org.openivm.spark.parser.gen.IvmSqlBaseLexer
import org.openivm.spark.parser.gen.IvmSqlBaseParser

/**
 * Spark [[ParserInterface]] wrapper that handles the OpenIVM materialized-view
 * DDL/profile statements and delegates everything else to Spark's own parser.
 *
 * Routing decision in [[parsePlan]]:
 *  - If the SQL text starts (after stripping leading whitespace / SQL comments) with
 *    `EXPLAIN CREATE MATERIALIZED VIEW`,
 *    `SHOW REFRESH SQL FOR CREATE MATERIALIZED VIEW`,
 *    `CREATE MATERIALIZED VIEW`, `REFRESH MATERIALIZED VIEW`,
 *    `ALTER MATERIALIZED VIEW ... ADVANCE SOURCE VERSIONS`,
 *    `DROP MATERIALIZED VIEW`, `SHOW OPENIVM REFRESH PROFILE`, or
 *    `SHOW OPENIVM QUERY LOG`, plus the streaming-table lifecycle statements
 *    while the OpenIVM feature gate is enabled
 *    (case-insensitive) → parsed by [[IvmSqlBaseParser]] / [[IvmAstBuilder]].
 *  - Everything else (including bare `EXPLAIN <query>` and `OPTIMIZE`) → [[delegate]].
 *
 * All methods other than [[parsePlan]] delegate to [[delegate]] unchanged.
 */
class IvmParser(session: SparkSession, override protected val delegate: ParserInterface) extends ParserInterfaceCompat {

  // -------------------------------------------------------------------------
  // parsePlan — only method with custom logic
  // -------------------------------------------------------------------------

  override def parsePlan(sqlText: String): LogicalPlan = {
    val streamingStatement = StreamingQuerySql.isStreamingStatement(sqlText)
    if (streamingStatement && FeatureGate.enabled(session)) parseIvmStatement(sqlText)
    else if (!streamingStatement && isIvmStatement(sqlText)) parseIvmStatement(sqlText)
    else delegate.parsePlan(sqlText)
  }

  // -------------------------------------------------------------------------
  // All other methods delegate unchanged
  // -------------------------------------------------------------------------

  override def parseExpression(sqlText: String): Expression =
    delegate.parseExpression(sqlText)

  override def parseTableIdentifier(sqlText: String): TableIdentifier =
    delegate.parseTableIdentifier(sqlText)

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
    delegate.parseFunctionIdentifier(sqlText)

  override def parseMultipartIdentifier(sqlText: String): Seq[String] =
    delegate.parseMultipartIdentifier(sqlText)

  override def parseTableSchema(sqlText: String): StructType =
    delegate.parseTableSchema(sqlText)

  override def parseDataType(sqlText: String): DataType =
    delegate.parseDataType(sqlText)

  override def parseQuery(sqlText: String): LogicalPlan =
    delegate.parseQuery(sqlText)

  // -------------------------------------------------------------------------
  // Private helpers
  // -------------------------------------------------------------------------

  /**
   * Returns true if [[sqlText]] (after stripping any leading whitespace and SQL comments)
   * starts with one of the IVM statement heads.
   *
   * Two regex passes:
   *  1. Strip a run of leading whitespace / single-line (`-- ...`) / block (`/* ... */`)
   *     comments.
   *  2. Check the trimmed head against the keyword pattern.
   */
  private def isIvmStatement(sqlText: String): Boolean = {
    val m        = IvmParser.LeadingJunk.matcher(sqlText)
    val stripped = if (m.find()) sqlText.substring(m.end()) else sqlText
    IvmParser.IvmKeyword.matcher(stripped).find()
  }

  /** Run the ANTLR-generated parser and build a [[LogicalPlan]] via [[IvmAstBuilder]]. */
  private def parseIvmStatement(sqlText: String): LogicalPlan = {
    val inputStream = CharStreams.fromString(sqlText)

    var parseError: Option[String] = None
    val errorListener = new BaseErrorListener {
      override def syntaxError(
          recognizer: Recognizer[_, _],
          offendingSymbol: AnyRef,
          line: Int,
          charPositionInLine: Int,
          msg: String,
          e: RecognitionException
      ): Unit =
        if (parseError.isEmpty)
          parseError = Some(s"$msg (line $line, pos $charPositionInLine)")
    }

    val lexer = new IvmSqlBaseLexer(inputStream)
    lexer.removeErrorListeners()
    lexer.addErrorListener(errorListener)

    val tokenStream = new CommonTokenStream(lexer)
    val parser      = new IvmSqlBaseParser(tokenStream)
    parser.removeErrorListeners()
    parser.addErrorListener(errorListener)

    val tree = parser.ivmStatement()
    if (parseError.isEmpty && tokenStream.LA(1) != Token.EOF) {
      val token = tokenStream.LT(1)
      parseError = Some(
        s"extraneous input '${token.getText}' expecting <EOF> " +
          s"(line ${token.getLine}, pos ${token.getCharPositionInLine})"
      )
    }

    parseError match {
      case Some(errorMsg) =>
        throw SparkParserCompat.parseException(sqlText, errorMsg)
      case None =>
        IvmAstBuilder.buildPlan(session, delegate, sqlText, tree)
    }
  }
}

private object IvmParser {

  /**
   * Matches the longest run of leading whitespace and SQL comments.
   * `Pattern.DOTALL` is needed so that `.*?` inside `/* ... */` crosses newlines.
   */
  val LeadingJunk: Pattern = Pattern.compile(
    "\\A(?:\\s+|--[^\\n]*(?:\\n|\\z)|/\\*.*?\\*/)+",
    Pattern.DOTALL
  )

  /**
   * Case-insensitive check that the (already stripped) text begins with one of the
   * IVM statement heads.
   */
  val IvmKeyword: Pattern = Pattern.compile(
    "\\A(?:" +
      "explain\\s+create\\s+materialized\\s+view|" +
      "show\\s+refresh\\s+sql\\s+for\\s+create\\s+materialized\\s+view|" +
      "(?:create|refresh|alter|drop)\\s+materialized\\s+view|" +
      "show\\s+openivm\\s+refresh\\s+profile|" +
      "show\\s+openivm\\s+query\\s+log" +
      ")\\b",
    Pattern.CASE_INSENSITIVE
  )
}
