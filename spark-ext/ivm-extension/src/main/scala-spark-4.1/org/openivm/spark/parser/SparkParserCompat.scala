package org.openivm.spark.parser

import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias, UnresolvedWith}
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.types.StructType

private[parser] trait ParserInterfaceCompat extends ParserInterface {
  protected def delegate: ParserInterface

  override def parseRoutineParam(sqlText: String): StructType =
    delegate.parseRoutineParam(sqlText)
}

private[parser] object SparkParserCompat {

  def parseException(sqlText: String, message: String): ParseException =
    new ParseException(
      command = Some(sqlText),
      start = Origin(),
      errorClass = "PARSE_SYNTAX_ERROR",
      messageParameters = Map("error" -> message, "hint" -> "")
    )

  def rebindCtes(
      unresolved: UnresolvedWith,
      bindPlan: LogicalPlan => LogicalPlan
  ): UnresolvedWith =
    unresolved.copy(
      cteRelations = unresolved.cteRelations.map { case (name, alias, recursionDepth) =>
        (name, bindPlan(alias).asInstanceOf[SubqueryAlias], recursionDepth)
      }
    )
}
