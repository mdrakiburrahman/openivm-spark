package org.openivm.spark.parser

import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias, UnresolvedWith}
import org.apache.spark.sql.catalyst.trees.Origin

private[parser] trait ParserInterfaceCompat extends ParserInterface {
  protected def delegate: ParserInterface
}

private[parser] object SparkParserCompat {

  def parseException(sqlText: String, message: String): ParseException =
    new ParseException(Some(sqlText), message, Origin(), Origin())

  def rebindCtes(
      unresolved: UnresolvedWith,
      bindPlan: LogicalPlan => LogicalPlan
  ): UnresolvedWith =
    unresolved.copy(
      cteRelations = unresolved.cteRelations.map { case (name, alias) =>
        name -> bindPlan(alias).asInstanceOf[SubqueryAlias]
      }
    )
}
