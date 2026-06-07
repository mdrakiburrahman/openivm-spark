package org.openivm.spark.common

/** The Spark SQL program the assembler returns.
  *
  * Multi-statement: callers invoke `spark.sql(...).collect()` once per `statements` element,
  * in order.  For a single-shot execution use `asSingleStatement`.
  */
final case class AssembledRefresh(statements: Seq[String]) {
  def asSingleStatement: String = statements.mkString(";\n") + ";"
}
