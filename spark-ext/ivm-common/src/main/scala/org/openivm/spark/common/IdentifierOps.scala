package org.openivm.spark.common

/** SQL identifier-quoting helpers mixed into each assembler.
  *
  * All public methods here are `protected` — they are deliberately unexported outside
  * the assembler hierarchy.
  */
private[common] trait IdentifierOps {

  /** Wraps `name` in backticks, doubling any embedded backtick characters. */
  protected def quoteIdent(name: String): String = {
    val escaped = name.replace("`", "``")
    s"`$escaped`"
  }

  /** Splits a dot-qualified name into its segments and backtick-quotes each one.
    *
    * Example: `"mydb.schema.v"` → `` `mydb`.`schema`.`v` ``
    *
    * Security note: any semicolons or other SQL-significant characters in a segment
    * are enclosed inside the backtick pair and are therefore inert.
    */
  protected def quoteMvName(name: String): String =
    name.split("\\.", -1).map(quoteIdent).mkString(".")

  /** Builds a null-safe join condition for the given key columns.
    *
    * Returns `"TRUE"` for an empty key list (scalar aggregate, single-row MV).
    */
  protected def buildKeyCondition(keys: Seq[String], leftAlias: String, rightAlias: String): String =
    if (keys.isEmpty) "TRUE"
    else keys.map(k => s"$leftAlias.${quoteIdent(k)} <=> $rightAlias.${quoteIdent(k)}").mkString(" AND ")

  /** Builds an `IN (SELECT … FROM viewName)` filter for the given key columns.
    *
    * Returns `"1 = 1"` if `keys` is empty (defensive; callers should avoid that path).
    */
  protected def buildInCondition(keys: Seq[String], viewName: String): String =
    if (keys.isEmpty) "1 = 1"
    else {
      val keyList = keys.map(quoteIdent).mkString(", ")
      if (keys.size == 1) s"$keyList IN (SELECT $keyList FROM $viewName)"
      else s"($keyList) IN (SELECT $keyList FROM $viewName)"
    }
}
