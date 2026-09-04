package org.openivm.spark.common

/** Identifier-aware rewriting of the `memory.main.<identifier>` relation
  * references openivm emits.
  *
  * openivm plans against the DuckDB catalog, so every relation it reads — a
  * tracked Spark source, an `openivm_delta_<n>` staging table, an
  * `openivm_data_<view>` state table — is emitted as `memory.main.<name>`
  * (sometimes as the equivalent backticked multipart identifier
  * `` `memory`.`main`.`<name>` ``). Both the compiler (initial load) and the
  * refresh rewriter have to map those back onto Spark identifiers, and both
  * must additionally re-attach the user's Delta snapshot pin to a frozen
  * source.
  *
  * The rewrite MUST be identifier-bounded. A plain
  * `sql.replace("memory.main.customer", …)` loop over a source-name collection
  * also matches the prefix of `memory.main.customer_address`, which either
  * injects a pin into the middle of the longer identifier
  * (`customer VERSION AS OF 3_address`) or consumes it so the longer name's own
  * rewrite never fires — silently dropping ITS pin and reading live rows.
  * Matching the whole identifier in one pass removes the ordering hazard
  * entirely, so neither call site depends on iteration order or name lengths.
  */
object MemoryMainRefs {

  /** `memory.main.<identifier>`, bare or backticked, bounded on both sides:
    * the possessive `++` consumes the identifier to its end (so `customer` can
    * never match a prefix of `customer_address`) and the lookbehind keeps the
    * `memory` segment from starting inside a longer identifier.
    */
  private val Ref =
    """(?i)(?<![A-Za-z0-9_.`])(?:`?memory`?\s*+\.\s*+`?main`?\s*+\.\s*+`?([A-Za-z0-9_]++)`?)""".r

  /** Replace every `memory.main.<identifier>` reference in `sql` with
    * `replacement(<identifier>)`. The identifier is passed exactly as written;
    * callers that key their maps case-insensitively must fold it themselves.
    */
  def rewrite(sql: String)(replacement: String => String): String =
    if (sql == null || sql.isEmpty) sql
    else
      Ref.replaceAllIn(
        sql,
        m => java.util.regex.Matcher.quoteReplacement(replacement(m.group(1)))
      )
}
