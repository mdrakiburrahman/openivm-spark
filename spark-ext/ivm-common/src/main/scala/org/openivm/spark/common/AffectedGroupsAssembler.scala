package org.openivm.spark.common

import java.util.concurrent.atomic.AtomicInteger

/** Assembler for partition-scoped or group-scoped DELETE+INSERT patterns.
  *
  * Supported RefreshTypes:
  *   5  WINDOW_PARTITION  — partition-scoped DELETE+INSERT (uses `partitionColumn` as the key)
  *   6  GROUP_RECOMPUTE   — DELIM-join / correlated-subquery recompute (uses `groupKeys`)
  *
  * Strategy (three statements per refresh):
  *   1. CREATE OR REPLACE TEMP VIEW affected_keys_<n> — the distinct set of affected keys
  *      derived from the delta subquery.
  *   2. DELETE FROM <mv> WHERE <keys> IN (SELECT <keys> FROM affected_keys_<n>)
  *   3. INSERT INTO <mv> SELECT * FROM (<deltaSql>) WHERE <keys> IN (…)
  *      NOTE: statement 3 is a skeleton placeholder — a follow-up replaces deltaSql with the
  *      full base-table recompute query (GROUP BY over the unmodified source).
  *
  * The counter `_n` ensures temp-view names are unique across multiple MV refreshes
  * within the same Spark session.
  */
object AffectedGroupsAssembler extends Assembler with IdentifierOps {

  private val counter = new AtomicInteger(0)

  def kind: String = "affected-groups"

  def supports(refreshType: Int): Boolean = refreshType match {
    case RefreshTypeCode.WindowPartition | RefreshTypeCode.GroupRecompute => true
    case _                                                                => false
  }

  def assemble(in: AssemblyInput): AssembledRefresh = {
    val n        = counter.incrementAndGet()
    val mv       = quoteMvName(in.mvName)
    val keys     = effectiveKeys(in)
    val viewName = s"affected_keys_$n"
    val keyList  = keys.map(quoteIdent).mkString(", ")

    val createView =
      if (keys.isEmpty)
        s"CREATE OR REPLACE TEMP VIEW $viewName AS SELECT 1 AS _placeholder FROM (${in.deltaSql})"
      else
        s"CREATE OR REPLACE TEMP VIEW $viewName AS SELECT DISTINCT $keyList FROM (${in.deltaSql})"

    val inCond     = buildInCondition(keys, viewName)
    val deleteStmt = s"DELETE FROM $mv WHERE $inCond"
    // TODO: Replace deltaSql with the full base-table recompute query (SELECT … GROUP BY keys)
    val insertStmt = s"INSERT INTO $mv SELECT * FROM (${in.deltaSql}) WHERE $inCond"

    AssembledRefresh(Seq(createView, deleteStmt, insertStmt))
  }

  /** Type 5 uses partitionColumn as the sole key; type 6 uses groupKeys. */
  private def effectiveKeys(in: AssemblyInput): Seq[String] =
    if (in.groupKeys.nonEmpty) in.groupKeys
    else in.partitionColumn.toSeq
}
