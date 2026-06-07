package org.openivm.spark.common

/** Assembler for additive / signed-projection MERGE patterns.
  *
  * Supported RefreshTypes:
  *   0  AGGREGATE_GROUP    — additive monoid (SUM/COUNT/AVG/STDDEV), keyed MERGE
  *   1  SIMPLE_AGGREGATE   — scalar (no GROUP BY), single-row MERGE
  *   2  SIMPLE_PROJECTION  — rowid-keyed, sign-based DELETE / UPDATE / INSERT
  *   4  AGGREGATE_HAVING   — same as type 0 + post-pass DELETE WHERE NOT <having>
  *   8  DISTINCT_INCREMENTAL — COUNT(*)-monoid MERGE (rows where count→0 are deleted
  *                             by the CTE itself before the MERGE runs)
  *
  * The `deltaSql` in AssemblyInput is expected to be a CTE-form string starting with
  * `WITH refresh_cte AS (…)`.  The assembler appends the MERGE body after it, relying
  * on Spark 3.5's support for CTE-prefixed DML statements.
  */
object MergeAssembler extends Assembler with IdentifierOps {

  def kind: String = "merge"

  def supports(refreshType: Int): Boolean = refreshType match {
    case RefreshTypeCode.AggregateGroup | RefreshTypeCode.SimpleAggregate | RefreshTypeCode.SimpleProjection |
        RefreshTypeCode.AggregateHaving | RefreshTypeCode.DistinctIncremental =>
      true
    case _ => false
  }

  def assemble(in: AssemblyInput): AssembledRefresh = in.refreshType match {
    case RefreshTypeCode.SimpleProjection => assembleProjection(in)
    case RefreshTypeCode.AggregateHaving  => assembleHaving(in)
    case _                                => assembleMonoid(in)
  }

  // ── private helpers ──────────────────────────────────────────────────────

  private def assembleMonoid(in: AssemblyInput): AssembledRefresh = {
    val mv       = quoteMvName(in.mvName)
    val onClause = buildKeyCondition(in.groupKeys, "v", "d")
    val stmt =
      s"""|${in.deltaSql}
          |MERGE INTO $mv v
          |USING refresh_cte d
          |ON $onClause
          |WHEN MATCHED THEN UPDATE SET *
          |WHEN NOT MATCHED THEN INSERT *""".stripMargin
    AssembledRefresh(Seq(stmt))
  }

  /** SIMPLE_PROJECTION: rowid-keyed MERGE with sign-driven DELETE / UPDATE / INSERT clauses.
    *
    * _ivm_sign = -1 → DELETE the matched row (retraction)
    * _ivm_sign = +1 → UPDATE matched row or INSERT new row (assertion)
    */
  private def assembleProjection(in: AssemblyInput): AssembledRefresh = {
    val mv    = quoteMvName(in.mvName)
    val rowId = quoteIdent(in.rowIdColumn.getOrElse("_ivm_rowid"))
    val stmt =
      s"""|${in.deltaSql}
          |MERGE INTO $mv v
          |USING refresh_cte d
          |ON v.$rowId <=> d.$rowId
          |WHEN MATCHED AND d.`_ivm_sign` = -1 THEN DELETE
          |WHEN MATCHED AND d.`_ivm_sign` = 1 THEN UPDATE SET *
          |WHEN NOT MATCHED AND d.`_ivm_sign` = 1 THEN INSERT *""".stripMargin
    AssembledRefresh(Seq(stmt))
  }

  /** AGGREGATE_HAVING: identical to the monoid MERGE plus a second statement that
    * removes rows no longer satisfying the HAVING predicate.
    */
  private def assembleHaving(in: AssemblyInput): AssembledRefresh = {
    val mv           = quoteMvName(in.mvName)
    val monoidResult = assembleMonoid(in)
    val pred         = in.havingPredicate.getOrElse("TRUE")
    val deleteStmt   = s"DELETE FROM $mv WHERE NOT ($pred)"
    AssembledRefresh(monoidResult.statements :+ deleteStmt)
  }
}
