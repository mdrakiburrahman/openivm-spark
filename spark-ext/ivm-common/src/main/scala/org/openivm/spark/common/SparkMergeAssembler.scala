package org.openivm.spark.common

/** Dispatcher: selects the appropriate [[Assembler]] for the given [[AssemblyInput]] and
  * produces the [[AssembledRefresh]] program.
  *
  * Assembler routing table:
  * {{{
  *   RefreshType  Assembler
  *   ──────────── ─────────────────────────────────────────────────
  *   0  AGGREGATE_GROUP (additive)   MergeAssembler
  *   1  SIMPLE_AGGREGATE             MergeAssembler
  *   2  SIMPLE_PROJECTION            MergeAssembler (rowid / signed)
  *   3  FULL_REFRESH                 FullRefreshAssembler
  *   4  AGGREGATE_HAVING             MergeAssembler + post-pass DELETE
  *   5  WINDOW_PARTITION             AffectedGroupsAssembler
  *   6  GROUP_RECOMPUTE              AffectedGroupsAssembler
  *   7  TopK                         ── never emitted by classifier ──
  *   8  DISTINCT_INCREMENTAL         MergeAssembler (count-monoid)
  *   9  SEMI_ANTI_RECOMPUTE          AuxStateAssembler
  * }}}
  *
  * Any RefreshType with no registered assembler (including the dead TopK enum value 7)
  * throws [[UnsupportedOperationException]] with a descriptive message.  Additional
  * paths are filled in as operator-specific refinements land.
  */
object SparkMergeAssembler {

  private val assemblers: Seq[Assembler] = Seq(
    MergeAssembler,
    AffectedGroupsAssembler,
    AuxStateAssembler,
    FullRefreshAssembler
  )

  def assemble(in: AssemblyInput): AssembledRefresh =
    assemblers.find(_.supports(in.refreshType)) match {
      case Some(asm) => asm.assemble(in)
      case None =>
        val msg = in.refreshType match {
          case RefreshTypeCode.TopK =>
            s"RefreshType ${in.refreshType} (${in.refreshTypeName}) is never emitted by the " +
              s"classifier; Top-K views ride on the inner query's RefreshType at CREATE time"
          case _ =>
            s"No assembler registered for RefreshType ${in.refreshType} (${in.refreshTypeName})"
        }
        throw new UnsupportedOperationException(msg)
    }
}
