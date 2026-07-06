package org.openivm.spark.common

import java.util.concurrent.atomic.AtomicInteger

/** Assembler for SEMI/ANTI-join match-count maintenance via a per-MV auxiliary Delta table.
  *
  * Supported RefreshType:
  *   9  SEMI_ANTI_RECOMPUTE — per-left-tuple _match_count bookkeeping
  *
  * Strategy (five statements per refresh — mirrors refresh_compiler_aux.cpp:88-216):
  *   1. Snapshot old visibility from the aux table.
  *   2. MERGE right-side delta match-count changes into the aux table.
  *   3. Compute the set of left-tuples whose visibility flipped.
  *   4. DELETE flipped-off rows from the MV.
  *   5. INSERT flipped-on rows into the MV.
  *
  * Steps 2 and 5 are skeleton placeholders marked TODO — the real right-side
  * delta derivation and the join back to the base table are wired in a follow-up.
  */
object AuxStateAssembler extends Assembler with IdentifierOps {

  private val counter = new AtomicInteger(0)

  def kind: String = "aux-state"

  def supports(refreshType: Int): Boolean =
    refreshType == RefreshTypeCode.SemiAntiRecompute

  def assemble(in: AssemblyInput): AssembledRefresh = {
    val n           = counter.incrementAndGet()
    val mv          = quoteMvName(in.mvName)
    val auxName     = in.auxTable.map(quoteMvName).getOrElse(quoteMvName(s"${in.mvName}_aux"))
    val snapView    = s"aux_before_$n"
    val flippedView = s"flipped_$n"

    // Step 1: snapshot current visibility into a temp view
    val step1 =
      s"CREATE OR REPLACE TEMP VIEW $snapView AS SELECT *, _match_count > 0 AS _visible_old FROM $auxName"

    // Step 2: apply right-side delta to aux state
    // TODO: replace deltaSql placeholder with the actual right-side match-count delta
    val step2 =
      s"""|MERGE INTO $auxName a
          |USING (${in.deltaSql}) d
          |ON a._ivm_left_pk <=> d._ivm_left_pk
          |WHEN MATCHED THEN UPDATE SET a._match_count = a._match_count + d._delta_match
          |WHEN NOT MATCHED THEN INSERT *""".stripMargin

    // Step 3: identify tuples whose visibility changed
    val step3 =
      s"""|CREATE OR REPLACE TEMP VIEW $flippedView AS
          |SELECT a.*, b._visible_old
          |FROM $auxName a
          |JOIN $snapView b ON a._ivm_left_pk <=> b._ivm_left_pk
          |WHERE (a._match_count > 0) <> b._visible_old""".stripMargin

    // Step 4a: delete rows that flipped from visible to invisible
    val step4Delete =
      s"DELETE FROM $mv WHERE _ivm_left_pk IN (SELECT _ivm_left_pk FROM $flippedView WHERE _visible_old)"

    // Step 4b: insert rows that flipped from invisible to visible
    // TODO: join back to base table to reconstruct full row projection
    val step4Insert =
      s"INSERT INTO $mv SELECT * FROM $flippedView WHERE NOT _visible_old"

    AssembledRefresh(Seq(step1, step2, step3, step4Delete, step4Insert))
  }
}
