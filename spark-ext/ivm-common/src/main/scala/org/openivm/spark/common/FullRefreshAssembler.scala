package org.openivm.spark.common

/** Assembler for full-refresh views — emits a single INSERT OVERWRITE statement.
  *
  * Supported RefreshType:
  *   3  FULL_REFRESH — used for unkeyed MIN/MAX, SEMI/ANTI+aggregation, window-over-JOIN,
  *                     recursive CTEs, and views containing volatile functions.
  *
  * The orchestrator passes the original view body SQL as `deltaSql`; this assembler
  * wraps it in an INSERT OVERWRITE TABLE envelope.
  */
object FullRefreshAssembler extends Assembler with IdentifierOps {

  def kind: String = "full-refresh"

  def supports(refreshType: Int): Boolean =
    refreshType == RefreshTypeCode.FullRefresh

  def assemble(in: AssemblyInput): AssembledRefresh = {
    val mv = quoteMvName(in.mvName)
    AssembledRefresh(Seq(s"INSERT OVERWRITE TABLE $mv SELECT * FROM (${in.deltaSql})"))
  }
}
