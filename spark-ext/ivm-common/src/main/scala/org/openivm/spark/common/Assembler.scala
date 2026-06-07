package org.openivm.spark.common

/** Contract shared by all four assembler kinds.
  *
  * Each implementation is a singleton object in its own file:
  *   - MergeAssembler          (additive MERGE — types 0, 1, 2, 4, 8)
  *   - AffectedGroupsAssembler (DELETE+INSERT — types 5, 6)
  *   - AuxStateAssembler       (aux-state match-count — type 9)
  *   - FullRefreshAssembler    (INSERT OVERWRITE — type 3)
  */
trait Assembler {
  def kind: String
  def supports(refreshType: Int): Boolean
  def assemble(in: AssemblyInput): AssembledRefresh
}
