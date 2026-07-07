package org.openivm.spark.common

/** Inputs the assembler needs from the orchestrator.
  *
  * Deliberately independent of `org.openivm.spark.compiler.CompiledRefresh` to avoid a
  * cyclic dependency (ivm-compiler depends on ivm-common, not the other way around).
  */
final case class AssemblyInput(
    refreshType: Int,
    refreshTypeName: String,
    /** The lpts-emitted refresh SQL in Spark dialect; may be a CTE-chained program, a bare
      * subquery, or the original view body — the assembler picks up the relevant pieces.
      */
    deltaSql: String,
    /** Fully-qualified MV name as it should appear in the Spark catalog (db.schema.v).
      * Each dot-separated segment is backtick-quoted by the assembler before use.
      */
    mvName: String,
    /** Fully-qualified MV storage location (Delta path). */
    mvLocation: String,
    /** GROUP BY key columns for types 0 / 4 / 8. Empty for types 1 / 2 / 3 / 9. */
    groupKeys: Seq[String] = Nil,
    /** Synthetic rowid column for SIMPLE_PROJECTION views (type 2). Defaults to `_ivm_rowid`. */
    rowIdColumn: Option[String] = None,
    /** HAVING predicate string for type 4 — applied as a post-pass DELETE. */
    havingPredicate: Option[String] = None,
    /** Partition column for WINDOW_PARTITION (type 5). */
    partitionColumn: Option[String] = None,
    /** Aux-state table name for SEMI_ANTI_RECOMPUTE (type 9). */
    auxTable: Option[String] = None
)
