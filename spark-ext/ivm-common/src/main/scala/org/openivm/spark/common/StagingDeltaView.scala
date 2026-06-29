package org.openivm.spark.common

import org.apache.spark.sql.types.StructType

/** Helpers that translate one source table's pending [[StagingDelta]] entries
  * into the `openivm_delta_<source>` TEMP VIEW expected by openivm-emitted
  * refresh SQL.
  *
  * The view UNION ALL-s every consumable staging Delta path with the correct
  * `openivm_multiplicity`:
  *  - `+1` for inserts/overwrites/update-after,
  *  - `-1` for deletes/update-before,
  *  - **preserved verbatim** for `MV_VIEW_DELTA` rows (an upstream MV's
  *    persisted view-delta carries its own signed multiplicity column —
  *    overriding it would silently corrupt downstream aggregates).
  *  - a synthesized empty view if every delta is unsupported
  *    (e.g. `MERGE_SRC`).
  */
object StagingDeltaView {

  object CachedViewDeltaRef {
    private val Prefix = "openivm-cache:"

    def encode(globalTempView: String): String = Prefix + globalTempView

    def decode(stagingPath: String): Option[String] =
      stagingPath.stripPrefix(Prefix) match {
        case decoded if decoded != stagingPath && decoded.nonEmpty => Some(decoded)
        case _                                                     => None
      }

    def sqlRef(globalTempView: String): String =
      s"`global_temp`.`${globalTempView.replace("`", "``")}`"
  }

  /** The openivm-internal TEMP VIEW name for a source table's delta. */
  def deltaViewName(sourceTable: String): String = {
    val short = sourceTable.split("\\.").last
    s"openivm_delta_$short"
  }

  /** Build a `CREATE OR REPLACE TEMP VIEW openivm_delta_<sourceTable>` SQL
    * that UNION ALL-s all consumable staging Delta paths with the correct
    * `openivm_multiplicity`.
    *
    * When `deltas` is empty (no pending staging entries for `sourceTable`),
    * returns an empty view with the correct schema so that multi-source compiled
    * SQL (e.g. UNION DISTINCT across two tables) can still reference the view
    * without a TABLE_OR_VIEW_NOT_FOUND error.
    */
  def buildSourceDeltaViewSql(
      sourceTable: String,
      sourceSchema: StructType,
      deltas: Seq[StagingDelta]
  ): String = {
    val short    = sourceTable.split("\\.").last
    val viewName = s"`openivm_delta_$short`"
    val cols     = sourceSchema.fieldNames.map(n => s"`${n.replace("`", "``")}`").mkString(", ")

    val parts: Seq[String] = deltas.flatMap { d =>
      val escapedPath = d.stagingPath.replace("`", "``")
      d.opType match {
        // ── MV-over-MV: preserve the upstream's existing multiplicity column ──
        //
        // The upstream MV's incremental refresh wrote `openivm_delta_<upstream>`
        // as a Delta table containing user columns + `openivm_multiplicity`
        // (and, for some refresh types, `openivm_count_star`). The downstream's
        // source-delta view must PRESERVE those bookkeeping columns; overriding
        // them to +1 would collapse the signed multiplicities and silently
        // corrupt any downstream count-monoid aggregate.
        //
        // openivm-spark's SparkRefreshRewriter writes the view-delta WITHOUT
        // `openivm_timestamp` (it strips the timestamp predicate at rewrite
        // time — see `stripTimestampPredicate` in SparkRefreshRewriter). The
        // downstream's openivm-emitted refresh SQL also has its timestamp
        // predicate stripped, so the only timestamp consumer is this temp
        // view's schema contract — synthesize one from the staging row's
        // wall-clock txn_ts so the schema is uniform across mv-delta and
        // base-table delta entries in a multi-source UNION ALL.
        case StagingDelta.OpTypes.MvViewDelta =>
          val ts = d.txnTs.toString
          val sourceRef = CachedViewDeltaRef
            .decode(d.stagingPath)
            .map(CachedViewDeltaRef.sqlRef)
            .getOrElse(s"delta.`$escapedPath`")
          Some(
            s"""SELECT $cols, openivm_multiplicity, CAST('$ts' AS TIMESTAMP) AS openivm_timestamp
               |FROM $sourceRef""".stripMargin
          )

        case StagingDelta.OpTypes.Insert | StagingDelta.OpTypes.Overwrite | StagingDelta.OpTypes.UpdateAfter =>
          val ts = d.txnTs.toString
          Some(
            s"""SELECT $cols, CAST('$ts' AS TIMESTAMP) AS openivm_timestamp, CAST(1 AS INT) AS openivm_multiplicity
               |FROM delta.`$escapedPath`""".stripMargin
          )

        case StagingDelta.OpTypes.Delete | StagingDelta.OpTypes.UpdateBefore =>
          val ts = d.txnTs.toString
          Some(
            s"""SELECT $cols, CAST('$ts' AS TIMESTAMP) AS openivm_timestamp, CAST(-1 AS INT) AS openivm_multiplicity
               |FROM delta.`$escapedPath`""".stripMargin
          )

        // MERGE_SRC and any unknown opType are dropped — fall through to the
        // empty-view fallback below if every entry was unsupported.
        case _ => None
      }
    }

    if (parts.isEmpty) {
      val nullCols = sourceSchema.fieldNames.map(n => s"NULL AS `${n.replace("`", "``")}`").mkString(", ")
      s"""CREATE OR REPLACE TEMP VIEW $viewName AS
         |SELECT $cols, CURRENT_TIMESTAMP() AS openivm_timestamp, CAST(0 AS INT) AS openivm_multiplicity
         |FROM (SELECT $nullCols) WHERE 1=0""".stripMargin
    } else {
      s"""CREATE OR REPLACE TEMP VIEW $viewName AS
         |${parts.mkString("\nUNION ALL\n")}""".stripMargin
    }
  }

  /** DROP the TEMP VIEW for a source table's delta. Idempotent. */
  def dropSourceDeltaViewSql(sourceTable: String): String = {
    val short = sourceTable.split("\\.").last
    s"DROP VIEW IF EXISTS `openivm_delta_$short`"
  }
}
