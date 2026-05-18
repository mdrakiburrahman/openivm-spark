package org.openivm.spark.common

import org.apache.spark.sql.types.StructType

/** Helpers that translate one source table's pending [[StagingDelta]] entries
  * into the `openivm_delta_<source>` TEMP VIEW expected by openivm-emitted
  * refresh SQL.
  *
  * The view UNION ALL-s every consumable staging Delta path with the correct
  * `openivm_multiplicity` (`+1` for inserts/overwrites/update-after, `-1` for
  * deletes/update-before, and a synthesized empty view if every delta is
  * unsupported — e.g. MERGE_SRC).
  */
object StagingDeltaView {

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
      val mult: Option[Int] = d.opType match {
        case "INSERT" | "OVERWRITE" | "UPDATE_AFTER" => Some(1)
        case "DELETE" | "UPDATE_BEFORE"              => Some(-1)
        case _                                       => None
      }
      mult.map { m =>
        val ts          = d.txnTs.toString
        val escapedPath = d.stagingPath.replace("`", "``")
        s"""SELECT $cols, CAST('$ts' AS TIMESTAMP) AS openivm_timestamp, CAST($m AS INT) AS openivm_multiplicity
           |FROM delta.`$escapedPath`""".stripMargin
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
