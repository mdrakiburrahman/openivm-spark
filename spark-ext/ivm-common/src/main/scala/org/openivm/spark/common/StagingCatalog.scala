package org.openivm.spark.common

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{array_contains, col}
import org.apache.spark.sql.types._

import java.sql.Timestamp

/**
 * A single DML delta written by the DML interceptor for one base table.
 *
 * @param baseTable   qualified name of the base table being modified
 * @param opType      operation type. See [[StagingDelta.OpTypes]] for the
 *                    formal set + multiplicity-handling contract.
 * @param stagingPath path of the Delta table holding the staged rows
 * @param txnTs       wall-clock timestamp of the originating DML transaction
 * @param consumedBy  MV names that have already applied this delta (idempotency guard)
 */
final case class StagingDelta(
    baseTable: String,
    opType: String,
    stagingPath: String,
    txnTs: Timestamp,
    consumedBy: Seq[String]
)

/** Formal set of supported `opType` values + their multiplicity-handling
  * contract.
  *
  * `StagingDeltaView.buildSourceDeltaViewSql` decides how to assemble the
  * `openivm_delta_<source>` temp view per opType:
  *
  *  - `INSERT`, `OVERWRITE`, `UPDATE_AFTER` — synthesise
  *    `openivm_multiplicity = +1` for every row at the staging path.
  *  - `DELETE`, `UPDATE_BEFORE` — synthesise `openivm_multiplicity = -1`.
  *  - `MERGE_SRC` — currently dropped (returns None from the multiplicity
  *    helper); rows fall through to the empty-view fallback.
  *  - `MV_VIEW_DELTA` — **preserves the existing multiplicity column at the
  *    staging path** rather than synthesising one. Used by the MV-over-MV
  *    cascade: an upstream MV's incremental refresh writes a view-delta
  *    Delta table with `openivm_multiplicity` + `openivm_timestamp` columns
  *    already populated; the downstream's refresh consumes it as-is.
  *
  * Invariants:
  *  - Any code that introduces a new opType MUST also extend
  *    `StagingDeltaView.buildSourceDeltaViewSql` and add a docstring entry
  *    here.
  *  - Never treat `MV_VIEW_DELTA` like an `INSERT`/`OVERWRITE` — doing so
  *    would overwrite the upstream's signed multiplicities to +1 and silently
  *    corrupt downstream aggregates.
  */
object StagingDelta {
  object OpTypes {
    val Insert       = "INSERT"
    val Delete       = "DELETE"
    val UpdateBefore = "UPDATE_BEFORE"
    val UpdateAfter  = "UPDATE_AFTER"
    val MergeSrc     = "MERGE_SRC"
    val Overwrite    = "OVERWRITE"

    /** Marker for an upstream MV's persisted view-delta. The staging path
      * IS a Delta table whose columns are `<userCols> + openivm_timestamp +
      * openivm_multiplicity`. Downstream's `openivm_delta_<src>` temp view
      * preserves these columns verbatim.
      */
    val MvViewDelta = "MV_VIEW_DELTA"
  }
}

/**
 * Delta-backed catalog for DML staging records.
 *
 * All operations target `<warehouse>/_ivm/_meta/staging`.
 * Callers MUST invoke [[ensureTables]] once before any other method.
 */
object StagingCatalog extends DeltaRetrySupport {

  private val MetaSubPath = "_ivm/_meta/staging"

  // -------------------------------------------------------------------------
  // Internal helpers
  // -------------------------------------------------------------------------

  private def tablePath(spark: SparkSession): String = {
    val warehouseDir = spark.conf.get("spark.sql.warehouse.dir").stripSuffix("/")
    s"$warehouseDir/$MetaSubPath"
  }

  private def sqlLit(s: String): String =
    s"'${s.replace("\\", "\\\\").replace("'", "\\'")}'"

  private val StagingSchema: StructType = StructType(
    Array(
      StructField("base_table", StringType, nullable = false),
      StructField("op_type", StringType, nullable = false),
      StructField("staging_path", StringType, nullable = false),
      StructField("txn_ts", TimestampType, nullable = false),
      StructField("consumed_by", ArrayType(StringType, containsNull = false), nullable = false)
    )
  )

  private def rowToDelta(row: Row): StagingDelta =
    StagingDelta(
      baseTable = row.getAs[String]("base_table"),
      opType = row.getAs[String]("op_type"),
      stagingPath = row.getAs[String]("staging_path"),
      txnTs = row.getAs[Timestamp]("txn_ts"),
      consumedBy = row.getSeq[String](row.fieldIndex("consumed_by"))
    )

  private def deltaToRow(d: StagingDelta): Row =
    Row(d.baseTable, d.opType, d.stagingPath, d.txnTs, d.consumedBy.toArray)

  // -------------------------------------------------------------------------
  // Public API
  // -------------------------------------------------------------------------

  /** Idempotent: creates `_ivm._meta.staging` if absent. */
  def ensureTables(spark: SparkSession): Unit =
    DeltaTable
      .createIfNotExists(spark)
      .location(tablePath(spark))
      .addColumn(StructField("base_table", StringType, nullable = false))
      .addColumn(StructField("op_type", StringType, nullable = false))
      .addColumn(StructField("staging_path", StringType, nullable = false))
      .addColumn(StructField("txn_ts", TimestampType, nullable = false))
      .addColumn(
        StructField("consumed_by", ArrayType(StringType, containsNull = false), nullable = false)
      )
      .execute()

  /**
   * Record a new DML delta.  Uses MERGE on (base_table, staging_path) so the call is
   * idempotent: re-recording the same path does not overwrite `consumed_by`.
   */
  def record(spark: SparkSession, delta: StagingDelta): Unit = withDeltaRetry {
    val sourceDF = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(deltaToRow(delta)), 1),
      StagingSchema
    )
    DeltaTable
      .forPath(spark, tablePath(spark))
      .as("target")
      .merge(
        sourceDF.as("source"),
        "target.base_table = source.base_table AND target.staging_path = source.staging_path"
      )
      .whenNotMatched()
      .insertAll()
      .execute()
  }

  /**
   * Returns every staging row for `sources` that has NOT yet been consumed by `viewName`,
   * ordered by txn_ts ascending.
   *
   * @param sources    base-table names to scan. Only rows where `base_table IN sources`
   *                   are returned.
   * @param watermarks per-source low-water-mark filter. A row for source `S` is returned
   *                   only if `txn_ts > watermarks(S)`. Sources absent from the map are
   *                   unfiltered (legacy semantics). Used by MV-over-MV cascade to prevent
   *                   newly-created downstream MVs from double-applying upstream view-deltas
   *                   that pre-date their creation.
   */
  def collectFor(
      spark: SparkSession,
      viewName: String,
      sources: Seq[String],
      watermarks: Map[String, Timestamp] = Map.empty
  ): Seq[StagingDelta] = {
    if (sources.isEmpty) return Seq.empty
    val base = DeltaTable
      .forPath(spark, tablePath(spark))
      .toDF
      .where(col("base_table").isin(sources: _*) && !array_contains(col("consumed_by"), viewName))

    val filtered =
      if (watermarks.isEmpty) base
      else {
        // Build a (base_table → ts) lookup with a SQL CASE expression so the filter
        // pushes down to Delta scan.
        val cases = watermarks.toSeq.map { case (src, ts) =>
          s"WHEN base_table = ${sqlLit(src)} THEN txn_ts > CAST(${sqlLit(ts.toString)} AS TIMESTAMP)"
        }
        if (cases.isEmpty) base
        else {
          val expr = s"CASE ${cases.mkString(" ")} ELSE TRUE END"
          base.where(expr)
        }
      }

    filtered
      .orderBy("txn_ts")
      .collect()
      .map(rowToDelta)
      .toSeq
  }

  /**
   * Capture the current per-source watermark — the MAX(txn_ts) over all
   * `StagingCatalog` rows for each given source.
   *
   * Returns an empty map entry (skipped) for sources with no rows yet, so callers
   * who add the result to `MvMetadata.properties` only store keys that filter
   * actual rows.
   *
   * Used at downstream MV CREATE time so the new MV's first REFRESH ignores
   * upstream view-delta rows that were recorded before the MV existed (would
   * otherwise double-apply once + recompute-from-scratch via the initial CTAS).
   */
  def currentWatermarks(spark: SparkSession, sources: Seq[String]): Map[String, Timestamp] = {
    if (sources.isEmpty) return Map.empty
    import org.apache.spark.sql.functions.max
    DeltaTable
      .forPath(spark, tablePath(spark))
      .toDF
      .where(col("base_table").isin(sources: _*))
      .groupBy(col("base_table"))
      .agg(max(col("txn_ts")).as("max_ts"))
      .collect()
      .flatMap { row =>
        val src = row.getAs[String]("base_table")
        Option(row.getAs[Timestamp]("max_ts")).map(ts => src -> ts)
      }
      .toMap
  }

  /**
   * Append `viewName` to `consumed_by` for each staging row identified by `paths`.
   * Uses `array_union` so repeated calls with the same viewName are idempotent.
   */
  def markConsumed(spark: SparkSession, viewName: String, paths: Seq[String]): Unit = withDeltaRetry {
    if (paths.isEmpty) return
    val markerSchema = StructType(
      Array(
        StructField("staging_path", StringType, nullable = false),
        StructField("new_view", StringType, nullable = false)
      )
    )
    val rows     = paths.map(p => Row(p, viewName))
    val markerDF = spark.createDataFrame(spark.sparkContext.parallelize(rows, 1), markerSchema)
    DeltaTable
      .forPath(spark, tablePath(spark))
      .as("target")
      .merge(markerDF.as("source"), "target.staging_path = source.staging_path")
      .whenMatched()
      .updateExpr(Map("consumed_by" -> "array_union(target.consumed_by, array(source.new_view))"))
      .execute()
  }

  /**
   * Delete every staging row whose `consumed_by` covers ALL currently tracked MVs for its
   * `base_table`.  Used after a successful refresh to prune fully-replayed deltas.
   *
   * @param viewsByTable maps each base_table name to the set of MV names that depend on it
   */
  def pruneFullyConsumed(spark: SparkSession, viewsByTable: Map[String, Seq[String]]): Unit = withDeltaRetry {
    if (viewsByTable.isEmpty) return
    val dt = DeltaTable.forPath(spark, tablePath(spark))
    viewsByTable.foreach { case (baseTable, mvs) =>
      if (mvs.nonEmpty) {
        val mvsExpr = mvs.map(sqlLit).mkString(", ")
        dt.delete(
          s"base_table = ${sqlLit(baseTable)} AND " +
            s"size(array_except(array($mvsExpr), consumed_by)) = 0"
        )
      }
    }
  }

  /**
   * Delete every staging row with the given `baseTable`. Used by
   * `DropMaterializedViewCommand` when an MV is dropped: every downstream
   * MV's pending `MV_VIEW_DELTA` or trigger row from this MV becomes stale
   * (its `staging_path` may point at deleted view-delta dirs, and a future
   * CREATE of the same name might inherit unconsumed rows).
   *
   * Idempotent: no error if no rows match.
   */
  def removeForBaseTable(spark: SparkSession, baseTable: String): Unit = withDeltaRetry {
    DeltaTable
      .forPath(spark, tablePath(spark))
      .delete(s"base_table = ${sqlLit(baseTable)}")
  }
}
