package org.openivm.spark.common

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{Row, SparkSession}
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
 *
 * == Snapshot caching ==
 *
 * Read calls ([[collectFor]], [[currentWatermarks]]) are served from an
 * in-process Delta-version-aware snapshot cache so a TPC-DI-scale
 * benchmark (per-MV `collectFor` × 49 MVs × 3 batches, plus a
 * `currentWatermarks` per MV CREATE) does not pay the full
 * `DeltaTable.toDF.collect()` cost on every staging metadata read. The
 * cache is invalidated explicitly after every write
 * ([[record]], [[markConsumed]], [[pruneFullyConsumed]],
 * [[removeForBaseTable]]) and implicitly when the Delta log version on
 * disk advances since the last load.
 */
object StagingCatalog extends DeltaRetrySupport {

  private val MetaSubPath = "_ivm/_meta/staging"

  /** In-process Delta-version-aware cache for the staging snapshot. */
  private[common] val snapshotCache: DeltaSnapshotCache[Seq[StagingDelta]] =
    new DeltaSnapshotCache[Seq[StagingDelta]]()

  private def reloadFromDelta(spark: SparkSession, path: String): Seq[StagingDelta] = {
    DeltaTable
      .forPath(spark, path)
      .toDF
      .collect()
      .map(rowToDelta)
      .toSeq
  }

  private def snapshot(spark: SparkSession): Seq[StagingDelta] = {
    val path = tablePath(spark)
    snapshotCache.snapshot(spark, path, s => reloadFromDelta(s, path))
  }

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
  def record(spark: SparkSession, delta: StagingDelta): Unit = StagingCatalog.synchronized {
    val path = tablePath(spark)
    withDeltaRetry {
      val sourceDF = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(deltaToRow(delta)), 1),
        StagingSchema
      )
      DeltaTable
        .forPath(spark, path)
        .as("target")
        .merge(
          sourceDF.as("source"),
          "target.base_table = source.base_table AND target.staging_path = source.staging_path"
        )
        .whenNotMatched()
        .insertAll()
        .execute()
    }
    snapshotCache.invalidate(path)
  }

  /**
   * Returns every staging row for `sources` that has NOT yet been consumed by `viewName`,
   * ordered by txn_ts ascending.
   *
   * Reads from the snapshot cache: this is one of the hottest reads
   * (one call per REFRESH × per MV).
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
    val sourceSet = sources.toSet
    val all       = snapshot(spark)
    val matched = all.filter { d =>
      sourceSet.contains(d.baseTable) && !d.consumedBy.contains(viewName) && {
        watermarks.get(d.baseTable) match {
          case Some(wm) => d.txnTs.after(wm)
          case None     => true
        }
      }
    }
    matched.sortBy(_.txnTs.getTime)
  }

  /**
   * Capture the current per-source watermark — the MAX(txn_ts) over all
   * `StagingCatalog` rows for each given source.
   *
   * Reads from the snapshot cache.
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
    val sourceSet = sources.toSet
    snapshot(spark).iterator
      .filter(d => sourceSet.contains(d.baseTable))
      .foldLeft(Map.empty[String, Timestamp]) { (acc, d) =>
        acc.get(d.baseTable) match {
          case Some(existing) if existing.getTime >= d.txnTs.getTime => acc
          case _                                                     => acc + (d.baseTable -> d.txnTs)
        }
      }
  }

  /**
   * Append `viewName` to `consumed_by` for each staging row identified by `paths`.
   * Uses `array_union` so repeated calls with the same viewName are idempotent.
   */
  def markConsumed(spark: SparkSession, viewName: String, paths: Seq[String]): Unit =
    StagingCatalog.synchronized {
      withDeltaRetry {
        if (paths.isEmpty) return
        val path = tablePath(spark)
        val markerSchema = StructType(
          Array(
            StructField("staging_path", StringType, nullable = false),
            StructField("new_view", StringType, nullable = false)
          )
        )
        val rows     = paths.map(p => Row(p, viewName))
        val markerDF = spark.createDataFrame(spark.sparkContext.parallelize(rows, 1), markerSchema)
        DeltaTable
          .forPath(spark, path)
          .as("target")
          .merge(markerDF.as("source"), "target.staging_path = source.staging_path")
          .whenMatched()
          .updateExpr(Map("consumed_by" -> "array_union(target.consumed_by, array(source.new_view))"))
          .execute()
        snapshotCache.invalidate(path)
      }
    }

  /**
   * Delete every staging row whose `consumed_by` covers ALL currently tracked MVs for its
   * `base_table`.  Used after a successful refresh to prune fully-replayed deltas.
   *
   * @param viewsByTable maps each base_table name to the set of MV names that depend on it
   */
  def pruneFullyConsumed(spark: SparkSession, viewsByTable: Map[String, Seq[String]]): Unit =
    StagingCatalog.synchronized {
      withDeltaRetry {
        if (viewsByTable.isEmpty) return
        val path = tablePath(spark)
        val dt   = DeltaTable.forPath(spark, path)
        viewsByTable.foreach { case (baseTable, mvs) =>
          if (mvs.nonEmpty) {
            val mvsExpr = mvs.map(sqlLit).mkString(", ")
            dt.delete(
              s"base_table = ${sqlLit(baseTable)} AND " +
                s"size(array_except(array($mvsExpr), consumed_by)) = 0"
            )
          }
        }
        snapshotCache.invalidate(path)
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
  def removeForBaseTable(spark: SparkSession, baseTable: String): Unit =
    StagingCatalog.synchronized {
      val path = tablePath(spark)
      withDeltaRetry {
        DeltaTable
          .forPath(spark, path)
          .delete(s"base_table = ${sqlLit(baseTable)}")
      }
      snapshotCache.invalidate(path)
    }
}
