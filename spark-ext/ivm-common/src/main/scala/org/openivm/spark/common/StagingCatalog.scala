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
 * @param opType      operation type: INSERT | DELETE | UPDATE_BEFORE | UPDATE_AFTER |
 *                    MERGE_SRC | OVERWRITE
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
   */
  def collectFor(spark: SparkSession, viewName: String, sources: Seq[String]): Seq[StagingDelta] = {
    if (sources.isEmpty) return Seq.empty
    DeltaTable
      .forPath(spark, tablePath(spark))
      .toDF
      .where(col("base_table").isin(sources: _*) && !array_contains(col("consumed_by"), viewName))
      .orderBy("txn_ts")
      .collect()
      .map(rowToDelta)
      .toSeq
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
}
