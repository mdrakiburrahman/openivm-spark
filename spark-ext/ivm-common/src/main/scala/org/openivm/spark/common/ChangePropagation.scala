package org.openivm.spark.common

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StructType, TimestampType}

/**
 * Abstract source of "what changed on base table X since watermark W".
 *
 * Two implementations:
 *  - [[InterceptChangePropagation]] reads change rows that [[StagingCatalog]]
 *    captured at DML-interception time.
 *  - [[CdfChangePropagation]] reads Delta Change Data Feed rows on demand at
 *    refresh time and assumes nothing about how writers reach the base table.
 *
 * The active implementation is selected per [[SparkSession]] via
 * [[ChangePropagationFactory.forSession]], which consults
 * [[FeatureGate.ChangeFeedModeKey]].
 */
trait ChangePropagation {

  /**
   * Validate that every source table is configured correctly for this mode.
   * Called once at `CREATE MATERIALIZED VIEW` time, before the compile bridge.
   *
   * Intercept: no-op.
   * CDF: throws `AnalysisException` if any source lacks
   *      `delta.enableChangeDataFeed = true`.
   */
  def validateSources(spark: SparkSession, sources: Seq[String]): Unit

  /**
   * Capture the "starting point" watermark per source at MV CREATE time, so
   * the first REFRESH ignores changes already absorbed by the initial CTAS.
   *
   * Intercept: the highest staging-row txn_ts per source.
   * CDF: the current Delta version per source.
   */
  def currentWatermarks(spark: SparkSession, sources: Seq[String]): Map[String, ChangeWatermark]

  /**
   * Collect the change batches that this refresh will consume.  One batch per
   * source with pending data (a source with no pending data is absent from
   * the returned sequence; the caller still registers an empty temp view for
   * it via [[buildSourceDeltaViewSql]] so multi-source UNION ALL resolves).
   */
  def collectChanges(
      spark: SparkSession,
      viewName: String,
      sources: Seq[String],
      persisted: Map[String, ChangeWatermark]
  ): Seq[ChangeBatch]

  /**
   * Build `CREATE OR REPLACE TEMP VIEW openivm_delta_<short>` SQL for one
   * source.  `batches` will contain at most one entry per source in the CDF
   * impl, and zero-or-many in the intercept impl.  An empty `batches`
   * produces the schema-shaped empty-view fallback already required by
   * multi-source openivm-emitted refresh SQL.
   *
   * Default impl returns the SQL; override [[registerSourceDeltaView]] if an
   * implementation needs to bypass SQL parsing (e.g. CDF uses the DataFrame
   * API to call `readChangeFeed` so it doesn't depend on Spark's TVF parser
   * fully understanding `table_changes()` in every Delta build).
   */
  def buildSourceDeltaViewSql(
      sourceTable: String,
      sourceSchema: StructType,
      batches: Seq[ChangeBatch]
  ): String

  /**
   * Register the `openivm_delta_<short>` TEMP VIEW for `sourceTable`.
   * Default impl runs `spark.sql(buildSourceDeltaViewSql(...))`; CDF impl
   * overrides this to bypass SQL parsing of the `table_changes()` TVF.
   * Returns the SQL string that was effectively executed (for diagnostic
   * logging).
   */
  def registerSourceDeltaView(
      spark: SparkSession,
      sourceTable: String,
      sourceSchema: StructType,
      batches: Seq[ChangeBatch]
  ): String = {
    if (batches.isEmpty) {
      registerEmptyDeltaView(spark, sourceTable, sourceSchema)
    } else {
      val sql = buildSourceDeltaViewSql(sourceTable, sourceSchema, batches)
      spark.sql(sql)
      sql
    }
  }

  /**
   * Register a zero-row `openivm_delta_<short>` TEMP VIEW carrying the EXACT
   * `sourceSchema` (plus the `openivm_timestamp` / `openivm_multiplicity`
   * bookkeeping columns) via the DataFrame API.
   *
   * Building the empty view from the live `sourceSchema` preserves each
   * struct-typed column's native `StructType` verbatim.  A SQL
   * `CAST(NULL AS <ddl>)` round-trip (via `DataType.sql`) can reconstruct a
   * subtly different nested type, which the openivm-emitted delta scan's
   * struct-field extraction then mis-resolves — e.g. `crm_customer_mgmt`'s
   * nested `Customer` struct, where a flattened phone field resolved to the
   * whole struct and broke `concat_ws` at analysis time.  Zero rows, so the
   * incremental path is unchanged; column order matches the non-empty views
   * (source columns, then timestamp, then multiplicity).
   */
  protected final def registerEmptyDeltaView(
      spark: SparkSession,
      sourceTable: String,
      sourceSchema: StructType
  ): String = {
    val short    = sourceTable.split("\\.").last
    val viewName = s"openivm_delta_$short"
    val schema   = sourceSchema.add("openivm_timestamp", TimestampType).add("openivm_multiplicity", IntegerType)
    spark
      .createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      .createOrReplaceTempView(viewName)
    s"/* empty typed delta view `$viewName` (${schema.length} cols) registered via DataFrame API */"
  }

  /**
   * Mark the batches as consumed by `viewName`.  Called after the refresh
   * SQL program completes successfully.
   */
  def markConsumed(spark: SparkSession, viewName: String, batches: Seq[ChangeBatch]): Unit

  /**
   * Prune fully-consumed state.  `viewsByTable` is the live MV catalog map
   * (`baseTable -> Seq[viewName]`).  Intercept impl prunes staging rows
   * fully consumed by all downstream MVs.  CDF impl is a no-op (Delta log
   * retention is the source of truth).
   */
  def pruneConsumed(spark: SparkSession, viewsByTable: Map[String, Seq[String]]): Unit

  /**
   * Tear down all bookkeeping rows whose base-table key is `baseTable`.
   * Called by `DROP MATERIALIZED VIEW` when the MV itself was a downstream
   * source.
   */
  def removeForBaseTable(spark: SparkSession, baseTable: String): Unit

  /**
   * `true` when the analyzer's [[org.openivm.spark.analyzer.IvmDmlInterceptorRule]]
   * (and its physical strategy) must be installed for this mode to function.
   * The extension entry-point consults this to gate registration.
   */
  def requiresDmlInterception: Boolean

  /**
   * `true` when MV backing Delta tables must be created with
   * `delta.enableChangeDataFeed = true` so downstream MVs can cascade via
   * the same change-propagation path.
   */
  def requiresMvCdf: Boolean

  /** Mode identifier, mostly for diagnostics / logging. */
  def mode: ChangeFeedMode
}
