package org.openivm.spark.common

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types._

import java.security.MessageDigest
import java.sql.Timestamp

/**
 * Metadata record for a single materialized view tracked by openivm-spark.
 *
 * @param name                    fully-qualified table identifier (e.g. db.v)
 * @param querySql                original CREATE MATERIALIZED VIEW body
 * @param refreshType             RefreshType enum ordinal (0..9)
 * @param refreshTypeName         human-readable form of the refresh type
 * @param lastVersion             Delta version of the MV table at last successful refresh
 * @param sourceTables            list of base tables the MV depends on
 * @param sourceSchemaFingerprint SHA-256 hex of the sorted (table → StructType.toDDL) encoding
 * @param location                HDFS / object-store path of the MV Delta table
 * @param createdAt               creation timestamp
 * @param properties              free-form key/value properties
 */
final case class MvMetadata(
    name: TableIdentifier,
    querySql: String,
    refreshType: Int,
    refreshTypeName: String,
    lastVersion: Long,
    sourceTables: Seq[String],
    sourceSchemaFingerprint: String,
    location: String,
    createdAt: Timestamp,
    properties: Map[String, String]
) {

  /** Decode `_ivm_watermark:<src>=<ts>` properties into a per-source low
    * water-mark map. Used by `StagingCatalog.collectFor` at REFRESH time to
    * skip staging rows that pre-date this MV's CREATE (otherwise a newly
    * created downstream MV would double-apply upstream view-deltas it has
    * already absorbed via its initial CTAS).
    */
  def sourceWatermarks: Map[String, Timestamp] =
    properties.iterator
      .filter(_._1.startsWith(MvMetadata.WatermarkKeyPrefix))
      .flatMap { case (k, v) =>
        val src = k.stripPrefix(MvMetadata.WatermarkKeyPrefix)
        scala.util.Try(Timestamp.valueOf(v)).toOption.map(ts => src -> ts)
      }
      .toMap

  /** Whether this persisted MV instance writes a downstream-consumable
    * `openivm_delta_<view>` on REFRESH. Falls back to the coarse refresh-type
    * capability for pre-property metadata rows created before
    * `_ivm_emits_cascade_view_delta` existed.
    */
  def emitsCascadeViewDelta: Boolean =
    properties
      .get(MvMetadata.EmitsCascadeViewDeltaKey)
      .map(_.trim.toLowerCase(java.util.Locale.ROOT))
      .collect {
        case "true"  => true
        case "false" => false
      }
      .getOrElse(RefreshTypeCode.emitsCascadeViewDelta(refreshType))
}

object MvMetadata {

  /** Property-key prefix for per-source low-water-mark timestamps. The
    * full key is `<WatermarkKeyPrefix><qualified-source-name>`, value is
    * the timestamp's `Timestamp.toString` form (`yyyy-MM-dd HH:mm:ss[.fff]`).
    */
  val WatermarkKeyPrefix: String = "_ivm_watermark:"

  /** Property key recording whether this concrete MV emits a persisted
    * cascade-usable view-delta at REFRESH time.
    */
  val EmitsCascadeViewDeltaKey: String = "_ivm_emits_cascade_view_delta"

  /** Property key recording the cached `CompiledRefresh.sql` from the
    * DuckDB-CLI compile-bridge at CREATE time, so REFRESH can skip
    * re-invoking the bridge. Empty / absent for legacy MVs created before
    * this caching was added; in that case REFRESH compiles lazily and
    * back-fills.
    */
  val CompiledSqlKey: String = "_ivm_compiled_sql"

  /** Property key recording the cached `CompiledRefresh.initialLoadSql`
    * companion to [[CompiledSqlKey]]. Kept under the same MV row so the
    * pair is atomic.
    */
  val CompiledInitialLoadSqlKey: String = "_ivm_compiled_initial_load_sql"

  /** Build the property-map entries for the given source-watermarks. */
  def watermarkProperties(watermarks: Map[String, Timestamp]): Map[String, String] =
    watermarks.map { case (src, ts) => s"$WatermarkKeyPrefix$src" -> ts.toString }

  /** Build the property entry capturing this MV instance's cascade-delta capability. */
  def cascadeViewDeltaProperties(enabled: Boolean): Map[String, String] =
    Map(EmitsCascadeViewDeltaKey -> enabled.toString)

  /** Build the property entries persisting the DuckDB-CLI compile result
    * so REFRESH can reuse it. Returns an empty map when `compiledSql` is
    * empty (legacy / compile-failed views) — the absence of the key is the
    * sentinel that REFRESH must compile on-the-fly.
    */
  def compiledProperties(compiledSql: String, initialLoadSql: String): Map[String, String] = {
    val a = if (compiledSql.nonEmpty) Map(CompiledSqlKey -> compiledSql) else Map.empty
    val b =
      if (initialLoadSql.nonEmpty) Map(CompiledInitialLoadSqlKey -> initialLoadSql) else Map.empty
    a ++ b
  }
}

/**
 * Delta-backed catalog for materialized-view metadata.
 *
 * All operations target `<warehouse>/_ivm/_meta/mv_metadata`.
 * Callers MUST invoke [[ensureTables]] once before any other method.
 *
 * == Snapshot caching ==
 *
 * Read calls ([[list]], [[lookup]], [[viewsForSource]]) are served from
 * an in-process Delta-version-aware snapshot cache so a TPC-DI-scale
 * benchmark (49 MVs × 3 batches × per-DML `viewsForSource`) does not
 * pay the full `DeltaTable.toDF.collect()` cost on every metadata read.
 * The cache is invalidated explicitly after every write
 * ([[upsert]], [[advance]], [[remove]]) and implicitly when the Delta
 * log version on disk advances since the last load (covers the rare
 * multi-driver case).
 */
object MvCatalog extends DeltaRetrySupport {

  private val MetaSubPath = "_ivm/_meta/mv_metadata"

  /** In-process Delta-version-aware cache for the mv_metadata snapshot.
    * Keyed by absolute table path so two SparkSessions in the same JVM
    * (e.g. parallel ivm-it forks pointing at the same warehouse) share
    * cache lookups.
    */
  private[common] val snapshotCache: DeltaSnapshotCache[Seq[MvMetadata]] =
    new DeltaSnapshotCache[Seq[MvMetadata]]()

  /** Reload the snapshot from Delta by issuing a single `DeltaTable.toDF.collect()`. */
  private def reloadFromDelta(spark: SparkSession, path: String): Seq[MvMetadata] = {
    val rows = DeltaTable
      .forPath(spark, path)
      .toDF
      .orderBy("name")
      .collect()
    rows.map(rowToMetadata).toSeq
  }

  private def snapshot(spark: SparkSession): Seq[MvMetadata] = {
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

  private def serializeName(id: TableIdentifier): String =
    id.database.fold(id.identifier)(db => s"$db.${id.identifier}")

  private def deserializeName(s: String): TableIdentifier =
    CatalystSqlParser.parseTableIdentifier(s)

  private def sqlLit(s: String): String =
    s"'${s.replace("\\", "\\\\").replace("'", "\\'")}'"

  private val MvSchema: StructType = StructType(
    Array(
      StructField("name", StringType, nullable = false),
      StructField("query_sql", StringType, nullable = false),
      StructField("refresh_type", IntegerType, nullable = false),
      StructField("refresh_type_name", StringType, nullable = false),
      StructField("last_version", LongType, nullable = false),
      StructField("source_tables", ArrayType(StringType, containsNull = false), nullable = false),
      StructField("source_schema_fingerprint", StringType, nullable = false),
      StructField("location", StringType, nullable = false),
      StructField("created_at", TimestampType, nullable = false),
      StructField("properties", MapType(StringType, StringType), nullable = true)
    )
  )

  private def rowToMetadata(row: Row): MvMetadata =
    MvMetadata(
      name = deserializeName(row.getAs[String]("name")),
      querySql = row.getAs[String]("query_sql"),
      refreshType = row.getAs[Int]("refresh_type"),
      refreshTypeName = row.getAs[String]("refresh_type_name"),
      lastVersion = row.getAs[Long]("last_version"),
      sourceTables = row.getSeq[String](row.fieldIndex("source_tables")),
      sourceSchemaFingerprint = row.getAs[String]("source_schema_fingerprint"),
      location = row.getAs[String]("location"),
      createdAt = row.getAs[Timestamp]("created_at"),
      properties = Option(row.getAs[Map[String, String]]("properties")).getOrElse(Map.empty)
    )

  private def metaToRow(meta: MvMetadata): Row = Row(
    serializeName(meta.name),
    meta.querySql,
    meta.refreshType,
    meta.refreshTypeName,
    meta.lastVersion,
    meta.sourceTables.toArray,
    meta.sourceSchemaFingerprint,
    meta.location,
    meta.createdAt,
    if (meta.properties.isEmpty) null else meta.properties
  )

  // -------------------------------------------------------------------------
  // Public API
  // -------------------------------------------------------------------------

  /** Idempotent: creates `_ivm._meta.mv_metadata` if absent. */
  def ensureTables(spark: SparkSession): Unit = {
    DeltaTable
      .createIfNotExists(spark)
      .location(tablePath(spark))
      .addColumn(StructField("name", StringType, nullable = false))
      .addColumn(StructField("query_sql", StringType, nullable = false))
      .addColumn(StructField("refresh_type", IntegerType, nullable = false))
      .addColumn(StructField("refresh_type_name", StringType, nullable = false))
      .addColumn(StructField("last_version", LongType, nullable = false))
      .addColumn(
        StructField("source_tables", ArrayType(StringType, containsNull = false), nullable = false)
      )
      .addColumn(StructField("source_schema_fingerprint", StringType, nullable = false))
      .addColumn(StructField("location", StringType, nullable = false))
      .addColumn(StructField("created_at", TimestampType, nullable = false))
      .addColumn(StructField("properties", MapType(StringType, StringType), nullable = true))
      .execute()
  }

  /** MERGE-based upsert keyed on `name`. Inserts new rows; updates all mutable fields.
   *
   *  Wrapped in a JVM-wide `synchronized` block so concurrent CREATE calls
   *  from multiple driver threads (e.g. the parallel-wave scheduler in
   *  [[org.openivm.spark.parity.TpcDiSpec]]) serialize on the single
   *  `_ivm/_meta/mv_metadata` Delta table, eliminating
   *  `DELTA_CONCURRENT_APPEND` flooding. Cross-process conflicts (unusual for
   *  openivm-spark which runs single-driver) are still handled by
   *  [[DeltaRetrySupport.withDeltaRetry]] inside the lock.
   *
   *  Invalidates the snapshot cache after a successful commit so subsequent
   *  reads in the same process see the write without an extra log
   *  round-trip.
   */
  def upsert(spark: SparkSession, meta: MvMetadata): Unit = MvCatalog.synchronized {
    val path = tablePath(spark)
    withDeltaRetry {
      val sourceDF = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(metaToRow(meta)), 1),
        MvSchema
      )
      DeltaTable
        .forPath(spark, path)
        .as("target")
        .merge(sourceDF.as("source"), "target.name = source.name")
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .execute()
    }
    snapshotCache.invalidate(path)
  }

  /** Returns None if the MV is not tracked. Read from the snapshot cache
    * (Delta-log version-checked) so the call costs O(MV count) in memory
    * instead of a full Delta scan per invocation.
    */
  def lookup(spark: SparkSession, name: TableIdentifier): Option[MvMetadata] = {
    val serialized = serializeName(name)
    snapshot(spark).find(m => serializeName(m.name) == serialized)
  }

  /** Lists every tracked materialized view, ordered by name. Read from the
    * snapshot cache (Delta-log version-checked).
    */
  def list(spark: SparkSession): Seq[MvMetadata] = snapshot(spark)

  /**
   * Returns MVs whose `source_tables` array contains `table`.
   * Used by the DML interceptor to determine whether a staging row is needed.
   *
   * Read from the snapshot cache: this is the single hottest catalog read
   * because it fires on every base-table DML.
   */
  def viewsForSource(spark: SparkSession, table: String): Seq[MvMetadata] =
    snapshot(spark).filter(_.sourceTables.contains(table))

  /** Advance `last_version` after a successful refresh. No-op if the MV is not tracked.
    *
    * Two notable properties:
    *
    *  - **Monotonic**: the UPDATE predicate is `name = … AND last_version <
    *    newVersion`, so a cross-process race that delivers stale `advance`
    *    calls in reverse order cannot rewind the recorded version.
    *  - **Lock-free**: the global `MvCatalog.synchronized` lock has been
    *    removed from this hot end-of-REFRESH write so 8-way parallel dbt
    *    refreshes don't serialise on a single JVM monitor. Delta OCC +
    *    [[DeltaRetrySupport.withDeltaRetry]] handles the (rare) write
    *    conflicts that remain. See `RefreshMutex` in
    *    `MaterializedViewCommands.scala` which still serialises per-MV
    *    REFRESH execution; this method is invoked under that per-MV lock
    *    so two concurrent `advance` calls necessarily target different
    *    rows of mv_metadata.
    */
  def advance(spark: SparkSession, name: TableIdentifier, newVersion: Long): Unit = {
    val path = tablePath(spark)
    withDeltaRetry {
      DeltaTable
        .forPath(spark, path)
        .updateExpr(
          s"name = ${sqlLit(serializeName(name))} AND last_version < $newVersion",
          Map("last_version" -> newVersion.toString)
        )
    }
    snapshotCache.invalidate(path)
  }

  /**
   * Update the free-form `properties` map for a single MV without touching
   * any other field (including `last_version`). Used by the REFRESH path
   * to back-fill the [[MvMetadata.CompiledSqlKey]] /
   * [[MvMetadata.CompiledInitialLoadSqlKey]] cache for legacy MVs created
   * before compile-result caching was added.
   *
   * Implementation overwrites the whole properties map (Delta has no
   * partial-map update primitive), so callers MUST pass the FULL desired
   * properties — typically `existing ++ newKeys`.
   */
  def updateProperties(
      spark: SparkSession,
      name: TableIdentifier,
      properties: Map[String, String]
  ): Unit = MvCatalog.synchronized {
    val path = tablePath(spark)
    withDeltaRetry {
      val literalMap =
        if (properties.isEmpty) "map()"
        else {
          val kvs = properties.toSeq
            .map { case (k, v) => s"${sqlLit(k)}, ${sqlLit(v)}" }
            .mkString(", ")
          s"map($kvs)"
        }
      DeltaTable
        .forPath(spark, path)
        .updateExpr(
          s"name = ${sqlLit(serializeName(name))}",
          Map("properties" -> literalMap)
        )
    }
    snapshotCache.invalidate(path)
  }

  /**
   * Delete the tracking row.
   * Idempotent: no error if the MV is already gone (safe for DROP MV IF EXISTS).
   */
  def remove(spark: SparkSession, name: TableIdentifier): Unit = MvCatalog.synchronized {
    val path = tablePath(spark)
    withDeltaRetry {
      DeltaTable
        .forPath(spark, path)
        .delete(s"name = ${sqlLit(serializeName(name))}")
    }
    snapshotCache.invalidate(path)
  }

  /**
   * SHA-256 hex fingerprint of the source-table schemas, optionally extended
   * with per-source MV identity hashes.
   *
   * Deterministic: input sorted by source name, encoded as `name=DDL` lines.
   * When `mvIdentityBySource` is non-empty, each entry contributes an
   * additional line `name#mv-identity=<hex>` so that a DROP + recreate of an
   * upstream MV with the same user schema but different body produces a
   * different fingerprint — `RefreshMaterializedViewCommand.run`'s
   * `INCOMPATIBLE_VIEW_SCHEMA_CHANGE` guard then catches the mismatch.
   *
   * Changes whenever any column name or type changes, or any tracked
   * upstream MV's identity changes.
   */
  def schemaFingerprint(
      sources: Map[String, StructType],
      mvIdentityBySource: Map[String, String] = Map.empty
  ): String = {
    val schemaLines = sources.toSeq
      .sortBy(_._1)
      .map { case (name, schema) => s"$name=${schema.toDDL}" }
    val mvIdLines = mvIdentityBySource.toSeq
      .sortBy(_._1)
      .map { case (name, ident) => s"$name#mv-identity=$ident" }
    val content = (schemaLines ++ mvIdLines).mkString("\n")
    val digest  = MessageDigest.getInstance("SHA-256").digest(content.getBytes("UTF-8"))
    digest.map("%02x".format(_)).mkString
  }

  /**
   * Identity hash for an upstream MV: SHA-256 over
   * `<metaName(name)>|<location>|<querySql>`. Used by
   * [[schemaFingerprint]]'s `mvIdentityBySource` argument so a DROP + recreate
   * of the same MV name with a different body is detected as drift.
   */
  def mvIdentity(meta: MvMetadata): String = {
    val serialized = meta.name.database.fold(meta.name.identifier)(db => s"$db.${meta.name.identifier}")
    val content    = s"$serialized|${meta.location}|${meta.querySql}"
    val digest     = MessageDigest.getInstance("SHA-256").digest(content.getBytes("UTF-8"))
    digest.map("%02x".format(_)).mkString
  }
}
