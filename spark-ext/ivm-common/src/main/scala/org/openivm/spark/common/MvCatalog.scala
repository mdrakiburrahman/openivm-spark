package org.openivm.spark.common

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.functions.{array_contains, col}
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
}

object MvMetadata {

  /** Property-key prefix for per-source low-water-mark timestamps. The
    * full key is `<WatermarkKeyPrefix><qualified-source-name>`, value is
    * the timestamp's `Timestamp.toString` form (`yyyy-MM-dd HH:mm:ss[.fff]`).
    */
  val WatermarkKeyPrefix: String = "_ivm_watermark:"

  /** Build the property-map entries for the given source-watermarks. */
  def watermarkProperties(watermarks: Map[String, Timestamp]): Map[String, String] =
    watermarks.map { case (src, ts) => s"$WatermarkKeyPrefix$src" -> ts.toString }
}

/**
 * Delta-backed catalog for materialized-view metadata.
 *
 * All operations target `<warehouse>/_ivm/_meta/mv_metadata`.
 * Callers MUST invoke [[ensureTables]] once before any other method.
 */
object MvCatalog extends DeltaRetrySupport {

  private val MetaSubPath = "_ivm/_meta/mv_metadata"

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
   *  Wrapped in a JVM-wide `synchronized` block so concurrent CREATE / REFRESH calls
   *  from multiple driver threads (e.g. the parallel-wave scheduler in
   *  [[org.openivm.spark.parity.TpcDiSpec]]) serialize on the single
   *  `_ivm/_meta/mv_metadata` Delta table, eliminating
   *  `DELTA_CONCURRENT_APPEND` flooding. Cross-process conflicts (unusual for
   *  openivm-spark which runs single-driver) are still handled by
   *  [[DeltaRetrySupport.withDeltaRetry]] inside the lock.
   */
  def upsert(spark: SparkSession, meta: MvMetadata): Unit = MvCatalog.synchronized {
    withDeltaRetry {
      val sourceDF = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(metaToRow(meta)), 1),
        MvSchema
      )
      DeltaTable
        .forPath(spark, tablePath(spark))
        .as("target")
        .merge(sourceDF.as("source"), "target.name = source.name")
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .execute()
    }
  }

  /** Returns None if the MV is not tracked. */
  def lookup(spark: SparkSession, name: TableIdentifier): Option[MvMetadata] =
    DeltaTable
      .forPath(spark, tablePath(spark))
      .toDF
      .where(col("name") === serializeName(name))
      .collect()
      .headOption
      .map(rowToMetadata)

  /** Lists every tracked materialized view, ordered by name. */
  def list(spark: SparkSession): Seq[MvMetadata] =
    DeltaTable
      .forPath(spark, tablePath(spark))
      .toDF
      .orderBy("name")
      .collect()
      .map(rowToMetadata)
      .toSeq

  /**
   * Returns MVs whose `source_tables` array contains `table`.
   * Used by the DML interceptor to determine whether a staging row is needed.
   */
  def viewsForSource(spark: SparkSession, table: String): Seq[MvMetadata] =
    DeltaTable
      .forPath(spark, tablePath(spark))
      .toDF
      .where(array_contains(col("source_tables"), table))
      .collect()
      .map(rowToMetadata)
      .toSeq

  /** Advance `last_version` after a successful refresh. No-op if the MV is not tracked. */
  def advance(spark: SparkSession, name: TableIdentifier, newVersion: Long): Unit =
    MvCatalog.synchronized {
      withDeltaRetry {
        DeltaTable
          .forPath(spark, tablePath(spark))
          .updateExpr(
            s"name = ${sqlLit(serializeName(name))}",
            Map("last_version" -> newVersion.toString)
          )
      }
    }

  /**
   * Delete the tracking row.
   * Idempotent: no error if the MV is already gone (safe for DROP MV IF EXISTS).
   */
  def remove(spark: SparkSession, name: TableIdentifier): Unit = MvCatalog.synchronized {
    withDeltaRetry {
      DeltaTable
        .forPath(spark, tablePath(spark))
        .delete(s"name = ${sqlLit(serializeName(name))}")
    }
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
