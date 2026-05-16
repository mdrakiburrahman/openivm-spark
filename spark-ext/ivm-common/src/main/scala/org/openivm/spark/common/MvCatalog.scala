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
)

/**
 * Delta-backed catalog for materialized-view metadata.
 *
 * All operations target `<warehouse>/_ivm/_meta/mv_metadata`.
 * Callers MUST invoke [[ensureTables]] once before any other method.
 */
object MvCatalog {

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
   *  Retries up to [[UpsertMaxRetries]] times on [[io.delta.exceptions.DeltaConcurrentModificationException]]
   *  so that concurrent upserts of distinct MVs all succeed via Delta's OCC.
   */
  def upsert(spark: SparkSession, meta: MvMetadata): Unit = {
    val MaxRetries = 5
    var attempt    = 0
    while (true) {
      try {
        upsertOnce(spark, meta)
        return
      } catch {
        case _: io.delta.exceptions.DeltaConcurrentModificationException if attempt < MaxRetries =>
          attempt += 1
          Thread.sleep(math.min(100L * attempt, 1000L))
      }
    }
  }

  private def upsertOnce(spark: SparkSession, meta: MvMetadata): Unit = {
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
    DeltaTable
      .forPath(spark, tablePath(spark))
      .updateExpr(
        s"name = ${sqlLit(serializeName(name))}",
        Map("last_version" -> newVersion.toString)
      )

  /**
   * Delete the tracking row.
   * Idempotent: no error if the MV is already gone (safe for DROP MV IF EXISTS).
   */
  def remove(spark: SparkSession, name: TableIdentifier): Unit =
    DeltaTable
      .forPath(spark, tablePath(spark))
      .delete(s"name = ${sqlLit(serializeName(name))}")

  /**
   * SHA-256 hex fingerprint of the source-table schemas.
   * Deterministic: input sorted by table name, encoded as `name=DDL` joined by newlines.
   * Changes whenever any column name or type changes.
   */
  def schemaFingerprint(sources: Map[String, StructType]): String = {
    val content = sources.toSeq
      .sortBy(_._1)
      .map { case (name, schema) => s"$name=${schema.toDDL}" }
      .mkString("\n")
    val digest = MessageDigest.getInstance("SHA-256").digest(content.getBytes("UTF-8"))
    digest.map("%02x".format(_)).mkString
  }
}
