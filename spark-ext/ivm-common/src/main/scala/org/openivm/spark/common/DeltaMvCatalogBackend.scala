package org.openivm.spark.common

import io.delta.tables.DeltaTable
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.functions.{array_contains, col, lit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.JavaConverters._

/** Delta-backed authoritative MV catalog for multi-driver deployments. */
private[common] object DeltaMvCatalogBackend extends MvCatalogBackend with DeltaRetrySupport {

  private val Name                    = "name"
  private val QuerySql                = "query_sql"
  private val RefreshType             = "refresh_type"
  private val RefreshTypeName         = "refresh_type_name"
  private val LastVersion             = "last_version"
  private val SourceTables            = "source_tables"
  private val SourceSchemaFingerprint = "source_schema_fingerprint"
  private val Location                = "location"
  private val CreatedAt               = "created_at"
  private val Properties              = "properties"

  private val schema = StructType(
    Seq(
      StructField(Name, StringType, nullable = false),
      StructField(QuerySql, StringType, nullable = false),
      StructField(RefreshType, IntegerType, nullable = false),
      StructField(RefreshTypeName, StringType, nullable = false),
      StructField(LastVersion, LongType, nullable = false),
      StructField(SourceTables, ArrayType(StringType, containsNull = false), nullable = false),
      StructField(SourceSchemaFingerprint, StringType, nullable = false),
      StructField(Location, StringType, nullable = false),
      StructField(CreatedAt, TimestampType, nullable = false),
      StructField(Properties, MapType(StringType, StringType, valueContainsNull = false), nullable = false)
    )
  )

  private def path(spark: SparkSession): String = FeatureGate.catalogPath(spark).stripSuffix("/") + "/mv_metadata"

  private def serializedName(id: TableIdentifier): String =
    id.database.fold(id.identifier)(db => s"$db.${id.identifier}")

  private def singleRow(spark: SparkSession, meta: MvMetadata) = {
    val row = Row(
      serializedName(meta.name),
      meta.querySql,
      meta.refreshType,
      meta.refreshTypeName,
      meta.lastVersion,
      meta.sourceTables,
      meta.sourceSchemaFingerprint,
      meta.location,
      meta.createdAt,
      meta.properties
    )
    spark.createDataFrame(spark.sparkContext.parallelize(Seq(row), 1), schema)
  }

  private def decode(row: Row): MvMetadata =
    MvMetadata(
      name = CatalystSqlParser.parseTableIdentifier(row.getAs[String](Name)),
      querySql = row.getAs[String](QuerySql),
      refreshType = row.getAs[Int](RefreshType),
      refreshTypeName = row.getAs[String](RefreshTypeName),
      lastVersion = row.getAs[Long](LastVersion),
      sourceTables = row.getAs[Seq[String]](SourceTables),
      sourceSchemaFingerprint = row.getAs[String](SourceSchemaFingerprint),
      location = row.getAs[String](Location),
      createdAt = row.getAs[java.sql.Timestamp](CreatedAt),
      properties = row.getJavaMap[String, String](row.fieldIndex(Properties)).asScala.toMap
    )

  override def ensureTables(spark: SparkSession): Unit = withDeltaRetry {
    DeltaTable.createIfNotExists(spark).location(path(spark)).addColumns(schema).partitionedBy(Name).execute()
  }

  override def upsert(spark: SparkSession, meta: MvMetadata): Unit = withDeltaRetry {
    ensureTables(spark)
    val incoming     = singleRow(spark, meta)
    val expectedName = serializedName(meta.name)
    DeltaTable
      .forPath(spark, path(spark))
      .as("target")
      .merge(
        incoming.as("incoming"),
        col(s"target.$Name") === lit(expectedName) && col(s"target.$Name") === col(s"incoming.$Name")
      )
      .whenMatched()
      .updateAll()
      .whenNotMatched()
      .insertAll()
      .execute()
  }

  override def lookup(spark: SparkSession, name: TableIdentifier): Option[MvMetadata] = {
    ensureTables(spark)
    spark.read
      .format("delta")
      .load(path(spark))
      .filter(col(Name) === lit(serializedName(name)))
      .take(1)
      .headOption
      .map(decode)
  }

  override def list(spark: SparkSession): Seq[MvMetadata] = {
    ensureTables(spark)
    spark.read.format("delta").load(path(spark)).orderBy(col(Name)).collect().toSeq.map(decode)
  }

  override def viewsForSource(spark: SparkSession, table: String): Seq[MvMetadata] = {
    ensureTables(spark)
    spark.read
      .format("delta")
      .load(path(spark))
      .filter(array_contains(col(SourceTables), table))
      .orderBy(col(Name))
      .collect()
      .toSeq
      .map(decode)
  }

  override def advance(spark: SparkSession, name: TableIdentifier, newVersion: Long): Unit = withDeltaRetry {
    ensureTables(spark)
    DeltaTable
      .forPath(spark, path(spark))
      .update(
        col(Name) === lit(serializedName(name)) && col(LastVersion) < lit(newVersion),
        Map(LastVersion -> lit(newVersion))
      )
  }

  override def updateProperties(
      spark: SparkSession,
      name: TableIdentifier,
      properties: Map[String, String]
  ): Unit = lookup(spark, name).foreach(meta => upsert(spark, meta.copy(properties = properties)))

  override def remove(spark: SparkSession, name: TableIdentifier): Unit = withDeltaRetry {
    ensureTables(spark)
    DeltaTable.forPath(spark, path(spark)).delete(col(Name) === lit(serializedName(name)))
  }
}
