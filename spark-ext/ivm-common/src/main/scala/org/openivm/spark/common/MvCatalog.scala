package org.openivm.spark.common

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types._
import org.openivm.spark.common.rocksdb.{OpenIvmRocksDB, OpenIvmRocksDBBatchOps, OpenIvmRocksDBRegistry, RocksDBCodec}
import org.slf4j.LoggerFactory

import java.io.File
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, Paths, SimpleFileVisitor}
import java.security.MessageDigest
import java.sql.Timestamp
import scala.util.Try

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
    *
    * Only `TxnTs`-encoded values (the intercept-mode default) round-trip to
    * `Timestamp`.  CDF-mode `DeltaVersion` values are silently skipped here
    * because nothing inside the intercept path consumes them.  Use
    * [[changeWatermarks]] for the mode-agnostic form.
    */
  def sourceWatermarks: Map[String, Timestamp] =
    properties.iterator
      .filter(_._1.startsWith(MvMetadata.WatermarkKeyPrefix))
      .flatMap { case (k, v) =>
        val src = k.stripPrefix(MvMetadata.WatermarkKeyPrefix)
        scala.util.Try(Timestamp.valueOf(v)).toOption.map(ts => src -> ts)
      }
      .toMap

  /** Decode `_ivm_watermark:<src>=<value>` properties into a mode-agnostic
    * per-source watermark map.  Each value is dispatched to
    * [[ChangeWatermark.decodeDeltaVersion]] first (recognises the `v:` prefix
    * emitted by `CdfChangePropagation`) and then falls back to
    * [[ChangeWatermark.decodeTxnTs]] (the historical `Timestamp.toString`
    * shape emitted by the intercept path).  Values that decode to neither are
    * dropped — that is the safe default because the consumer
    * (`ChangePropagation`) handles a missing watermark by treating the source
    * as fully unconsumed.
    */
  def changeWatermarks: Map[String, ChangeWatermark] =
    properties.iterator
      .filter(_._1.startsWith(MvMetadata.WatermarkKeyPrefix))
      .flatMap { case (k, v) =>
        val src = k.stripPrefix(MvMetadata.WatermarkKeyPrefix)
        ChangeWatermark
          .decodeDeltaVersion(v)
          .orElse(ChangeWatermark.decodeTxnTs(v))
          .map(src -> _)
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

  /** Legacy naive compile-cache keys.  Do not write new values here: they
    * were not schema/tier keyed.  They remain readable only for full-refresh
    * hidden-column recovery on old metadata rows.
    */
  val CompiledSqlKey: String            = "_ivm_compiled_sql"
  val CompiledInitialLoadSqlKey: String = "_ivm_compiled_initial_load_sql"

  /** Last observable compile-time cost-model hint captured during REFRESH. */
  val LastCostModelHintKey: String = "_ivm_last_cost_model_hint"

  /** Last observable unified refresh-intelligence decision captured during REFRESH. */
  val RefreshDecisionKey: String = "_ivm_refresh_decision"

  private val CompileCachePrefix: String                = "_ivm_compile_cache"
  private val CompileCacheSqlSuffix: String             = "sql"
  private val CompileCacheInitialLoadSuffix: String     = "initial_load_sql"
  private val CompileCacheRefreshTypeSuffix: String     = "refresh_type"
  private val CompileCacheRefreshTypeNameSuffix: String = "refresh_type_name"

  /** Build the property-map entries for the given source-watermarks. */
  def watermarkProperties(watermarks: Map[String, Timestamp]): Map[String, String] =
    watermarks.map { case (src, ts) => s"$WatermarkKeyPrefix$src" -> ts.toString }

  /** Build the property-map entries from a mode-agnostic
    * [[ChangeWatermark]] map.  Encodes the watermark per
    * [[ChangeWatermark.encode]] so the active [[ChangePropagation]] can
    * decode it back without inspecting any other persisted property.
    */
  def changeWatermarkProperties(watermarks: Map[String, ChangeWatermark]): Map[String, String] =
    watermarks.map { case (src, wm) => s"$WatermarkKeyPrefix$src" -> wm.encode }

  /** Build the property entry capturing this MV instance's cascade-delta capability. */
  def cascadeViewDeltaProperties(enabled: Boolean): Map[String, String] =
    Map(EmitsCascadeViewDeltaKey -> enabled.toString)

  /** Tier component for the W7.1 compile cache key.  It includes only facts
    * that may change the emitted SQL shape/classification, not quantitative
    * stats that should be handled by Spark-side rewrites after cache lookup.
    */
  def compileCacheTier(facts: WorkloadFacts): String = {
    val shapes = facts.deltaShape.toSeq
      .sortBy(_._1)
      .map { case (table, shape) => s"$table=${shape.compileFactValue}" }
      .mkString(",")
    val fks = facts.fkRelations
      .sortBy(fk => (fk.childTable, fk.childColumns.mkString(","), fk.parentTable, fk.parentColumns.mkString(",")))
      .map(fk =>
        s"${fk.childTable}(${fk.childColumns.mkString(",")})->${fk.parentTable}(${fk.parentColumns.mkString(",")})/${fk.rely}"
      )
      .mkString(";")
    val uniques = facts.uniqueKeys
      .sortBy(key => (key.table, key.columns.mkString(",")))
      .map(key => s"${key.table}(${key.columns.mkString(",")})/${key.rely}")
      .mkString(";")
    val raw =
      s"dialect=${facts.targetDialect}|compileOnly=${facts.compileOnly}|cascade=${facts.forceViewDeltaCascade}|" +
        s"insertOnly=${facts.assumeInsertOnly}|scd2RangeJoinAccel=${facts.scd2RangeJoinAccel}|" +
        s"declareRelyFk=${facts.declareRelyFk}|shape=$shapes|fk=$fks|" +
        s"unique=$uniques"
    val digest = MessageDigest.getInstance("SHA-256").digest(raw.getBytes("UTF-8"))
    digest.map("%02x".format(_)).mkString
  }

  def compileCacheSqlKey(sourceSchemaFingerprint: String, tier: String): String =
    compileCacheKey(sourceSchemaFingerprint, tier, CompileCacheSqlSuffix)

  def compileCacheInitialLoadSqlKey(sourceSchemaFingerprint: String, tier: String): String =
    compileCacheKey(sourceSchemaFingerprint, tier, CompileCacheInitialLoadSuffix)

  def compileCacheRefreshTypeKey(sourceSchemaFingerprint: String, tier: String): String =
    compileCacheKey(sourceSchemaFingerprint, tier, CompileCacheRefreshTypeSuffix)

  def compileCacheRefreshTypeNameKey(sourceSchemaFingerprint: String, tier: String): String =
    compileCacheKey(sourceSchemaFingerprint, tier, CompileCacheRefreshTypeNameSuffix)

  def cachedCompiledSql(
      properties: Map[String, String],
      sourceSchemaFingerprint: String,
      tier: String
  ): Option[String] =
    properties.get(compileCacheSqlKey(sourceSchemaFingerprint, tier)).filter(_.nonEmpty)

  def cachedInitialLoadSql(
      properties: Map[String, String],
      sourceSchemaFingerprint: String,
      tier: String
  ): Option[String] =
    properties.get(compileCacheInitialLoadSqlKey(sourceSchemaFingerprint, tier)).filter(_.nonEmpty)

  def anyCachedInitialLoadSql(properties: Map[String, String], sourceSchemaFingerprint: String): Option[String] = {
    val prefix = s"$CompileCachePrefix:$sourceSchemaFingerprint:"
    properties.collectFirst {
      case (key, value)
          if key.startsWith(prefix) && key.endsWith(s":$CompileCacheInitialLoadSuffix") && value.nonEmpty =>
        value
    }
  }

  /** Build schema/tier-scoped cache entries for the shape-stable openivm compile
    * result.  The cached SQL is still passed through SparkRefreshRewriter on
    * every REFRESH, which recreates per-refresh `openivm_old_*` / `*_merge`
    * temp views and substitutes fresh scratch paths.
    */
  def compiledProperties(
      sourceSchemaFingerprint: String,
      tier: String,
      compiledSql: String,
      initialLoadSql: String,
      refreshType: Int,
      refreshTypeName: String
  ): Map[String, String] = {
    val sql =
      if (compiledSql.nonEmpty) Map(compileCacheSqlKey(sourceSchemaFingerprint, tier) -> compiledSql) else Map.empty
    val init = if (initialLoadSql.nonEmpty) {
      Map(compileCacheInitialLoadSqlKey(sourceSchemaFingerprint, tier) -> initialLoadSql)
    } else Map.empty
    sql ++ init ++ Map(
      compileCacheRefreshTypeKey(sourceSchemaFingerprint, tier)     -> refreshType.toString,
      compileCacheRefreshTypeNameKey(sourceSchemaFingerprint, tier) -> refreshTypeName
    )
  }

  private def compileCacheKey(sourceSchemaFingerprint: String, tier: String, suffix: String): String =
    s"$CompileCachePrefix:$sourceSchemaFingerprint:$tier:$suffix"
}

/** RocksDB-backed catalog for materialized-view metadata. */
object MvCatalog {

  private val log = LoggerFactory.getLogger(getClass)

  private val IndexColumnFamilies = IndexDbColumnFamilies.All
  private val PerMvColumnFamilies = Seq("meta", "properties", "consumed")

  private val MvIndexCf     = "mv_index"
  private val SourceToMvsCf = "source_to_mvs"
  private val MetaCf        = "meta"
  private val PropertiesCf  = "properties"

  private val EmptyBytes = Array.emptyByteArray

  private val NameMetaKey                    = RocksDBCodec.utf8("name")
  private val QuerySqlMetaKey                = RocksDBCodec.utf8("query_sql")
  private val RefreshTypeMetaKey             = RocksDBCodec.utf8("refresh_type")
  private val RefreshTypeNameMetaKey         = RocksDBCodec.utf8("refresh_type_name")
  private val LastVersionMetaKey             = RocksDBCodec.utf8("last_version")
  private val SourceTablesMetaKey            = RocksDBCodec.utf8("source_tables")
  private val SourceSchemaFingerprintMetaKey = RocksDBCodec.utf8("source_schema_fingerprint")
  private val LocationMetaKey                = RocksDBCodec.utf8("location")
  private val CreatedAtMetaKey               = RocksDBCodec.utf8("created_at")

  private def canonicalLocalPath(path: String): String =
    new File(RocksDBCodec.requireLocalPath(path)).getCanonicalPath

  private def warehouseRoot(spark: SparkSession): Path =
    Paths.get(canonicalLocalPath(FeatureGate.stateWarehouse(spark)))

  private def indexDbPath(spark: SparkSession): String =
    warehouseRoot(spark).resolve("_openivm").resolve("index").resolve("rocksdb").toString

  private def perMvDbPath(spark: SparkSession, serializedName: String): String =
    warehouseRoot(spark)
      .resolve("_openivm")
      .resolve("mvs")
      .resolve(RocksDBCodec.safePathSegment(serializedName))
      .resolve("rocksdb")
      .toString

  private def openIndexDb(spark: SparkSession): OpenIvmRocksDB =
    OpenIvmRocksDBRegistry.getOrOpen(spark, indexDbPath(spark), IndexColumnFamilies)

  private def openPerMvDb(spark: SparkSession, serializedName: String): OpenIvmRocksDB =
    OpenIvmRocksDBRegistry.getOrOpen(spark, perMvDbPath(spark, serializedName), PerMvColumnFamilies)

  private def openExistingPerMvDbAt(spark: SparkSession, path: String): Option[OpenIvmRocksDB] = {
    val canonicalPath = canonicalLocalPath(path)
    val dbPath        = Paths.get(canonicalPath)
    if (Files.exists(dbPath.resolve("CURRENT"))) {
      Some(OpenIvmRocksDBRegistry.getOrOpen(spark, canonicalPath, PerMvColumnFamilies))
    } else {
      None
    }
  }

  private def serializeName(id: TableIdentifier): String =
    id.database.fold(id.identifier)(db => s"$db.${id.identifier}")

  private def deserializeName(s: String): TableIdentifier =
    CatalystSqlParser.parseTableIdentifier(s)

  private def copyBytes(bytes: Array[Byte]): Array[Byte] =
    java.util.Arrays.copyOf(bytes, bytes.length)

  private def closeQuietly(resource: AnyRef): Unit =
    resource match {
      case closeable: AutoCloseable =>
        try closeable.close()
        catch {
          case _: Throwable => ()
        }
      case _ => ()
    }

  private def collectPrefix(
      db: OpenIvmRocksDB,
      columnFamily: String,
      prefix: Array[Byte]
  ): Seq[(Array[Byte], Array[Byte])] = {
    val iterator = db.prefixScan(columnFamily, prefix)
    try {
      iterator.map { case (key, value) => copyBytes(key) -> copyBytes(value) }.toVector
    } finally {
      closeQuietly(iterator.asInstanceOf[AnyRef])
    }
  }

  private def getUtf8(db: OpenIvmRocksDB, columnFamily: String, key: Array[Byte]): Option[String] =
    db.get(columnFamily, key).map(RocksDBCodec.fromUtf8)

  private def getLong(db: OpenIvmRocksDB, columnFamily: String, key: Array[Byte]): Option[Long] =
    getUtf8(db, columnFamily, key).flatMap(value => Try(value.toLong).toOption)

  private def getInt(db: OpenIvmRocksDB, columnFamily: String, key: Array[Byte]): Option[Int] =
    getUtf8(db, columnFamily, key).flatMap(value => Try(value.toInt).toOption)

  private def encodeSourceTables(sourceTables: Seq[String]): Array[Byte] =
    RocksDBCodec.compositeKey(sourceTables.map(RocksDBCodec.utf8))

  private def decodeSourceTables(encoded: Array[Byte]): Seq[String] =
    if (encoded.isEmpty) Seq.empty
    else RocksDBCodec.splitComposite(encoded).map(RocksDBCodec.fromUtf8)

  private def sourceToMvKey(sourceTable: String, serializedName: String): Array[Byte] =
    RocksDBCodec.compositeKey(Seq(RocksDBCodec.utf8(sourceTable), RocksDBCodec.utf8(serializedName)))

  private def sourceToMvsPrefix(sourceTable: String): Array[Byte] =
    RocksDBCodec.compositeKey(Seq(RocksDBCodec.utf8(sourceTable), EmptyBytes))

  private def lookupPath(indexDb: OpenIvmRocksDB, serializedName: String): Option[String] =
    indexDb
      .get(MvIndexCf, RocksDBCodec.utf8(serializedName))
      .map(RocksDBCodec.fromUtf8)
      .map(canonicalLocalPath)

  private def readProperties(db: OpenIvmRocksDB): Map[String, String] =
    collectPrefix(db, PropertiesCf, EmptyBytes).map { case (key, value) =>
      RocksDBCodec.fromUtf8(key) -> RocksDBCodec.fromUtf8(value)
    }.toMap

  private def readMetadata(db: OpenIvmRocksDB): Option[MvMetadata] =
    for {
      serializedName          <- getUtf8(db, MetaCf, NameMetaKey)
      querySql                <- getUtf8(db, MetaCf, QuerySqlMetaKey)
      refreshType             <- getInt(db, MetaCf, RefreshTypeMetaKey)
      refreshTypeName         <- getUtf8(db, MetaCf, RefreshTypeNameMetaKey)
      lastVersion             <- getLong(db, MetaCf, LastVersionMetaKey)
      sourceTablesEncoded     <- db.get(MetaCf, SourceTablesMetaKey)
      sourceSchemaFingerprint <- getUtf8(db, MetaCf, SourceSchemaFingerprintMetaKey)
      location                <- getUtf8(db, MetaCf, LocationMetaKey)
      createdAtMillis         <- getLong(db, MetaCf, CreatedAtMetaKey)
    } yield MvMetadata(
      name = deserializeName(serializedName),
      querySql = querySql,
      refreshType = refreshType,
      refreshTypeName = refreshTypeName,
      lastVersion = lastVersion,
      sourceTables = decodeSourceTables(sourceTablesEncoded),
      sourceSchemaFingerprint = sourceSchemaFingerprint,
      location = location,
      createdAt = new Timestamp(createdAtMillis),
      properties = readProperties(db)
    )

  private def readMetadataAtPath(spark: SparkSession, path: String): Option[MvMetadata] =
    openExistingPerMvDbAt(spark, path).flatMap(readMetadata)

  private def indexedSources(indexDb: OpenIvmRocksDB, serializedName: String): Set[String] =
    collectPrefix(indexDb, SourceToMvsCf, EmptyBytes).flatMap { case (key, _) =>
      RocksDBCodec.splitComposite(key, 2) match {
        case Seq(sourceBytes, mvBytes) if RocksDBCodec.fromUtf8(mvBytes) == serializedName =>
          Some(RocksDBCodec.fromUtf8(sourceBytes))
        case _ =>
          None
      }
    }.toSet

  private def rewriteProperties(
      db: OpenIvmRocksDB,
      batch: org.rocksdb.WriteBatch,
      properties: Map[String, String]
  ): Unit = {
    collectPrefix(db, PropertiesCf, EmptyBytes).foreach { case (key, _) =>
      OpenIvmRocksDBBatchOps.delete(db, batch, PropertiesCf, key)
    }
    properties.toSeq.sortBy(_._1).foreach { case (key, value) =>
      OpenIvmRocksDBBatchOps.put(db, batch, PropertiesCf, RocksDBCodec.utf8(key), RocksDBCodec.utf8(value))
    }
  }

  private def writeMetadata(
      db: OpenIvmRocksDB,
      batch: org.rocksdb.WriteBatch,
      meta: MvMetadata
  ): Unit = {
    OpenIvmRocksDBBatchOps.put(db, batch, MetaCf, NameMetaKey, RocksDBCodec.utf8(serializeName(meta.name)))
    OpenIvmRocksDBBatchOps.put(db, batch, MetaCf, QuerySqlMetaKey, RocksDBCodec.utf8(meta.querySql))
    OpenIvmRocksDBBatchOps.put(db, batch, MetaCf, RefreshTypeMetaKey, RocksDBCodec.utf8(meta.refreshType.toString))
    OpenIvmRocksDBBatchOps.put(db, batch, MetaCf, RefreshTypeNameMetaKey, RocksDBCodec.utf8(meta.refreshTypeName))
    OpenIvmRocksDBBatchOps.put(db, batch, MetaCf, LastVersionMetaKey, RocksDBCodec.utf8(meta.lastVersion.toString))
    OpenIvmRocksDBBatchOps.put(db, batch, MetaCf, SourceTablesMetaKey, encodeSourceTables(meta.sourceTables))
    OpenIvmRocksDBBatchOps.put(
      db,
      batch,
      MetaCf,
      SourceSchemaFingerprintMetaKey,
      RocksDBCodec.utf8(meta.sourceSchemaFingerprint)
    )
    OpenIvmRocksDBBatchOps.put(db, batch, MetaCf, LocationMetaKey, RocksDBCodec.utf8(meta.location))
    OpenIvmRocksDBBatchOps.put(db, batch, MetaCf, CreatedAtMetaKey, RocksDBCodec.utf8(meta.createdAt.getTime.toString))
  }

  private def deleteRecursively(path: Path): Unit =
    if (Files.exists(path)) {
      Files.walkFileTree(
        path,
        new SimpleFileVisitor[Path] {
          override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
            Files.deleteIfExists(file)
            FileVisitResult.CONTINUE
          }

          override def postVisitDirectory(dir: Path, exc: java.io.IOException): FileVisitResult = {
            if (exc != null) throw exc
            Files.deleteIfExists(dir)
            FileVisitResult.CONTINUE
          }
        }
      )
      ()
    }

  private def maybeMigrateLegacyTables(spark: SparkSession, indexDb: OpenIvmRocksDB, warehouse: String): Unit = {
    val legacyMvMeta  = Paths.get(warehouse, "_ivm", "_meta", "mv_metadata").toString
    val legacyStaging = Paths.get(warehouse, "_ivm", "_meta", "staging").toString

    val hasLegacyMvMeta  = DeltaTable.isDeltaTable(spark, legacyMvMeta)
    val hasLegacyStaging = DeltaTable.isDeltaTable(spark, legacyStaging)
    if (!hasLegacyMvMeta && !hasLegacyStaging) {
      return
    }

    val alreadyMigrated = {
      val iterator = indexDb.prefixScan(MvIndexCf, Array.emptyByteArray)
      try iterator.hasNext
      finally closeQuietly(iterator.asInstanceOf[AnyRef])
    }
    if (alreadyMigrated) {
      return
    }

    def legacyRowToMetadata(row: Row): MvMetadata =
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

    def legacyRowToStaging(row: Row): StagingDelta =
      StagingDelta(
        baseTable = row.getAs[String]("base_table"),
        opType = row.getAs[String]("op_type"),
        stagingPath = row.getAs[String]("staging_path"),
        txnTs = row.getAs[Timestamp]("txn_ts"),
        consumedBy = row.getSeq[String](row.fieldIndex("consumed_by"))
      )

    val mvRows =
      if (hasLegacyMvMeta) spark.read.format("delta").load(legacyMvMeta).collect().toSeq else Seq.empty
    mvRows.map(legacyRowToMetadata).foreach(meta => upsert(spark, meta))

    val stagingRows =
      if (hasLegacyStaging) spark.read.format("delta").load(legacyStaging).collect().toSeq else Seq.empty
    stagingRows.map(legacyRowToStaging).foreach { delta =>
      StagingCatalog.record(spark, delta)
      delta.consumedBy.distinct.filter(_.trim.nonEmpty).foreach { viewName =>
        StagingCatalog.markConsumed(spark, viewName, Seq(delta.stagingPath))
      }
    }

    val hadoopConf = spark.sessionState.newHadoopConf()
    def dropDir(path: String): Unit = {
      val p  = new org.apache.hadoop.fs.Path(path)
      val fs = p.getFileSystem(hadoopConf)
      if (fs.exists(p)) fs.delete(p, true)
    }

    dropDir(legacyMvMeta)
    dropDir(legacyStaging)

    log.info(s"openivm-rocksdb migrated ${mvRows.size} MVs + ${stagingRows.size} staging rows from legacy Delta tables")
  }

  def ensureTables(spark: SparkSession): Unit = {
    val indexDb = openIndexDb(spark)
    maybeMigrateLegacyTables(spark, indexDb, warehouseRoot(spark).toString)
    ()
  }

  def upsert(spark: SparkSession, meta: MvMetadata): Unit = {
    val serializedName = serializeName(meta.name)
    val perMvPath      = perMvDbPath(spark, serializedName)
    val indexDb        = openIndexDb(spark)
    val perMvDb        = openPerMvDb(spark, serializedName)
    val oldSources =
      readMetadata(perMvDb).map(_.sourceTables.toSet).getOrElse(Set.empty[String]) ++ indexedSources(
        indexDb,
        serializedName
      )
    val newSources = meta.sourceTables.toSet

    perMvDb.withBatch { batch =>
      writeMetadata(perMvDb, batch, meta)
      rewriteProperties(perMvDb, batch, meta.properties)
    }

    indexDb.withBatch { batch =>
      (oldSources -- newSources).toSeq.sorted.foreach { sourceTable =>
        OpenIvmRocksDBBatchOps.delete(indexDb, batch, SourceToMvsCf, sourceToMvKey(sourceTable, serializedName))
      }
      newSources.toSeq.sorted.foreach { sourceTable =>
        OpenIvmRocksDBBatchOps.put(
          indexDb,
          batch,
          SourceToMvsCf,
          sourceToMvKey(sourceTable, serializedName),
          EmptyBytes
        )
      }
      OpenIvmRocksDBBatchOps.put(
        indexDb,
        batch,
        MvIndexCf,
        RocksDBCodec.utf8(serializedName),
        RocksDBCodec.utf8(perMvPath)
      )
    }
  }

  def lookup(spark: SparkSession, name: TableIdentifier): Option[MvMetadata] = {
    val indexDb        = openIndexDb(spark)
    val serializedName = serializeName(name)
    lookupPath(indexDb, serializedName).flatMap(path => readMetadataAtPath(spark, path))
  }

  def list(spark: SparkSession): Seq[MvMetadata] = {
    val indexDb = openIndexDb(spark)
    collectPrefix(indexDb, MvIndexCf, EmptyBytes)
      .sortBy { case (key, _) => RocksDBCodec.fromUtf8(key) }
      .flatMap { case (_, value) =>
        readMetadataAtPath(spark, RocksDBCodec.fromUtf8(value))
      }
  }

  def viewsForSource(spark: SparkSession, table: String): Seq[MvMetadata] = {
    val indexDb = openIndexDb(spark)
    val names = collectPrefix(indexDb, SourceToMvsCf, sourceToMvsPrefix(table))
      .flatMap { case (key, _) =>
        RocksDBCodec.splitComposite(key, 2) match {
          case Seq(_, mvBytes) => Some(RocksDBCodec.fromUtf8(mvBytes))
          case _               => None
        }
      }
      .distinct
      .sorted

    names.flatMap { serializedName =>
      lookupPath(indexDb, serializedName).flatMap(path => readMetadataAtPath(spark, path))
    }
  }

  def advance(spark: SparkSession, name: TableIdentifier, newVersion: Long): Unit = {
    val indexDb        = openIndexDb(spark)
    val serializedName = serializeName(name)

    lookupPath(indexDb, serializedName)
      .flatMap(path => openExistingPerMvDbAt(spark, path))
      .foreach { perMvDb =>
        val current = getLong(perMvDb, MetaCf, LastVersionMetaKey).getOrElse(-1L)
        if (newVersion > current) {
          perMvDb.withBatch { batch =>
            val latest = getLong(perMvDb, MetaCf, LastVersionMetaKey).getOrElse(-1L)
            if (newVersion > latest) {
              OpenIvmRocksDBBatchOps.put(
                perMvDb,
                batch,
                MetaCf,
                LastVersionMetaKey,
                RocksDBCodec.utf8(newVersion.toString)
              )
            }
          }
        }
      }
  }

  def updateProperties(
      spark: SparkSession,
      name: TableIdentifier,
      properties: Map[String, String]
  ): Unit = {
    val indexDb        = openIndexDb(spark)
    val serializedName = serializeName(name)

    lookupPath(indexDb, serializedName)
      .flatMap(path => openExistingPerMvDbAt(spark, path))
      .foreach { perMvDb =>
        if (readProperties(perMvDb) != properties) {
          perMvDb.withBatch { batch =>
            rewriteProperties(perMvDb, batch, properties)
          }
        }
      }
  }

  def remove(spark: SparkSession, name: TableIdentifier): Unit = {
    val serializedName = serializeName(name)
    val indexDb        = openIndexDb(spark)
    val indexedPath    = lookupPath(indexDb, serializedName)
    val candidatePath  = indexedPath.getOrElse(perMvDbPath(spark, serializedName))
    val sourceTables =
      readMetadataAtPath(spark, candidatePath).map(_.sourceTables.toSet).getOrElse(Set.empty[String]) ++ indexedSources(
        indexDb,
        serializedName
      )

    OpenIvmRocksDBRegistry.close(candidatePath)
    deleteRecursively(Paths.get(candidatePath))

    if (indexedPath.nonEmpty || sourceTables.nonEmpty) {
      indexDb.withBatch { batch =>
        OpenIvmRocksDBBatchOps.delete(indexDb, batch, MvIndexCf, RocksDBCodec.utf8(serializedName))
        sourceTables.toSeq.sorted.foreach { sourceTable =>
          OpenIvmRocksDBBatchOps.delete(indexDb, batch, SourceToMvsCf, sourceToMvKey(sourceTable, serializedName))
        }
      }
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
    val serialized = serializeName(meta.name)
    val content    = s"$serialized|${meta.location}|${meta.querySql}"
    val digest     = MessageDigest.getInstance("SHA-256").digest(content.getBytes("UTF-8"))
    digest.map("%02x".format(_)).mkString
  }
}
