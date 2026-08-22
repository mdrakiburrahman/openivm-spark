package org.openivm.spark.common

import org.apache.spark.sql.SparkSession
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

  /** User-supplied `CLUSTER BY` columns (declaration order), or empty when the
    * MV was created without an explicit `CLUSTER BY` clause.
    */
  def clusterColumns: Seq[String] =
    properties
      .get(MvMetadata.ClusterColumnsKey)
      .map(_.split(",").iterator.map(_.trim).filter(_.nonEmpty).toSeq)
      .getOrElse(Seq.empty)

  /** Whether the analyzed CREATE query contains a logical join. Missing or
    * malformed metadata is treated conservatively as joined so legacy views
    * cannot opt into insert-only aggregate shortcuts accidentally.
    */
  def queryHasJoin: Boolean =
    properties
      .get(MvMetadata.QueryHasJoinKey)
      .map(_.trim.toLowerCase(java.util.Locale.ROOT))
      .collect {
        case "true"  => true
        case "false" => false
      }
      .getOrElse(true)

  /** SQL suffix applied by the user-facing Spark VIEW when the physical Delta
    * table stores a larger incremental state (currently Top-K). Empty for a
    * materialized view whose user-facing object is the Delta table itself.
    */
  def backingViewSuffix: Option[String] =
    properties.get(MvMetadata.BackingViewSuffixKey).map(_.trim).filter(_.nonEmpty)

  /** Whether writes must target the sibling `<mv>__ivm_data` Delta table. */
  def usesBackingDataTable: Boolean =
    refreshType == RefreshTypeCode.AggregateHaving || backingViewSuffix.nonEmpty
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

  /** SQL suffix (for example `ORDER BY ... LIMIT ...`) evaluated by the
    * user-facing VIEW over an unlimited incremental backing table.
    */
  val BackingViewSuffixKey: String = "_ivm_backing_view_suffix"

  /** Property recording whether Spark's analyzed CREATE plan contains a join. */
  val QueryHasJoinKey: String = "_ivm_query_has_join"

  /** Compiled initial-load SQL persisted at CREATE.  The FULL_REFRESH path
    * reads it to reproduce the hidden bookkeeping columns (e.g.
    * `openivm_count_star`) that the raw user query omits, independent of the
    * opt-in compile-classification cache below.
    */
  val CompiledInitialLoadSqlKey: String = "_ivm_compiled_initial_load_sql"

  /** Property key recording the user-supplied `CLUSTER BY` columns from
    * `CREATE MATERIALIZED VIEW ... CLUSTER BY (...)`. Comma-separated, in
    * declaration order. Empty/absent when the MV has no explicit clustering.
    */
  val ClusterColumnsKey: String = "_ivm_cluster_cols"

  /** Last observable compile-time cost-model hint captured during REFRESH. */
  val LastCostModelHintKey: String = "_ivm_last_cost_model_hint"

  def queryShapeProperties(hasJoin: Boolean): Map[String, String] =
    Map(QueryHasJoinKey -> hasJoin.toString)

  /** CREATE-time compiled refresh type captured for refresh-span correlation. */
  val CompileRefreshTypeKey: String = "_ivm_compile_refresh_type"

  /** CREATE-time refresh classification reason captured for refresh-span correlation. */
  val RefreshReasonKey: String = "_ivm_refresh_reason"

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

  /** Build the property entry recording the user-supplied `CLUSTER BY` columns.
    * Returns an empty map when there are no clustering columns.
    */
  def clusterColumnsProperties(cols: Seq[String]): Map[String, String] =
    if (cols.isEmpty) Map.empty
    else Map(ClusterColumnsKey -> cols.mkString(","))

  /** Build the property entries recording the CREATE-time classification
    * metadata that REFRESH spans can reconstruct later without recompiling.
    */
  def refreshClassificationProperties(
      compileRefreshTypeName: String,
      refreshReason: String
  ): Map[String, String] = {
    val compile = Option(compileRefreshTypeName).map(_.trim).filter(_.nonEmpty).map(CompileRefreshTypeKey -> _).toMap
    val reason  = Option(refreshReason).map(_.trim).filter(_.nonEmpty).map(RefreshReasonKey -> _).toMap
    compile ++ reason
  }

  /** Tier component for the compile cache key.  It includes only facts
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
private[common] object RocksDbMvCatalogBackend extends MvCatalogBackend {

  private val log = LoggerFactory.getLogger(getClass)

  private val PerMvColumnFamilies            = OpenIvmStatePaths.PerMvColumnFamilies
  private val SourceDependencyColumnFamilies = OpenIvmStatePaths.SourceDependencyColumnFamilies

  private val DependentMvsCf = "dependent_mvs"
  private val MetaCf         = "meta"
  private val PropertiesCf   = "properties"

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

  private def perMvDbPath(spark: SparkSession, serializedName: String): String =
    OpenIvmStatePaths.perMvDbPath(spark, serializedName)

  private def openPerMvDb(spark: SparkSession, serializedName: String): OpenIvmRocksDB =
    OpenIvmRocksDBRegistry.getOrOpen(spark, perMvDbPath(spark, serializedName), PerMvColumnFamilies)

  private def openSourceDependencyDb(spark: SparkSession, sourceTable: String): OpenIvmRocksDB =
    OpenIvmRocksDBRegistry.getOrOpen(
      spark,
      OpenIvmStatePaths.sourceDependencyDbPath(spark, sourceTable),
      SourceDependencyColumnFamilies
    )

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

  private def dependentMvKey(serializedName: String): Array[Byte] = RocksDBCodec.utf8(serializedName)

  private def readProperties(db: OpenIvmRocksDB): Map[String, String] =
    collectPrefix(db, PropertiesCf, EmptyBytes).map { case (key, value) =>
      RocksDBCodec.fromUtf8(key) -> RocksDBCodec.fromUtf8(value)
    }.toMap

  private def readMetadata(db: OpenIvmRocksDB): Option[MvMetadata] =
    db.withSession {
      val keys = Seq(
        NameMetaKey,
        QuerySqlMetaKey,
        RefreshTypeMetaKey,
        RefreshTypeNameMetaKey,
        LastVersionMetaKey,
        SourceTablesMetaKey,
        SourceSchemaFingerprintMetaKey,
        LocationMetaKey,
        CreatedAtMetaKey
      )
      val values                           = db.multiGet(MetaCf, keys)
      def utf8(index: Int): Option[String] = values(index).map(RocksDBCodec.fromUtf8)

      for {
        serializedName          <- utf8(0)
        querySql                <- utf8(1)
        refreshType             <- utf8(2).flatMap(value => Try(value.toInt).toOption)
        refreshTypeName         <- utf8(3)
        lastVersion             <- utf8(4).flatMap(value => Try(value.toLong).toOption)
        sourceTablesEncoded     <- values(5)
        sourceSchemaFingerprint <- utf8(6)
        location                <- utf8(7)
        createdAtMillis         <- utf8(8).flatMap(value => Try(value.toLong).toOption)
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
    }

  private def readMetadataAtPath(spark: SparkSession, path: String): Option[MvMetadata] =
    openExistingPerMvDbAt(spark, path).flatMap(readMetadata)

  private def dependentViewNames(spark: SparkSession, sourceTable: String): Seq[String] = {
    val path = OpenIvmStatePaths.sourceDependencyDbPath(spark, sourceTable)
    if (OpenIvmStatePaths.isExistingDb(path)) {
      collectPrefix(openSourceDependencyDb(spark, sourceTable), DependentMvsCf, EmptyBytes).map { case (key, _) =>
        RocksDBCodec.fromUtf8(key)
      }.sorted
    } else {
      val names = list(spark)
        .filter(_.sourceTables.contains(sourceTable))
        .map(meta => serializeName(meta.name))
        .distinct
        .sorted
      if (names.nonEmpty) {
        val db = openSourceDependencyDb(spark, sourceTable)
        db.withBatch { batch =>
          names.foreach(name => OpenIvmRocksDBBatchOps.put(db, batch, DependentMvsCf, dependentMvKey(name), EmptyBytes))
        }
      }
      names
    }
  }

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

  def ensureTables(spark: SparkSession): Unit = {
    Files.createDirectories(OpenIvmStatePaths.mvsRoot(spark))
    Files.createDirectories(OpenIvmStatePaths.sourcesRoot(spark))
    ()
  }

  def upsert(spark: SparkSession, meta: MvMetadata): Unit = {
    val serializedName = serializeName(meta.name)
    val perMvDb        = openPerMvDb(spark, serializedName)
    val oldSources = perMvDb.withSession {
      readMetadata(perMvDb).map(_.sourceTables.toSet).getOrElse(Set.empty[String])
    }
    val newSources = meta.sourceTables.toSet

    // Reverse-index entries are published first. viewsForSource validates
    // them against authoritative per-MV metadata, so an interrupted update
    // can leave a harmless extra entry but cannot hide a real dependency.
    newSources.toSeq.sorted.foreach { sourceTable =>
      val db = openSourceDependencyDb(spark, sourceTable)
      db.withBatch { batch =>
        OpenIvmRocksDBBatchOps.put(db, batch, DependentMvsCf, dependentMvKey(serializedName), EmptyBytes)
      }
    }

    perMvDb.withSession {
      perMvDb.withBatch { batch =>
        writeMetadata(perMvDb, batch, meta)
        rewriteProperties(perMvDb, batch, meta.properties)
      }
    }

    (oldSources -- newSources).toSeq.sorted.foreach { sourceTable =>
      val path = OpenIvmStatePaths.sourceDependencyDbPath(spark, sourceTable)
      if (OpenIvmStatePaths.isExistingDb(path)) {
        val db = openSourceDependencyDb(spark, sourceTable)
        db.withBatch { batch =>
          OpenIvmRocksDBBatchOps.delete(db, batch, DependentMvsCf, dependentMvKey(serializedName))
        }
      }
    }
  }

  def lookup(spark: SparkSession, name: TableIdentifier): Option[MvMetadata] = {
    val serializedName = serializeName(name)
    readMetadataAtPath(spark, perMvDbPath(spark, serializedName))
  }

  def list(spark: SparkSession): Seq[MvMetadata] = {
    OpenIvmStatePaths.existingMvDbPaths(spark).flatMap(readMetadataAtPath(spark, _)).sortBy(m => serializeName(m.name))
  }

  def viewsForSource(spark: SparkSession, table: String): Seq[MvMetadata] = {
    dependentViewNames(spark, table)
      .flatMap { serializedName =>
        readMetadataAtPath(spark, perMvDbPath(spark, serializedName))
      }
      .filter(_.sourceTables.contains(table))
  }

  def advance(spark: SparkSession, name: TableIdentifier, newVersion: Long): Unit = {
    val serializedName = serializeName(name)

    openExistingPerMvDbAt(spark, perMvDbPath(spark, serializedName))
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
    val serializedName = serializeName(name)

    openExistingPerMvDbAt(spark, perMvDbPath(spark, serializedName))
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
    val candidatePath  = perMvDbPath(spark, serializedName)
    val sourceTables   = readMetadataAtPath(spark, candidatePath).map(_.sourceTables.toSet).getOrElse(Set.empty[String])

    OpenIvmRocksDBRegistry.close(candidatePath)
    deleteRecursively(Paths.get(candidatePath))

    sourceTables.toSeq.sorted.foreach { sourceTable =>
      val path = OpenIvmStatePaths.sourceDependencyDbPath(spark, sourceTable)
      if (OpenIvmStatePaths.isExistingDb(path)) {
        val db = openSourceDependencyDb(spark, sourceTable)
        db.withBatch { batch =>
          OpenIvmRocksDBBatchOps.delete(db, batch, DependentMvsCf, dependentMvKey(serializedName))
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

/** Session-selected facade over the local RocksDB and shared Delta catalog backends. */
object MvCatalog {
  private def backend(spark: SparkSession): MvCatalogBackend = MvCatalogBackend.forSession(spark)

  def ensureTables(spark: SparkSession): Unit = backend(spark).ensureTables(spark)

  def upsert(spark: SparkSession, meta: MvMetadata): Unit = backend(spark).upsert(spark, meta)

  def lookup(spark: SparkSession, name: TableIdentifier): Option[MvMetadata] = backend(spark).lookup(spark, name)

  def list(spark: SparkSession): Seq[MvMetadata] = backend(spark).list(spark)

  def viewsForSource(spark: SparkSession, table: String): Seq[MvMetadata] =
    backend(spark).viewsForSource(spark, table)

  def advance(spark: SparkSession, name: TableIdentifier, newVersion: Long): Unit =
    backend(spark).advance(spark, name, newVersion)

  def updateProperties(
      spark: SparkSession,
      name: TableIdentifier,
      properties: Map[String, String]
  ): Unit = backend(spark).updateProperties(spark, name, properties)

  def remove(spark: SparkSession, name: TableIdentifier): Unit = backend(spark).remove(spark, name)

  def schemaFingerprint(
      sources: Map[String, StructType],
      mvIdentityBySource: Map[String, String] = Map.empty
  ): String = RocksDbMvCatalogBackend.schemaFingerprint(sources, mvIdentityBySource)

  def mvIdentity(meta: MvMetadata): String = RocksDbMvCatalogBackend.mvIdentity(meta)
}
