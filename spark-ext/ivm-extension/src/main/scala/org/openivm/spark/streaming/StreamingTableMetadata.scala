package org.openivm.spark.streaming

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.{DeltaLog, Snapshot}
import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import org.apache.spark.sql.delta.clustering.ClusteringMetadataDomain
import org.apache.spark.sql.delta.skipping.clustering.{ClusteredTableUtils, ClusteringColumnInfo}
import org.apache.spark.sql.delta.skipping.clustering.temp.ClusterBySpec
import org.apache.spark.sql.delta.stats.SkippingEligibleDataType
import org.apache.spark.sql.connector.expressions.Expressions
import org.apache.spark.sql.types.{DataType, StructType}
import org.openivm.spark.common.DeltaTableVersion

import java.nio.charset.StandardCharsets
import java.util.Locale
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

final case class StreamingTableTarget(
    name: Seq[String],
    identity: String,
    sqlIdentifier: String,
    dataPath: String,
    deltaTableId: String,
    tableId: String
) {

  def checkpointLocation: String =
    new Path(new Path(dataPath), StreamingTableMetadata.CheckpointDirectory).toString

  def queryName: String =
    s"openivm_streaming_${StreamingTableDefinition.sha256(s"$identity\n$dataPath").take(32)}"
}

final case class StreamingTableManifest(
    formatVersion: Int,
    target: StreamingTableTarget,
    definitionHash: String,
    semanticJson: String,
    diagnosticJson: String,
    operationalJson: String,
    operationalHash: String,
    sourcePaths: Seq[String]
)

final case class StreamingTableResetIntent(
    targetIdentity: String,
    targetPath: String,
    oldDeltaTableId: String,
    replacementDefinitionHash: String,
    sourcePaths: Seq[String],
    descendants: Seq[StreamingTableCascadeTarget] = Seq.empty,
    completedDescendantIdentities: Seq[String] = Seq.empty,
    upstreamDropped: Boolean = false
)

final case class StreamingTableCascadeTarget(
    kind: String = "streaming",
    name: Seq[String],
    identity: String,
    dataPath: String,
    deltaTableId: String,
    tableId: String,
    sourcePaths: Seq[String]
)

/** Catalog and Hadoop-filesystem ownership contract for one streaming table. */
object StreamingTableMetadata {

  val CheckpointDirectory: String = "_openivm-checkpoint"
  val ArchiveDirectory: String    = "_openivm-archive"
  val ArchiveEventFile: String    = "_openivm-archive-event.json"
  val OpenIvmDirectory: String    = "_openivm"
  val DefinitionFile: String      = "definition-v1.json"
  val ResetIndexDirectory: String = "_openivm-streaming-reset-index"

  val OwnerMarkerKey: String          = "openivm.streaming.owner"
  val OwnerMarkerValue: String        = "v1"
  val TableIdMarkerKey: String        = "openivm.streaming.table-id"
  val TargetIdentityMarkerKey: String = "openivm.streaming.target-identity"
  val TargetPathMarkerKey: String     = "openivm.streaming.target-path"
  val DeltaIdMarkerKey: String        = "openivm.streaming.delta-id"

  private val ReservedProperties = Set(
    OwnerMarkerKey,
    TableIdMarkerKey,
    TargetIdentityMarkerKey,
    TargetPathMarkerKey,
    DeltaIdMarkerKey
  )
  private val Mapper       = new ObjectMapper()
  private val MaxJsonBytes = 1024 * 1024
  private[streaming] val ManifestReadRetryTimeoutMsKey =
    "spark.openivm.streaming.manifestReadRetryTimeoutMs"
  private[streaming] val ManifestReadRetryIntervalMsKey =
    "spark.openivm.streaming.manifestReadRetryIntervalMs"
  private val DefaultManifestReadRetryTimeoutMs  = 15000L
  private val DefaultManifestReadRetryIntervalMs = 250L

  def canonicalIdentity(spark: SparkSession, name: Seq[String]): String = {
    if (name.isEmpty || name.exists(part => Option(part).forall(_.trim.isEmpty)))
      StreamingTableErrors.invalid("Streaming-table names must contain at least one non-empty identifier")
    val caseSensitive = spark.sessionState.conf.caseSensitiveAnalysis
    qualifiedNameParts(spark, name)
      .map(_.trim)
      .map(part => if (caseSensitive) part else part.toLowerCase(Locale.ROOT))
      .map(part => s"${part.length}:$part")
      .mkString("/")
  }

  def qualifiedNameParts(spark: SparkSession, name: Seq[String]): Seq[String] = {
    if (name.isEmpty || name.exists(part => Option(part).forall(_.trim.isEmpty)))
      StreamingTableErrors.invalid("Streaming-table names must contain at least one non-empty identifier")
    name.map(_.trim) match {
      case Seq(table) =>
        val (catalog, namespace) = currentCatalogAndNamespace(spark)
        Seq(catalog, namespace, table)
      case Seq(namespace, table) =>
        val (catalog, _) = currentCatalogAndNamespace(spark)
        Seq(catalog, namespace, table)
      case parts => parts
    }
  }

  def quoteMultipart(name: Seq[String]): String =
    name.map(part => s"`${part.replace("`", "``")}`").mkString(".")

  def normalizePath(spark: SparkSession, path: Path): String = {
    val fs        = path.getFileSystem(spark.sessionState.newHadoopConf())
    val qualified = fs.makeQualified(path)
    val uri       = qualified.toUri.normalize()
    val scheme    = Option(uri.getScheme).getOrElse("")
    val authority = Option(uri.getAuthority).filter(_.nonEmpty)
    val rawPath   = Option(uri.getPath).getOrElse("")
    val pathPart  = if (rawPath == "/") rawPath else rawPath.stripSuffix("/")
    authority match {
      case Some(value)             => s"$scheme://$value$pathPart"
      case None if scheme.nonEmpty => s"$scheme:$pathPart"
      case None                    => pathPart
    }
  }

  def normalizePath(spark: SparkSession, path: String): String = normalizePath(spark, new Path(path))

  def isWithinWarehouse(spark: SparkSession, path: String): Boolean =
    pathIsWithin(
      normalizePath(spark, spark.conf.get("spark.sql.warehouse.dir")),
      normalizePath(spark, path)
    )

  def validateRequestedLocation(spark: SparkSession, location: String, name: Seq[String]): Unit = {
    val path       = new Path(location)
    val fs         = path.getFileSystem(spark.sessionState.newHadoopConf())
    val normalized = normalizePath(spark, fs.makeQualified(path))
    if (isUnsafeTargetRoot(spark, fs.makeQualified(path)))
      StreamingTableErrors.invalid(
        s"LOCATION '$location' is a filesystem or warehouse root and cannot be a streaming-table target"
      )
    val qualified = qualifiedNameParts(spark, name)
    if (qualified.size >= 2) {
      val namespace         = qualified.dropRight(1).mkString(".")
      val namespaceLocation = spark.catalog.getDatabase(namespace).locationUri
      if (normalized == normalizePath(spark, namespaceLocation))
        StreamingTableErrors.invalid(
          s"LOCATION '$location' is the catalog namespace root '$namespaceLocation' and cannot be deleted safely"
        )
    }
  }

  def plannedTargetPath(
      spark: SparkSession,
      name: Seq[String],
      location: Option[String]
  ): Option[String] =
    location.map(value => normalizePath(spark, value)).orElse {
      val qualified = qualifiedNameParts(spark, name)
      if (qualified.size == 3 && qualified.head.equalsIgnoreCase("spark_catalog")) {
        val identifier = new TableIdentifier(
          qualified.last,
          Some(qualified(1)),
          Some(qualified.head)
        )
        Some(normalizePath(spark, new Path(spark.sessionState.catalog.defaultTablePath(identifier))))
      } else
        spark.conf
          .getOption("spark.openivm.managedTablesRoot")
          .map(_.trim)
          .filter(_.nonEmpty)
          .map(root => normalizePath(spark, new Path(new Path(root), qualified.takeRight(2).mkString("/"))))
    }

  def validateTargetPath(spark: SparkSession, path: String): Unit = {
    val hadoopPath = new Path(path)
    val fs         = hadoopPath.getFileSystem(spark.sessionState.newHadoopConf())
    if (isUnsafeTargetRoot(spark, fs.makeQualified(hadoopPath)))
      StreamingTableErrors.invalid(s"Unsafe streaming-table target path '$path'")
  }

  def validateProvider(spec: StreamingTableSpec): Unit =
    spec.provider.foreach { provider =>
      if (!provider.equalsIgnoreCase("delta"))
        StreamingTableErrors.invalid(
          s"Only USING DELTA is supported for streaming tables; found '${provider.trim}'"
        )
    }

  def validateOutputSchema(schema: StructType, partitions: Seq[String], spark: SparkSession): Unit = {
    if (schema.isEmpty) StreamingTableErrors.invalid("Streaming-table queries must produce at least one column")
    val resolver = spark.sessionState.analyzer.resolver
    val duplicates = schema.fieldNames.indices.exists { index =>
      schema.fieldNames.take(index).exists(resolver(_, schema.fieldNames(index)))
    }
    if (duplicates) StreamingTableErrors.invalid("Streaming-table output has duplicate column names")

    val normalizedPartitions = partitions.map(_.trim)
    if (normalizedPartitions.exists(_.isEmpty))
      StreamingTableErrors.invalid("PARTITIONED BY contains an empty column name")
    if (
      normalizedPartitions.indices.exists { index =>
        normalizedPartitions.take(index).exists(resolver(_, normalizedPartitions(index)))
      }
    )
      StreamingTableErrors.invalid("PARTITIONED BY contains duplicate columns")
    normalizedPartitions.foreach { partition =>
      if (!schema.fieldNames.exists(resolver(_, partition)))
        StreamingTableErrors.invalid(
          s"PARTITIONED BY column '$partition' is not present in the streaming query output"
        )
    }
  }

  def validateDestinationLayout(
      schema: StructType,
      partitions: Seq[String],
      clusterColumns: Seq[Seq[String]],
      tableProperties: Map[String, String],
      spark: SparkSession
  ): Unit = {
    validateOutputSchema(schema, partitions, spark)
    if (tableProperties.keys.exists(_.equalsIgnoreCase(ClusteredTableUtils.PROP_CLUSTERING_COLUMNS)))
      StreamingTableErrors.invalid(
        s"TBLPROPERTIES may not set Delta-managed ${ClusteredTableUtils.PROP_CLUSTERING_COLUMNS}"
      )
    ClusteredTableUtils.validateExistingTableFeatureProperties(tableProperties)
    if (partitions.nonEmpty && clusterColumns.nonEmpty)
      StreamingTableErrors.invalid("PARTITIONED BY and CLUSTER BY cannot be used together on one destination")
    if (clusterColumns.nonEmpty) {
      ClusteredTableUtils.validateNumClusteringColumns(clusterColumns)
      val duplicates = clusterColumns.indices.exists { index =>
        clusterColumns.take(index).exists(sameColumnReference(_, clusterColumns(index), spark))
      }
      if (duplicates)
        StreamingTableErrors.invalid("CLUSTER BY contains duplicate column references")
      clusterColumns.foreach { reference =>
        if (reference.isEmpty || reference.exists(part => Option(part).forall(_.trim.isEmpty)))
          StreamingTableErrors.invalid("CLUSTER BY contains an empty column reference")
        val dataType = resolveColumnReference(schema, reference, spark)
        if (!SkippingEligibleDataType(dataType))
          StreamingTableErrors.invalid(
            s"CLUSTER BY column ${renderColumnReference(reference)} has unsupported datatype ${dataType.catalogString}"
          )
      }
      val nativeProperties = tableProperties ++ ClusteredTableUtils.getTableFeatureProperties(tableProperties)
      val metadata = Metadata(
        schemaString = schema.json,
        partitionColumns = partitions,
        configuration = nativeProperties
      )
      val protocol = Protocol.forNewTable(spark, Some(metadata))
      ClusteredTableUtils.validateClusteringColumnsInStatsSchema(
        protocol,
        metadata,
        ClusterBySpec(clusterColumns.map(parts => Expressions.column(renderColumnReference(parts))))
      )
    }
  }

  def validateUserProperties(properties: Map[String, String]): Unit = {
    val normalized = properties.keys.map(_.toLowerCase(Locale.ROOT)).toSeq
    if (normalized.distinct.size != normalized.size)
      StreamingTableErrors.invalid("TBLPROPERTIES contains duplicate keys that differ only by case")
    val attempted = normalized.toSet.intersect(ReservedProperties)
    if (attempted.nonEmpty)
      StreamingTableErrors.invalid(
        s"TBLPROPERTIES may not set extension-reserved keys: ${attempted.toSeq.sorted.mkString(", ")}"
      )
  }

  private def resolveColumnReference(
      schema: StructType,
      reference: Seq[String],
      spark: SparkSession
  ): DataType = {
    val resolver = spark.sessionState.analyzer.resolver
    reference.foldLeft[DataType](schema) { case (current, part) =>
      current match {
        case struct: StructType =>
          struct
            .find(field => resolver(field.name, part))
            .map(_.dataType)
            .getOrElse(
              StreamingTableErrors.invalid(
                s"CLUSTER BY column ${renderColumnReference(reference)} is not present in the query output"
              )
            )
        case _ =>
          StreamingTableErrors.invalid(
            s"CLUSTER BY reference ${renderColumnReference(reference)} descends through a non-struct output column"
          )
      }
    }
  }

  private def sameColumnReference(
      left: Seq[String],
      right: Seq[String],
      spark: SparkSession
  ): Boolean =
    left.size == right.size &&
      left.zip(right).forall { case (a, b) => spark.sessionState.analyzer.resolver(a, b) }

  def renderColumnReference(reference: Seq[String]): String =
    reference.map(part => s"`${part.replace("`", "``")}`").mkString(".")

  def catalogTableExists(spark: SparkSession, name: Seq[String]): Boolean =
    spark.catalog.tableExists(quoteMultipart(name))

  def resolveDeltaTarget(
      spark: SparkSession,
      name: Seq[String],
      requireTableIdMarker: Boolean
  ): StreamingTableTarget = {
    val sqlIdentifier = quoteMultipart(name)
    val log = DeltaTableVersion
      .deltaLogOption(spark, sqlIdentifier)
      .getOrElse(
        StreamingTableErrors.invalid(
          s"Target $sqlIdentifier is not a readable Delta table; refusing to adopt or overwrite it"
        )
      )
    targetFromLog(spark, name, log, requireTableIdMarker)
  }

  def verifyOwned(
      spark: SparkSession,
      target: StreamingTableTarget,
      manifest: StreamingTableManifest
  ): Unit = {
    if (manifest.formatVersion != StreamingTableDefinition.FormatVersion)
      StreamingTableErrors.invalid(
        s"Unsupported streaming-table definition format ${manifest.formatVersion} for ${target.sqlIdentifier}"
      )
    if (
      manifest.target.identity != target.identity ||
      manifest.target.dataPath != target.dataPath ||
      manifest.target.deltaTableId != target.deltaTableId ||
      manifest.target.tableId != target.tableId
    )
      StreamingTableErrors.invalid(
        s"Streaming-table ownership metadata for ${target.sqlIdentifier} does not match its catalog/path/Delta identity"
      )
  }

  def readOwnedManifest(spark: SparkSession, name: Seq[String]): StreamingTableManifest = {
    val target   = resolveDeltaTarget(spark, name, requireTableIdMarker = true)
    val manifest = readManifest(spark, target)
    verifyOwned(spark, target, manifest)
    manifest
  }

  def createOwnedTarget(
      spark: SparkSession,
      spec: StreamingTableSpec,
      schema: StructType
  ): StreamingTableTarget = {
    validateProvider(spec)
    validateUserProperties(spec.tableProperties)
    validateDestinationLayout(
      schema,
      spec.partitionColumns,
      spec.clusterColumns,
      spec.tableProperties,
      spark
    )
    if (catalogTableExists(spark, spec.name))
      StreamingTableErrors.invalid(
        s"Target ${quoteMultipart(spec.name)} already exists and cannot be adopted as a streaming table"
      )
    spec.location.foreach { location =>
      validateRequestedLocation(spark, location, spec.name)
      assertLocationAbsent(spark, location)
    }

    val tableId  = UUID.randomUUID().toString
    val identity = canonicalIdentity(spark, spec.name)
    val properties = spec.tableProperties ++ Map(
      OwnerMarkerKey          -> OwnerMarkerValue,
      TableIdMarkerKey        -> tableId,
      TargetIdentityMarkerKey -> identity
    )
    spark.sql(createTableSql(spec, schema, properties)).collect()

    val initiallyCreated = resolveDeltaTarget(spark, spec.name, requireTableIdMarker = false)
    val setIdentitySql =
      s"ALTER TABLE ${initiallyCreated.sqlIdentifier} SET TBLPROPERTIES " +
        s"(${sqlString(DeltaIdMarkerKey)}=${sqlString(initiallyCreated.deltaTableId)}, " +
        s"${sqlString(TargetPathMarkerKey)}=${sqlString(initiallyCreated.dataPath)})"
    spark.sql(setIdentitySql).collect()

    val target = resolveDeltaTarget(spark, spec.name, requireTableIdMarker = true)
    if (target.tableId != tableId)
      StreamingTableErrors.invalid(
        s"Target ${target.sqlIdentifier} changed ownership identity while it was being created"
      )
    target
  }

  def validateExistingTargetAgainstManifest(
      spark: SparkSession,
      target: StreamingTableTarget,
      manifest: StreamingTableManifest
  ): Unit = {
    val log = DeltaTableVersion
      .deltaLogOption(spark, target.sqlIdentifier)
      .getOrElse(
        StreamingTableErrors.invalid(s"Target ${target.sqlIdentifier} is no longer a readable Delta table")
      )
    val snapshot = log.update()
    if (snapshot.metadata.id != target.deltaTableId)
      StreamingTableErrors.invalid(
        s"Target ${target.sqlIdentifier} was replaced after ownership verification"
      )
    val semantic       = Mapper.readTree(manifest.semanticJson)
    val expectedSchema = requiredText(semantic, "targetSchema", target.sqlIdentifier)
    if (StreamingTableDefinition.targetSchemaSignature(snapshot.metadata.schema) != expectedSchema)
      StreamingTableErrors.invalid(
        s"Target ${target.sqlIdentifier} schema drifted from its stored streaming-table definition"
      )
    val partitionNode = requiredNode(semantic, "partitionColumns", target.sqlIdentifier)
    if (!partitionNode.isArray)
      StreamingTableErrors.invalid(s"Stored partition definition for ${target.sqlIdentifier} is corrupt")
    val expectedPartitions = partitionNode
      .elements()
      .asScala
      .toSeq
      .map { value =>
        if (!value.isTextual)
          StreamingTableErrors.invalid(
            s"Stored partition definition for ${target.sqlIdentifier} is corrupt"
          )
        value.asText()
      }
    val resolver = spark.sessionState.analyzer.resolver
    if (
      snapshot.metadata.partitionColumns.size != expectedPartitions.size ||
      !snapshot.metadata.partitionColumns.zip(expectedPartitions).forall { case (actual, expected) =>
        resolver(actual, expected)
      }
    )
      StreamingTableErrors.invalid(
        s"Target ${target.sqlIdentifier} partitioning drifted from its stored streaming-table definition"
      )
    validateStoredClustering(snapshot, storedClusterColumns(semantic, target.sqlIdentifier), spark, target)
    val properties = requiredNode(semantic, "tableProperties", target.sqlIdentifier)
    if (!properties.isObject)
      StreamingTableErrors.invalid(s"Stored table properties for ${target.sqlIdentifier} are corrupt")
    properties.properties().asScala.foreach { field =>
      val expected = field.getValue
      if (!expected.isTextual)
        StreamingTableErrors.invalid(s"Stored table property '${field.getKey}' is corrupt")
      val actual = snapshot.metadata.configuration.get(field.getKey)
      val matches = actual.exists { value =>
        val stored = expected.asText()
        if (stored.startsWith("[REDACTED:") && stored.endsWith("]"))
          stored == s"[REDACTED:${StreamingTableDefinition.sha256(value).take(16)}]"
        else stored == value
      }
      if (!matches)
        StreamingTableErrors.invalid(
          s"Target ${target.sqlIdentifier} no longer has stored table property '${field.getKey}'"
        )
    }
  }

  def validateNoSourceTargetOverlap(
      spark: SparkSession,
      sourcePaths: Seq[String],
      targetPath: Option[String]
  ): Unit =
    targetPath.foreach { target =>
      sourcePaths.foreach { source =>
        if (pathsOverlap(spark, source, target))
          StreamingTableErrors.invalid(
            s"Streaming source '$source' overlaps target '$target'; source and target must be disjoint"
          )
      }
    }

  private def storedClusterColumns(semantic: JsonNode, targetName: String): Seq[Seq[String]] =
    Option(semantic.get("clusterColumns")) match {
      case None => Seq.empty
      case Some(node) if !node.isArray =>
        StreamingTableErrors.invalid(s"Stored clustering definition for $targetName is corrupt")
      case Some(node) =>
        node.elements().asScala.toSeq.map { reference =>
          if (!reference.isArray)
            StreamingTableErrors.invalid(s"Stored clustering definition for $targetName is corrupt")
          val parts = reference.elements().asScala.toSeq.map { part =>
            if (!part.isTextual || part.asText().trim.isEmpty)
              StreamingTableErrors.invalid(s"Stored clustering definition for $targetName is corrupt")
            part.asText()
          }
          if (parts.isEmpty)
            StreamingTableErrors.invalid(s"Stored clustering definition for $targetName is corrupt")
          parts
        }
    }

  private def validateStoredClustering(
      snapshot: Snapshot,
      expected: Seq[Seq[String]],
      spark: SparkSession,
      target: StreamingTableTarget
  ): Unit = {
    val actualPhysical = ClusteringMetadataDomain.fromSnapshot(snapshot).map(_.clusteringColumns)
    val actual = actualPhysical
      .map { columns =>
        columns.map { physical =>
          Expressions.column(ClusteringColumnInfo(snapshot.schema, physical).logicalName).fieldNames.toSeq
        }
      }
      .getOrElse(Seq.empty)
    val protocolSupportsClustering = ClusteredTableUtils.isSupported(snapshot.protocol)
    if (expected.isEmpty) {
      if (actualPhysical.nonEmpty || protocolSupportsClustering)
        StreamingTableErrors.invalid(
          s"Target ${target.sqlIdentifier} is clustered but its stored definition is legacy-unclustered"
        )
    } else {
      if (!protocolSupportsClustering || actualPhysical.isEmpty)
        StreamingTableErrors.invalid(
          s"Target ${target.sqlIdentifier} is missing Delta clustering protocol or metadata"
        )
      val matches =
        expected.size == actual.size &&
          expected.zip(actual).forall { case (left, right) => sameColumnReference(left, right, spark) }
      if (!matches)
        StreamingTableErrors.invalid(
          s"Target ${target.sqlIdentifier} clustering drifted from its stored definition"
        )
    }
  }

  def definitionPath(target: StreamingTableTarget): Path =
    new Path(new Path(target.checkpointLocation), s"$OpenIvmDirectory/$DefinitionFile")

  def readManifest(spark: SparkSession, target: StreamingTableTarget): StreamingTableManifest = {
    val path       = definitionPath(target)
    val fs         = path.getFileSystem(spark.sessionState.newHadoopConf())
    val timeoutMs  = spark.conf.get(ManifestReadRetryTimeoutMsKey, DefaultManifestReadRetryTimeoutMs.toString).toLong
    val intervalMs = spark.conf.get(ManifestReadRetryIntervalMsKey, DefaultManifestReadRetryIntervalMs.toString).toLong
    val deadline   = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(math.max(0L, timeoutMs))

    var content: Option[String] = None
    while (content.isEmpty && System.nanoTime() <= deadline) {
      try {
        if (fs.exists(path))
          content = Some(readText(fs, path))
      } catch {
        case _: java.io.FileNotFoundException => ()
      }
      if (content.isEmpty && System.nanoTime() < deadline)
        Thread.sleep(math.max(1L, intervalMs))
    }

    content
      .map(parseManifest(_, target.sqlIdentifier))
      .getOrElse(
        StreamingTableErrors.invalid(
          s"Owned target ${target.sqlIdentifier} is missing $DefinitionFile; automatic DROP is intentionally " +
            "blocked because source-overlap safety metadata is unavailable. Restore the manifest or perform a " +
            "verified manual cleanup."
        )
      )
  }

  def writeManifest(
      spark: SparkSession,
      target: StreamingTableTarget,
      definition: StreamingTableDefinition
  ): StreamingTableManifest = {
    val manifest = StreamingTableManifest(
      formatVersion = definition.formatVersion,
      target = target,
      definitionHash = definition.fingerprint,
      semanticJson = definition.semanticJson,
      diagnosticJson = definition.diagnosticJson,
      operationalJson = definition.operationalJson,
      operationalHash = definition.operationalHash,
      sourcePaths = definition.sourcePaths
    )
    val path = definitionPath(target)
    writeNewAtomically(
      path.getFileSystem(spark.sessionState.newHadoopConf()),
      path,
      manifestJson(manifest).getBytes(StandardCharsets.UTF_8)
    )
    manifest
  }

  def replaceManifest(
      spark: SparkSession,
      target: StreamingTableTarget,
      definition: StreamingTableDefinition
  ): StreamingTableManifest = {
    val manifest = StreamingTableManifest(
      formatVersion = definition.formatVersion,
      target = target,
      definitionHash = definition.fingerprint,
      semanticJson = definition.semanticJson,
      diagnosticJson = definition.diagnosticJson,
      operationalJson = definition.operationalJson,
      operationalHash = definition.operationalHash,
      sourcePaths = definition.sourcePaths
    )
    val path = definitionPath(target)
    writeReplacingAtomically(
      spark,
      path,
      manifestJson(manifest).getBytes(StandardCharsets.UTF_8)
    )
    manifest
  }

  def resetIntentPath(spark: SparkSession, target: StreamingTableTarget): Path = {
    resetIntentPath(spark, target.identity, target.dataPath)
  }

  def resetIntentPath(spark: SparkSession, targetIdentity: String, targetPath: String): Path = {
    val dataPath = new Path(targetPath)
    val fs       = dataPath.getFileSystem(spark.sessionState.newHadoopConf())
    val root     = fs.makeQualified(dataPath)
    val parent   = root.getParent
    if (isUnsafeTargetRoot(spark, root))
      StreamingTableErrors.invalid(
        s"Refusing to create a reset journal beside unsafe target path '$targetPath'"
      )
    val digest = StreamingTableDefinition.sha256(s"$targetIdentity\n$targetPath").take(32)
    new Path(parent, s"_openivm-streaming-reset-$digest.json")
  }

  def resetIndexPath(spark: SparkSession, targetIdentity: String): Path = {
    val warehouse = new Path(normalizePath(spark, spark.conf.get("spark.sql.warehouse.dir")))
    val fs        = warehouse.getFileSystem(spark.sessionState.newHadoopConf())
    val root      = fs.makeQualified(warehouse)
    if (root.getParent == null || root.getParent == root || root.toUri.getPath == "/")
      StreamingTableErrors.invalid("Cannot locate a safe warehouse root for streaming reset recovery")
    val digest = StreamingTableDefinition.sha256(targetIdentity).take(32)
    new Path(new Path(root, ResetIndexDirectory), s"$digest.json")
  }

  def writeOrVerifyResetIntent(
      spark: SparkSession,
      target: StreamingTableTarget,
      replacementDefinitionHash: String,
      sourcePaths: Seq[String],
      descendants: Seq[StreamingTableCascadeTarget] = Seq.empty
  ): StreamingTableResetIntent = {
    val intent = StreamingTableResetIntent(
      targetIdentity = target.identity,
      targetPath = target.dataPath,
      oldDeltaTableId = target.deltaTableId,
      replacementDefinitionHash = replacementDefinitionHash,
      sourcePaths = sourcePaths.distinct.sorted,
      descendants = descendants
    )
    val path = resetIntentPath(spark, target)
    val fs   = path.getFileSystem(spark.sessionState.newHadoopConf())
    if (fs.exists(path)) {
      val existing = parseResetIntent(readText(fs, path), target.sqlIdentifier)
      if (existing != intent)
        StreamingTableErrors.invalid(
          s"Reset journal $path belongs to a different target or replacement definition; recovery is required"
        )
    } else {
      writeNewAtomically(fs, path, resetIntentJson(intent).getBytes(StandardCharsets.UTF_8))
    }
    val index   = resetIndexPath(spark, target.identity)
    val indexFs = index.getFileSystem(spark.sessionState.newHadoopConf())
    if (indexFs.exists(index)) {
      val indexed = parseResetIntent(readText(indexFs, index), target.sqlIdentifier)
      if (indexed != intent)
        StreamingTableErrors.invalid(
          s"Reset recovery index $index belongs to a different target or replacement definition"
        )
    } else {
      writeNewAtomically(indexFs, index, resetIntentJson(intent).getBytes(StandardCharsets.UTF_8))
    }
    intent
  }

  def updateResetIntent(
      spark: SparkSession,
      previous: StreamingTableResetIntent,
      updated: StreamingTableResetIntent
  ): Unit = {
    if (
      previous.targetIdentity != updated.targetIdentity ||
      previous.targetPath != updated.targetPath ||
      previous.oldDeltaTableId != updated.oldDeltaTableId ||
      previous.replacementDefinitionHash != updated.replacementDefinitionHash ||
      previous.sourcePaths != updated.sourcePaths ||
      previous.descendants != updated.descendants
    )
      StreamingTableErrors.invalid("Refusing to rewrite immutable reset-journal fields")
    val bytes = resetIntentJson(updated).getBytes(StandardCharsets.UTF_8)
    Seq(
      resetIntentPath(spark, previous.targetIdentity, previous.targetPath),
      resetIndexPath(spark, previous.targetIdentity)
    ).foreach { path =>
      val fs = path.getFileSystem(spark.sessionState.newHadoopConf())
      if (!fs.exists(path))
        StreamingTableErrors.invalid(s"Cannot advance missing reset journal $path")
      val persisted = parseResetIntent(readText(fs, path), path.toString)
      if (persisted == updated) ()
      else if (persisted != previous && !isResetProgressPredecessor(persisted, updated))
        StreamingTableErrors.invalid(s"Reset journal $path changed during recovery")
      else writeReplacingAtomically(spark, path, bytes)
    }
  }

  def clearResetIntent(spark: SparkSession, intent: StreamingTableResetIntent): Unit = {
    val path = resetIntentPath(spark, intent.targetIdentity, intent.targetPath)
    val fs   = path.getFileSystem(spark.sessionState.newHadoopConf())
    if (fs.exists(path)) {
      val persisted = parseResetIntent(readText(fs, path), path.toString)
      if (persisted != intent)
        StreamingTableErrors.invalid(s"Refusing to clear a reset journal owned by another recovery")
    }
    if (fs.exists(path) && !fs.delete(path, false))
      StreamingTableErrors.invalid(s"Failed to delete completed reset journal $path")
    if (fs.exists(path))
      StreamingTableErrors.invalid(s"Reset journal $path still exists after deletion")
    val index   = resetIndexPath(spark, intent.targetIdentity)
    val indexFs = index.getFileSystem(spark.sessionState.newHadoopConf())
    if (indexFs.exists(index)) {
      val persisted = parseResetIntent(readText(indexFs, index), index.toString)
      if (persisted != intent)
        StreamingTableErrors.invalid(s"Refusing to clear a reset index owned by another recovery")
    }
    if (indexFs.exists(index) && !indexFs.delete(index, false))
      StreamingTableErrors.invalid(s"Failed to delete completed reset recovery index $index")
    if (indexFs.exists(index))
      StreamingTableErrors.invalid(s"Reset recovery index $index still exists after deletion")
  }

  def pendingResetIntent(
      spark: SparkSession,
      target: StreamingTableTarget
  ): Option[StreamingTableResetIntent] = {
    val path = resetIntentPath(spark, target)
    val fs   = path.getFileSystem(spark.sessionState.newHadoopConf())
    if (!fs.exists(path)) None
    else {
      val sibling = parseResetIntent(readText(fs, path), target.sqlIdentifier)
      Some(reconcileResetCopies(spark, sibling))
    }
  }

  def pendingResetIntent(
      spark: SparkSession,
      targetIdentity: String
  ): Option[StreamingTableResetIntent] = {
    val index   = resetIndexPath(spark, targetIdentity)
    val indexFs = index.getFileSystem(spark.sessionState.newHadoopConf())
    if (!indexFs.exists(index)) None
    else {
      val intent = parseResetIntent(readText(indexFs, index), index.toString)
      if (intent.targetIdentity != targetIdentity)
        StreamingTableErrors.invalid(s"Reset recovery index $index has a mismatched target identity")
      val sibling   = resetIntentPath(spark, intent.targetIdentity, intent.targetPath)
      val siblingFs = sibling.getFileSystem(spark.sessionState.newHadoopConf())
      if (!siblingFs.exists(sibling))
        StreamingTableErrors.invalid(s"Reset recovery index $index points at a missing sibling journal $sibling")
      val siblingIntent = parseResetIntent(readText(siblingFs, sibling), sibling.toString)
      Some(reconcileResetCopies(spark, mergeResetProgress(intent, siblingIntent)))
    }
  }

  private def reconcileResetCopies(
      spark: SparkSession,
      candidate: StreamingTableResetIntent
  ): StreamingTableResetIntent = {
    val paths = Seq(
      resetIntentPath(spark, candidate.targetIdentity, candidate.targetPath),
      resetIndexPath(spark, candidate.targetIdentity)
    )
    val existing = paths.map { path =>
      val fs = path.getFileSystem(spark.sessionState.newHadoopConf())
      if (!fs.exists(path))
        StreamingTableErrors.invalid(s"Reset recovery is missing journal copy $path")
      parseResetIntent(readText(fs, path), path.toString)
    }
    val merged = existing.foldLeft(candidate)(mergeResetProgress)
    val bytes  = resetIntentJson(merged).getBytes(StandardCharsets.UTF_8)
    paths.zip(existing).foreach { case (path, intent) =>
      if (intent != merged) writeReplacingAtomically(spark, path, bytes)
    }
    merged
  }

  private def mergeResetProgress(
      left: StreamingTableResetIntent,
      right: StreamingTableResetIntent
  ): StreamingTableResetIntent = {
    if (!sameResetIdentity(left, right))
      StreamingTableErrors.invalid("Reset journal copies disagree on immutable recovery state")
    if (isResetProgressPredecessor(left, right)) right
    else if (isResetProgressPredecessor(right, left)) left
    else StreamingTableErrors.invalid("Reset journal copies contain incompatible recovery progress")
  }

  private def sameResetIdentity(
      left: StreamingTableResetIntent,
      right: StreamingTableResetIntent
  ): Boolean =
    left.targetIdentity == right.targetIdentity &&
      left.targetPath == right.targetPath &&
      left.oldDeltaTableId == right.oldDeltaTableId &&
      left.replacementDefinitionHash == right.replacementDefinitionHash &&
      left.sourcePaths == right.sourcePaths &&
      left.descendants == right.descendants

  private def isResetProgressPredecessor(
      previous: StreamingTableResetIntent,
      updated: StreamingTableResetIntent
  ): Boolean =
    sameResetIdentity(previous, updated) &&
      previous.completedDescendantIdentities.toSet.subsetOf(updated.completedDescendantIdentities.toSet) &&
      (!previous.upstreamDropped || updated.upstreamDropped)

  def deleteResetOwnedPath(
      spark: SparkSession,
      intent: StreamingTableResetIntent,
      sourcePaths: Seq[String]
  ): Unit = {
    val target = StreamingTableTarget(
      name = Seq.empty,
      identity = intent.targetIdentity,
      sqlIdentifier = "<reset-recovery>",
      dataPath = intent.targetPath,
      deltaTableId = intent.oldDeltaTableId,
      tableId = ""
    )
    assertSafeForDeletion(spark, target, sourcePaths)
    archiveCheckpoint(spark, target, "reset-recovery")
    val path = new Path(intent.targetPath)
    val fs   = path.getFileSystem(spark.sessionState.newHadoopConf())
    if (fs.exists(path) && !fs.delete(path, true))
      StreamingTableErrors.invalid(s"Failed to finish reset deletion for owned target '${intent.targetPath}'")
    if (fs.exists(path))
      StreamingTableErrors.invalid(s"Owned reset target '${intent.targetPath}' still exists after deletion")
  }

  def deleteCascadeOwnedPath(
      spark: SparkSession,
      target: StreamingTableCascadeTarget
  ): Unit = {
    val owned = StreamingTableTarget(
      name = target.name,
      identity = target.identity,
      sqlIdentifier = quoteMultipart(target.name),
      dataPath = target.dataPath,
      deltaTableId = target.deltaTableId,
      tableId = target.tableId
    )
    assertSafeForDeletion(spark, owned, target.sourcePaths)
    archiveCheckpoint(spark, owned, "cascade")
    val path = new Path(target.dataPath)
    val fs   = path.getFileSystem(spark.sessionState.newHadoopConf())
    if (fs.exists(path) && !fs.delete(path, true))
      StreamingTableErrors.invalid(s"Failed to finish cascade deletion for owned target '${target.dataPath}'")
    if (fs.exists(path))
      StreamingTableErrors.invalid(s"Owned cascade target '${target.dataPath}' still exists after deletion")
  }

  def assertSafeForDeletion(
      spark: SparkSession,
      target: StreamingTableTarget,
      sourcePaths: Seq[String]
  ): Unit = {
    val path = new Path(target.dataPath)
    val fs   = path.getFileSystem(spark.sessionState.newHadoopConf())
    val root = fs.makeQualified(path)
    if (isUnsafeTargetRoot(spark, root))
      StreamingTableErrors.invalid(s"Refusing to delete unsafe target root '${target.dataPath}'")
    sourcePaths.foreach { source =>
      if (pathsOverlap(spark, source, target.dataPath))
        StreamingTableErrors.invalid(
          s"Refusing to delete target '${target.dataPath}' because it overlaps source '$source'"
        )
    }
  }

  def dropOwnedCatalogAndData(
      spark: SparkSession,
      target: StreamingTableTarget,
      sourcePaths: Seq[String],
      archiveReason: String
  ): Unit = {
    assertSafeForDeletion(spark, target, sourcePaths)
    archiveCheckpoint(spark, target, archiveReason)
    spark.sql(s"DROP TABLE ${target.sqlIdentifier}").collect()
    if (catalogTableExists(spark, target.name))
      StreamingTableErrors.invalid(
        s"Catalog still reports ${target.sqlIdentifier} after DROP TABLE; refusing filesystem deletion"
      )
    val path = new Path(target.dataPath)
    val fs   = path.getFileSystem(spark.sessionState.newHadoopConf())
    if (fs.exists(path) && !fs.delete(path, true))
      StreamingTableErrors.invalid(s"Failed to delete owned streaming-table target '${target.dataPath}'")
    if (fs.exists(path))
      StreamingTableErrors.invalid(s"Target '${target.dataPath}' still exists after deletion")
  }

  def archiveCheckpoint(
      spark: SparkSession,
      target: StreamingTableTarget,
      reason: String
  ): Option[String] = {
    val checkpoint = new Path(target.checkpointLocation)
    val targetPath = new Path(target.dataPath)
    val parent = Option(targetPath.getParent).getOrElse(
      StreamingTableErrors.invalid(
        s"Cannot archive the checkpoint for ${target.sqlIdentifier} because its target has no parent path"
      )
    )
    val fs = checkpoint.getFileSystem(spark.sessionState.newHadoopConf())
    if (!fs.exists(checkpoint)) None
    else {
      val tableArchive = new Path(new Path(parent, ArchiveDirectory), targetPath.getName)
      if (!fs.exists(tableArchive) && !fs.mkdirs(tableArchive))
        StreamingTableErrors.invalid(
          s"Failed to create checkpoint archive directory '$tableArchive' for ${target.sqlIdentifier}"
        )
      var archivedAt         = System.currentTimeMillis()
      var archivedCheckpoint = new Path(tableArchive, s"$CheckpointDirectory-$archivedAt")
      while (fs.exists(archivedCheckpoint)) {
        archivedAt += 1L
        archivedCheckpoint = new Path(tableArchive, s"$CheckpointDirectory-$archivedAt")
      }
      val event = Mapper.createObjectNode()
      event.put("reason", reason)
      event.put("archivedAtUtcEpochMillis", archivedAt)
      event.put("targetIdentity", target.identity)
      event.put("targetPath", target.dataPath)
      event.put("deltaTableId", target.deltaTableId)
      event.put("tableId", target.tableId)
      val eventPath = new Path(checkpoint, ArchiveEventFile)
      if (!fs.exists(eventPath))
        writeNewAtomically(
          fs,
          eventPath,
          Mapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(event)
        )
      if (!fs.rename(checkpoint, archivedCheckpoint))
        StreamingTableErrors.invalid(
          s"Failed to archive checkpoint '${target.checkpointLocation}' to '$archivedCheckpoint'"
        )
      if (fs.exists(checkpoint) || !fs.exists(archivedCheckpoint))
        StreamingTableErrors.invalid(
          s"Checkpoint archive move from '${target.checkpointLocation}' to '$archivedCheckpoint' was incomplete"
        )
      Some(archivedCheckpoint.toString)
    }
  }

  private def targetFromLog(
      spark: SparkSession,
      name: Seq[String],
      log: DeltaLog,
      requireTableIdMarker: Boolean
  ): StreamingTableTarget = {
    val snapshot = log.update()
    if (snapshot.version < 0L)
      StreamingTableErrors.invalid(
        s"Target ${quoteMultipart(name)} has no committed Delta metadata and cannot be adopted"
      )
    val resolvedName  = qualifiedNameParts(spark, name)
    val identity      = canonicalIdentity(spark, resolvedName)
    val configuration = snapshot.metadata.configuration
    if (!configuration.get(OwnerMarkerKey).contains(OwnerMarkerValue))
      StreamingTableErrors.invalid(
        s"Target ${quoteMultipart(resolvedName)} is not marked as OpenIVM-owned; refusing adoption"
      )
    if (!configuration.get(TargetIdentityMarkerKey).contains(identity))
      StreamingTableErrors.invalid(
        s"Target ${quoteMultipart(resolvedName)} ownership marker is bound to another table identity"
      )
    val tableId = configuration
      .get(TableIdMarkerKey)
      .getOrElse(
        if (requireTableIdMarker)
          StreamingTableErrors.invalid(
            s"Target ${quoteMultipart(resolvedName)} is missing its streaming-table ownership identity"
          )
        else ""
      )
    val dataPath = normalizePath(spark, log.dataPath)
    if (requireTableIdMarker && !configuration.get(DeltaIdMarkerKey).contains(snapshot.metadata.id))
      StreamingTableErrors.invalid(
        s"Target ${quoteMultipart(resolvedName)} ownership marker does not match its current Delta table id"
      )
    if (requireTableIdMarker && !configuration.get(TargetPathMarkerKey).contains(dataPath))
      StreamingTableErrors.invalid(
        s"Target ${quoteMultipart(resolvedName)} ownership marker does not match its current Delta path"
      )
    StreamingTableTarget(
      name = resolvedName,
      identity = identity,
      sqlIdentifier = quoteMultipart(resolvedName),
      dataPath = dataPath,
      deltaTableId = snapshot.metadata.id,
      tableId = tableId
    )
  }

  private def assertLocationAbsent(spark: SparkSession, location: String): Unit = {
    val path = new Path(location)
    val fs   = path.getFileSystem(spark.sessionState.newHadoopConf())
    if (fs.exists(path))
      StreamingTableErrors.invalid(
        s"LOCATION '$location' already exists; refusing to adopt data for a streaming table"
      )
  }

  private def createTableSql(
      spec: StreamingTableSpec,
      schema: StructType,
      properties: Map[String, String]
  ): String = {
    val partitionClause =
      if (spec.partitionColumns.isEmpty) ""
      else s" PARTITIONED BY (${spec.partitionColumns.map(quoteIdentifier).mkString(", ")})"
    val clusterClause =
      if (spec.clusterColumns.isEmpty) ""
      else s" CLUSTER BY (${spec.clusterColumns.map(renderColumnReference).mkString(", ")})"
    val locationClause = spec.location.map(location => s" LOCATION ${sqlString(location)}").getOrElse("")
    val propertyClause =
      if (properties.isEmpty) ""
      else {
        val entries = properties.toSeq.sortBy(_._1).map { case (key, value) =>
          s"${sqlString(key)}=${sqlString(value)}"
        }
        s" TBLPROPERTIES (${entries.mkString(", ")})"
      }
    s"CREATE TABLE ${quoteMultipart(spec.name)} (${schema.toDDL}) USING DELTA" +
      partitionClause + clusterClause + locationClause + propertyClause
  }

  private def pathsOverlap(spark: SparkSession, first: String, second: String): Boolean = {
    val firstPath  = new Path(normalizePath(spark, first))
    val secondPath = new Path(normalizePath(spark, second))
    val firstUri   = firstPath.toUri
    val secondUri  = secondPath.toUri
    val sameFs =
      Option(firstUri.getScheme).map(_.toLowerCase(Locale.ROOT)) ==
        Option(secondUri.getScheme).map(_.toLowerCase(Locale.ROOT)) &&
        Option(firstUri.getAuthority).map(_.toLowerCase(Locale.ROOT)) ==
        Option(secondUri.getAuthority).map(_.toLowerCase(Locale.ROOT))
    def contains(parent: String, child: String): Boolean =
      child == parent || child.startsWith(parent.stripSuffix("/") + "/")
    sameFs &&
    (contains(firstUri.getPath, secondUri.getPath) || contains(secondUri.getPath, firstUri.getPath))
  }

  private def pathIsWithin(parent: String, child: String): Boolean = {
    val parentUri = new Path(parent).toUri
    val childUri  = new Path(child).toUri
    val sameFs =
      Option(parentUri.getScheme).map(_.toLowerCase(Locale.ROOT)) ==
        Option(childUri.getScheme).map(_.toLowerCase(Locale.ROOT)) &&
        Option(parentUri.getAuthority).map(_.toLowerCase(Locale.ROOT)) ==
        Option(childUri.getAuthority).map(_.toLowerCase(Locale.ROOT))
    sameFs && (
      childUri.getPath == parentUri.getPath ||
        childUri.getPath.startsWith(parentUri.getPath.stripSuffix("/") + "/")
    )
  }

  private def isUnsafeTargetRoot(spark: SparkSession, root: Path): Boolean = {
    val raw    = Option(root.toUri.getPath).getOrElse("")
    val parent = root.getParent
    val warehouse = normalizePath(
      spark,
      spark.conf.get("spark.sql.warehouse.dir")
    )
    raw.isEmpty ||
    raw == "/" ||
    parent == null ||
    parent == root ||
    parent.toUri.getPath == "/" ||
    normalizePath(spark, root) == warehouse
  }

  private def manifestJson(manifest: StreamingTableManifest): String = {
    val root = Mapper.createObjectNode()
    root.put("format", "openivm-streaming-table")
    root.put("formatVersion", manifest.formatVersion)
    val target = root.putObject("target")
    target.put("identity", manifest.target.identity)
    target.put("path", manifest.target.dataPath)
    target.put("deltaTableId", manifest.target.deltaTableId)
    target.put("tableId", manifest.target.tableId)
    val definition = root.putObject("definition")
    definition.put("fingerprint", manifest.definitionHash)
    definition.set[JsonNode]("semantic", Mapper.readTree(manifest.semanticJson))
    definition.set[JsonNode]("diagnostic", Mapper.readTree(manifest.diagnosticJson))
    definition.set[JsonNode]("operational", Mapper.readTree(manifest.operationalJson))
    definition.put("operationalHash", manifest.operationalHash)
    val paths = definition.putArray("sourcePaths")
    manifest.sourcePaths.sorted.foreach(path => paths.add(path))
    Mapper.writeValueAsString(root)
  }

  private def parseManifest(json: String, targetName: String): StreamingTableManifest =
    try {
      val root = Mapper.readTree(json)
      if (!Option(root.get("format")).exists(_.asText() == "openivm-streaming-table"))
        StreamingTableErrors.invalid(s"Unrecognized streaming-table manifest for $targetName")
      val formatVersion = requiredInt(root, "formatVersion", targetName)
      val targetNode    = requiredNode(root, "target", targetName)
      val target = StreamingTableTarget(
        name = Seq.empty,
        identity = requiredText(targetNode, "identity", targetName),
        sqlIdentifier = targetName,
        dataPath = requiredText(targetNode, "path", targetName),
        deltaTableId = requiredText(targetNode, "deltaTableId", targetName),
        tableId = requiredText(targetNode, "tableId", targetName)
      )
      val definition  = requiredNode(root, "definition", targetName)
      val fingerprint = requiredText(definition, "fingerprint", targetName)
      if (!fingerprint.matches("[0-9a-f]{64}"))
        StreamingTableErrors.invalid(s"Invalid definition fingerprint in manifest for $targetName")
      val semantic        = requiredNode(definition, "semantic", targetName).toString
      val diagnostic      = requiredNode(definition, "diagnostic", targetName).toString
      val operational     = requiredNode(definition, "operational", targetName).toString
      val operationalHash = requiredText(definition, "operationalHash", targetName)
      if (StreamingTableDefinition.sha256(semantic) != fingerprint)
        StreamingTableErrors.invalid(s"Manifest semantic fingerprint does not match its payload for $targetName")
      if (StreamingTableDefinition.sha256(operational) != operationalHash)
        StreamingTableErrors.invalid(s"Manifest operational hash does not match its payload for $targetName")
      val sourcePaths = Option(definition.get("sourcePaths"))
        .filter(_.isArray)
        .map(_.elements().asScala.toSeq.map { node =>
          if (!node.isTextual)
            StreamingTableErrors.invalid(s"Invalid sourcePaths entry in manifest for $targetName")
          node.asText()
        })
        .getOrElse(StreamingTableErrors.invalid(s"Missing sourcePaths in manifest for $targetName"))
      StreamingTableManifest(
        formatVersion,
        target,
        fingerprint,
        semantic,
        diagnostic,
        operational,
        operationalHash,
        sourcePaths
      )
    } catch {
      case error: org.apache.spark.sql.AnalysisException => throw error
      case NonFatal(error) =>
        StreamingTableErrors.invalid(
          s"Cannot parse streaming-table manifest for $targetName: " +
            Option(error.getMessage).getOrElse(error.toString)
        )
    }

  private def resetIntentJson(intent: StreamingTableResetIntent): String = {
    val root = Mapper.createObjectNode()
    root.put("format", "openivm-streaming-table-reset")
    root.put("formatVersion", StreamingTableDefinition.FormatVersion)
    root.put("targetIdentity", intent.targetIdentity)
    root.put("targetPath", intent.targetPath)
    root.put("oldDeltaTableId", intent.oldDeltaTableId)
    root.put("replacementDefinitionHash", intent.replacementDefinitionHash)
    val paths = root.putArray("sourcePaths")
    intent.sourcePaths.sorted.foreach(path => paths.add(path))
    val descendants = root.putArray("descendants")
    intent.descendants.foreach { descendant =>
      val node = descendants.addObject()
      node.put("kind", descendant.kind)
      val name = node.putArray("name")
      descendant.name.foreach(value => name.add(value))
      node.put("identity", descendant.identity)
      node.put("path", descendant.dataPath)
      node.put("deltaTableId", descendant.deltaTableId)
      node.put("tableId", descendant.tableId)
      val sourcePaths = node.putArray("sourcePaths")
      descendant.sourcePaths.sorted.foreach(sourcePaths.add)
    }
    val completed = root.putArray("completedDescendantIdentities")
    intent.completedDescendantIdentities.foreach(completed.add)
    root.put("upstreamDropped", intent.upstreamDropped)
    Mapper.writeValueAsString(root)
  }

  private def parseResetIntent(json: String, targetName: String): StreamingTableResetIntent =
    try {
      val root = Mapper.readTree(json)
      if (!Option(root.get("format")).exists(_.asText() == "openivm-streaming-table-reset"))
        StreamingTableErrors.invalid(s"Unrecognized reset journal for $targetName")
      if (requiredInt(root, "formatVersion", targetName) != StreamingTableDefinition.FormatVersion)
        StreamingTableErrors.invalid(s"Unsupported reset journal format for $targetName")
      val sourcePaths = requiredNode(root, "sourcePaths", targetName)
      if (!sourcePaths.isArray)
        StreamingTableErrors.invalid(s"Reset journal sourcePaths is corrupt for $targetName")
      val descendants = Option(root.get("descendants")).toSeq.flatMap { node =>
        if (!node.isArray)
          StreamingTableErrors.invalid(s"Reset journal descendants is corrupt for $targetName")
        node.elements().asScala.toSeq.map { descendant =>
          val nameNode = requiredNode(descendant, "name", targetName)
          if (!nameNode.isArray)
            StreamingTableErrors.invalid(s"Reset journal descendant name is corrupt for $targetName")
          val descendantSources = requiredNode(descendant, "sourcePaths", targetName)
          if (!descendantSources.isArray)
            StreamingTableErrors.invalid(s"Reset journal descendant sourcePaths is corrupt for $targetName")
          StreamingTableCascadeTarget(
            kind = Option(descendant.get("kind")).filter(_.isTextual).map(_.asText()).getOrElse("streaming"),
            name = nameNode.elements().asScala.toSeq.map { value =>
              if (!value.isTextual || value.asText().isEmpty)
                StreamingTableErrors.invalid(s"Reset journal descendant name is corrupt for $targetName")
              value.asText()
            },
            identity = requiredText(descendant, "identity", targetName),
            dataPath = requiredText(descendant, "path", targetName),
            deltaTableId = requiredText(descendant, "deltaTableId", targetName),
            tableId = requiredText(descendant, "tableId", targetName),
            sourcePaths = descendantSources
              .elements()
              .asScala
              .toSeq
              .map { value =>
                if (!value.isTextual)
                  StreamingTableErrors.invalid(
                    s"Reset journal descendant sourcePaths is corrupt for $targetName"
                  )
                value.asText()
              }
              .distinct
              .sorted
          )
        }
      }
      val completed = Option(root.get("completedDescendantIdentities")).toSeq.flatMap { node =>
        if (!node.isArray)
          StreamingTableErrors.invalid(
            s"Reset journal completedDescendantIdentities is corrupt for $targetName"
          )
        node.elements().asScala.toSeq.map { value =>
          if (!value.isTextual || value.asText().isEmpty)
            StreamingTableErrors.invalid(
              s"Reset journal completedDescendantIdentities is corrupt for $targetName"
            )
          value.asText()
        }
      }
      descendants.collectFirst {
        case descendant if descendant.kind != "streaming" && descendant.kind != "materialized" =>
          StreamingTableErrors.invalid(
            s"Reset journal descendant '${descendant.identity}' has unsupported kind '${descendant.kind}'"
          )
      }
      descendants
        .groupBy(_.identity)
        .collectFirst { case (identity, entries) if entries.size > 1 => identity }
        .foreach(identity => StreamingTableErrors.invalid(s"Reset journal contains duplicate descendant '$identity'"))
      val descendantIdentities = descendants.map(_.identity).toSet
      if (!completed.toSet.subsetOf(descendantIdentities))
        StreamingTableErrors.invalid(
          s"Reset journal completed descendants do not belong to the planned closure for $targetName"
        )
      val upstreamDropped = Option(root.get("upstreamDropped")).exists(_.asBoolean(false))
      if (upstreamDropped && completed.toSet != descendantIdentities)
        StreamingTableErrors.invalid(
          s"Reset journal marks the upstream dropped before every descendant completed for $targetName"
        )
      StreamingTableResetIntent(
        requiredText(root, "targetIdentity", targetName),
        requiredText(root, "targetPath", targetName),
        requiredText(root, "oldDeltaTableId", targetName),
        requiredText(root, "replacementDefinitionHash", targetName),
        sourcePaths
          .elements()
          .asScala
          .toSeq
          .map { value =>
            if (!value.isTextual)
              StreamingTableErrors.invalid(s"Reset journal sourcePaths is corrupt for $targetName")
            value.asText()
          }
          .distinct
          .sorted,
        descendants,
        completed.distinct,
        upstreamDropped
      )
    } catch {
      case error: org.apache.spark.sql.AnalysisException => throw error
      case NonFatal(error) =>
        StreamingTableErrors.invalid(
          s"Cannot parse reset journal for $targetName: ${Option(error.getMessage).getOrElse(error.toString)}"
        )
    }

  private def requiredNode(node: JsonNode, field: String, targetName: String): JsonNode =
    Option(node.get(field))
      .filterNot(_.isNull)
      .getOrElse(StreamingTableErrors.invalid(s"Manifest for $targetName is missing '$field'"))

  private def requiredText(node: JsonNode, field: String, targetName: String): String =
    Option(node.get(field))
      .filter(_.isTextual)
      .map(_.asText())
      .filter(_.nonEmpty)
      .getOrElse(StreamingTableErrors.invalid(s"Manifest for $targetName has invalid '$field'"))

  private def requiredInt(node: JsonNode, field: String, targetName: String): Int =
    Option(node.get(field))
      .filter(_.canConvertToInt)
      .map(_.asInt())
      .getOrElse(StreamingTableErrors.invalid(s"Manifest for $targetName has invalid '$field'"))

  private def writeNewAtomically(fs: FileSystem, path: Path, bytes: Array[Byte]): Unit = {
    val parent = path.getParent
    if (parent == null)
      StreamingTableErrors.invalid(s"Cannot publish metadata without a parent directory: $path")
    if (!fs.exists(parent) && !fs.mkdirs(parent) && !fs.exists(parent))
      StreamingTableErrors.invalid(s"Cannot create metadata directory $parent")
    if (fs.exists(path))
      StreamingTableErrors.invalid(s"Refusing to overwrite existing metadata file $path")

    val temporary = new Path(parent, s".${path.getName}.${UUID.randomUUID().toString}.tmp")
    var published = false
    try {
      val output = fs.create(temporary, false)
      try {
        output.write(bytes)
        output.hflush()
      } finally output.close()
      if (!fs.rename(temporary, path))
        StreamingTableErrors.invalid(s"Atomic metadata publication failed while renaming $temporary to $path")
      published = true
      if (!fs.exists(path))
        StreamingTableErrors.invalid(s"Metadata file $path is not visible after publication")
    } finally {
      if (!published && fs.exists(temporary) && !fs.delete(temporary, false))
        StreamingTableErrors.invalid(s"Failed to clean unpublished metadata file $temporary")
    }
  }

  private def writeReplacingAtomically(spark: SparkSession, path: Path, bytes: Array[Byte]): Unit = {
    val parent = path.getParent
    if (parent == null)
      StreamingTableErrors.invalid(s"Cannot replace metadata without a parent directory: $path")
    val manager = StreamingCheckpointFileManager.create(path, spark.sessionState.newHadoopConf())
    if (!manager.exists(parent))
      StreamingTableErrors.invalid(s"Cannot replace missing metadata directory $parent")
    if (!manager.exists(path))
      StreamingTableErrors.invalid(s"Cannot replace missing metadata file $path")

    var stream: StreamingCheckpointFileManager.CancellableOutputStream = null
    var closed                                                         = false
    try {
      stream = manager.createAtomic(path, overwriteIfPossible = true)
      stream.write(bytes)
      stream.hflush()
      stream.close()
      closed = true
      if (!manager.exists(path))
        StreamingTableErrors.invalid(s"Metadata file $path is not visible after replacement")
      val fs = path.getFileSystem(spark.sessionState.newHadoopConf())
      if (readText(fs, path) != new String(bytes, StandardCharsets.UTF_8))
        StreamingTableErrors.invalid(s"Atomic metadata replacement did not publish the requested content at $path")
    } finally {
      if (!closed && stream != null) stream.cancel()
    }
  }

  private def readText(fs: FileSystem, path: Path): String = {
    val input  = fs.open(path)
    val buffer = new Array[Byte](8192)
    val output = new java.io.ByteArrayOutputStream()
    try {
      var total = 0
      var read  = input.read(buffer)
      while (read >= 0) {
        total += read
        if (total > MaxJsonBytes)
          StreamingTableErrors.invalid(s"Metadata file $path exceeds $MaxJsonBytes bytes")
        output.write(buffer, 0, read)
        read = input.read(buffer)
      }
      new String(output.toByteArray, StandardCharsets.UTF_8)
    } finally {
      input.close()
      output.close()
    }
  }

  private def quoteIdentifier(identifier: String): String = s"`${identifier.replace("`", "``")}`"

  private def sqlString(value: String): String = s"'${value.replace("'", "''")}'"

  private def currentCatalogAndNamespace(spark: SparkSession): (String, String) = {
    val row = spark.sql("SELECT current_catalog() AS catalog, current_database() AS namespace").head()
    row.getAs[String]("catalog") -> row.getAs[String]("namespace")
  }
}
