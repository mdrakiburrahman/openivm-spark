package org.openivm.spark.streaming

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedAttribute, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Attribute, ExprId, Expression, Literal, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.{CTERelationDef, CTERelationRef, EventTimeWatermark, LogicalPlan}
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.connector.catalog.{Identifier, Table}
import org.apache.spark.sql.delta.DeltaOptions
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.files.TahoeFileIndex
import org.apache.spark.sql.delta.openivm.DeltaOptionsAccess
import org.apache.spark.sql.execution.datasources.{DataSource, HadoopFsRelation}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.delta.sources.DeltaSource
import org.apache.spark.sql.delta.DeltaLog
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.connector.read.streaming.SparkDataStream
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{DataType, Metadata, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.IdentityHashMap
import java.util.Locale
import java.util.regex.Pattern
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

private[streaming] sealed trait CompatibleStreamingPlan {
  def output: Seq[Attribute]
}

private[streaming] final case class CompatibleV1StreamingRelation(
    dataSource: DataSource,
    sourceName: String,
    output: Seq[Attribute]
) extends CompatibleStreamingPlan

private[streaming] final case class CompatibleStreamingDataSource(
    stream: SparkDataStream,
    output: Seq[Attribute],
    nameParts: Option[Seq[String]],
    identifier: Option[Identifier]
) extends CompatibleStreamingPlan

private[streaming] final case class CompatibleStreamingTable(
    table: Table,
    output: Seq[Attribute],
    options: Seq[(String, String)],
    identifier: Option[Identifier],
    catalog: Option[String],
    sourceName: String
) extends CompatibleStreamingPlan

private[streaming] final case class CompatibleV1Relation(
    relation: HadoopFsRelation,
    output: Seq[Attribute],
    catalogName: Option[Seq[String]],
    isStreaming: Boolean
) extends CompatibleStreamingPlan

/** Parsed extension-owned writer settings. Reader settings remain attached to
  * streaming relations by the parser and are collected into the definition.
  */
final case class StreamingRuntimeOptions(
    outputMode: String,
    trigger: String,
    triggerInterval: Option[String],
    onQueryChange: String,
    sinkOptions: Map[String, String]
) {

  def sparkOutputMode: OutputMode = outputMode match {
    case "append"   => OutputMode.Append()
    case "complete" => OutputMode.Complete()
    case other      => StreamingTableErrors.invalid(s"Unsupported streaming-table output mode '$other'")
  }

  def sparkTrigger: Trigger = trigger match {
    case "processingtime" => Trigger.ProcessingTime(triggerInterval.getOrElse("0 seconds"))
    case "availablenow"   => Trigger.AvailableNow()
    case other            => StreamingTableErrors.invalid(s"Unsupported streaming-table trigger '$other'")
  }
}

object StreamingRuntimeOptions {

  private val ExtensionOptionKeys = Set("outputmode", "trigger", "triggerinterval", "onquerychange")
  private val WriterOwnedKeys     = Set("path", "checkpointlocation", "queryname")

  def parse(rawOptions: Map[String, String]): StreamingRuntimeOptions = {
    val grouped = rawOptions.toSeq.groupBy { case (key, _) => normalizeKey(key) }
    grouped.collectFirst {
      case (key, entries) if entries.size > 1 =>
        StreamingTableErrors.invalid(s"Duplicate streaming-table option '$key'")
    }

    val options = grouped.map { case (key, entries) =>
      key -> Option(entries.head._2)
        .map(_.trim)
        .getOrElse(
          StreamingTableErrors.invalid(s"Streaming-table option '$key' must not be null")
        )
    }
    val forbidden = options.keySet.intersect(WriterOwnedKeys)
    if (forbidden.nonEmpty)
      StreamingTableErrors.invalid(
        s"Streaming-table OPTIONS may not override extension-owned ${forbidden.toSeq.sorted.mkString(", ")}"
      )

    val outputMode = options.getOrElse("outputmode", "append").toLowerCase(Locale.ROOT)
    if (outputMode == "update")
      StreamingTableErrors.invalid(
        "outputMode=update is not supported for streaming tables; use native append or complete mode"
      )
    if (outputMode != "append" && outputMode != "complete")
      StreamingTableErrors.invalid(s"Unsupported streaming-table outputMode '$outputMode'")

    val trigger = options.getOrElse("trigger", "processingTime").toLowerCase(Locale.ROOT)
    if (trigger == "continuous")
      StreamingTableErrors.invalid("Continuous triggers are not supported for streaming tables")
    if (trigger != "processingtime" && trigger != "availablenow")
      StreamingTableErrors.invalid(s"Unsupported streaming-table trigger '$trigger'")

    val specifiedInterval = options.get("triggerinterval").filter(_.nonEmpty)
    if (trigger == "availablenow" && options.contains("triggerinterval"))
      StreamingTableErrors.invalid("triggerInterval is incompatible with trigger=availableNow")
    if (trigger == "processingtime" && options.get("triggerinterval").exists(_.isEmpty))
      StreamingTableErrors.invalid("triggerInterval must not be empty")

    val onQueryChange = options.getOrElse("onquerychange", "fail").toLowerCase(Locale.ROOT)
    if (onQueryChange != "fail" && onQueryChange != "rebuild")
      StreamingTableErrors.invalid(
        s"Unsupported onQueryChange '$onQueryChange'; expected fail or rebuild"
      )

    trigger match {
      case "processingtime" => Trigger.ProcessingTime(specifiedInterval.getOrElse("0 seconds"))
      case "availablenow"   => Trigger.AvailableNow()
      case _                =>
    }

    StreamingRuntimeOptions(
      outputMode = outputMode,
      trigger = trigger,
      triggerInterval = if (trigger == "processingtime") Some(specifiedInterval.getOrElse("0 seconds")) else None,
      onQueryChange = onQueryChange,
      sinkOptions = options.filterNot { case (key, _) => ExtensionOptionKeys.contains(key) }
    )
  }

  private def normalizeKey(key: String): String = {
    val normalized = Option(key).map(_.trim).getOrElse("")
    if (normalized.isEmpty) StreamingTableErrors.invalid("Streaming-table option keys must not be empty")
    normalized.toLowerCase(Locale.ROOT)
  }
}

/** A source occurrence is intentionally represented independently of Catalyst
  * expression IDs. The occurrence order is preserved because two occurrences
  * of one Delta source can have different reader semantics.
  */
final case class StreamingSourceDefinition(
    occurrence: Int,
    identity: String,
    provider: String,
    schema: String,
    isStreaming: Boolean,
    semanticOptions: Map[String, String],
    operationalOptions: Map[String, String],
    deltaPath: Option[String],
    deltaTableId: Option[String]
)

final case class StreamingTableDefinition(
    formatVersion: Int,
    fingerprint: String,
    semanticJson: String,
    diagnosticJson: String,
    operationalJson: String,
    operationalHash: String,
    sources: Seq[StreamingSourceDefinition],
    sourcePaths: Seq[String],
    sourceIdentities: Seq[String]
) {

  def hasSameSemantics(other: StreamingTableDefinition): Boolean = fingerprint == other.fingerprint

  def hasSameOperation(other: StreamingTableDefinition): Boolean = operationalHash == other.operationalHash
}

/** Deterministic, versioned definition construction.
  *
  * The persisted representation is deliberately a structural description of a
  * fresh logical plan, never a serialized plan object. Reconciliation always
  * reparses and analyzes the submitted declaration.
  */
object StreamingTableDefinition {

  val FormatVersion: Int = 1

  private val Mapper = new ObjectMapper()
  private val OperationalReaderOptions = Set(
    "maxfilespertrigger",
    "maxbytespertrigger"
  )
  private val SecretKeyPattern =
    "(?i).*(password|secret|token|access[_-]?key|credential|private[_-]?key|keytab|oauth|sas).*".r
  private val InlineSecretPattern = Pattern.compile(
    """(?i)(['"]?(?:password|secret|token|access[_-]?key|credential|""" +
      """private[_-]?key|keytab|oauth|sas)['"]?\s*(?:=|:)\s*['"]?)([^,'"\s)]+)"""
  )
  private val CommonExpressionIdClass = "org.apache.spark.sql.catalyst.expressions.CommonExpressionId"
  private val CommonExpressionIdToken = "<common-expression-id>"
  private val LegacyCommonExpressionIdPattern = Pattern.compile(
    """product:org\.apache\.spark\.sql\.catalyst\.expressions\.CommonExpressionId""" +
      """\(\[value:java\.lang\.Long:"-?\d+",value:java\.lang\.Boolean:"(?:true|false)"\]\)"""
  )

  def build(
      spark: SparkSession,
      spec: StreamingTableSpec,
      analyzed: LogicalPlan,
      targetIdentity: String,
      runtime: StreamingRuntimeOptions
  ): StreamingTableDefinition = {
    val sources  = collectSources(spark, spec.query, analyzed)
    val semantic = Mapper.createObjectNode()
    semantic.put("formatVersion", FormatVersion)
    semantic.put("targetIdentity", targetIdentity)
    semantic.put(
      "declaredLocation",
      spec.location
        .map(location => StreamingTableMetadata.normalizePath(spark, location))
        .getOrElse("<catalog-managed>")
    )
    semantic.put("outputMode", runtime.outputMode)
    semantic.put("declarationPlan", structuralPlan(spec.query))
    semantic.put("analyzedPlan", structuralPlan(analyzed))
    semantic.put("outputSchema", schemaSignature(analyzed.schema))
    semantic.put("targetSchema", targetSchemaSignature(analyzed.schema))

    val partitions = semantic.putArray("partitionColumns")
    spec.partitionColumns.map(normalizeIdentifier).foreach(partition => partitions.add(partition))
    if (spec.clusterColumns.nonEmpty) {
      val clusters = semantic.putArray("clusterColumns")
      spec.clusterColumns.foreach { reference =>
        val parts = clusters.addArray()
        reference.foreach(parts.add)
      }
    }
    putMap(semantic, "tableProperties", redactForSemantic(spec.tableProperties))
    putMap(semantic, "sinkOptions", redactForSemantic(runtime.sinkOptions))
    val watermarks = semantic.putArray("watermarks")
    analyzed.collect { case watermark: EventTimeWatermark => watermark }.foreach { watermark =>
      val node = watermarks.addObject()
      node.put("eventTime", encodeExpression(watermark.eventTime))
      node.put("delay", watermark.delay.toString)
    }

    val sourceArray = semantic.putArray("sources")
    sources.foreach { source =>
      val node = sourceArray.addObject()
      node.put("occurrence", source.occurrence)
      node.put("identity", source.identity)
      node.put("provider", source.provider)
      node.put("schema", source.schema)
      node.put("streaming", source.isStreaming)
      source.deltaPath.foreach(path => node.put("deltaPath", path))
      source.deltaTableId.foreach(id => node.put("deltaTableId", id))
      putMap(node, "options", source.semanticOptions)
    }

    val operational = Mapper.createObjectNode()
    operational.put("trigger", runtime.trigger)
    runtime.triggerInterval.foreach(interval => operational.put("triggerInterval", interval))
    val sourceRates = operational.putArray("sourceRateLimits")
    sources.foreach { source =>
      val node = sourceRates.addObject()
      node.put("occurrence", source.occurrence)
      node.put("identity", source.identity)
      putMap(node, "options", source.operationalOptions)
    }

    val diagnostic = Mapper.createObjectNode()
    diagnostic.put("normalizedQuery", redactText(normalizeSql(spec.queryText)))
    diagnostic.put("declarationPlan", redactText(structuralPlan(spec.query)))
    diagnostic.put("analyzedPlan", redactText(structuralPlan(analyzed)))
    diagnostic.put("outputSchema", schemaSignature(analyzed.schema))
    diagnostic.put("outputMode", runtime.outputMode)
    diagnostic.put("targetIdentity", targetIdentity)
    diagnostic.put(
      "declaredLocation",
      spec.location
        .map(location => StreamingTableMetadata.normalizePath(spark, location))
        .getOrElse("<catalog-managed>")
    )
    if (spec.clusterColumns.nonEmpty) {
      val clusters = diagnostic.putArray("clusterColumns")
      spec.clusterColumns.foreach { reference =>
        val parts = clusters.addArray()
        reference.foreach(parts.add)
      }
    }
    putMap(diagnostic, "sinkOptions", redactForDisplay(runtime.sinkOptions))
    val diagnosticSources = diagnostic.putArray("sources")
    sources.foreach { source =>
      val node = diagnosticSources.addObject()
      node.put("occurrence", source.occurrence)
      node.put("identity", source.identity)
      node.put("provider", source.provider)
      node.put("schema", source.schema)
      node.put("streaming", source.isStreaming)
      source.deltaPath.foreach(path => node.put("deltaPath", path))
      source.deltaTableId.foreach(id => node.put("deltaTableId", id))
      putMap(node, "semanticOptions", redactForDisplay(source.semanticOptions))
      putMap(node, "operationalOptions", redactForDisplay(source.operationalOptions))
    }

    val semanticJson    = Mapper.writeValueAsString(semantic)
    val operationalJson = Mapper.writeValueAsString(operational)
    StreamingTableDefinition(
      formatVersion = FormatVersion,
      fingerprint = sha256(semanticJson),
      semanticJson = semanticJson,
      diagnosticJson = Mapper.writeValueAsString(diagnostic),
      operationalJson = operationalJson,
      operationalHash = sha256(operationalJson),
      sources = sources,
      sourcePaths = sources.flatMap(_.deltaPath).distinct,
      sourceIdentities = sources.map(_.identity).distinct
    )
  }

  def sha256(text: String): String =
    MessageDigest
      .getInstance("SHA-256")
      .digest(text.getBytes(StandardCharsets.UTF_8))
      .map("%02x".format(_))
      .mkString

  private[streaming] def semanticallyEquivalent(left: String, right: String): Boolean =
    normalizeSemanticJson(left) == normalizeSemanticJson(right)

  private def normalizeSemanticJson(json: String): String = {
    val root = Mapper.readTree(json).deepCopy[ObjectNode]()
    Seq("declarationPlan", "analyzedPlan").foreach { field =>
      Option(root.get(field))
        .filter(_.isTextual)
        .foreach(node => root.put(field, normalizeLegacyPlan(node.asText())))
    }
    Mapper.writeValueAsString(root)
  }

  private def normalizeLegacyPlan(plan: String): String =
    LegacyCommonExpressionIdPattern.matcher(plan).replaceAll(CommonExpressionIdToken)

  def redactText(text: String): String =
    InlineSecretPattern.matcher(Option(text).getOrElse("")).replaceAll("$1[REDACTED]")

  private final case class DeclaredSource(name: Seq[String], options: Map[String, String])

  private final case class SourceSeed(
      identity: String,
      provider: String,
      schema: String,
      isStreaming: Boolean,
      options: Map[String, String],
      deltaPath: Option[String],
      deltaTableId: Option[String]
  )

  private def collectSources(
      spark: SparkSession,
      declaration: LogicalPlan,
      analyzed: LogicalPlan
  ): Seq[StreamingSourceDefinition] = {
    val declared = allPlans(declaration).collect {
      case relation: UnresolvedRelation if relation.isStreaming =>
        DeclaredSource(
          relation.multipartIdentifier,
          readOptions(
            relation.options.asCaseSensitiveMap().asScala.toSeq,
            s"streaming source ${quotedMultipart(relation.multipartIdentifier)}"
          )
        )
    }
    val resolved = allPlans(analyzed).flatMap { plan =>
      StreamingPlanCompatibility
        .extract(plan)
        .map {
          case relation: CompatibleV1StreamingRelation =>
            sourceFromStreamingRelation(spark, relation)
          case relation: CompatibleStreamingDataSource =>
            sourceFromStreamingDataSource(spark, relation.stream, relation.output, relation.nameParts)
          case relation: CompatibleStreamingTable =>
            sourceFromTable(
              spark,
              relation.table,
              relation.output,
              isStreaming = true,
              relation.options,
              relation.identifier,
              relation.catalog,
              relation.sourceName
            )
          case relation: CompatibleV1Relation =>
            sourceFromV1Relation(
              spark,
              relation.relation,
              relation.output,
              relation.catalogName,
              relation.isStreaming
            )
        }
        .orElse {
          plan match {
            case relation: StreamingRelationV2 =>
              Some(
                sourceFromTable(
                  spark,
                  relation.table,
                  relation.output,
                  isStreaming = true,
                  relation.extraOptions.asCaseSensitiveMap().asScala.toSeq,
                  relation.identifier,
                  relation.catalog.map(_.name()),
                  relation.sourceName
                )
              )
            case relation: DataSourceV2Relation =>
              Some(
                sourceFromTable(
                  spark,
                  relation.table,
                  relation.output,
                  isStreaming = false,
                  relation.options.asCaseSensitiveMap().asScala.toSeq,
                  relation.identifier,
                  relation.catalog.map(_.name()),
                  relation.table.name()
                )
              )
            case _ => None
          }
        }
    }
    val resolvedStreaming = resolved.filter(_.isStreaming)
    if (analyzed.isStreaming && resolvedStreaming.isEmpty)
      StreamingTableErrors.invalid(
        "The analyzed streaming query contains no captured physical streaming source; refusing to " +
          "persist a definition without source identity, options, and overlap metadata"
      )
    if (declared.size > resolvedStreaming.size)
      StreamingTableErrors.invalid(
        s"Only ${resolvedStreaming.size} of ${declared.size} declared streaming sources resolved; refusing " +
          "to persist an incomplete definition"
      )

    var declaredIndex = 0
    resolved.zipWithIndex.map { case (seed, occurrence) =>
      val declaredOptions =
        if (seed.isStreaming && declaredIndex < declared.size) {
          val options = declared(declaredIndex).options
          declaredIndex += 1
          options
        } else Map.empty[String, String]
      val merged = mergeOptions(seed.options, declaredOptions, seed.identity)
      if (seed.provider == "delta") validateDeltaReaderOptions(spark, merged)
      val (operational, semantic) = merged.partition { case (key, _) =>
        OperationalReaderOptions.contains(key)
      }
      StreamingSourceDefinition(
        occurrence = occurrence,
        identity = seed.identity,
        provider = seed.provider,
        schema = seed.schema,
        isStreaming = seed.isStreaming,
        semanticOptions = redactForSemantic(semantic),
        operationalOptions = redactForSemantic(operational),
        deltaPath = seed.deltaPath,
        deltaTableId = seed.deltaTableId
      )
    }
  }

  private def sourceFromTable(
      spark: SparkSession,
      table: Table,
      output: Seq[Attribute],
      isStreaming: Boolean,
      options: Seq[(String, String)],
      identifier: Option[Identifier],
      catalog: Option[String],
      fallbackName: String
  ): SourceSeed = {
    val logicalIdentity = identifier
      .map(identifier => {
        val parts = catalog.toSeq ++ identifier.namespace().toSeq :+ identifier.name()
        StreamingTableMetadata.canonicalIdentity(spark, parts)
      })
      .getOrElse(s"provider:${table.getClass.getName}:${jsonString(fallbackName)}")
    val parsedOptions = readOptions(options, s"source $fallbackName")
    table match {
      case delta: DeltaTableV2 =>
        val snapshot = delta.deltaLog.update()
        if (snapshot.version < 0L)
          StreamingTableErrors.invalid(s"Delta source $fallbackName has no committed snapshot")
        val path = StreamingTableMetadata.normalizePath(spark, delta.deltaLog.dataPath)
        val effectiveOptions = mergeOptions(
          readOptions(delta.options.toSeq, s"Delta source $fallbackName"),
          parsedOptions,
          logicalIdentity
        )
        SourceSeed(
          identity = s"$logicalIdentity|delta:$path#${snapshot.metadata.id}",
          provider = "delta",
          schema = attributesSignature(output),
          isStreaming = isStreaming,
          options = effectiveOptions,
          deltaPath = Some(path),
          deltaTableId = Some(snapshot.metadata.id)
        )
      case other =>
        SourceSeed(
          identity = logicalIdentity,
          provider = other.getClass.getName,
          schema = attributesSignature(output),
          isStreaming = isStreaming,
          options = parsedOptions,
          deltaPath = None,
          deltaTableId = None
        )
    }
  }

  private def sourceFromV1Relation(
      spark: SparkSession,
      relation: HadoopFsRelation,
      output: Seq[Attribute],
      catalogName: Option[Seq[String]],
      isStreaming: Boolean
  ): SourceSeed =
    relation.location match {
      case deltaIndex: TahoeFileIndex =>
        val snapshot = deltaIndex.deltaLog.update()
        if (snapshot.version < 0L)
          StreamingTableErrors.invalid("Resolved Delta source has no committed snapshot")
        val path = StreamingTableMetadata.normalizePath(spark, deltaIndex.deltaLog.dataPath)
        val logicalIdentity = catalogName
          .map(parts => StreamingTableMetadata.canonicalIdentity(spark, parts))
          .getOrElse(s"delta-path:$path")
        SourceSeed(
          identity = s"$logicalIdentity|delta:$path#${snapshot.metadata.id}",
          provider = "delta",
          schema = attributesSignature(output),
          isStreaming = isStreaming,
          options = Map.empty,
          deltaPath = Some(path),
          deltaTableId = Some(snapshot.metadata.id)
        )
      case other =>
        val roots = other.rootPaths.map(path => StreamingTableMetadata.normalizePath(spark, path)).sorted
        SourceSeed(
          identity = s"v1:${relation.fileFormat.getClass.getName}:${roots.mkString("|")}",
          provider = relation.fileFormat.getClass.getName,
          schema = attributesSignature(output),
          isStreaming = isStreaming,
          options = Map.empty,
          deltaPath = None,
          deltaTableId = None
        )
    }

  private def sourceFromStreamingRelation(
      spark: SparkSession,
      relation: CompatibleV1StreamingRelation
  ): SourceSeed = {
    val source  = relation.dataSource
    val options = readOptions(source.options.toSeq, s"streaming source ${relation.sourceName}")
    val logicalIdentity = source.catalogTable
      .map(table => StreamingTableMetadata.canonicalIdentity(spark, table.identifier.nameParts))
      .orElse {
        source.paths.headOption.map(path => s"stream-path:${StreamingTableMetadata.normalizePath(spark, path)}")
      }
      .getOrElse(s"stream:${source.className}:${jsonString(relation.sourceName)}")
    if (source.className.equalsIgnoreCase("delta")) {
      val log = source.catalogTable
        .flatMap(table =>
          org.openivm.spark.common.DeltaTableVersion.deltaLogOption(
            spark,
            table.identifier.quotedString
          )
        )
        .orElse(source.paths.headOption.map(path => DeltaLog.forTable(spark, new Path(path))))
        .getOrElse(
          StreamingTableErrors.invalid(
            s"Delta streaming source ${relation.sourceName} did not resolve to a catalog table or path"
          )
        )
      val snapshot = log.update()
      if (snapshot.version < 0L)
        StreamingTableErrors.invalid(s"Delta streaming source ${relation.sourceName} has no committed snapshot")
      val path = StreamingTableMetadata.normalizePath(spark, log.dataPath)
      SourceSeed(
        identity = s"$logicalIdentity|delta:$path#${snapshot.metadata.id}",
        provider = "delta",
        schema = attributesSignature(relation.output),
        isStreaming = true,
        options = options,
        deltaPath = Some(path),
        deltaTableId = Some(snapshot.metadata.id)
      )
    } else {
      SourceSeed(
        identity = logicalIdentity,
        provider = source.className,
        schema = attributesSignature(relation.output),
        isStreaming = true,
        options = options,
        deltaPath = None,
        deltaTableId = None
      )
    }
  }

  private def sourceFromStreamingDataSource(
      spark: SparkSession,
      stream: org.apache.spark.sql.connector.read.streaming.SparkDataStream,
      output: Seq[Attribute],
      nameParts: Option[Seq[String]]
  ): SourceSeed = stream match {
    case delta: DeltaSource =>
      val snapshot = delta.deltaLog.update()
      if (snapshot.version < 0L)
        StreamingTableErrors.invalid("Resolved Delta streaming source has no committed snapshot")
      val path = StreamingTableMetadata.normalizePath(spark, delta.deltaLog.dataPath)
      val logicalIdentity = nameParts
        .map(StreamingTableMetadata.canonicalIdentity(spark, _))
        .getOrElse(s"delta-path:$path")
      val options = readOptions(
        DeltaOptionsAccess.rawOptions(delta.options).toSeq,
        s"Delta streaming source $logicalIdentity"
      )
      SourceSeed(
        identity = s"$logicalIdentity|delta:$path#${snapshot.metadata.id}",
        provider = "delta",
        schema = attributesSignature(output),
        isStreaming = true,
        options = options,
        deltaPath = Some(path),
        deltaTableId = Some(snapshot.metadata.id)
      )
    case other =>
      SourceSeed(
        identity = nameParts
          .map(StreamingTableMetadata.canonicalIdentity(spark, _))
          .getOrElse(s"stream:${other.getClass.getName}"),
        provider = other.getClass.getName,
        schema = attributesSignature(output),
        isStreaming = true,
        options = Map.empty,
        deltaPath = None,
        deltaTableId = None
      )
  }

  private def mergeOptions(
      actual: Map[String, String],
      declared: Map[String, String],
      sourceIdentity: String
  ): Map[String, String] = {
    actual.foreach { case (key, value) =>
      declared.get(key).foreach { declaredValue =>
        if (declaredValue != value && !OperationalReaderOptions.contains(key))
          StreamingTableErrors.invalid(
            s"Reader option '$key' for $sourceIdentity resolved as '$value' but declaration requested '$declaredValue'"
          )
      }
    }
    declared ++ actual
  }

  private def validateDeltaReaderOptions(spark: SparkSession, options: Map[String, String]): Unit = {
    val deltaOptions = new DeltaOptions(options, spark.sessionState.conf)
    Seq[Any](
      deltaOptions.maxFilesPerTrigger,
      deltaOptions.maxBytesPerTrigger,
      deltaOptions.ignoreChanges,
      deltaOptions.ignoreDeletes,
      deltaOptions.skipChangeCommits,
      deltaOptions.startingVersion,
      deltaOptions.startingTimestamp
    ).foreach(_ => ())
  }

  private def readOptions(entries: Seq[(String, String)], owner: String): Map[String, String] = {
    val grouped    = entries.groupBy { case (key, _) => key.toLowerCase(Locale.ROOT) }
    val duplicates = grouped.collect { case (key, values) if values.size > 1 => key }
    if (duplicates.nonEmpty)
      StreamingTableErrors.invalid(s"$owner has duplicate reader options ${duplicates.toSeq.sorted.mkString(", ")}")
    val options: Map[String, String] = grouped.map { case (key, values) =>
      key -> Option(values.head._2).getOrElse(
        StreamingTableErrors.invalid(s"$owner has a null reader option '$key'")
      )
    }.toMap
    if (options.contains("startingversion") && options.contains("startingtimestamp"))
      StreamingTableErrors.invalid(s"$owner cannot set both startingVersion and startingTimestamp")
    options
  }

  private def allPlans(root: LogicalPlan): Seq[LogicalPlan] = {
    val plans     = ArrayBuffer.empty[LogicalPlan]
    val seenPlans = new IdentityHashMap[LogicalPlan, java.lang.Boolean]()
    val seenExprs = new IdentityHashMap[Expression, java.lang.Boolean]()

    def visitPlan(plan: LogicalPlan): Unit =
      if (seenPlans.put(plan, java.lang.Boolean.TRUE) == null) {
        plans += plan
        plan.children.foreach(visitPlan)
        plan.expressions.foreach(visitExpression)
        plan.productIterator.foreach(visitValue)
      }

    def visitExpression(expression: Expression): Unit =
      if (seenExprs.put(expression, java.lang.Boolean.TRUE) == null) {
        expression match {
          case subquery: SubqueryExpression => visitPlan(subquery.plan)
          case _                            =>
        }
        expression.children.foreach(visitExpression)
        expression.productIterator.foreach(visitValue)
      }

    def visitValue(value: Any): Unit = value match {
      case plan: LogicalPlan      => visitPlan(plan)
      case expression: Expression => visitExpression(expression)
      case Some(inner)            => visitValue(inner)
      case values: scala.collection.Map[_, _] =>
        values.foreach { case (key, inner) =>
          visitValue(key)
          visitValue(inner)
        }
      case values: Iterable[_]         => values.foreach(visitValue)
      case values: Array[_]            => values.foreach(visitValue)
      case _: Table                    =>
      case _: DataType                 =>
      case _: Metadata                 =>
      case _: CaseInsensitiveStringMap =>
      case product: Product            => product.productIterator.foreach(visitValue)
      case _                           =>
    }

    visitPlan(root)
    plans.toVector
  }

  private def structuralPlan(plan: LogicalPlan): String = encodePlan(plan)

  private def encodePlan(plan: LogicalPlan): String = plan match {
    case relation: CTERelationDef =>
      s"plan:${plan.getClass.getName}(child=${encodePlan(relation.child)},underSubquery=${relation.underSubquery})"
    case relation: CTERelationRef =>
      s"plan:${plan.getClass.getName}(streaming=${relation.isStreaming},output=${attributesSignature(relation.output)})"
    case relation: UnresolvedRelation =>
      val options = readOptions(
        relation.options.asCaseSensitiveMap().asScala.toSeq,
        s"relation ${quotedMultipart(relation.multipartIdentifier)}"
      )
      s"plan:${plan.getClass.getName}(" +
        s"name=${encodeSeq(relation.multipartIdentifier.map(normalizeIdentifier))}," +
        s"streaming=${relation.isStreaming},options=${encodeOptions(semanticReaderOptions(options))})"
    case relation: StreamingRelationV2 =>
      s"plan:${plan.getClass.getName}(" +
        s"source=${jsonString(relation.sourceName)},table=${encodeTable(relation.table)}," +
        s"options=${encodeOptions(
            semanticReaderOptions(
              readOptions(
                relation.extraOptions.asCaseSensitiveMap().asScala.toSeq,
                relation.sourceName
              )
            )
          )}," +
        s"identifier=${encodeIdentifier(relation.identifier)},output=${attributesSignature(relation.output)}," +
        s"children=${encodeSeq(plan.children.map(encodePlan))})"
    case relation: DataSourceV2Relation =>
      s"plan:${plan.getClass.getName}(" +
        s"table=${encodeTable(relation.table)},streaming=${relation.isStreaming}," +
        s"identifier=${encodeIdentifier(relation.identifier)},output=${attributesSignature(relation.output)}," +
        s"children=${encodeSeq(plan.children.map(encodePlan))})"
    case _ =>
      StreamingPlanCompatibility
        .extract(plan)
        .map(encodeCompatiblePlan(plan, _))
        .getOrElse(s"plan:${plan.getClass.getName}(${encodeSeq(plan.productIterator.toSeq.map(encodeValue))})")
  }

  private def encodeCompatiblePlan(plan: LogicalPlan, relation: CompatibleStreamingPlan): String =
    relation match {
      case value: CompatibleV1StreamingRelation =>
        s"plan:${plan.getClass.getName}(" +
          s"source=${jsonString(value.sourceName)},provider=${jsonString(value.dataSource.className)}," +
          s"paths=${encodeSeq(value.dataSource.paths.map(path => jsonString(path)))}," +
          s"options=${encodeOptions(
              semanticReaderOptions(
                readOptions(
                  value.dataSource.options.toSeq,
                  value.sourceName
                )
              )
            )},output=${attributesSignature(value.output)})"
      case value: CompatibleStreamingDataSource =>
        s"plan:${plan.getClass.getName}(" +
          s"provider=${jsonString(value.stream.getClass.getName)},streaming=true," +
          s"identifier=${encodeIdentifier(value.identifier)}," +
          s"output=${attributesSignature(value.output)})"
      case value: CompatibleStreamingTable =>
        s"plan:${plan.getClass.getName}(" +
          s"table=${encodeTable(value.table)},streaming=true," +
          s"options=${encodeOptions(semanticReaderOptions(readOptions(value.options, value.sourceName)))}," +
          s"identifier=${encodeIdentifier(value.identifier)},output=${attributesSignature(value.output)})"
      case value: CompatibleV1Relation =>
        s"plan:${plan.getClass.getName}(" +
          s"provider=${jsonString(value.relation.fileFormat.getClass.getName)}," +
          s"roots=${encodeSeq(value.relation.location.rootPaths.map(_.toUri.normalize().toString).sorted)}," +
          s"streaming=${value.isStreaming},output=${attributesSignature(value.output)}," +
          s"catalog=${value.catalogName.map(encodeSeq).getOrElse("none")})"
    }

  private def encodeExpression(expression: Expression): String = expression match {
    case literal: Literal =>
      s"literal:${literal.dataType.catalogString}:${encodeValue(literal.value)}"
    case attribute: UnresolvedAttribute =>
      s"unresolved-attribute:${encodeSeq(attribute.nameParts.map(jsonString))}"
    case star: UnresolvedStar =>
      s"unresolved-star:${star.target.map(parts => encodeSeq(parts.map(jsonString))).getOrElse("none")}"
    case alias: UnresolvedAlias =>
      s"unresolved-alias:${encodeExpression(alias.child)}"
    case attribute: Attribute =>
      s"attribute:${jsonString(attribute.name)}:${attribute.dataType.catalogString}:${attribute.nullable}:" +
        s"${encodeSeq(attribute.qualifier)}:${jsonString(attribute.metadata.json)}"
    case subquery: SubqueryExpression =>
      s"expression:${expression.getClass.getName}(" +
        s"args=${encodeSeq(expression.productIterator.toSeq.map(encodeValue))},subquery=${encodePlan(subquery.plan)})"
    case _ =>
      s"expression:${expression.getClass.getName}(${encodeSeq(expression.productIterator.toSeq.map(encodeValue))})"
  }

  private def encodeValue(value: Any): String = value match {
    case null                   => "null"
    case _: ExprId              => "<expr-id>"
    case _: Origin              => "<origin>"
    case plan: LogicalPlan      => encodePlan(plan)
    case expression: Expression => encodeExpression(expression)
    case dataType: DataType     => s"type:${dataType.catalogString}"
    case metadata: Metadata     => s"metadata:${jsonString(metadata.json)}"
    case identifier: Identifier => encodeIdentifier(Some(identifier))
    case table: CatalogTable =>
      s"catalog:${encodeSeq(table.identifier.nameParts)}:${table.provider.getOrElse("")}:" +
        s"${table.storage.locationUri.map(uri => jsonString(uri.toString)).getOrElse("none")}:" +
        s"${encodeOptions(table.properties)}"
    case options: CaseInsensitiveStringMap =>
      encodeOptions(readOptions(options.asCaseSensitiveMap().asScala.toSeq, "relation"))
    case table: Table => encodeTable(table)
    case Some(inner)  => s"some:${encodeValue(inner)}"
    case None         => "none"
    case values: scala.collection.Map[_, _] =>
      values.toSeq
        .map { case (key, inner) => encodeValue(key) -> encodeValue(inner) }
        .sortBy(_._1)
        .map { case (key, inner) => s"$key=$inner" }
        .mkString("map[", ",", "]")
    case values: Iterable[_]                                                 => encodeSeq(values.toSeq.map(encodeValue))
    case values: Array[_]                                                    => encodeSeq(values.toSeq.map(encodeValue))
    case value: String                                                       => jsonString(value)
    case value: Product if value.getClass.getName == CommonExpressionIdClass => CommonExpressionIdToken
    case value: Product if value.getClass.getName.endsWith(".Statistics")    => "<statistics>"
    case value: Product =>
      s"product:${value.getClass.getName}(${encodeSeq(value.productIterator.toSeq.map(encodeValue))})"
    case value => s"value:${value.getClass.getName}:${jsonString(value.toString)}"
  }

  private def encodeTable(table: Table): String = table match {
    case delta: DeltaTableV2 =>
      val snapshot = delta.deltaLog.update()
      val path     = delta.deltaLog.dataPath.toUri.normalize().toString
      s"delta:${jsonString(table.name())}:${jsonString(path)}:${jsonString(snapshot.metadata.id)}"
    case other => s"provider:${other.getClass.getName}:${jsonString(other.name())}"
  }

  private def encodeIdentifier(identifier: Option[Identifier]): String =
    identifier
      .map(value => encodeSeq(value.namespace().toSeq :+ value.name()))
      .getOrElse("none")

  private def encodeOptions(options: Map[String, String]): String =
    options.toSeq
      .sortBy(_._1)
      .map { case (key, value) =>
        s"${jsonString(key)}=${jsonString(redactForSemanticValue(key, value))}"
      }
      .mkString("options[", ",", "]")

  private def semanticReaderOptions(options: Map[String, String]): Map[String, String] =
    options.filterNot { case (key, _) => OperationalReaderOptions.contains(key) }

  private def encodeSeq(values: Seq[String]): String = values.mkString("[", ",", "]")

  private def jsonString(value: String): String = Mapper.writeValueAsString(value)

  def schemaSignature(schema: StructType): String =
    schema.fields
      .map(field =>
        s"${jsonString(field.name)}:${field.dataType.catalogString}:${field.nullable}:" +
          jsonString(field.metadata.json)
      )
      .mkString("[", ",", "]")

  def targetSchemaSignature(schema: StructType): String =
    schema.fields
      .map(field => s"${jsonString(field.name)}:${field.dataType.catalogString}")
      .mkString("[", ",", "]")

  private def attributesSignature(attributes: Seq[Attribute]): String =
    attributes
      .map(attribute =>
        s"${jsonString(attribute.name)}:${attribute.dataType.catalogString}:${attribute.nullable}:" +
          s"${encodeSeq(attribute.qualifier)}:${jsonString(attribute.metadata.json)}"
      )
      .mkString("[", ",", "]")

  private def normalizeIdentifier(identifier: String): String = Option(identifier).map(_.trim).getOrElse("")

  private def quotedMultipart(parts: Seq[String]): String =
    parts.map(part => s"`${part.replace("`", "``")}`").mkString(".")

  private def normalizeSql(sql: String): String = {
    val result      = new StringBuilder
    var index       = 0
    var quote: Char = 0
    var space       = false
    while (index < sql.length) {
      val current = sql.charAt(index)
      if (quote != 0) {
        result.append(current)
        if (current == quote) {
          val escaped = index + 1 < sql.length && sql.charAt(index + 1) == quote
          if (escaped) {
            result.append(sql.charAt(index + 1))
            index += 1
          } else quote = 0
        }
      } else if (current == '\'' || current == '"' || current == '`') {
        if (space && result.nonEmpty) result.append(' ')
        space = false
        quote = current
        result.append(current)
      } else if (current == '-' && index + 1 < sql.length && sql.charAt(index + 1) == '-') {
        index += 2
        while (index < sql.length && sql.charAt(index) != '\n') index += 1
        space = result.nonEmpty
        index -= 1
      } else if (current == '/' && index + 1 < sql.length && sql.charAt(index + 1) == '*') {
        index += 2
        while (
          index + 1 < sql.length &&
          !(sql.charAt(index) == '*' && sql.charAt(index + 1) == '/')
        ) index += 1
        if (index + 1 < sql.length) index += 1
        space = result.nonEmpty
      } else if (current.isWhitespace) {
        space = result.nonEmpty
      } else {
        if (space) result.append(' ')
        space = false
        result.append(current)
      }
      index += 1
    }
    result.toString.trim
  }

  private def redactForSemantic(entries: Map[String, String]): Map[String, String] =
    entries.toSeq.sortBy(_._1).map { case (key, value) => key -> redactForSemanticValue(key, value) }.toMap

  private def redactForDisplay(entries: Map[String, String]): Map[String, String] =
    entries.toSeq
      .sortBy(_._1)
      .map { case (key, value) =>
        key -> (if (isSecretKey(key)) "[REDACTED]" else redactText(value))
      }
      .toMap

  private def redactForSemanticValue(key: String, value: String): String =
    if (isSecretKey(key)) s"[REDACTED:${sha256(value).take(16)}]" else value

  private def isSecretKey(key: String): Boolean = SecretKeyPattern.pattern.matcher(key).matches()

  private def putMap(
      parent: com.fasterxml.jackson.databind.node.ObjectNode,
      name: String,
      values: Map[String, String]
  ): Unit = {
    val node = parent.putObject(name)
    values.toSeq.sortBy(_._1).foreach { case (key, value) => node.put(key, value) }
  }
}

private[streaming] object StreamingTableErrors {

  def invalid(message: String): Nothing =
    throw new org.apache.spark.sql.AnalysisException(
      "_LEGACY_ERROR_TEMP_2273",
      Map("message" -> s"Streaming table: $message")
    )
}
