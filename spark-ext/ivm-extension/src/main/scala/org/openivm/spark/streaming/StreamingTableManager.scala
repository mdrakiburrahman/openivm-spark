package org.openivm.spark.streaming

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnsupportedOperationChecker
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark
import org.apache.spark.sql.openivm.StreamingDatasetAccess
import org.apache.spark.sql.delta.DeltaOptions
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryListener}
import org.apache.spark.sql.types.{TimestampNTZType, TimestampType}

import java.util.concurrent.ConcurrentHashMap
import java.util.{Collections, WeakHashMap}
import scala.collection.JavaConverters._

final case class StreamingTableStatus(
    tableName: String,
    queryId: Option[String],
    runId: Option[String],
    status: String,
    checkpointLocation: Option[String],
    definitionHash: Option[String],
    isActive: Boolean,
    lastProgress: Option[String],
    lastFailure: Option[String]
)

/** Native Structured Streaming lifecycle for extension-owned Delta targets. */
object StreamingTableManager {

  def create(spark: SparkSession, spec: StreamingTableSpec): StreamingTableStatus = {
    StreamingTableMetadata.validateProvider(spec)
    StreamingTableMetadata.validateUserProperties(spec.tableProperties)
    spec.location.foreach(location => StreamingTableMetadata.validateRequestedLocation(spark, location, spec.name))
    val runtime = StreamingRuntimeOptions.parse(spec.options)
    val frame   = StreamingDatasetAccess.ofRows(spark, spec.query)
    validateStreamingQuery(spark, frame, spec, runtime)

    val identity = StreamingTableMetadata.canonicalIdentity(spark, spec.name)
    val definition = StreamingTableDefinition.build(
      spark,
      spec,
      frame.queryExecution.analyzed,
      identity,
      runtime
    )
    if (definition.sourceIdentities.exists(source => source == identity || source.startsWith(identity + "|")))
      StreamingTableErrors.invalid(
        s"Streaming target ${StreamingTableMetadata.quoteMultipart(spec.name)} cannot also be a source"
      )
    val plannedPath = StreamingTableMetadata.plannedTargetPath(spark, spec.name, spec.location)
    plannedPath.foreach(path => StreamingTableMetadata.validateTargetPath(spark, path))
    StreamingTableMetadata.validateNoSourceTargetOverlap(spark, definition.sourcePaths, plannedPath)
    val lockKey = StreamingTableRegistry.provisionalKey(identity, plannedPath)

    StreamingTableRegistry.withTargetLock(spark, lockKey) {
      if (StreamingTableMetadata.catalogTableExists(spark, spec.name)) {
        val target   = StreamingTableMetadata.resolveDeltaTarget(spark, spec.name, requireTableIdMarker = true)
        val manifest = StreamingTableMetadata.readManifest(spark, target)
        StreamingTableMetadata.verifyOwned(spark, target, manifest)
        StreamingTableMetadata.validateNoSourceTargetOverlap(
          spark,
          definition.sourcePaths,
          Some(target.dataPath)
        )
        StreamingTableMetadata.validateExistingTargetAgainstManifest(spark, target, manifest)
        reconcileExisting(spark, frame, spec, runtime, definition, target, manifest)
      } else {
        if (plannedPath.isEmpty)
          StreamingTableErrors.invalid(
            "Cannot resolve a catalog-native target path before CREATE; specify LOCATION for this catalog"
          )
        val recovery = StreamingTableMetadata.pendingResetIntent(spark, identity)
        recovery.foreach { intent =>
          if (runtime.onQueryChange != "rebuild" || intent.replacementDefinitionHash != definition.fingerprint)
            StreamingTableErrors.invalid(
              s"A reset recovery for ${StreamingTableMetadata.quoteMultipart(spec.name)} is pending; " +
                "resubmit the exact replacement with onQueryChange=rebuild"
            )
          spec.location.map(location => StreamingTableMetadata.normalizePath(spark, location)).foreach { location =>
            if (location != intent.targetPath)
              StreamingTableErrors.invalid(
                s"Reset recovery location '$location' differs from journaled target '${intent.targetPath}'"
              )
          }
          if (
            spec.location.isEmpty &&
            !StreamingTableMetadata.isWithinWarehouse(spark, intent.targetPath)
          )
            StreamingTableErrors.invalid(
              "Reset recovery for an explicitly located target requires the original LOCATION clause"
            )
          StreamingTableMetadata.deleteResetOwnedPath(
            spark,
            intent,
            (intent.sourcePaths ++ definition.sourcePaths).distinct
          )
        }
        val target = StreamingTableMetadata.createOwnedTarget(spark, spec, frame.schema)
        StreamingTableMetadata.validateNoSourceTargetOverlap(
          spark,
          definition.sourcePaths,
          Some(target.dataPath)
        )
        val manifest = StreamingTableMetadata.writeManifest(spark, target, definition)
        val status   = startNative(spark, frame, runtime, target, manifest, "initializing")
        recovery.foreach(intent => StreamingTableMetadata.clearResetIntent(spark, intent))
        status
      }
    }
  }

  def stop(spark: SparkSession, name: Seq[String]): StreamingTableStatus = {
    val target   = StreamingTableMetadata.resolveDeltaTarget(spark, name, requireTableIdMarker = true)
    val manifest = StreamingTableMetadata.readManifest(spark, target)
    StreamingTableMetadata.verifyOwned(spark, target, manifest)
    val registryKey = StreamingTableRegistry.targetKey(target)
    val lockKey     = StreamingTableRegistry.lifecycleLockKey(target)
    StreamingTableRegistry.withTargetLock(spark, lockKey) {
      val current = StreamingTableMetadata.resolveDeltaTarget(spark, name, requireTableIdMarker = true)
      if (
        current.dataPath != target.dataPath ||
        current.deltaTableId != target.deltaTableId ||
        current.tableId != target.tableId
      )
        StreamingTableErrors.invalid(
          s"Target ${target.sqlIdentifier} changed before STOP acquired its lifecycle guard"
        )
      stopNative(spark, current, registryKey)
      statusFor(spark, current, manifest, forcedStatus = Some("stopped"))
    }
  }

  def drop(spark: SparkSession, name: Seq[String], ifExists: Boolean): StreamingTableStatus = {
    if (!StreamingTableMetadata.catalogTableExists(spark, name)) {
      if (ifExists)
        return StreamingTableStatus(
          tableName = StreamingTableMetadata.quoteMultipart(name),
          queryId = None,
          runId = None,
          status = "not_found",
          checkpointLocation = None,
          definitionHash = None,
          isActive = false,
          lastProgress = None,
          lastFailure = None
        )
      StreamingTableErrors.invalid(
        s"Streaming table ${StreamingTableMetadata.quoteMultipart(name)} does not exist"
      )
    }

    val target   = StreamingTableMetadata.resolveDeltaTarget(spark, name, requireTableIdMarker = true)
    val manifest = StreamingTableMetadata.readManifest(spark, target)
    StreamingTableMetadata.verifyOwned(spark, target, manifest)
    val registryKey = StreamingTableRegistry.targetKey(target)
    val lockKey     = StreamingTableRegistry.lifecycleLockKey(target)
    StreamingTableRegistry.withTargetLock(spark, lockKey) {
      stopNative(spark, target, registryKey)
      val current = StreamingTableMetadata.resolveDeltaTarget(spark, name, requireTableIdMarker = true)
      if (
        current.dataPath != target.dataPath ||
        current.deltaTableId != target.deltaTableId ||
        current.tableId != target.tableId
      )
        StreamingTableErrors.invalid(
          s"Target ${target.sqlIdentifier} changed after its writer stopped; refusing deletion"
        )
      StreamingTableMetadata.dropOwnedCatalogAndData(spark, current, manifest.sourcePaths)
      StreamingTableRegistry.forget(spark, registryKey)
      StreamingTableStatus(
        tableName = target.sqlIdentifier,
        queryId = None,
        runId = None,
        status = "dropped",
        checkpointLocation = Some(target.checkpointLocation),
        definitionHash = Some(manifest.definitionHash),
        isActive = false,
        lastProgress = None,
        lastFailure = None
      )
    }
  }

  def show(spark: SparkSession, namespace: Option[Seq[String]]): Seq[StreamingTableStatus] = {
    val catalogNames = candidateCatalogNames(spark, namespace)
    val knownTargets = StreamingTableRegistry
      .registeredTargets(spark)
      .filter(target => namespace.forall(matchesNamespace(spark, target.name, _)))
    val registryNames = knownTargets.map(_.name)
    (catalogNames ++ registryNames)
      .groupBy(name => StreamingTableMetadata.canonicalIdentity(spark, name))
      .toSeq
      .sortBy(_._1)
      .flatMap { case (identity, names) =>
        val name  = names.head
        val known = knownTargets.find(_.identity == identity)
        val target =
          try Some(StreamingTableMetadata.resolveDeltaTarget(spark, name, requireTableIdMarker = true))
          catch {
            case _: org.apache.spark.sql.AnalysisException => None
          }
        target
          .map { owned =>
            try {
              val manifest = StreamingTableMetadata.readManifest(spark, owned)
              StreamingTableMetadata.verifyOwned(spark, owned, manifest)
              statusFor(spark, owned, manifest, forcedStatus = None)
            } catch {
              case error: org.apache.spark.sql.AnalysisException =>
                StreamingTableStatus(
                  tableName = owned.sqlIdentifier,
                  queryId = None,
                  runId = None,
                  status = "metadata_error",
                  checkpointLocation = Some(owned.checkpointLocation),
                  definitionHash = None,
                  isActive = false,
                  lastProgress = None,
                  lastFailure = Some(
                    StreamingTableDefinition.redactText(Option(error.getMessage).getOrElse(error.toString))
                  )
                )
            }
          }
          .orElse {
            known.map { target =>
              StreamingTableStatus(
                tableName = target.sqlIdentifier,
                queryId = None,
                runId = None,
                status = "metadata_error",
                checkpointLocation = Some(target.checkpointLocation),
                definitionHash = None,
                isActive = false,
                lastProgress = None,
                lastFailure = Some(
                  "The known owned target can no longer be resolved; it may have been moved, replaced, or deleted"
                )
              )
            }
          }
      }
  }

  private def reconcileExisting(
      spark: SparkSession,
      frame: DataFrame,
      spec: StreamingTableSpec,
      runtime: StreamingRuntimeOptions,
      definition: StreamingTableDefinition,
      target: StreamingTableTarget,
      manifest: StreamingTableManifest
  ): StreamingTableStatus = {
    reconcileResetJournal(spark, target, manifest, definition, runtime)
    if (manifest.definitionHash != definition.fingerprint) {
      if (runtime.onQueryChange != "rebuild")
        StreamingTableErrors.invalid(
          s"Streaming table ${target.sqlIdentifier} has a different semantic definition; " +
            "submit OPTIONS ('onQueryChange'='rebuild') to replace this owned target"
        )
      rebuild(spark, frame, spec, runtime, definition, target, manifest)
    } else {
      val registryKey   = StreamingTableRegistry.targetKey(target)
      val active        = StreamingTableRegistry.findActive(spark, registryKey, target)
      val changedTuning = manifest.operationalHash != definition.operationalHash
      if (active.nonEmpty && !changedTuning)
        statusFor(spark, target, manifest, forcedStatus = Some("active"))
      else {
        if (active.nonEmpty) stopNative(spark, target, registryKey)
        val effectiveManifest =
          if (changedTuning) StreamingTableMetadata.replaceManifest(spark, target, definition) else manifest
        startNative(
          spark,
          frame,
          runtime,
          target,
          effectiveManifest,
          if (changedTuning) "restarted" else "initializing"
        )
      }
    }
  }

  private def rebuild(
      spark: SparkSession,
      frame: DataFrame,
      spec: StreamingTableSpec,
      runtime: StreamingRuntimeOptions,
      definition: StreamingTableDefinition,
      target: StreamingTableTarget,
      manifest: StreamingTableManifest
  ): StreamingTableStatus = {
    val registryKey = StreamingTableRegistry.targetKey(target)
    val intent = StreamingTableMetadata.writeOrVerifyResetIntent(
      spark,
      target,
      definition.fingerprint,
      manifest.sourcePaths
    )
    stopNative(spark, target, registryKey)

    val current = StreamingTableMetadata.resolveDeltaTarget(spark, spec.name, requireTableIdMarker = true)
    if (
      current.dataPath != target.dataPath ||
      current.deltaTableId != target.deltaTableId ||
      current.tableId != target.tableId
    )
      StreamingTableErrors.invalid(
        s"Target ${target.sqlIdentifier} changed during rebuild admission; refusing destructive reset"
      )
    StreamingTableMetadata.dropOwnedCatalogAndData(spark, current, manifest.sourcePaths)
    StreamingTableRegistry.forget(spark, registryKey)

    val replacement = StreamingTableMetadata.createOwnedTarget(spark, spec, frame.schema)
    StreamingTableMetadata.validateNoSourceTargetOverlap(
      spark,
      definition.sourcePaths,
      Some(replacement.dataPath)
    )
    val replacementManifest = StreamingTableMetadata.writeManifest(spark, replacement, definition)
    val status              = startNative(spark, frame, runtime, replacement, replacementManifest, "rebuilding")
    StreamingTableMetadata.clearResetIntent(spark, intent)
    status
  }

  private def reconcileResetJournal(
      spark: SparkSession,
      target: StreamingTableTarget,
      manifest: StreamingTableManifest,
      definition: StreamingTableDefinition,
      runtime: StreamingRuntimeOptions
  ): Unit =
    StreamingTableMetadata.pendingResetIntent(spark, target).foreach { intent =>
      val matchesReplacement =
        intent.targetIdentity == target.identity &&
          intent.targetPath == target.dataPath &&
          intent.replacementDefinitionHash == definition.fingerprint
      val completedReplacement =
        matchesReplacement &&
          manifest.definitionHash == definition.fingerprint &&
          target.deltaTableId != intent.oldDeltaTableId
      if (completedReplacement) StreamingTableMetadata.clearResetIntent(spark, intent)
      else if (!matchesReplacement || runtime.onQueryChange != "rebuild")
        StreamingTableErrors.invalid(
          s"Streaming table ${target.sqlIdentifier} has an incomplete reset journal; " +
            "resubmit the exact replacement with onQueryChange=rebuild or recover manually"
        )
      else if (target.deltaTableId != intent.oldDeltaTableId)
        StreamingTableErrors.invalid(
          s"Streaming table ${target.sqlIdentifier} reset journal does not match the current target generation"
        )
    }

  private def startNative(
      spark: SparkSession,
      frame: DataFrame,
      runtime: StreamingRuntimeOptions,
      target: StreamingTableTarget,
      manifest: StreamingTableManifest,
      status: String
  ): StreamingTableStatus = {
    val key = StreamingTableRegistry.targetKey(target)
    StreamingTableRegistry.ensureListener(spark)
    var writer = frame.writeStream
      .format("delta")
      .outputMode(runtime.outputMode)
      .options(runtime.sinkOptions)
      .option("checkpointLocation", target.checkpointLocation)
      .queryName(target.queryName)
    writer = writer.trigger(runtime.sparkTrigger)
    val query = writer.toTable(target.sqlIdentifier)
    StreamingTableRegistry.register(spark, key, target, manifest.definitionHash, query)
    val observed = statusFor(spark, target, manifest, forcedStatus = Some(status))
    if (!observed.isActive && observed.lastFailure.nonEmpty) observed.copy(status = "failed") else observed
  }

  private def stopNative(spark: SparkSession, target: StreamingTableTarget, key: String): Unit =
    StreamingTableRegistry.findActive(spark, key, target).foreach { query =>
      query.stop()
      if (query.isActive)
        StreamingTableErrors.invalid(
          s"Native writer ${query.id} for ${target.sqlIdentifier} remained active after stop()"
        )
      StreamingTableRegistry.markStopped(spark, key, query)
    }

  private def statusFor(
      spark: SparkSession,
      target: StreamingTableTarget,
      manifest: StreamingTableManifest,
      forcedStatus: Option[String]
  ): StreamingTableStatus = {
    val key   = StreamingTableRegistry.targetKey(target)
    val query = StreamingTableRegistry.findActive(spark, key, target)
    val entry = StreamingTableRegistry.entry(spark, key, target)
    val snapshot = entry.synchronized {
      StreamingTableRegistry.EntrySnapshot(
        queryId = Option(entry.queryId),
        runId = Option(entry.runId),
        lastFailure = Option(entry.lastFailure),
        lastProgress = Option(entry.lastProgress)
      )
    }
    val progress = query
      .flatMap(current => Option(current.lastProgress).map(value => StreamingTableDefinition.redactText(value.json)))
      .orElse(snapshot.lastProgress)
    progress.foreach(value => StreamingTableRegistry.recordProgress(spark, key, value))
    val failure = query
      .flatMap(_.exception.map(error => StreamingTableDefinition.redactText(error.getMessage)))
      .orElse(snapshot.lastFailure)
    val active = query.exists(_.isActive)
    val derived =
      if (active) "active"
      else if (failure.nonEmpty) "failed"
      else "stopped"
    val reportedStatus =
      forcedStatus.filterNot(forced => forced == "active" && !active).getOrElse(derived)
    StreamingTableStatus(
      tableName = target.sqlIdentifier,
      queryId = query.map(_.id.toString).orElse(snapshot.queryId),
      runId = query.map(_.runId.toString).orElse(snapshot.runId),
      status = reportedStatus,
      checkpointLocation = Some(target.checkpointLocation),
      definitionHash = Some(manifest.definitionHash),
      isActive = active,
      lastProgress = progress,
      lastFailure = failure
    )
  }

  private def validateStreamingQuery(
      spark: SparkSession,
      frame: DataFrame,
      spec: StreamingTableSpec,
      runtime: StreamingRuntimeOptions
  ): Unit = {
    if (!frame.isStreaming)
      StreamingTableErrors.invalid(
        "CREATE STREAMING TABLE requires at least one STREAM source; batch SELECTs are not supported"
      )
    StreamingTableMetadata.validateOutputSchema(frame.schema, spec.partitionColumns, spark)
    val deltaOptions = new DeltaOptions(runtime.sinkOptions, spark.sessionState.conf)
    Seq[Any](
      deltaOptions.canMergeSchema,
      deltaOptions.canOverwriteSchema,
      deltaOptions.rearrangeOnly,
      deltaOptions.txnVersion
    ).foreach(_ => ())
    frame.queryExecution.analyzed.collect { case watermark: EventTimeWatermark => watermark }.foreach { watermark =>
      watermark.eventTime.dataType match {
        case TimestampType | TimestampNTZType =>
        case other =>
          StreamingTableErrors.invalid(
            s"WATERMARK column ${watermark.eventTime.sql} must resolve to a timestamp, found ${other.catalogString}"
          )
      }
      if (EventTimeWatermark.getDelayMs(watermark.delay) < 0L)
        StreamingTableErrors.invalid("WATERMARK delay must not be negative")
    }
    UnsupportedOperationChecker.checkForStreaming(frame.queryExecution.analyzed, runtime.sparkOutputMode)
  }

  private def candidateCatalogNames(spark: SparkSession, namespace: Option[Seq[String]]): Seq[Seq[String]] =
    namespace match {
      case Some(parts) =>
        val qualified = StreamingTableMetadata.quoteMultipart(parts)
        spark
          .sql(s"SHOW TABLES IN $qualified")
          .collect()
          .toSeq
          .filterNot(_.getAs[Boolean]("isTemporary"))
          .map(row => parts :+ row.getAs[String]("tableName"))
      case None =>
        spark
          .sql("SHOW TABLES")
          .collect()
          .toSeq
          .filterNot(_.getAs[Boolean]("isTemporary"))
          .map(row => Seq(row.getAs[String]("tableName")))
    }

  private def matchesNamespace(spark: SparkSession, name: Seq[String], namespace: Seq[String]): Boolean = {
    name.size > namespace.size &&
    name.take(namespace.size).zip(namespace).forall { case (target, requested) =>
      spark.sessionState.analyzer.resolver(target, requested)
    }
  }
}

private[streaming] object StreamingTableRegistry {

  final class Entry(val target: StreamingTableTarget) {
    @volatile var query: StreamingQuery  = null
    @volatile var queryId: String        = null
    @volatile var runId: String          = null
    @volatile var definitionHash: String = null
    @volatile var lastFailure: String    = null
    @volatile var lastProgress: String   = null
  }

  final case class EntrySnapshot(
      queryId: Option[String],
      runId: Option[String],
      lastFailure: Option[String],
      lastProgress: Option[String]
  )

  private final class ContextRegistry {
    val locks            = new ConcurrentHashMap[String, Object]()
    val entries          = new ConcurrentHashMap[String, Entry]()
    val queryToTarget    = new ConcurrentHashMap[String, String]()
    val listenerSessions = Collections.synchronizedMap(new WeakHashMap[SparkSession, java.lang.Boolean]())
  }

  private val registries = Collections.synchronizedMap(new WeakHashMap[SparkContext, ContextRegistry]())

  def provisionalKey(identity: String, declaredPath: Option[String]): String =
    s"identity:$identity"

  def targetKey(target: StreamingTableTarget): String = s"${target.identity}\n${target.dataPath}"

  def lifecycleLockKey(target: StreamingTableTarget): String = s"identity:${target.identity}"

  def withTargetLock[A](spark: SparkSession, key: String)(body: => A): A = {
    val registry = context(spark)
    val proposed = new Object
    val existing = registry.locks.putIfAbsent(key, proposed)
    val lock     = if (existing == null) proposed else existing
    lock.synchronized(body)
  }

  def ensureListener(spark: SparkSession): Unit = {
    val registry = context(spark)
    val install = registry.listenerSessions.synchronized {
      if (registry.listenerSessions.containsKey(spark)) false
      else {
        registry.listenerSessions.put(spark, java.lang.Boolean.TRUE)
        true
      }
    }
    if (install) spark.streams.addListener(new RegistryListener(registry))
  }

  def register(
      spark: SparkSession,
      key: String,
      target: StreamingTableTarget,
      definitionHash: String,
      query: StreamingQuery
  ): Unit = {
    val registry  = context(spark)
    val candidate = new Entry(target)
    val existing  = registry.entries.putIfAbsent(key, candidate)
    val entry     = if (existing == null) candidate else existing
    entry.synchronized {
      entry.query = query
      entry.queryId = query.id.toString
      entry.runId = query.runId.toString
      entry.definitionHash = definitionHash
      entry.lastFailure = null
    }
    registry.queryToTarget.put(query.id.toString, key)
    if (!query.isActive) markStopped(spark, key, query)
  }

  def findActive(spark: SparkSession, key: String, target: StreamingTableTarget): Option[StreamingQuery] = {
    val registry = context(spark)
    val cached = Option(registry.entries.get(key)).flatMap { entry =>
      val query = entry.synchronized { Option(entry.query) }
      query.foreach { current =>
        if (!current.isActive) markStopped(spark, key, current)
      }
      query.filter(_.isActive)
    }
    cached.orElse {
      spark.streams.active.find(_.name == target.queryName).map { query =>
        register(spark, key, target, Option(registry.entries.get(key)).map(_.definitionHash).orNull, query)
        query
      }
    }
  }

  def markStopped(spark: SparkSession, key: String, query: StreamingQuery): Unit = {
    val registry = context(spark)
    Option(registry.entries.get(key)).foreach { entry =>
      entry.synchronized {
        if (entry.runId == query.runId.toString) {
          entry.query = null
          entry.lastFailure = query.exception.map(error => StreamingTableDefinition.redactText(error.getMessage)).orNull
        }
      }
    }
  }

  def recordProgress(spark: SparkSession, key: String, progress: String): Unit =
    Option(context(spark).entries.get(key)).foreach { entry =>
      entry.synchronized {
        entry.lastProgress = progress
      }
    }

  def entry(spark: SparkSession, key: String, target: StreamingTableTarget): Entry = {
    val registry  = context(spark)
    val candidate = new Entry(target)
    val existing  = registry.entries.putIfAbsent(key, candidate)
    if (existing == null) candidate else existing
  }

  def forget(spark: SparkSession, key: String): Unit = {
    val registry = context(spark)
    Option(registry.entries.remove(key)).foreach { entry =>
      Option(entry.queryId).foreach(registry.queryToTarget.remove)
    }
  }

  def registeredTargets(spark: SparkSession): Seq[StreamingTableTarget] =
    context(spark).entries.values().asScala.toSeq.map(_.target)

  private def context(spark: SparkSession): ContextRegistry =
    registries.synchronized {
      val contextKey = spark.sparkContext
      val existing   = registries.get(contextKey)
      if (existing != null) existing
      else {
        val created = new ContextRegistry
        registries.put(contextKey, created)
        contextKey.addSparkListener(new SparkListener {
          override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit =
            removeContext(contextKey)
        })
        created
      }
    }

  private def removeContext(sparkContext: SparkContext): Unit =
    registries.synchronized {
      Option(registries.remove(sparkContext)).foreach { registry =>
        registry.entries.clear()
        registry.locks.clear()
        registry.queryToTarget.clear()
        registry.listenerSessions.synchronized {
          registry.listenerSessions.clear()
        }
      }
    }

  private final class RegistryListener(registry: ContextRegistry) extends StreamingQueryListener {
    override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = ()

    override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
      val progress = event.progress
      Option(registry.queryToTarget.get(progress.id.toString)).foreach { key =>
        Option(registry.entries.get(key)).foreach { entry =>
          entry.synchronized {
            if (entry.runId == progress.runId.toString)
              entry.lastProgress = StreamingTableDefinition.redactText(progress.json)
          }
        }
      }
    }

    override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit =
      Option(registry.queryToTarget.get(event.id.toString)).foreach { key =>
        Option(registry.entries.get(key)).foreach { entry =>
          entry.synchronized {
            if (entry.runId == event.runId.toString) {
              entry.query = null
              entry.lastFailure = event.exception.map(StreamingTableDefinition.redactText).orNull
            }
          }
        }
      }
  }
}
