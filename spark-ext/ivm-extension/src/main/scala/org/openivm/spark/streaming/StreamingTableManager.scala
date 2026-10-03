package org.openivm.spark.streaming

import org.apache.spark.SparkContext
import org.apache.hadoop.fs.Path
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnsupportedOperationChecker
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark
import org.apache.spark.sql.openivm.StreamingDatasetAccess
import org.apache.spark.sql.delta.{DeltaLog, DeltaOptions}
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryListener}
import org.apache.spark.sql.types.{TimestampNTZType, TimestampType}
import org.openivm.spark.commands.{MaterializedViewLifecycle, RefreshMutex}
import org.openivm.spark.common.{
  MvCatalog,
  MvMetadata,
  StreamingDependencyCatalog,
  StreamingDependencySource,
  StreamingDependencyTarget
}

import java.util.concurrent.ConcurrentHashMap
import java.util.{Collections, UUID, WeakHashMap}
import scala.jdk.CollectionConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

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

  private final class InsightOperation(
      val operationId: String,
      val targetRelation: String,
      val command: String
  ) {
    var branchCode: Option[String]                     = None
    private var observed: Option[StreamingTableStatus] = None

    def selectBranch(code: String): Unit =
      branchCode = Some(code)

    def observe(status: StreamingTableStatus): Unit =
      observed = Some(status)

    def terminalStatus(result: StreamingTableStatus): StreamingTableStatus =
      observed
        .map { previous =>
          result.copy(
            queryId = result.queryId.orElse(previous.queryId),
            runId = result.runId.orElse(previous.runId),
            checkpointLocation = result.checkpointLocation.orElse(previous.checkpointLocation),
            definitionHash = result.definitionHash.orElse(previous.definitionHash),
            lastProgress = result.lastProgress.orElse(previous.lastProgress),
            lastFailure = result.lastFailure.orElse(previous.lastFailure)
          )
        }
        .getOrElse(result)

    def observedStatus: Option[StreamingTableStatus] = observed
  }

  @volatile private var beforeCascadeDropHookForTesting: StreamingTableCascadeTarget => Unit =
    (_: StreamingTableCascadeTarget) => ()

  private[streaming] def setBeforeCascadeDropHookForTesting(
      hook: StreamingTableCascadeTarget => Unit
  ): Unit =
    beforeCascadeDropHookForTesting = hook

  def create(spark: SparkSession, spec: StreamingTableSpec): StreamingTableStatus =
    withInsightOperation(spark, spec.name.mkString("."), command = "create") { insight =>
      createInternal(spark, spec, insight)
    }

  private def createInternal(
      spark: SparkSession,
      spec: StreamingTableSpec,
      insight: InsightOperation
  ): StreamingTableStatus = {
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
    val dependencySources = resolveManagedDependencySources(spark, definition)
    val lockKeys = StreamingTableRegistry.provisionalKey(identity, plannedPath) +:
      dependencySources.map(source => StreamingTableRegistry.lifecycleLockKey(source.parentIdentity))
    val globalLockKeys = identity +: dependencySources.map(_.parentIdentity)

    RefreshMutex.withLocks(globalLockKeys) {
      StreamingTableRegistry.withTargetLocks(spark, lockKeys) {
        val verifiedDependencySources = resolveManagedDependencySources(spark, definition)
        if (verifiedDependencySources != dependencySources)
          StreamingTableErrors.invalid(
            s"Streaming sources for ${StreamingTableMetadata.quoteMultipart(spec.name)} changed during lifecycle admission"
          )
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
          reconcileExisting(spark, frame, spec, runtime, definition, target, manifest, insight)
        } else {
          if (plannedPath.isEmpty)
            StreamingTableErrors.invalid(
              "Cannot resolve a catalog-native target path before CREATE; specify LOCATION for this catalog"
            )
          val recovery = StreamingTableMetadata.pendingResetIntent(spark, identity)
          recovery match {
            case Some(intent) =>
              if (runtime.onQueryChange != "rebuild" || intent.replacementDefinitionHash != definition.fingerprint)
                StreamingTableErrors.invalid(
                  s"A reset recovery for ${StreamingTableMetadata.quoteMultipart(spec.name)} is pending; " +
                    "resubmit the exact replacement with onQueryChange=rebuild"
                )
              spec.location
                .map(location => StreamingTableMetadata.normalizePath(spark, location))
                .foreach { location =>
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
              val incompleteDescendants =
                intent.descendants.map(_.identity).filterNot(intent.completedDescendantIdentities.contains)
              if (incompleteDescendants.nonEmpty)
                StreamingTableErrors.invalid(
                  s"Reset recovery for ${StreamingTableMetadata.quoteMultipart(spec.name)} cannot recreate the " +
                    s"upstream target while downstream cleanup is incomplete: ${incompleteDescendants.mkString(", ")}"
                )

              insight.selectBranch(org.openivm.spark.insights.OpenIvmInsightsContract.BranchCode.S7)
              intent.rebuildDecision match {
                case Some(decision) =>
                  StreamingInsightEvents.semanticBranch(
                    spark,
                    insight.operationId,
                    insight.targetRelation,
                    org.openivm.spark.insights.OpenIvmInsightsContract.BranchCode.S7,
                    decision,
                    policy = runtime.onQueryChange,
                    details = recoveryDetails(intent)
                  )
                case None =>
                  StreamingInsightEvents.branch(
                    spark,
                    insight.operationId,
                    insight.targetRelation,
                    org.openivm.spark.insights.OpenIvmInsightsContract.BranchCode.S7,
                    policy = runtime.onQueryChange,
                    details = recoveryDetails(intent)
                  )
              }
              StreamingInsightEvents.cascadePlan(
                spark,
                insight.operationId,
                insight.targetRelation,
                intent.targetIdentity,
                intent.descendants,
                intent.completedDescendantIdentities.toSet,
                reason = intent.rebuildDecision.map(_.code).getOrElse("rebuild_recovery")
              )
              StreamingInsightEvents.recoveryState(
                spark,
                insight.operationId,
                insight.targetRelation,
                intent,
                phase = "resuming"
              )
              emitAlreadyCompletedDescendants(spark, insight, intent)
              val archived = StreamingTableMetadata.deleteResetOwnedPath(
                spark,
                intent,
                (intent.sourcePaths ++ definition.sourcePaths).distinct,
                operationId = Some(insight.operationId)
              )
              emitArchiveAction(
                spark,
                insight,
                insight.targetRelation,
                targetKind = "streaming",
                targetIdentity = intent.targetIdentity,
                causedBy = None,
                archived
              )
              StreamingInsightEvents.action(
                spark,
                insight.operationId,
                insight.targetRelation,
                targetKind = "streaming",
                targetIdentity = intent.targetIdentity,
                causedBy = None,
                stage = "target_cleanup",
                code = org.openivm.spark.insights.OpenIvmInsightsContract.Code.StreamingTargetDropped,
                message = "Removed the residual streaming target during rebuild recovery.",
                status = "recovery_cleanup_completed",
                details = Seq("target_path" -> intent.targetPath)
              )

            case None =>
              insight.selectBranch(org.openivm.spark.insights.OpenIvmInsightsContract.BranchCode.S1)
              StreamingInsightEvents.branch(
                spark,
                insight.operationId,
                insight.targetRelation,
                org.openivm.spark.insights.OpenIvmInsightsContract.BranchCode.S1,
                policy = runtime.onQueryChange,
                details = StreamingInsightEvents.fingerprintDetails("requested", definition.fingerprint) ++ Seq(
                  "target_identity" -> identity,
                  "target_path"     -> plannedPath,
                  "source_count"    -> definition.sources.size
                )
              )
          }
          val target = StreamingTableMetadata.createOwnedTarget(spark, spec, frame.schema)
          StreamingInsightEvents.action(
            spark,
            insight.operationId,
            target.sqlIdentifier,
            targetKind = "streaming",
            targetIdentity = target.identity,
            causedBy = None,
            stage = "target",
            code =
              if (recovery.nonEmpty)
                org.openivm.spark.insights.OpenIvmInsightsContract.Code.StreamingTargetRecreated
              else org.openivm.spark.insights.OpenIvmInsightsContract.Code.StreamingTargetCreated,
            message =
              if (recovery.nonEmpty) "Recreated the owned streaming target."
              else "Created the owned streaming target.",
            details = Seq(
              "target_path"         -> target.dataPath,
              "checkpoint_location" -> target.checkpointLocation
            )
          )
          StreamingTableMetadata.validateNoSourceTargetOverlap(
            spark,
            definition.sourcePaths,
            Some(target.dataPath)
          )
          val manifest = StreamingTableMetadata.writeManifest(spark, target, definition)
          publishDependencyTarget(spark, target, definition, verifiedDependencySources)
          val status = startNative(spark, frame, runtime, target, manifest, "initializing")
          insight.observe(status)
          StreamingInsightEvents.action(
            spark,
            insight.operationId,
            target.sqlIdentifier,
            targetKind = "streaming",
            targetIdentity = target.identity,
            causedBy = None,
            stage = "query",
            code = org.openivm.spark.insights.OpenIvmInsightsContract.Code.StreamingQueryStarted,
            message =
              if (recovery.nonEmpty) "Started the recreated native streaming query."
              else "Started the new native streaming query.",
            status = status.status,
            runtime = Some(status)
          )
          recovery.foreach { intent =>
            StreamingTableMetadata.clearResetIntent(spark, intent)
            StreamingInsightEvents.recoveryState(
              spark,
              insight.operationId,
              insight.targetRelation,
              intent.copy(
                completedDescendantIdentities = intent.descendants.map(_.identity),
                upstreamDropped = true
              ),
              phase = "completed"
            )
          }
          status
        }
      }
    }
  }

  def stop(spark: SparkSession, name: Seq[String]): StreamingTableStatus =
    withInsightOperation(spark, name.mkString("."), command = "stop") { insight =>
      stopInternal(spark, name, insight)
    }

  private def stopInternal(
      spark: SparkSession,
      name: Seq[String],
      insight: InsightOperation
  ): StreamingTableStatus = {
    val target   = StreamingTableMetadata.resolveDeltaTarget(spark, name, requireTableIdMarker = true)
    val manifest = StreamingTableMetadata.readManifest(spark, target)
    StreamingTableMetadata.verifyOwned(spark, target, manifest)
    val registryKey = StreamingTableRegistry.targetKey(target)
    val lockKey     = StreamingTableRegistry.lifecycleLockKey(target)
    RefreshMutex.withLock(target.identity) {
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
        val before = statusFor(spark, current, manifest, forcedStatus = None)
        insight.observe(before)
        stopNative(spark, current, registryKey)
        val status = statusFor(spark, current, manifest, forcedStatus = Some("stopped"))
        insight.observe(status)
        StreamingInsightEvents.action(
          spark,
          insight.operationId,
          current.sqlIdentifier,
          targetKind = "streaming",
          targetIdentity = current.identity,
          causedBy = None,
          stage = "query",
          code = org.openivm.spark.insights.OpenIvmInsightsContract.Code.StreamingQueryStopped,
          message =
            if (before.isActive) "Stopped the native streaming query."
            else "The native streaming query was already stopped.",
          status = if (before.isActive) "stopped" else "already_stopped",
          runtime = Some(status)
        )
        status
      }
    }
  }

  def drop(spark: SparkSession, name: Seq[String], ifExists: Boolean): StreamingTableStatus =
    withInsightOperation(spark, name.mkString("."), command = "drop") { insight =>
      dropInternal(spark, name, ifExists, insight)
    }

  private def dropInternal(
      spark: SparkSession,
      name: Seq[String],
      ifExists: Boolean,
      insight: InsightOperation
  ): StreamingTableStatus = {
    if (!StreamingTableMetadata.catalogTableExists(spark, name)) {
      val identity = StreamingTableMetadata.canonicalIdentity(spark, name)
      insight.selectBranch(org.openivm.spark.insights.OpenIvmInsightsContract.BranchCode.S8)
      StreamingInsightEvents.branch(
        spark,
        insight.operationId,
        insight.targetRelation,
        org.openivm.spark.insights.OpenIvmInsightsContract.BranchCode.S8,
        policy = "explicit_drop",
        details = Seq(
          "target_identity" -> identity,
          "target_exists"   -> false,
          "if_exists"       -> ifExists
        )
      )
      StreamingInsightEvents.cascadePlan(
        spark,
        insight.operationId,
        insight.targetRelation,
        identity,
        descendants = Seq.empty,
        completedIdentities = Set.empty,
        reason = "explicit_drop"
      )
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
    val descendants = resolveCascadeDescendantsForRebuild(spark, target, manifest)
    insight.selectBranch(org.openivm.spark.insights.OpenIvmInsightsContract.BranchCode.S8)
    StreamingInsightEvents.branch(
      spark,
      insight.operationId,
      insight.targetRelation,
      org.openivm.spark.insights.OpenIvmInsightsContract.BranchCode.S8,
      policy = "explicit_drop",
      details = Seq(
        "target_identity"  -> target.identity,
        "target_exists"    -> true,
        "if_exists"        -> ifExists,
        "descendant_count" -> descendants.size
      )
    )
    StreamingInsightEvents.cascadePlan(
      spark,
      insight.operationId,
      insight.targetRelation,
      target.identity,
      descendants,
      completedIdentities = Set.empty,
      reason = "explicit_drop"
    )
    val streamingLockKeys = (target.identity +: descendants.filter(_.kind == "streaming").map(_.identity))
      .map(StreamingTableRegistry.lifecycleLockKey)
    val materializedLockKeys =
      target.identity +: descendants.map(_.identity)
    RefreshMutex.withLocks(materializedLockKeys) {
      StreamingTableRegistry.withTargetLocks(spark, streamingLockKeys) {
        val verifiedDescendants =
          resolveCascadeDescendantsForRebuild(spark, target, manifest)
        if (verifiedDescendants != descendants)
          StreamingTableErrors.invalid(
            s"Downstream dependencies for ${target.sqlIdentifier} changed during DROP admission"
          )
        dropResolvedCascade(
          spark,
          descendants,
          StreamingArchiveContext(
            action = "cascade",
            operationId = insight.operationId,
            rootTarget = target.identity,
            causedBy = Some(target.identity)
          ),
          insightOperation = org.openivm.spark.insights.OpenIvmInsightsContract.Operation.Streaming
        )
        val registryKey = StreamingTableRegistry.targetKey(target)
        val before      = statusFor(spark, target, manifest, forcedStatus = None)
        insight.observe(before)
        stopNative(spark, target, registryKey)
        val stopped = before.copy(status = "stopped", isActive = false)
        StreamingInsightEvents.action(
          spark,
          insight.operationId,
          target.sqlIdentifier,
          targetKind = "streaming",
          targetIdentity = target.identity,
          causedBy = None,
          stage = "query",
          code = org.openivm.spark.insights.OpenIvmInsightsContract.Code.StreamingQueryStopped,
          message =
            if (before.isActive) "Stopped the root native streaming query before DROP."
            else "The root native streaming query was already stopped before DROP.",
          status = if (before.isActive) "stopped" else "already_stopped",
          runtime = Some(stopped)
        )
        val current = StreamingTableMetadata.resolveDeltaTarget(spark, name, requireTableIdMarker = true)
        if (
          current.dataPath != target.dataPath ||
          current.deltaTableId != target.deltaTableId ||
          current.tableId != target.tableId
        )
          StreamingTableErrors.invalid(
            s"Target ${target.sqlIdentifier} changed after its writer stopped; refusing deletion"
          )
        val archived = StreamingTableMetadata.dropOwnedCatalogAndData(
          spark,
          current,
          manifest.sourcePaths,
          StreamingArchiveContext(
            action = "drop",
            operationId = insight.operationId,
            rootTarget = current.identity
          )
        )
        emitArchiveAction(
          spark,
          insight,
          current.sqlIdentifier,
          targetKind = "streaming",
          targetIdentity = current.identity,
          causedBy = None,
          archived
        )
        StreamingInsightEvents.action(
          spark,
          insight.operationId,
          current.sqlIdentifier,
          targetKind = "streaming",
          targetIdentity = current.identity,
          causedBy = None,
          stage = "target_cleanup",
          code = org.openivm.spark.insights.OpenIvmInsightsContract.Code.StreamingTargetDropped,
          message = "Dropped the owned streaming target.",
          details = Seq("target_path" -> current.dataPath)
        )
        StreamingTableRegistry.forget(spark, registryKey)
        StreamingDependencyCatalog.remove(spark, target.identity)
        val status = StreamingTableStatus(
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
        status
      }
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
              case NonFatal(error) =>
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
      manifest: StreamingTableManifest,
      insight: InsightOperation
  ): StreamingTableStatus = {
    reconcileResetJournal(spark, target, manifest, definition, runtime)
    val exactDefinition = manifest.definitionHash == definition.fingerprint
    val compatibleLegacyDefinition =
      !exactDefinition &&
        StreamingTableDefinition.semanticallyEquivalent(manifest.semanticJson, definition.semanticJson)
    if (!exactDefinition && !compatibleLegacyDefinition) {
      val rebuildDecision = StreamingTableDefinition.rebuildDecision(
        manifest.definitionHash,
        manifest.semanticJson,
        manifest.diagnosticJson,
        definition
      )
      if (runtime.onQueryChange != "rebuild") {
        val current = statusFor(spark, target, manifest, forcedStatus = None)
        insight.observe(current)
        insight.selectBranch(org.openivm.spark.insights.OpenIvmInsightsContract.BranchCode.S6)
        StreamingInsightEvents.semanticBranch(
          spark,
          insight.operationId,
          target.sqlIdentifier,
          org.openivm.spark.insights.OpenIvmInsightsContract.BranchCode.S6,
          rebuildDecision,
          policy = runtime.onQueryChange,
          details = Seq(
            "target_identity" -> target.identity,
            "target_path"     -> target.dataPath,
            "active"          -> current.isActive
          ),
          level = org.openivm.spark.insights.OpenIvmInsightsContract.Level.Warn
        )
        StreamingTableErrors.invalid(
          s"Streaming table ${target.sqlIdentifier} has a different semantic definition; " +
            "submit OPTIONS ('onQueryChange'='rebuild') to replace this owned target"
        )
      }
      rebuild(spark, frame, spec, runtime, definition, target, manifest, rebuildDecision, insight)
    } else {
      publishDependencyTarget(spark, target, definition)
      val registryKey   = StreamingTableRegistry.targetKey(target)
      val active        = StreamingTableRegistry.findActive(spark, registryKey, target)
      val changedTuning = manifest.operationalHash != definition.operationalHash
      val before        = statusFor(spark, target, manifest, forcedStatus = None)
      insight.observe(before)
      val effectiveManifest =
        if (compatibleLegacyDefinition || changedTuning)
          StreamingTableMetadata.replaceManifest(spark, target, definition)
        else manifest
      if (active.nonEmpty && !changedTuning) {
        insight.selectBranch(org.openivm.spark.insights.OpenIvmInsightsContract.BranchCode.S2)
        StreamingInsightEvents.branch(
          spark,
          insight.operationId,
          target.sqlIdentifier,
          org.openivm.spark.insights.OpenIvmInsightsContract.BranchCode.S2,
          policy = runtime.onQueryChange,
          details = StreamingInsightEvents.fingerprintDetails("definition", definition.fingerprint) ++ Seq(
            "target_identity"              -> target.identity,
            "compatible_legacy_definition" -> compatibleLegacyDefinition
          )
        )
        val status = statusFor(spark, target, effectiveManifest, forcedStatus = Some("active"))
        insight.observe(status)
        StreamingInsightEvents.action(
          spark,
          insight.operationId,
          target.sqlIdentifier,
          targetKind = "streaming",
          targetIdentity = target.identity,
          causedBy = None,
          stage = "query",
          code = org.openivm.spark.insights.OpenIvmInsightsContract.Code.StreamingTargetReused,
          message = "Reused the active native streaming query.",
          status = "active_reused",
          runtime = Some(status)
        )
        status
      } else {
        val branchCode =
          if (changedTuning) org.openivm.spark.insights.OpenIvmInsightsContract.BranchCode.S4
          else org.openivm.spark.insights.OpenIvmInsightsContract.BranchCode.S3
        insight.selectBranch(branchCode)
        StreamingInsightEvents.branch(
          spark,
          insight.operationId,
          target.sqlIdentifier,
          branchCode,
          policy = runtime.onQueryChange,
          details = StreamingInsightEvents.fingerprintDetails("definition", definition.fingerprint) ++
            (if (changedTuning)
               StreamingInsightEvents.fingerprintDetails("previous_operational", manifest.operationalHash) ++
                 StreamingInsightEvents.fingerprintDetails("requested_operational", definition.operationalHash)
             else Seq.empty) ++
            Seq(
              "target_identity"              -> target.identity,
              "was_active"                   -> before.isActive,
              "compatible_legacy_definition" -> compatibleLegacyDefinition
            )
        )
        if (active.nonEmpty) {
          stopNative(spark, target, registryKey)
          StreamingInsightEvents.action(
            spark,
            insight.operationId,
            target.sqlIdentifier,
            targetKind = "streaming",
            targetIdentity = target.identity,
            causedBy = None,
            stage = "query",
            code = org.openivm.spark.insights.OpenIvmInsightsContract.Code.StreamingQueryStopped,
            message = "Stopped the active native query before applying operational tuning.",
            status = "stopped_for_restart",
            runtime = Some(before.copy(status = "stopped", isActive = false))
          )
        }
        val status = startNative(
          spark,
          frame,
          runtime,
          target,
          effectiveManifest,
          if (changedTuning) "restarted" else "initializing"
        )
        insight.observe(status)
        StreamingInsightEvents.action(
          spark,
          insight.operationId,
          target.sqlIdentifier,
          targetKind = "streaming",
          targetIdentity = target.identity,
          causedBy = None,
          stage = "query",
          code =
            if (changedTuning)
              org.openivm.spark.insights.OpenIvmInsightsContract.Code.StreamingQueryRestarted
            else org.openivm.spark.insights.OpenIvmInsightsContract.Code.StreamingQueryResumed,
          message =
            if (changedTuning) "Restarted the native streaming query with operational tuning."
            else "Resumed the native streaming query from its checkpoint.",
          status = status.status,
          runtime = Some(status)
        )
        status
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
      manifest: StreamingTableManifest,
      rebuildDecision: StreamingRebuildDecision,
      insight: InsightOperation
  ): StreamingTableStatus = {
    val pending = StreamingTableMetadata.pendingResetIntent(spark, target)
    val descendants = pending
      .map(_.descendants)
      .getOrElse(resolveCascadeDescendantsForRebuild(spark, target, manifest))
    val lockKeys = (target.identity +: descendants.filter(_.kind == "streaming").map(_.identity))
      .map(StreamingTableRegistry.lifecycleLockKey)
    val materializedLockKeys = target.identity +: descendants.map(_.identity)
    RefreshMutex.withLocks(materializedLockKeys) {
      StreamingTableRegistry.withTargetLocks(spark, lockKeys) {
        val branchCode =
          if (pending.nonEmpty) org.openivm.spark.insights.OpenIvmInsightsContract.BranchCode.S7
          else org.openivm.spark.insights.OpenIvmInsightsContract.BranchCode.S5
        val authoritativeDecision = pending
          .flatMap(_.rebuildDecision)
          .getOrElse(rebuildDecision)
        insight.selectBranch(branchCode)
        StreamingInsightEvents.semanticBranch(
          spark,
          insight.operationId,
          target.sqlIdentifier,
          branchCode,
          authoritativeDecision,
          policy = runtime.onQueryChange,
          details = Seq(
            "target_identity"  -> target.identity,
            "target_path"      -> target.dataPath,
            "descendant_count" -> descendants.size
          ) ++ pending.toSeq.flatMap(recoveryDetails)
        )
        StreamingInsightEvents.cascadePlan(
          spark,
          insight.operationId,
          target.sqlIdentifier,
          target.identity,
          descendants,
          pending.map(_.completedDescendantIdentities.toSet).getOrElse(Set.empty),
          reason = authoritativeDecision.code
        )
        pending.foreach { intent =>
          StreamingInsightEvents.recoveryState(
            spark,
            insight.operationId,
            target.sqlIdentifier,
            intent,
            phase = "resuming"
          )
        }
        val intent = pending.getOrElse {
          val verified = resolveCascadeDescendantsForRebuild(spark, target, manifest)
          if (verified != descendants)
            StreamingTableErrors.invalid(
              s"Downstream dependencies for ${target.sqlIdentifier} changed during rebuild admission"
            )
          val written = StreamingTableMetadata.writeOrVerifyResetIntent(
            spark,
            target,
            definition.fingerprint,
            manifest.sourcePaths,
            rebuildDecision,
            descendants,
            operationId = insight.operationId
          )
          StreamingDependencyCatalog.backupNow(spark)
          written
        }
        val afterDescendants = dropCascadeDescendants(spark, intent, insight)
        val registryKey      = StreamingTableRegistry.targetKey(target)
        val before           = statusFor(spark, target, manifest, forcedStatus = None)
        insight.observe(before)
        stopNative(spark, target, registryKey)
        StreamingInsightEvents.action(
          spark,
          insight.operationId,
          target.sqlIdentifier,
          targetKind = "streaming",
          targetIdentity = target.identity,
          causedBy = None,
          stage = "query",
          code = org.openivm.spark.insights.OpenIvmInsightsContract.Code.StreamingQueryStopped,
          message =
            if (before.isActive) "Stopped the root native streaming query before rebuild."
            else "The root native streaming query was already stopped before rebuild.",
          status = if (before.isActive) "stopped_for_rebuild" else "already_stopped",
          runtime = Some(before.copy(status = "stopped", isActive = false))
        )

        val current = StreamingTableMetadata.resolveDeltaTarget(spark, spec.name, requireTableIdMarker = true)
        if (
          current.dataPath != target.dataPath ||
          current.deltaTableId != target.deltaTableId ||
          current.tableId != target.tableId
        )
          StreamingTableErrors.invalid(
            s"Target ${target.sqlIdentifier} changed during rebuild admission; refusing destructive reset"
          )
        val archived = StreamingTableMetadata.dropOwnedCatalogAndData(
          spark,
          current,
          manifest.sourcePaths,
          StreamingArchiveContext(
            action = "rebuild",
            operationId = insight.operationId,
            rootTarget = intent.targetIdentity,
            rebuildDecision = intent.rebuildDecision
          )
        )
        emitArchiveAction(
          spark,
          insight,
          current.sqlIdentifier,
          targetKind = "streaming",
          targetIdentity = current.identity,
          causedBy = None,
          archived
        )
        StreamingInsightEvents.action(
          spark,
          insight.operationId,
          current.sqlIdentifier,
          targetKind = "streaming",
          targetIdentity = current.identity,
          causedBy = None,
          stage = "target_cleanup",
          code = org.openivm.spark.insights.OpenIvmInsightsContract.Code.StreamingTargetDropped,
          message = "Dropped the previous owned streaming target generation.",
          status = "dropped_for_rebuild",
          details = Seq("target_path" -> current.dataPath)
        )
        StreamingTableRegistry.forget(spark, registryKey)
        StreamingDependencyCatalog.remove(spark, target.identity)
        val upstreamDropped = afterDescendants.copy(upstreamDropped = true)
        StreamingTableMetadata.updateResetIntent(spark, afterDescendants, upstreamDropped)

        val replacement = StreamingTableMetadata.createOwnedTarget(spark, spec, frame.schema)
        StreamingInsightEvents.action(
          spark,
          insight.operationId,
          replacement.sqlIdentifier,
          targetKind = "streaming",
          targetIdentity = replacement.identity,
          causedBy = None,
          stage = "target",
          code = org.openivm.spark.insights.OpenIvmInsightsContract.Code.StreamingTargetRecreated,
          message = "Recreated the owned streaming target after semantic change.",
          details = Seq(
            "target_path"         -> replacement.dataPath,
            "checkpoint_location" -> replacement.checkpointLocation
          )
        )
        StreamingTableMetadata.validateNoSourceTargetOverlap(
          spark,
          definition.sourcePaths,
          Some(replacement.dataPath)
        )
        val replacementManifest = StreamingTableMetadata.writeManifest(spark, replacement, definition)
        publishDependencyTarget(spark, replacement, definition)
        val status = startNative(spark, frame, runtime, replacement, replacementManifest, "rebuilding")
        insight.observe(status)
        StreamingInsightEvents.action(
          spark,
          insight.operationId,
          replacement.sqlIdentifier,
          targetKind = "streaming",
          targetIdentity = replacement.identity,
          causedBy = None,
          stage = "query",
          code = org.openivm.spark.insights.OpenIvmInsightsContract.Code.StreamingQueryStarted,
          message = "Started the rebuilt native streaming query.",
          status = status.status,
          runtime = Some(status)
        )
        StreamingTableMetadata.clearResetIntent(spark, upstreamDropped)
        pending.foreach { _ =>
          StreamingInsightEvents.recoveryState(
            spark,
            insight.operationId,
            target.sqlIdentifier,
            upstreamDropped,
            phase = "completed"
          )
        }
        status
      }
    }
  }

  private[spark] def withCascadeLocks[A](
      spark: SparkSession,
      descendants: Seq[StreamingTableCascadeTarget]
  )(body: => A): A = {
    val streamingKeys = descendants
      .filter(_.kind == "streaming")
      .map(target => StreamingTableRegistry.lifecycleLockKey(target.identity))
    StreamingTableRegistry.withTargetLocks(spark, streamingKeys)(body)
  }

  private[spark] def dropResolvedCascade(
      spark: SparkSession,
      descendants: Seq[StreamingTableCascadeTarget],
      context: StreamingArchiveContext,
      insightOperation: String = org.openivm.spark.insights.OpenIvmInsightsContract.Operation.Drop
  ): Unit =
    descendants.foreach { descendant =>
      val targetName = descendant.name.mkString(".")
      val causedBy   = descendant.causedBy.orElse(context.causedBy)
      StreamingInsightEvents.action(
        spark,
        context.operationId,
        targetName,
        targetKind = descendant.kind,
        targetIdentity = descendant.identity,
        causedBy = causedBy,
        stage = "dependent_cleanup",
        code = org.openivm.spark.insights.OpenIvmInsightsContract.Code.StreamingDescendantCleanupStarted,
        message = "Started cleanup of a dependent managed target.",
        status = "running",
        operation = insightOperation
      )
      try {
        val result = dropCascadeTarget(spark, descendant, context)
        emitCascadeDropActions(
          spark,
          context.operationId,
          descendant,
          causedBy,
          result,
          insightOperation
        )
        StreamingInsightEvents.action(
          spark,
          context.operationId,
          targetName,
          targetKind = descendant.kind,
          targetIdentity = descendant.identity,
          causedBy = causedBy,
          stage = "dependent_cleanup",
          code = org.openivm.spark.insights.OpenIvmInsightsContract.Code.StreamingDescendantCleanupCompleted,
          message = "Completed cleanup of a dependent managed target.",
          operation = insightOperation
        )
      } catch {
        case error: Throwable =>
          StreamingInsightEvents.action(
            spark,
            context.operationId,
            targetName,
            targetKind = descendant.kind,
            targetIdentity = descendant.identity,
            causedBy = causedBy,
            stage = "dependent_cleanup",
            code = org.openivm.spark.insights.OpenIvmInsightsContract.Code.StreamingDescendantCleanupFailed,
            message = "Dependent managed-target cleanup failed.",
            status = "failed",
            level = org.openivm.spark.insights.OpenIvmInsightsContract.Level.Error,
            operation = insightOperation,
            details = Seq("error_class" -> error.getClass.getName)
          )
          throw error
      }
    }

  private def resolveCascadeDescendants(
      spark: SparkSession,
      root: StreamingTableTarget
  ): Seq[StreamingTableCascadeTarget] = {
    val rootRecord = StreamingDependencyCatalog
      .lookup(spark, root.identity)
      .getOrElse(
        StreamingTableErrors.invalid(
          s"Streaming table ${root.sqlIdentifier} is missing durable dependency metadata"
        )
      )
    validateDependencyTarget(rootRecord, root)
    resolveCascadeDescendants(spark, streamingNode(spark, rootRecord))
  }

  private def resolveCascadeDescendantsForRebuild(
      spark: SparkSession,
      root: StreamingTableTarget,
      manifest: StreamingTableManifest
  ): Seq[StreamingTableCascadeTarget] = {
    val rootNode = StreamingDependencyCatalog.lookup(spark, root.identity) match {
      case Some(rootRecord) =>
        validateDependencyTarget(rootRecord, root)
        streamingNode(spark, rootRecord)
      case None =>
        CascadeNode(
          StreamingTableCascadeTarget(
            kind = "streaming",
            name = root.name,
            identity = root.identity,
            dataPath = root.dataPath,
            deltaTableId = root.deltaTableId,
            tableId = root.tableId,
            sourcePaths = manifest.sourcePaths
          ),
          formatVersion = 1
        )
    }
    resolveCascadeDescendants(spark, rootNode)
  }

  private[spark] def resolveMaterializedCascadeDescendants(
      spark: SparkSession,
      meta: MvMetadata
  ): Seq[StreamingTableCascadeTarget] =
    resolveCascadeDescendants(spark, materializedNode(spark, meta))

  private final case class CascadeNode(
      target: StreamingTableCascadeTarget,
      formatVersion: Int
  )

  private def resolveCascadeDescendants(
      spark: SparkSession,
      root: CascadeNode
  ): Seq[StreamingTableCascadeTarget] = {
    val visiting = mutable.Set.empty[String]
    val visited  = mutable.Set.empty[String]
    val ordered  = mutable.ArrayBuffer.empty[StreamingTableCascadeTarget]

    def visit(parent: CascadeNode): Unit = {
      if (!visiting.add(parent.target.identity))
        StreamingTableErrors.invalid(s"Managed dependency graph contains a cycle at '${parent.target.identity}'")
      directCascadeChildren(spark, parent).foreach { child =>
        if (!visited.contains(child.target.identity)) {
          visit(child)
          ordered += child.target.copy(causedBy = Some(parent.target.identity))
          visited += child.target.identity
        }
      }
      visiting -= parent.target.identity
    }

    visit(root)
    ordered.toSeq
  }

  private def directCascadeChildren(
      spark: SparkSession,
      parent: CascadeNode
  ): Seq[CascadeNode] = {
    val streamingChildren = StreamingDependencyCatalog.directChildren(spark, parent.target.identity).map { child =>
      val source = child.sources
        .find(_.parentIdentity == parent.target.identity)
        .getOrElse(
          StreamingTableErrors.invalid(
            s"Streaming dependency child '${child.identity}' is missing its parent edge"
          )
        )
      if (
        source.parentPath != parent.target.dataPath ||
        source.parentDeltaTableId != parent.target.deltaTableId ||
        source.parentTableId != parent.target.tableId ||
        source.formatVersion != parent.formatVersion
      )
        StreamingTableErrors.invalid(
          s"Streaming dependency child '${child.identity}' is bound to a stale generation of " +
            s"'${parent.target.identity}'"
        )
      streamingNode(spark, child)
    }
    val sourceAliasValues = sourceAliases(spark, parent.target)
    val caseSensitive     = spark.sessionState.conf.caseSensitiveAnalysis
    def normalize(value: String): String =
      if (caseSensitive) value else value.toLowerCase(java.util.Locale.ROOT)
    val parentShortName = normalize(parent.target.name.last)
    val materializedChildren = (sourceAliasValues
      .flatMap(source => MvCatalog.viewsForSource(spark, source)) ++
      MvCatalog
        .list(spark)
        .filter(
          _.sourceTables.exists { source =>
            normalize(source.split("\\.").last.stripPrefix("`").stripSuffix("`")) == parentShortName
          }
        ))
      .groupBy(meta => materializedName(meta.name))
      .map(_._2.head)
      .toSeq
      .map(materializedNode(spark, _))
    (streamingChildren ++ materializedChildren)
      .groupBy(_.target.identity)
      .map(_._2.head)
      .toSeq
      .sortBy(_.target.identity)
  }

  private def streamingNode(
      spark: SparkSession,
      record: StreamingDependencyTarget
  ): CascadeNode = {
    val target = StreamingTableMetadata.resolveDeltaTarget(
      spark,
      record.name,
      requireTableIdMarker = true
    )
    validateDependencyTarget(record, target)
    val manifest = StreamingTableMetadata.readManifest(spark, target)
    StreamingTableMetadata.verifyOwned(spark, target, manifest)
    CascadeNode(
      StreamingTableCascadeTarget(
        kind = "streaming",
        name = target.name,
        identity = target.identity,
        dataPath = target.dataPath,
        deltaTableId = target.deltaTableId,
        tableId = target.tableId,
        sourcePaths = manifest.sourcePaths
      ),
      record.formatVersion
    )
  }

  private def materializedNode(spark: SparkSession, expected: MvMetadata): CascadeNode = {
    val meta = MvCatalog
      .lookup(spark, expected.name)
      .getOrElse(
        StreamingTableErrors.invalid(
          s"Materialized view '${materializedName(expected.name)}' is missing catalog metadata"
        )
      )
    if (meta != expected)
      StreamingTableErrors.invalid(
        s"Materialized view '${materializedName(expected.name)}' changed during cascade resolution"
      )
    val snapshot = DeltaLog.forTable(spark, new Path(meta.location)).update()
    if (snapshot.version < 0L)
      StreamingTableErrors.invalid(
        s"Materialized view '${materializedName(meta.name)}' has no readable Delta generation"
      )
    CascadeNode(
      StreamingTableCascadeTarget(
        kind = "materialized",
        name = tableNameParts(meta.name),
        identity = StreamingDependencyCatalog.materializedIdentity(materializedName(meta.name)),
        dataPath = StreamingTableMetadata.normalizePath(spark, meta.location),
        deltaTableId = snapshot.metadata.id,
        tableId = MvCatalog.mvIdentity(meta),
        sourcePaths = Seq.empty
      ),
      formatVersion = 1
    )
  }

  private def sourceAliases(spark: SparkSession, target: StreamingTableCascadeTarget): Seq[String] =
    (Seq(target.name.last) ++
      (if (target.name.size >= 2) Seq(target.name.takeRight(2).mkString(".")) else Seq.empty) ++
      (if (target.name.size == 1) Seq(s"${spark.catalog.currentDatabase}.${target.name.last}") else Seq.empty) ++
      Seq(target.name.mkString("."))).distinct

  private def tableNameParts(name: TableIdentifier): Seq[String] =
    name.catalog.toSeq ++ name.database.toSeq ++ Seq(name.table)

  private def materializedName(name: TableIdentifier): String =
    name.database.fold(name.table)(database => s"$database.${name.table}")

  private def validateDependencyTarget(
      record: StreamingDependencyTarget,
      target: StreamingTableTarget
  ): Unit =
    if (
      record.identity != target.identity ||
      record.name != target.name ||
      record.dataPath != target.dataPath ||
      record.deltaTableId != target.deltaTableId ||
      record.tableId != target.tableId
    )
      StreamingTableErrors.invalid(
        s"Streaming dependency metadata for ${target.sqlIdentifier} does not match its owned target generation"
      )

  private def dropCascadeDescendants(
      spark: SparkSession,
      initial: StreamingTableResetIntent,
      insight: InsightOperation
  ): StreamingTableResetIntent =
    initial.descendants.foldLeft(initial) { (intent, descendant) =>
      val targetName = descendant.name.mkString(".")
      val causedBy   = descendant.causedBy.orElse(Some(intent.targetIdentity))
      if (intent.completedDescendantIdentities.contains(descendant.identity)) {
        StreamingInsightEvents.action(
          spark,
          insight.operationId,
          targetName,
          targetKind = descendant.kind,
          targetIdentity = descendant.identity,
          causedBy = causedBy,
          stage = "dependent_cleanup",
          code = org.openivm.spark.insights.OpenIvmInsightsContract.Code.StreamingDescendantCleanupCompleted,
          message = "Reused completed descendant cleanup from the rebuild journal.",
          status = "already_completed",
          details = recoveryDetails(intent)
        )
        intent
      } else {
        StreamingInsightEvents.action(
          spark,
          insight.operationId,
          targetName,
          targetKind = descendant.kind,
          targetIdentity = descendant.identity,
          causedBy = causedBy,
          stage = "dependent_cleanup",
          code = org.openivm.spark.insights.OpenIvmInsightsContract.Code.StreamingDescendantCleanupStarted,
          message = "Started cleanup of a descendant for streaming rebuild.",
          status = "running",
          details = recoveryDetails(intent)
        )
        try {
          beforeCascadeDropHookForTesting(descendant)
          val result = dropCascadeTarget(
            spark,
            descendant,
            StreamingArchiveContext(
              action = "cascade",
              operationId = insight.operationId,
              rootTarget = intent.targetIdentity,
              causedBy = causedBy,
              rebuildDecision = intent.rebuildDecision
            )
          )
          val updated = intent.copy(
            completedDescendantIdentities = intent.completedDescendantIdentities :+ descendant.identity
          )
          StreamingTableMetadata.updateResetIntent(spark, intent, updated)
          emitCascadeDropActions(
            spark,
            insight.operationId,
            descendant,
            causedBy,
            result,
            org.openivm.spark.insights.OpenIvmInsightsContract.Operation.Streaming
          )
          StreamingInsightEvents.action(
            spark,
            insight.operationId,
            targetName,
            targetKind = descendant.kind,
            targetIdentity = descendant.identity,
            causedBy = causedBy,
            stage = "dependent_cleanup",
            code = org.openivm.spark.insights.OpenIvmInsightsContract.Code.StreamingDescendantCleanupCompleted,
            message = "Completed cleanup of a descendant for streaming rebuild.",
            details = recoveryDetails(updated)
          )
          updated
        } catch {
          case error: Throwable =>
            StreamingInsightEvents.action(
              spark,
              insight.operationId,
              targetName,
              targetKind = descendant.kind,
              targetIdentity = descendant.identity,
              causedBy = causedBy,
              stage = "dependent_cleanup",
              code = org.openivm.spark.insights.OpenIvmInsightsContract.Code.StreamingDescendantCleanupFailed,
              message = "Descendant cleanup failed during streaming rebuild.",
              status = "failed",
              level = org.openivm.spark.insights.OpenIvmInsightsContract.Level.Error,
              details = Seq("error_class" -> error.getClass.getName) ++ recoveryDetails(intent)
            )
            throw error
        }
      }
    }

  private final case class CascadeDropResult(
      archiveLocation: Option[String],
      runtime: Option[StreamingTableStatus]
  )

  private def dropCascadeTarget(
      spark: SparkSession,
      descendant: StreamingTableCascadeTarget,
      context: StreamingArchiveContext
  ): CascadeDropResult =
    descendant.kind match {
      case "streaming" =>
        val target = StreamingTableTarget(
          name = descendant.name,
          identity = descendant.identity,
          sqlIdentifier = StreamingTableMetadata.quoteMultipart(descendant.name),
          dataPath = descendant.dataPath,
          deltaTableId = descendant.deltaTableId,
          tableId = descendant.tableId
        )
        val registryKey = StreamingTableRegistry.targetKey(target)
        val result = if (StreamingTableMetadata.catalogTableExists(spark, descendant.name)) {
          val current =
            StreamingTableMetadata.resolveDeltaTarget(spark, descendant.name, requireTableIdMarker = true)
          if (
            current.identity != descendant.identity ||
            current.dataPath != descendant.dataPath ||
            current.deltaTableId != descendant.deltaTableId ||
            current.tableId != descendant.tableId
          )
            StreamingTableErrors.invalid(
              s"Downstream target ${target.sqlIdentifier} changed during cascade cleanup"
            )
          val manifest = StreamingTableMetadata.readManifest(spark, current)
          StreamingTableMetadata.verifyOwned(spark, current, manifest)
          if (manifest.sourcePaths.distinct.sorted != descendant.sourcePaths.distinct.sorted)
            StreamingTableErrors.invalid(
              s"Downstream target ${target.sqlIdentifier} source metadata changed during cascade cleanup"
            )
          val before = statusFor(spark, current, manifest, forcedStatus = None)
          stopNative(spark, target, registryKey)
          val archived = StreamingTableMetadata.dropOwnedCatalogAndData(
            spark,
            current,
            manifest.sourcePaths,
            context
          )
          CascadeDropResult(
            archiveLocation = archived,
            runtime = Some(before.copy(status = "stopped", isActive = false))
          )
        } else {
          stopNative(spark, target, registryKey)
          CascadeDropResult(
            archiveLocation = StreamingTableMetadata.deleteCascadeOwnedPath(spark, descendant, context),
            runtime = None
          )
        }
        StreamingTableRegistry.forget(spark, registryKey)
        StreamingDependencyCatalog.remove(spark, descendant.identity)
        result

      case "materialized" =>
        val name = tableIdentifier(descendant.name)
        val meta = MvCatalog
          .lookup(spark, name)
          .getOrElse(
            StreamingTableErrors.invalid(
              s"Downstream materialized view '${materializedName(name)}' is missing during cascade cleanup"
            )
          )
        val current = materializedNode(spark, meta).target
        if (current.copy(causedBy = descendant.causedBy) != descendant)
          StreamingTableErrors.invalid(
            s"Downstream materialized view '${materializedName(name)}' changed during cascade cleanup"
          )
        val archived = StreamingTableMetadata.archiveMaterializedTarget(spark, descendant, context)
        MaterializedViewLifecycle.dropOne(spark, name, meta, context.operationId)
        CascadeDropResult(archiveLocation = Some(archived), runtime = None)

      case other =>
        StreamingTableErrors.invalid(s"Unsupported managed cascade target kind '$other'")
    }

  private def publishDependencyTarget(
      spark: SparkSession,
      target: StreamingTableTarget,
      definition: StreamingTableDefinition,
      resolvedSources: Seq[StreamingDependencySource] = Seq.empty
  ): Unit = {
    val sources =
      if (resolvedSources.nonEmpty) resolvedSources else resolveManagedDependencySources(spark, definition)

    StreamingDependencyCatalog.publish(
      spark,
      StreamingDependencyTarget(
        identity = target.identity,
        name = target.name,
        dataPath = target.dataPath,
        deltaTableId = target.deltaTableId,
        tableId = target.tableId,
        definitionHash = definition.fingerprint,
        formatVersion = definition.formatVersion,
        sources = sources
      )
    )
  }

  private def resolveManagedDependencySources(
      spark: SparkSession,
      definition: StreamingTableDefinition
  ): Seq[StreamingDependencySource] =
    definition.sources
      .flatMap { source =>
        (source.deltaPath, source.deltaTableId) match {
          case (Some(path), Some(deltaTableId)) =>
            val snapshot   = DeltaLog.forTable(spark, new Path(path)).update()
            val properties = snapshot.metadata.configuration
            if (
              !properties.get(StreamingTableMetadata.OwnerMarkerKey).contains(StreamingTableMetadata.OwnerMarkerValue)
            )
              resolveMaterializedDependencySource(spark, path, deltaTableId)
            else {
              val parentIdentity = properties
                .get(StreamingTableMetadata.TargetIdentityMarkerKey)
                .getOrElse(
                  StreamingTableErrors.invalid(s"Owned streaming source '$path' is missing its target identity")
                )
              val parentTableId = properties
                .get(StreamingTableMetadata.TableIdMarkerKey)
                .getOrElse(
                  StreamingTableErrors.invalid(s"Owned streaming source '$path' is missing its table identity")
                )
              val parentPath = properties
                .get(StreamingTableMetadata.TargetPathMarkerKey)
                .getOrElse(
                  StreamingTableErrors.invalid(s"Owned streaming source '$path' is missing its target path")
                )
              val recordedDeltaId = properties
                .get(StreamingTableMetadata.DeltaIdMarkerKey)
                .getOrElse(
                  StreamingTableErrors.invalid(s"Owned streaming source '$path' is missing its Delta identity")
                )
              if (
                StreamingTableMetadata.normalizePath(spark, parentPath) !=
                  StreamingTableMetadata.normalizePath(spark, path) ||
                  snapshot.metadata.id != deltaTableId ||
                  recordedDeltaId != deltaTableId
              )
                StreamingTableErrors.invalid(
                  s"Owned streaming source '$path' changed generation while dependency metadata was collected"
                )
              val parent = StreamingDependencyCatalog
                .lookup(spark, parentIdentity)
                .getOrElse(
                  StreamingTableErrors.invalid(
                    s"Owned streaming source '$path' is missing durable dependency metadata"
                  )
                )
              if (
                parent.dataPath != StreamingTableMetadata.normalizePath(spark, path) ||
                parent.deltaTableId != deltaTableId ||
                parent.tableId != parentTableId
              )
                StreamingTableErrors.invalid(
                  s"Owned streaming source '$path' does not match its durable dependency metadata"
                )
              Some(
                StreamingDependencySource(
                  parentIdentity = parentIdentity,
                  parentPath = parent.dataPath,
                  parentDeltaTableId = parent.deltaTableId,
                  parentTableId = parent.tableId,
                  formatVersion = parent.formatVersion
                )
              )
            }
          case _ => None
        }
      }
      .groupBy(_.parentIdentity)
      .map(_._2.head)
      .toSeq
      .sortBy(_.parentIdentity)

  private def resolveMaterializedDependencySource(
      spark: SparkSession,
      path: String,
      deltaTableId: String
  ): Option[StreamingDependencySource] = {
    val normalized = StreamingTableMetadata.normalizePath(spark, path)
    val matches = MvCatalog
      .list(spark)
      .filter(meta => StreamingTableMetadata.normalizePath(spark, meta.location) == normalized)
    matches match {
      case Seq() => None
      case Seq(meta) =>
        Some(
          StreamingDependencySource(
            parentIdentity = StreamingDependencyCatalog.materializedIdentity(materializedName(meta.name)),
            parentPath = normalized,
            parentDeltaTableId = deltaTableId,
            parentTableId = MvCatalog.mvIdentity(meta),
            formatVersion = 1
          )
        )
      case many =>
        StreamingTableErrors.invalid(
          s"Delta source '$path' matches multiple managed materialized views: " +
            many.map(meta => materializedName(meta.name)).sorted.mkString(", ")
        )
    }
  }

  private def tableIdentifier(parts: Seq[String]): TableIdentifier =
    parts match {
      case Seq(table)                    => TableIdentifier(table)
      case Seq(database, table)          => TableIdentifier(table, Some(database))
      case Seq(catalog, database, table) => TableIdentifier(table, Some(database), Some(catalog))
      case _ =>
        StreamingTableErrors.invalid(
          s"Unsupported managed table name '${StreamingTableMetadata.quoteMultipart(parts)}'"
        )
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
      .queryName(target.queryName(runtime.displayName))
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
    StreamingTableMetadata.validateDestinationLayout(
      frame.schema,
      spec.partitionColumns,
      spec.clusterColumns,
      spec.tableProperties,
      spark
    )
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

  private def withInsightOperation(
      spark: SparkSession,
      targetRelation: String,
      command: String
  )(body: InsightOperation => StreamingTableStatus): StreamingTableStatus = {
    val insight   = new InsightOperation(UUID.randomUUID().toString, targetRelation, command)
    val startedAt = System.nanoTime()
    StreamingInsightEvents.operationStarted(spark, insight.operationId, targetRelation, command)
    try {
      val status = body(insight)
      StreamingInsightEvents.operationFinished(
        spark,
        insight.operationId,
        targetRelation,
        command,
        outcome = status.status,
        durationMs = (System.nanoTime() - startedAt) / 1000000L,
        failed = false,
        branchCode = insight.branchCode,
        runtime = Some(insight.terminalStatus(status))
      )
      status
    } catch {
      case error: Throwable =>
        StreamingInsightEvents.operationFinished(
          spark,
          insight.operationId,
          targetRelation,
          command,
          outcome = s"${command}_failed",
          durationMs = (System.nanoTime() - startedAt) / 1000000L,
          failed = true,
          branchCode = insight.branchCode,
          runtime = insight.observedStatus,
          error = Some(error)
        )
        throw error
    }
  }

  private def recoveryDetails(intent: StreamingTableResetIntent): Seq[(String, Any)] = {
    val completed = intent.completedDescendantIdentities.toSet
    Seq(
      "journal_operation_id"            -> intent.operationId,
      "completed_descendant_identities" -> intent.completedDescendantIdentities,
      "remaining_descendant_identities" -> intent.descendants.map(_.identity).filterNot(completed),
      "upstream_dropped"                -> intent.upstreamDropped
    )
  }

  private def emitAlreadyCompletedDescendants(
      spark: SparkSession,
      insight: InsightOperation,
      intent: StreamingTableResetIntent
  ): Unit =
    intent.descendants
      .filter(target => intent.completedDescendantIdentities.contains(target.identity))
      .foreach { descendant =>
        StreamingInsightEvents.action(
          spark,
          insight.operationId,
          descendant.name.mkString("."),
          targetKind = descendant.kind,
          targetIdentity = descendant.identity,
          causedBy = descendant.causedBy.orElse(Some(intent.targetIdentity)),
          stage = "dependent_cleanup",
          code = org.openivm.spark.insights.OpenIvmInsightsContract.Code.StreamingDescendantCleanupCompleted,
          message = "Reused completed descendant cleanup from the rebuild journal.",
          status = "already_completed",
          details = recoveryDetails(intent)
        )
      }

  private def emitArchiveAction(
      spark: SparkSession,
      insight: InsightOperation,
      targetRelation: String,
      targetKind: String,
      targetIdentity: String,
      causedBy: Option[String],
      archiveLocation: Option[String]
  ): Unit =
    StreamingInsightEvents.action(
      spark,
      insight.operationId,
      targetRelation,
      targetKind,
      targetIdentity,
      causedBy,
      stage = "archive",
      code = org.openivm.spark.insights.OpenIvmInsightsContract.Code.StreamingCheckpointArchived,
      message = "Processed the managed target archive.",
      status = if (archiveLocation.nonEmpty) "archived" else "not_present",
      details = Seq("archive_location" -> archiveLocation)
    )

  private def emitCascadeDropActions(
      spark: SparkSession,
      operationId: String,
      descendant: StreamingTableCascadeTarget,
      causedBy: Option[String],
      result: CascadeDropResult,
      operation: String
  ): Unit = {
    val targetRelation = descendant.name.mkString(".")
    result.runtime.foreach { status =>
      StreamingInsightEvents.action(
        spark,
        operationId,
        targetRelation,
        targetKind = descendant.kind,
        targetIdentity = descendant.identity,
        causedBy = causedBy,
        stage = "query",
        code = org.openivm.spark.insights.OpenIvmInsightsContract.Code.StreamingQueryStopped,
        message = "Stopped the descendant native streaming query.",
        status = "stopped_for_cascade",
        runtime = Some(status),
        operation = operation
      )
    }
    StreamingInsightEvents.action(
      spark,
      operationId,
      targetRelation,
      targetKind = descendant.kind,
      targetIdentity = descendant.identity,
      causedBy = causedBy,
      stage = "dependent_archive",
      code = org.openivm.spark.insights.OpenIvmInsightsContract.Code.StreamingDescendantArchiveCompleted,
      message = "Processed the descendant managed-target archive.",
      status = if (result.archiveLocation.nonEmpty) "archived" else "not_present",
      operation = operation,
      details = Seq("archive_location" -> result.archiveLocation)
    )
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

  def lifecycleLockKey(identity: String): String = s"identity:$identity"

  def withTargetLock[A](spark: SparkSession, key: String)(body: => A): A = {
    val registry = context(spark)
    val proposed = new Object
    val existing = registry.locks.putIfAbsent(key, proposed)
    val lock     = if (existing == null) proposed else existing
    lock.synchronized(body)
  }

  def withTargetLocks[A](spark: SparkSession, keys: Seq[String])(body: => A): A = {
    val ordered = keys.distinct.sorted
    def acquire(index: Int): A =
      if (index == ordered.size) body
      else withTargetLock(spark, ordered(index))(acquire(index + 1))
    acquire(0)
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
      spark.streams.active.find(query => target.matchesQueryName(query.name)).map { query =>
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
