package org.openivm.spark.telemetry

import org.apache.hadoop.fs.Path
import org.openivm.spark.common.FeatureGate

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

/** Public ingestion contract for campaign-scoped OpenIVM execution spans. */
object OpenIvmTelemetryContract {

  val SchemaId: String       = "openivm.execution-span"
  val SchemaVersion: Int     = 1
  val SchemaResource: String = "/openivm-telemetry-span-v1.schema.json"

  val TelemetryUriKey: String      = FeatureGate.TelemetryUriKey
  val CampaignIdConfKey: String    = FeatureGate.TelemetryCampaignIdKey
  val CorrelationIdConfKey: String = FeatureGate.TelemetryCorrelationIdKey
  val PhaseConfKey: String         = FeatureGate.TelemetryPhaseKey
  val CampaignIdProperty: String   = "openivm.campaign_id"
  val CorrelationProperty: String  = "openivm.correlation_id"
  val PhaseProperty: String        = "openivm.phase"
  val RequestIdProperty: String    = "openivm.request_id"
  val DbtNodeIdProperty: String    = "openivm.node_id"
  val CompletedDirectory: String   = "completed"
  val TemporaryDirectory: String   = "_temporary"
  val VersionDirectory: String     = s"v$SchemaVersion"
  val CompletedFileSuffix: String  = ".json"
  val PartialFileSuffix: String    = ".partial"

  final case class ExecutionIdentity(
      campaignId: String,
      requestId: String,
      correlationId: String,
      dbtNodeId: String,
      materializedView: String,
      operation: String,
      phase: String
  ) {
    private[telemetry] def validated: ExecutionIdentity =
      copy(
        campaignId = safeIdentity("campaign identity", campaignId),
        requestId = safeIdentity("request identity", requestId),
        correlationId = safeIdentity("correlation identity", correlationId),
        dbtNodeId = safeIdentity("dbt node identity", dbtNodeId),
        materializedView = safeIdentity("materialized-view identity", materializedView),
        operation = safeOperation(operation),
        phase = safeIdentity("phase identity", phase)
      )
  }

  final case class SourceVersion(relation: String, startVersion: Long, endVersion: Long) {
    private[telemetry] def validated: SourceVersion = {
      val safeRelation = safeIdentity("source relation identity", relation)
      if (startVersion < 0L || endVersion < startVersion)
        throw new OpenIvmTelemetryExportException(
          "OpenIVM telemetry source versions must be non-negative and ordered"
        )
      copy(relation = safeRelation)
    }
  }

  def completedFileName(identity: ExecutionIdentity): String = {
    val value = identity.validated
    val canonical = Seq(
      SchemaVersion.toString,
      value.campaignId,
      value.requestId,
      value.correlationId,
      value.dbtNodeId,
      value.materializedView,
      value.operation,
      value.phase
    ).mkString("\u001f")
    val digest = MessageDigest
      .getInstance("SHA-256")
      .digest(canonical.getBytes(StandardCharsets.UTF_8))
      .map("%02x".format(_))
      .mkString
    s"$digest$CompletedFileSuffix"
  }

  def isCompletedPath(path: Path): Boolean =
    Option(path)
      .flatMap(p => Option(p.getParent).map(parent => p -> parent))
      .flatMap { case (file, version) =>
        Option(version.getParent).map(completed => (file, version, completed))
      }
      .exists { case (file, version, completed) =>
        file.getName.endsWith(CompletedFileSuffix) &&
        version.getName == VersionDirectory &&
        completed.getName == CompletedDirectory
      }

  private[telemetry] def validatedSourceVersions(values: Seq[SourceVersion]): Seq[SourceVersion] =
    Option(values)
      .getOrElse(Seq.empty)
      .map(_.validated)
      .groupBy(_.relation)
      .toSeq
      .sortBy(_._1)
      .map { case (relation, versions) =>
        val starts = versions.map(_.startVersion)
        val ends   = versions.map(_.endVersion)
        SourceVersion(relation, starts.min, ends.max).validated
      }

  private def safeOperation(value: String): String =
    safeIdentity("operation identity", value).toLowerCase(java.util.Locale.ROOT) match {
      case "create"  => "create"
      case "refresh" => "refresh"
      case _ =>
        throw new OpenIvmTelemetryExportException(
          "OpenIVM telemetry operation identity must be create or refresh"
        )
    }

  private def safeIdentity(label: String, value: String): String = {
    val normalized = Option(value).map(_.trim).getOrElse("")
    val prohibited =
      normalized.exists(ch => Character.isISOControl(ch) || ch == '/' || ch == '\\' || ch == '?' || ch == '#') ||
        normalized.contains("://")
    if (normalized.isEmpty)
      throw new OpenIvmTelemetryExportException(s"OpenIVM telemetry $label is required")
    if (normalized.length > 512 || prohibited)
      throw new OpenIvmTelemetryExportException(
        s"OpenIVM telemetry $label is too long or contains prohibited URI/path characters"
      )
    normalized
  }
}
