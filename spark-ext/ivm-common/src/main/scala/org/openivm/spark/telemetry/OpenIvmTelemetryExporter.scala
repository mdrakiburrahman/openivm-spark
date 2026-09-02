package org.openivm.spark.telemetry

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.FeatureGate
import org.slf4j.MDC

import java.net.URI
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.Arrays
import java.util.UUID
import scala.collection.JavaConverters._

final class OpenIvmTelemetryExportException(message: String) extends IllegalStateException(message)

private[telemetry] trait CompletedSpanPublisher {
  def publish(identity: OpenIvmTelemetryContract.ExecutionIdentity, payload: String): Path

  def reuseCompleted(identity: OpenIvmTelemetryContract.ExecutionIdentity, replayPayload: String): Boolean = false
}

private[telemetry] final case class ConfiguredTelemetryExport(
    identity: OpenIvmTelemetryContract.ExecutionIdentity,
    publisher: CompletedSpanPublisher
)

/** Atomic completed-object publisher for Hadoop-compatible filesystems,
  * including OneLake's ABFS implementation.
  */
private[telemetry] final class OpenIvmTelemetryExporter(
    rootUri: String,
    hadoopConf: Configuration,
    fileSystemOverride: Option[Path => FileSystem],
    partialId: () => String
) extends CompletedSpanPublisher {

  def this(rootUri: String, hadoopConf: Configuration) =
    this(rootUri, hadoopConf, None, () => UUID.randomUUID().toString)

  private val Json            = new ObjectMapper()
  private val MaxPayloadBytes = 4 * 1024 * 1024

  private def rootPath: Path = new Path(rootUri)

  private[telemetry] def completedVersionPath: Path =
    new Path(new Path(rootPath, OpenIvmTelemetryContract.CompletedDirectory), OpenIvmTelemetryContract.VersionDirectory)

  private[telemetry] def temporaryVersionPath: Path =
    new Path(new Path(rootPath, OpenIvmTelemetryContract.TemporaryDirectory), OpenIvmTelemetryContract.VersionDirectory)

  private[telemetry] def completedPath(identity: OpenIvmTelemetryContract.ExecutionIdentity): Path =
    new Path(completedVersionPath, OpenIvmTelemetryContract.completedFileName(identity))

  private def fileSystem(path: Path): FileSystem =
    fileSystemOverride.fold(path.getFileSystem(hadoopConf))(factory => factory(path))

  override def reuseCompleted(
      identity: OpenIvmTelemetryContract.ExecutionIdentity,
      replayPayload: String
  ): Boolean = {
    val validatedIdentity = identity.validated
    try {
      val finalPath = completedPath(validatedIdentity)
      val fs        = fileSystem(finalPath)
      if (!fs.exists(finalPath)) false
      else {
        validateReusableSuccess(fs, finalPath, validatedIdentity, replayPayload)
        true
      }
    } catch {
      case e: OpenIvmTelemetryExportException => throw e
      case t: Throwable =>
        throw new OpenIvmTelemetryExportException(
          s"OpenIVM telemetry completed-object reuse at ${OpenIvmTelemetryExporter.redactUri(rootUri)} failed " +
            s"(${t.getClass.getSimpleName})"
        )
    }
  }

  override def publish(identity: OpenIvmTelemetryContract.ExecutionIdentity, payload: String): Path = {
    val validatedIdentity = identity.validated
    val bytes             = payload.getBytes(StandardCharsets.UTF_8)
    if (bytes.length > MaxPayloadBytes)
      throw new OpenIvmTelemetryExportException(
        s"OpenIVM telemetry payload exceeds the $MaxPayloadBytes byte completed-object limit"
      )

    var fs: FileSystem  = null
    var partial: Path   = null
    var cleanupRequired = false
    try {
      val finalPath = completedPath(validatedIdentity)
      fs = fileSystem(finalPath)
      ensureDirectory(fs, completedVersionPath)
      ensureDirectory(fs, temporaryVersionPath)

      if (fs.exists(finalPath)) {
        requireMatchingContent(fs, finalPath, bytes)
        return finalPath
      }

      partial = new Path(
        temporaryVersionPath,
        s"${finalPath.getName}.${partialId()}${OpenIvmTelemetryContract.PartialFileSuffix}"
      )
      val output = fs.create(partial, /* overwrite = */ false)
      cleanupRequired = true
      try {
        output.write(bytes)
        syncIfSupported(output.hflush())
        syncIfSupported(output.hsync())
      } finally output.close()

      if (fs.exists(finalPath)) {
        requireMatchingContent(fs, finalPath, bytes)
        return finalPath
      }

      if (fs.rename(partial, finalPath)) {
        cleanupRequired = fs.exists(partial)
        finalPath
      } else if (fs.exists(finalPath)) {
        requireMatchingContent(fs, finalPath, bytes)
        finalPath
      } else {
        throw new OpenIvmTelemetryExportException(
          s"OpenIVM telemetry failed to atomically publish a completed object to " +
            s"${OpenIvmTelemetryExporter.redactUri(rootUri)}"
        )
      }
    } catch {
      case e: OpenIvmTelemetryExportException => throw e
      case t: Throwable =>
        throw new OpenIvmTelemetryExportException(
          s"OpenIVM telemetry export to ${OpenIvmTelemetryExporter.redactUri(rootUri)} failed " +
            s"(${t.getClass.getSimpleName})"
        )
    } finally {
      if (cleanupRequired && fs != null && partial != null) {
        try {
          if (fs.exists(partial) && !fs.delete(partial, /* recursive = */ false))
            throw new OpenIvmTelemetryExportException(
              s"OpenIVM telemetry could not clean a non-ingestible partial object at " +
                s"${OpenIvmTelemetryExporter.redactUri(rootUri)}"
            )
        } catch {
          case e: OpenIvmTelemetryExportException => throw e
          case t: Throwable =>
            throw new OpenIvmTelemetryExportException(
              s"OpenIVM telemetry partial cleanup at ${OpenIvmTelemetryExporter.redactUri(rootUri)} failed " +
                s"(${t.getClass.getSimpleName})"
            )
        }
      }
    }
  }

  private def ensureDirectory(fs: FileSystem, path: Path): Unit =
    if (!fs.exists(path) && !fs.mkdirs(path))
      throw new OpenIvmTelemetryExportException(
        s"OpenIVM telemetry could not create a publication directory at " +
          s"${OpenIvmTelemetryExporter.redactUri(rootUri)}"
      )

  private def requireMatchingContent(fs: FileSystem, path: Path, expected: Array[Byte]): Unit = {
    val status = fs.getFileStatus(path)
    if (status.getLen != expected.length)
      throw differentContent(path)
    val existing = new Array[Byte](expected.length)
    val input    = fs.open(path)
    try input.readFully(existing)
    finally input.close()
    if (!Arrays.equals(existing, expected)) throw differentContent(path)
  }

  private def validateReusableSuccess(
      fs: FileSystem,
      path: Path,
      identity: OpenIvmTelemetryContract.ExecutionIdentity,
      replayPayload: String
  ): Unit = {
    val existing =
      try Json.readTree(readExisting(fs, path))
      catch {
        case _: Throwable => throw invalidReusable(path)
      }
    val replay =
      try Json.readTree(replayPayload)
      catch {
        case _: Throwable => throw invalidReusable(path)
      }
    validateSuccessPayload(existing, identity, path)
    validateSuccessPayload(replay, identity, path)
    Seq(
      "compile_refresh_type",
      "effective_refresh_type",
      "refresh_reason",
      "time_travel_pin_status",
      "time_travel_pin_reason"
    ).foreach { field =>
      if (existing.path(field).asText() != replay.path(field).asText()) throw invalidReusable(path)
    }
    if (sourceEndVersions(existing, path) != sourceEndVersions(replay, path)) throw invalidReusable(path)
    if (
      identity.operation == "create" &&
      (existing.path("source_versions") != replay.path("source_versions") ||
        existing.path("pending_delta_count").asLong() != replay.path("pending_delta_count").asLong())
    )
      throw invalidReusable(path)
  }

  private def validateSuccessPayload(
      payload: JsonNode,
      identity: OpenIvmTelemetryContract.ExecutionIdentity,
      path: Path
  ): Unit = {
    OpenIvmTelemetryContract.W6RequiredSuccessFields.foreach { field =>
      if (!payload.hasNonNull(field)) throw invalidReusable(path)
    }
    requireText(payload, "schema_id", OpenIvmTelemetryContract.SchemaId, path)
    if (payload.path("schema_version").asInt(-1) != OpenIvmTelemetryContract.SchemaVersion)
      throw invalidReusable(path)
    requireText(payload, "campaign_id", identity.campaignId, path)
    requireText(payload, "request_id", identity.requestId, path)
    requireText(payload, "correlation_id", identity.correlationId, path)
    requireText(payload, "dbt_node_id", identity.dbtNodeId, path)
    requireText(payload, "materialized_view", identity.materializedView, path)
    requireText(payload, "operation", identity.operation, path)
    requireText(payload, "phase", identity.phase, path)
    if (!OpenIvmTelemetryContract.ReusableSuccessOutcomes.contains(payload.path("outcome").asText()))
      throw invalidReusable(path)
    if (
      payload.path("compile_refresh_type").asText().isEmpty ||
      payload.path("effective_refresh_type").asText().isEmpty ||
      payload.path("refresh_reason").asText().isEmpty ||
      payload.path("time_travel_pin_status").asText().isEmpty ||
      payload.path("time_travel_pin_reason").asText().isEmpty ||
      !payload.path("source_versions").isArray ||
      payload.path("duration_ms").asLong(-1L) < 0L ||
      payload.path("pending_delta_count").asLong(-1L) < 0L
    )
      throw invalidReusable(path)
    val started   = parseInstant(payload, "engine_started_at", path)
    val completed = parseInstant(payload, "engine_completed_at", path)
    if (completed.isBefore(started)) throw invalidReusable(path)
  }

  private def sourceEndVersions(payload: JsonNode, path: Path): Map[String, Long] = {
    val versions = payload
      .path("source_versions")
      .elements()
      .asScala
      .map { version =>
        val relation = version.path("relation").asText()
        val start    = version.path("start_version").asLong(-1L)
        val end      = version.path("end_version").asLong(-1L)
        if (relation.isEmpty || start < 0L || end < start) throw invalidReusable(path)
        relation -> end
      }
      .toSeq
    if (versions.map(_._1).distinct.size != versions.size) throw invalidReusable(path)
    versions.toMap
  }

  private def readExisting(fs: FileSystem, path: Path): Array[Byte] = {
    val length = fs.getFileStatus(path).getLen
    if (length < 0L || length > MaxPayloadBytes) throw invalidReusable(path)
    val bytes = new Array[Byte](length.toInt)
    val input = fs.open(path)
    try input.readFully(bytes)
    finally input.close()
    bytes
  }

  private def requireText(payload: JsonNode, field: String, expected: String, path: Path): Unit =
    if (payload.path(field).asText() != expected) throw invalidReusable(path)

  private def parseInstant(payload: JsonNode, field: String, path: Path): Instant =
    try Instant.parse(payload.path(field).asText())
    catch { case _: Throwable => throw invalidReusable(path) }

  private def invalidReusable(path: Path): OpenIvmTelemetryExportException =
    new OpenIvmTelemetryExportException(
      s"OpenIVM telemetry completed object ${path.getName} is not a complete matching accepted success"
    )

  private def differentContent(path: Path): OpenIvmTelemetryExportException =
    new OpenIvmTelemetryExportException(
      s"OpenIVM telemetry completed object ${path.getName} already exists with different content"
    )

  private def syncIfSupported(sync: => Unit): Unit =
    try sync
    catch { case _: UnsupportedOperationException => () }
}

private[telemetry] object OpenIvmTelemetryExporter {

  def configuredForSpark(
      spark: SparkSession,
      materializedView: String,
      operation: String,
      requestId: Option[String],
      dbtNodeId: Option[String]
  ): Option[ConfiguredTelemetryExport] =
    resolveConfigured(
      telemetryUri = FeatureGate.telemetryUri(spark),
      materializedView = materializedView,
      operation = operation,
      requestId = requestId,
      dbtNodeId = dbtNodeId,
      localPropertyLookup = key => Option(spark.sparkContext.getLocalProperty(key)),
      mdcLookup = key => Option(MDC.get(key)),
      confLookup = key => spark.conf.getOption(key).orElse(spark.sparkContext.getConf.getOption(key)),
      publisherFactory = uri => new OpenIvmTelemetryExporter(uri, spark.sessionState.newHadoopConf())
    )

  private[telemetry] def resolveConfigured(
      telemetryUri: Option[String],
      materializedView: String,
      operation: String,
      requestId: Option[String],
      dbtNodeId: Option[String],
      localPropertyLookup: String => Option[String],
      mdcLookup: String => Option[String],
      confLookup: String => Option[String],
      publisherFactory: String => CompletedSpanPublisher
  ): Option[ConfiguredTelemetryExport] =
    telemetryUri.map(_.trim).filter(_.nonEmpty).map { uri =>
      val campaignId = firstNonBlank(
        valuesFor(Seq(OpenIvmTelemetryContract.CampaignIdProperty, "campaign_id"), localPropertyLookup),
        valuesFor(Seq(OpenIvmTelemetryContract.CampaignIdProperty, "campaign_id"), mdcLookup),
        valuesFor(Seq(OpenIvmTelemetryContract.CampaignIdConfKey), confLookup)
      ).getOrElse {
        throw new OpenIvmTelemetryExportException("OpenIVM telemetry campaign identity is required")
      }
      val correlationId = firstNonBlank(
        valuesFor(Seq(OpenIvmTelemetryContract.CorrelationProperty, "correlation_id"), localPropertyLookup),
        valuesFor(Seq(OpenIvmTelemetryContract.CorrelationProperty, "correlation_id"), mdcLookup),
        valuesFor(Seq(OpenIvmTelemetryContract.CorrelationIdConfKey), confLookup),
        requestId.iterator
      ).getOrElse {
        throw new OpenIvmTelemetryExportException("OpenIVM telemetry correlation identity is required")
      }
      val phase = firstNonBlank(
        valuesFor(Seq(OpenIvmTelemetryContract.PhaseProperty, "phase"), localPropertyLookup),
        valuesFor(Seq(OpenIvmTelemetryContract.PhaseProperty, "phase"), mdcLookup),
        valuesFor(Seq(OpenIvmTelemetryContract.PhaseConfKey), confLookup)
      ).getOrElse {
        throw new OpenIvmTelemetryExportException("OpenIVM telemetry phase identity is required")
      }
      val identity = OpenIvmTelemetryContract
        .ExecutionIdentity(
          campaignId = campaignId,
          requestId = requestId.getOrElse(
            throw new OpenIvmTelemetryExportException("OpenIVM telemetry request identity is required")
          ),
          correlationId = correlationId,
          dbtNodeId = dbtNodeId.getOrElse(
            throw new OpenIvmTelemetryExportException("OpenIVM telemetry dbt node identity is required")
          ),
          materializedView = materializedView,
          operation = operation,
          phase = phase
        )
        .validated
      ConfiguredTelemetryExport(identity, publisherFactory(uri))
    }

  private def valuesFor(keys: Seq[String], lookup: String => Option[String]): Iterator[String] =
    keys.iterator.flatMap(key => lookup(key).iterator.map(_.trim).filter(_.nonEmpty))

  private def firstNonBlank(sources: Iterator[String]*): Option[String] =
    sources.iterator.flatten.find(_.nonEmpty)

  private[telemetry] def redactUri(raw: String): String =
    try {
      val uri    = new URI(Option(raw).getOrElse(""))
      val scheme = Option(uri.getScheme).getOrElse("unknown")
      val authority = Option(uri.getHost).orElse(
        Option(uri.getRawAuthority).map(_.split("@").lastOption.getOrElse(""))
      )
      authority.filter(_.nonEmpty) match {
        case Some(host) => s"$scheme://$host/..."
        case None       => s"$scheme:/..."
      }
    } catch {
      case _: Throwable => "<redacted-uri>"
    }
}
