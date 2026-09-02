package org.openivm.spark.telemetry

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.FeatureGate
import org.slf4j.MDC

import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.Arrays
import java.util.UUID

final class OpenIvmTelemetryExportException(message: String) extends IllegalStateException(message)

private[telemetry] trait CompletedSpanPublisher {
  def publish(identity: OpenIvmTelemetryContract.ExecutionIdentity, payload: String): Path
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
