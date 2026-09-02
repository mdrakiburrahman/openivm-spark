package org.openivm.spark.telemetry

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, RawLocalFileSystem}
import org.openivm.spark.common.FeatureGate
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.{Callable, Executors, TimeUnit}

class OpenIvmTelemetryExporterSpec extends AnyFunSpec with Matchers with BeforeAndAfterEach {

  private val roots = scala.collection.mutable.ArrayBuffer.empty[File]

  override protected def afterEach(): Unit = {
    roots.foreach(deleteDir)
    roots.clear()
    super.afterEach()
  }

  private def newRoot(): File = {
    val root = new File(s"target/openivm-telemetry-${UUID.randomUUID().toString}")
    root.mkdirs() shouldBe true
    roots += root
    root
  }

  private def deleteDir(file: File): Unit = {
    if (file.isDirectory) Option(file.listFiles()).foreach(_.foreach(deleteDir))
    file.delete()
    ()
  }

  private def identity(suffix: String): OpenIvmTelemetryContract.ExecutionIdentity =
    OpenIvmTelemetryContract.ExecutionIdentity(
      campaignId = "central-frozen-fast",
      requestId = s"request-$suffix",
      correlationId = s"correlation-$suffix",
      dbtNodeId = s"model.benchmark.$suffix",
      materializedView = s"benchmark.$suffix",
      operation = "refresh",
      phase = "refresh"
    )

  private final class RenameFailingLocalFileSystem extends RawLocalFileSystem {
    override def rename(src: Path, dst: Path): Boolean = false
  }

  describe("OpenIvmTelemetryExporter") {
    it("does nothing when the telemetry URI is unset") {
      val configured = OpenIvmTelemetryExporter.resolveConfigured(
        telemetryUri = None,
        materializedView = "benchmark.disabled",
        operation = "refresh",
        requestId = None,
        dbtNodeId = None,
        localPropertyLookup = _ => None,
        mdcLookup = _ => None,
        confLookup = _ => None,
        publisherFactory = _ => fail("disabled telemetry must not construct a publisher")
      )

      configured shouldBe None
      FeatureGate.TelemetryUriKey shouldBe "spark.openivm.telemetry.uri"
    }

    it("resolves the complete nonsecret execution identity and fails closed when it is incomplete") {
      val root = newRoot()
      val values = Map(
        OpenIvmTelemetryContract.CampaignIdProperty  -> "central-frozen-fast",
        OpenIvmTelemetryContract.CorrelationProperty -> "correlation-context",
        OpenIvmTelemetryContract.PhaseProperty       -> "refresh"
      )
      val configured = OpenIvmTelemetryExporter.resolveConfigured(
        telemetryUri = Some(root.toURI.toString),
        materializedView = "benchmark.identity",
        operation = "refresh",
        requestId = Some("request-context"),
        dbtNodeId = Some("model.benchmark.identity"),
        localPropertyLookup = values.get,
        mdcLookup = _ => None,
        confLookup = _ => None,
        publisherFactory = _ => new OpenIvmTelemetryExporter(root.toURI.toString, new Configuration())
      )

      configured.map(_.identity) shouldBe Some(
        OpenIvmTelemetryContract.ExecutionIdentity(
          campaignId = "central-frozen-fast",
          requestId = "request-context",
          correlationId = "correlation-context",
          dbtNodeId = "model.benchmark.identity",
          materializedView = "benchmark.identity",
          operation = "refresh",
          phase = "refresh"
        )
      )

      val error = intercept[OpenIvmTelemetryExportException] {
        OpenIvmTelemetryExporter.resolveConfigured(
          telemetryUri = Some(root.toURI.toString),
          materializedView = "benchmark.identity",
          operation = "refresh",
          requestId = Some("request-context"),
          dbtNodeId = Some("model.benchmark.identity"),
          localPropertyLookup = _ => None,
          mdcLookup = _ => None,
          confLookup = _ => None,
          publisherFactory = _ => fail("incomplete identity must fail before publisher construction")
        )
      }
      error.getMessage should include("campaign identity")
    }

    it("publishes through a unique partial path and exposes only the completed v1 object") {
      val root     = newRoot()
      val exporter = new OpenIvmTelemetryExporter(root.toURI.toString, new Configuration())
      val id       = identity("atomic")
      val finalPath = exporter.publish(
        id,
        """{"schema_id":"openivm.execution-span","schema_version":1,"request_id":"request-atomic"}"""
      )

      OpenIvmTelemetryContract.isCompletedPath(finalPath) shouldBe true
      finalPath.getName shouldBe OpenIvmTelemetryContract.completedFileName(id)
      val fs = finalPath.getFileSystem(new Configuration())
      new String(readAll(fs, finalPath), StandardCharsets.UTF_8) should include("request-atomic")

      val temporaryRoot = exporter.temporaryVersionPath
      if (fs.exists(temporaryRoot)) fs.listStatus(temporaryRoot) shouldBe empty
      OpenIvmTelemetryContract.isCompletedPath(
        new Path(temporaryRoot, s"${finalPath.getName}.leftover.partial")
      ) shouldBe false
    }

    it("cleans failed partials and never exposes them as completed spans") {
      val root = newRoot()
      val conf = new Configuration()
      val fs   = new RenameFailingLocalFileSystem()
      fs.initialize(new URI("file:///"), conf)
      val exporter = new OpenIvmTelemetryExporter(
        root.toURI.toString,
        conf,
        Some(_ => fs),
        () => "injected-partial"
      )

      val error = intercept[OpenIvmTelemetryExportException] {
        exporter.publish(identity("rename-failure"), """{"schema_version":1}""")
      }
      error.getMessage should include("failed to atomically publish")
      fs.exists(exporter.completedPath(identity("rename-failure"))) shouldBe false
      if (fs.exists(exporter.temporaryVersionPath)) {
        fs.listStatus(exporter.temporaryVersionPath) shouldBe empty
      }
    }

    it("treats identical duplicates as idempotent and rejects mismatched content") {
      val root     = newRoot()
      val exporter = new OpenIvmTelemetryExporter(root.toURI.toString, new Configuration())
      val id       = identity("duplicate")
      val first    = exporter.publish(id, """{"schema_version":1,"duration_ms":7}""")
      val second   = exporter.publish(id, """{"schema_version":1,"duration_ms":7}""")

      second shouldBe first
      val mismatch = intercept[OpenIvmTelemetryExportException] {
        exporter.publish(id, """{"schema_version":1,"duration_ms":8}""")
      }
      mismatch.getMessage should include("different content")
      val fs = first.getFileSystem(new Configuration())
      new String(readAll(fs, first), StandardCharsets.UTF_8) should include(""""duration_ms":7""")
    }

    it("redacts URI user-info, query, fragment, and path details from failures") {
      val rawUri   = "missingfs://user:secret@example.com/private/campaign/path?sig=secret-token#fragment"
      val redacted = OpenIvmTelemetryExporter.redactUri(rawUri)

      redacted should include("missingfs")
      redacted should include("example.com")
      redacted should not include "user"
      redacted should not include "secret"
      redacted should not include "private"
      redacted should not include "sig"

      val exporter = new OpenIvmTelemetryExporter(rawUri, new Configuration())
      val error = intercept[OpenIvmTelemetryExportException] {
        exporter.publish(identity("redaction"), """{"schema_version":1}""")
      }
      error.getMessage should include(redacted)
      error.getMessage should not include "secret-token"
      error.getMessage should not include "/private/campaign/path"
    }

    it("publishes concurrent request identities to unique completed objects") {
      val root     = newRoot()
      val exporter = new OpenIvmTelemetryExporter(root.toURI.toString, new Configuration())
      val pool     = Executors.newFixedThreadPool(8)
      try {
        val futures = (1 to 24).map { index =>
          pool.submit(new Callable[Path] {
            override def call(): Path =
              exporter.publish(identity(index.toString), s"""{"schema_version":1,"sequence":$index}""")
          })
        }
        val paths = futures.map(_.get(30L, TimeUnit.SECONDS))
        paths.distinct should have size 24
        paths.foreach(OpenIvmTelemetryContract.isCompletedPath(_) shouldBe true)

        val fs = paths.head.getFileSystem(new Configuration())
        fs.listStatus(exporter.completedVersionPath).count(_.isFile) shouldBe 24
        if (fs.exists(exporter.temporaryVersionPath)) {
          fs.listStatus(exporter.temporaryVersionPath) shouldBe empty
        }
      } finally {
        pool.shutdownNow()
      }
    }
  }

  private def readAll(fs: FileSystem, path: Path): Array[Byte] = {
    val status = fs.getFileStatus(path)
    status.getLen should be <= Int.MaxValue.toLong
    val bytes = new Array[Byte](status.getLen.toInt)
    val in    = fs.open(path)
    try in.readFully(bytes)
    finally in.close()
    bytes
  }
}
