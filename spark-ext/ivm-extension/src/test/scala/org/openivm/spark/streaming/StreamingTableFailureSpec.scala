package org.openivm.spark.streaming

import org.apache.hadoop.fs.Path
import org.scalatest.concurrent.Eventually
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class StreamingTableFailureSpec extends AnyFunSpec with StreamingTableTestFixture with Eventually {

  describe("native streaming failure propagation") {
    it("surfaces an asynchronous native query failure through SHOW without a fallback write path") {
      val source = "strt_failure_source"
      val target = "strt_failure_target"
      createSource(source)
      spark.udf.register(
        "strt_fail_on_boom",
        (value: String) => if (value == "boom") throw new IllegalStateException("expected streaming failure") else value
      )
      val query = readStream(source).selectExpr("id", "strt_fail_on_boom(value) AS value", "part")
      val status = createStreaming(
        target,
        query,
        s"SELECT id, strt_fail_on_boom(value), part FROM STREAM $source"
      )
      val native = process(status)

      appendRows(source, "(1, 'boom', 'p')")
      an[Throwable] should be thrownBy {
        native.processAllAvailable()
      }
      eventually(timeout(Span(60, Seconds)), interval(Span(100, Millis))) {
        val shown = StreamingTableManager.show(spark, None).find(_.tableName == status.tableName).get
        shown.status shouldBe "failed"
        shown.lastFailure.get should include("expected streaming failure")
      }
    }

    it("retries a manifest read while an owned target manifest becomes visible") {
      implicit val executionContext: ExecutionContext = ExecutionContext.global
      val source                                      = "strt_manifest_retry_source"
      val target                                      = "strt_manifest_retry_target"
      createSource(source)
      createStreaming(
        target,
        readStream(source),
        s"SELECT id, value, part FROM STREAM $source"
      )
      StreamingTableManager.stop(spark, Seq(target))

      val targetInfo = StreamingTableMetadata.resolveDeltaTarget(spark, Seq(target), requireTableIdMarker = true)
      val path       = StreamingTableMetadata.definitionPath(targetInfo)
      val backup     = new Path(path.getParent, s"${path.getName}.backup")
      val fs         = path.getFileSystem(spark.sessionState.newHadoopConf())
      fs.rename(path, backup) shouldBe true
      spark.conf.set(StreamingTableMetadata.ManifestReadRetryTimeoutMsKey, "2000")
      spark.conf.set(StreamingTableMetadata.ManifestReadRetryIntervalMsKey, "25")

      val restore = Future {
        Thread.sleep(250)
        fs.rename(backup, path)
      }
      try {
        val manifest = StreamingTableMetadata.readManifest(spark, targetInfo)
        manifest.target.tableId shouldBe targetInfo.tableId
        Await.result(restore, 5.seconds) shouldBe true
      } finally {
        spark.conf.unset(StreamingTableMetadata.ManifestReadRetryTimeoutMsKey)
        spark.conf.unset(StreamingTableMetadata.ManifestReadRetryIntervalMsKey)
        if (fs.exists(backup)) fs.rename(backup, path)
      }
    }

    it("reports a persistently missing owned manifest without failing SHOW") {
      val source = "strt_manifest_missing_source"
      val target = "strt_manifest_missing_target"
      createSource(source)
      createStreaming(
        target,
        readStream(source),
        s"SELECT id, value, part FROM STREAM $source"
      )
      StreamingTableManager.stop(spark, Seq(target))

      val targetInfo = StreamingTableMetadata.resolveDeltaTarget(spark, Seq(target), requireTableIdMarker = true)
      val path       = StreamingTableMetadata.definitionPath(targetInfo)
      val fs         = path.getFileSystem(spark.sessionState.newHadoopConf())
      fs.delete(path, false) shouldBe true
      spark.conf.set(StreamingTableMetadata.ManifestReadRetryTimeoutMsKey, "50")
      spark.conf.set(StreamingTableMetadata.ManifestReadRetryIntervalMsKey, "10")
      try {
        val shown = StreamingTableManager.show(spark, None).find(_.tableName == targetInfo.sqlIdentifier).get
        shown.status shouldBe "metadata_error"
        shown.lastFailure.get should include("source-overlap safety metadata is unavailable")
      } finally {
        spark.conf.unset(StreamingTableMetadata.ManifestReadRetryTimeoutMsKey)
        spark.conf.unset(StreamingTableMetadata.ManifestReadRetryIntervalMsKey)
      }
    }
  }
}
