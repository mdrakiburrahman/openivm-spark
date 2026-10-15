package org.openivm.spark.streaming

import org.apache.spark.sql.AnalysisException
import org.scalatest.funspec.AnyFunSpec

class StreamingProgressRetentionSpec extends AnyFunSpec with StreamingTableTestFixture {
  private val retentionKey = "spark.sql.streaming.maxBatchesToRetainInMemory"

  describe("native progress retention admission") {
    it("rejects non-positive retention before creating a target or its owned path") {
      val source = "retention_create_source"
      createSource(source)
      Seq("0", "-1").zipWithIndex.foreach { case (value, index) =>
        val target = s"retention_create_target_$index"
        val path   = StreamingTableMetadata.plannedTargetPath(spark, Seq(target), None).get
        spark.conf.set(retentionKey, value)
        try {
          val error = intercept[AnalysisException] {
            createStreaming(target, readStream(source), s"SELECT id, value, part FROM STREAM $source")
          }
          error.getMessage should include(retentionKey)
          error.getMessage should include("positive")
          spark.catalog.tableExists(target) shouldBe false
          pathExists(path) shouldBe false
          StreamingTableMetadata.pendingResetIntent(
            spark,
            StreamingTableMetadata.canonicalIdentity(spark, Seq(target))
          ) shouldBe None
          spark.conf.get(retentionKey) shouldBe value
        } finally spark.conf.unset(retentionKey)
      }
    }

    it("accepts default and positive retention but rejects a destructive rebuild before stopping its writer") {
      val source = "retention_rebuild_source"
      val target = "retention_rebuild_target"
      createSource(source)
      val first = createStreaming(target, readStream(source), s"SELECT id, value, part FROM STREAM $source")
      appendRows(source, "(1, 'one', 'p')")
      process(first)
      val before = StreamingTableMetadata.resolveDeltaTarget(spark, Seq(target), requireTableIdMarker = true)
      Seq("0", "-2").foreach { value =>
        spark.conf.set(retentionKey, value)
        try {
          intercept[AnalysisException] {
            createStreaming(
              target,
              readStream(source).where("id > 0"),
              s"SELECT id, value, part FROM STREAM $source WHERE id > 0",
              options = Map("onQueryChange" -> "rebuild")
            )
          }.getMessage should include(retentionKey)
          spark.streams.get(first.queryId.get).isActive shouldBe true
          StreamingTableMetadata.resolveDeltaTarget(spark, Seq(target), true) shouldBe before
          StreamingTableMetadata.pendingResetIntent(spark, before.identity) shouldBe None
          archivedCheckpoints(before) shouldBe empty
        } finally spark.conf.unset(retentionKey)
      }
      spark.conf.set(retentionKey, "1")
      try {
        val resumed = createStreaming(target, readStream(source), s"SELECT id, value, part FROM STREAM $source")
        resumed.runId shouldBe first.runId
        spark.conf.get(retentionKey) shouldBe "1"
        assertBagEqual(target, s"SELECT * FROM $source")
        val positive = "retention_positive_target"
        val started  = createStreaming(positive, readStream(source), s"SELECT id, value, part FROM STREAM $source")
        process(started)
        assertBagEqual(positive, s"SELECT * FROM $source")
      } finally spark.conf.unset(retentionKey)
    }
  }
}
