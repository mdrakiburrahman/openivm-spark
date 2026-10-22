package org.openivm.spark.streaming

import org.apache.spark.sql.AnalysisException
import org.scalatest.funspec.AnyFunSpec

class StreamingProgressRetentionSpec extends AnyFunSpec with StreamingTableTestFixture {
  private val retentionKey = "spark.sql.streaming.numRecentProgressUpdates"

  describe("native progress retention admission") {
    it("keeps native progress when in-memory batch caching is disabled") {
      val cacheKey = "spark.sql.streaming.maxBatchesToRetainInMemory"
      val source   = "retention_cache_disabled_source"
      val target   = "retention_cache_disabled_target"
      createSource(source)
      appendRows(source, "(1, 'one', 'p')")
      spark.conf.set(cacheKey, "0")
      spark.conf.set(retentionKey, "2")
      try {
        val status = createStreaming(target, readStream(source), s"SELECT id, value, part FROM STREAM $source")
        val native = process(status)
        native.lastProgress should not be null
        native.recentProgress should not be empty
        spark.conf.get(cacheKey) shouldBe "0"
        assertBagEqual(target, s"SELECT * FROM $source")
      } finally {
        spark.conf.unset(cacheKey)
        spark.conf.unset(retentionKey)
      }
    }

    it("rejects retention that cannot preserve progress before creating a target or its owned path") {
      val source = "retention_create_source"
      createSource(source)
      Seq("0", "-1", "1").zipWithIndex.foreach { case (value, index) =>
        val target = s"retention_create_target_$index"
        val path   = StreamingTableMetadata.plannedTargetPath(spark, Seq(target), None).get
        spark.conf.set(retentionKey, value)
        try {
          val error = intercept[AnalysisException] {
            createStreaming(target, readStream(source), s"SELECT id, value, part FROM STREAM $source")
          }
          error.getMessage should include(retentionKey)
          error.getMessage should include("at least 2")
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
      Seq("0", "-2", "1").foreach { value =>
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
      spark.conf.set(retentionKey, "2")
      try {
        val resumed = createStreaming(target, readStream(source), s"SELECT id, value, part FROM STREAM $source")
        resumed.runId shouldBe first.runId
        spark.conf.get(retentionKey) shouldBe "2"
        assertBagEqual(target, s"SELECT * FROM $source")
        val positive = "retention_positive_target"
        val started  = createStreaming(positive, readStream(source), s"SELECT id, value, part FROM STREAM $source")
        process(started).lastProgress should not be null
        assertBagEqual(positive, s"SELECT * FROM $source")
      } finally spark.conf.unset(retentionKey)
    }

    it("shows native progress history is empty at a retention count of one") {
      val source = "retention_one_source"
      val result = "retention_one_result"
      createSource(source)
      appendRows(source, "(1, 'one', 'p')")
      spark.conf.set(retentionKey, "1")
      try {
        val native = readStream(source).writeStream
          .format("memory")
          .queryName(result)
          .option(
            "checkpointLocation",
            StreamingTableMetadata.plannedTargetPath(spark, Seq("retention_one_checkpoint"), None).get
          )
          .start()
        try {
          native.processAllAvailable()
          native.exception shouldBe None
          native.lastProgress shouldBe null
          native.recentProgress shouldBe empty
          spark.table(result).count() shouldBe 1L
        } finally native.stop()
      } finally {
        spark.catalog.dropTempView(result)
        spark.conf.unset(retentionKey)
      }
    }
  }
}
