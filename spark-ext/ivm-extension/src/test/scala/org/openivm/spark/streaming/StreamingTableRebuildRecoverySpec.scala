package org.openivm.spark.streaming

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta.DeltaLog
import org.scalatest.funspec.AnyFunSpec

class StreamingTableRebuildRecoverySpec extends AnyFunSpec with StreamingTableTestFixture {

  describe("owned streaming-table rebuilds") {
    it("rejects a catalog namespace root as LOCATION before creating a target") {
      val source = "strt_namespace_root_source"
      val target = "strt_namespace_root_target"
      createSource(source)
      val namespaceRoot = spark.catalog.getDatabase("default").locationUri

      an[org.apache.spark.sql.AnalysisException] should be thrownBy {
        createStreaming(
          target,
          readStream(source),
          s"SELECT id, value, part FROM STREAM $source",
          location = Some(namespaceRoot)
        )
      }
      spark.catalog.tableExists(target) shouldBe false
    }

    it("allows schema, partition, and property changes only through an explicit rebuild") {
      val source = "strt_rebuild_shape_source"
      val target = "strt_rebuild_shape_target"
      createSource(source)
      val first = createStreaming(
        target,
        readStream(source).select("id", "value", "part"),
        s"SELECT id, value, part FROM STREAM $source"
      )
      appendRows(source, "(1, 'one', 'east'), (2, 'two', 'west')")
      process(first)

      val replacement = readStream(source).select("id", "part")
      an[org.apache.spark.sql.AnalysisException] should be thrownBy {
        createStreaming(
          target,
          replacement,
          s"SELECT id, part FROM STREAM $source",
          partitions = Seq("part"),
          properties = Map("openivm.test.rebuild" -> "yes")
        )
      }
      assertBagEqual(
        target,
        "SELECT 1 AS id, 'one' AS value, 'east' AS part UNION ALL SELECT 2, 'two', 'west'"
      )

      val rebuilt = createStreaming(
        target,
        replacement,
        s"SELECT id, part FROM STREAM $source",
        partitions = Seq("part"),
        properties = Map("openivm.test.rebuild" -> "yes"),
        options = Map("onQueryChange" -> "rebuild")
      )
      process(rebuilt)

      val log = DeltaLog.forTable(spark, new Path(targetPath(target))).update()
      log.metadata.partitionColumns shouldBe Seq("part")
      log.metadata.configuration.get("openivm.test.rebuild") shouldBe Some("yes")
      assertBagEqual(target, "SELECT 1 AS id, 'east' AS part UNION ALL SELECT 2, 'west'")
    }

    it("recovers a matching reset intent after catalog removal and residual target data") {
      val source = "strt_reset_recovery_source"
      val target = "strt_reset_recovery_target"
      createSource(source)
      val first = createStreaming(
        target,
        readStream(source),
        s"SELECT id, value, part FROM STREAM $source"
      )
      appendRows(source, "(1, 'keep', 'p'), (2, 'replace', 'p')")
      process(first)
      StreamingTableManager.stop(spark, Seq(target))

      val targetInfo = StreamingTableMetadata.resolveDeltaTarget(
        spark,
        Seq(target),
        requireTableIdMarker = true
      )
      val manifest    = StreamingTableMetadata.readManifest(spark, targetInfo)
      val replacement = readStream(source).where("id = 2").select("id", "value", "part")
      val runtime     = StreamingRuntimeOptions.parse(Map("onQueryChange" -> "rebuild"))
      val definition = StreamingTableDefinition.build(
        spark,
        StreamingTableSpec(
          Seq(target),
          s"SELECT id, value, part FROM STREAM $source WHERE id = 2",
          replacement.queryExecution.logical,
          options = Map("onQueryChange" -> "rebuild")
        ),
        replacement.queryExecution.analyzed,
        targetInfo.identity,
        runtime
      )
      StreamingTableMetadata.writeOrVerifyResetIntent(
        spark,
        targetInfo,
        definition.fingerprint,
        manifest.sourcePaths
      )
      StreamingTableMetadata.dropOwnedCatalogAndData(spark, targetInfo, manifest.sourcePaths)

      an[org.apache.spark.sql.AnalysisException] should be thrownBy {
        createStreaming(
          target,
          readStream(source).where("id = 1").select("id", "value", "part"),
          s"SELECT id, value, part FROM STREAM $source WHERE id = 1",
          options = Map("onQueryChange" -> "rebuild")
        )
      }
      spark.catalog.tableExists(target) shouldBe false

      val residual = new Path(targetInfo.dataPath)
      val fs       = residual.getFileSystem(spark.sessionState.newHadoopConf())
      fs.mkdirs(residual) shouldBe true

      val recovered = createStreaming(
        target,
        replacement,
        s"SELECT id, value, part FROM STREAM $source WHERE id = 2",
        options = Map("onQueryChange" -> "rebuild")
      )
      process(recovered)
      assertBagEqual(target, "SELECT 2 AS id, 'replace' AS value, 'p' AS part")
      StreamingTableMetadata.pendingResetIntent(spark, targetInfo.identity) shouldBe None
    }
  }
}
