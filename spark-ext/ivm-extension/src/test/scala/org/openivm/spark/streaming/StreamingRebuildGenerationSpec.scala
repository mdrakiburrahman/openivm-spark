package org.openivm.spark.streaming

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.openivm.StreamingDatasetAccess
import org.openivm.spark.common.DeltaTableVersion
import org.scalatest.funspec.AnyFunSpec

class StreamingRebuildGenerationSpec extends AnyFunSpec with StreamingTableTestFixture {

  describe("same-path streaming target generations") {
    it("rejects an obsolete snapshot-pinned reader before mutation and starts a fresh replacement reader") {
      val source = "generation_checkpoint_source"
      val target = "generation_checkpoint_target"
      val child  = "generation_checkpoint_child"
      createSource(source)
      val first = createStreaming(
        target,
        readStream(source),
        s"SELECT id, value, part FROM STREAM $source",
        properties = Map("delta.checkpointInterval" -> "1")
      )
      (1 to 3).foreach { id =>
        appendRows(source, s"($id, 'value', 'p')")
        process(first)
      }
      StreamingTableManager.stop(spark, Seq(target))
      val original = StreamingTableMetadata.resolveDeltaTarget(spark, Seq(target), requireTableIdMarker = true)
      val path     = new Path(original.dataPath)
      val fs       = path.getFileSystem(spark.sessionState.newHadoopConf())
      fs.listStatus(new Path(path, "_delta_log")).exists(_.getPath.getName.contains(".checkpoint.")) shouldBe true
      DeltaLog.invalidateCache(spark, path)
      val warm        = DeltaLog.forTable(spark, path)
      val oldSnapshot = warm.update()
      val oldVersion  = oldSnapshot.version
      oldVersion should be > 1L
      val joined = readStream(source)
        .alias("s")
        .join(spark.read.format("delta").option("versionAsOf", oldVersion).table(target).alias("d"), "id")
        .select("s.id", "s.value", "s.part")
      val preparedReader = StreamingDatasetAccess.ofRows(spark, joined.queryExecution.analyzed)
      StreamingTableDefinition.build(
        spark,
        StreamingTableSpec(
          Seq(child),
          s"SELECT s.id, s.value, s.part FROM STREAM $source s JOIN $target VERSION AS OF $oldVersion d ON s.id = d.id",
          preparedReader.queryExecution.logical
        ),
        preparedReader.queryExecution.analyzed,
        StreamingTableMetadata.canonicalIdentity(spark, Seq(child)),
        StreamingRuntimeOptions.parse(Map.empty)
      )

      val rebuilt = createStreaming(
        target,
        readStream(source).where("id >= 2"),
        s"SELECT id, value, part FROM STREAM $source WHERE id >= 2",
        options = Map("onQueryChange" -> "rebuild")
      )
      process(rebuilt)
      val replacement = StreamingTableMetadata.resolveDeltaTarget(spark, Seq(target), requireTableIdMarker = true)
      replacement.dataPath shouldBe original.dataPath
      replacement.deltaTableId should not be original.deltaTableId
      replacement.tableId should not be original.tableId
      DeltaLog.forTable(spark, path).update().version should be < oldVersion
      StreamingTableMetadata.pendingResetIntent(spark, original.identity) shouldBe None
      archivedCheckpoints(original) should have size 1
      assertBagEqual(target, "SELECT 2 AS id, 'value' AS value, 'p' AS part UNION ALL SELECT 3, 'value', 'p'")

      oldSnapshot.uncache()
      val error = intercept[AnalysisException] {
        createStreaming(
          child,
          preparedReader,
          s"SELECT s.id, s.value, s.part FROM STREAM $source s JOIN $target VERSION AS OF $oldVersion d ON s.id = d.id"
        )
      }
      error.getMessage should include("changed generation")
      error.getMessage should include("Recreate")
      spark.catalog.tableExists(child) shouldBe false
      pathExists(StreamingTableMetadata.plannedTargetPath(spark, Seq(child), None).get) shouldBe false
      val downstream = createStreaming(child, readStream(target), s"SELECT id, value, part FROM STREAM $target")
      process(downstream)
      appendRows(source, "(4, 'next', 'p')")
      process(rebuilt)
      process(downstream)
      assertBagEqual(child, s"SELECT * FROM $source WHERE id >= 2")
      assertBagEqual(target, s"SELECT * FROM $source WHERE id >= 2")
    }

    it("preserves same-generation time travel when ordinary source versions advance") {
      val source = "generation_same_source"
      val dim    = "generation_same_dimension"
      val target = "generation_same_target"
      createSource(source)
      createSource(dim)
      appendRows(source, "(1, 'one', 'p'), (2, 'two', 'p'), (3, 'three', 'p')")
      appendRows(dim, "(1, 'dimension', 'p'), (2, 'dimension', 'p')")
      val version = DeltaTableVersion.requireLatest(spark, dim)
      val joined = readStream(source)
        .alias("s")
        .join(spark.read.format("delta").option("versionAsOf", version).table(dim).alias("d"), "id")
        .select("s.id", "s.value", "s.part")
      val prepared = StreamingDatasetAccess.ofRows(spark, joined.queryExecution.analyzed)
      appendRows(dim, "(3, 'dimension', 'p')")
      val status = createStreaming(
        target,
        prepared,
        s"SELECT s.id, s.value, s.part FROM STREAM $source s JOIN $dim VERSION AS OF $version d ON s.id = d.id"
      )
      process(status)
      assertBagEqual(target, "SELECT 1 AS id, 'one' AS value, 'p' AS part UNION ALL SELECT 2, 'two', 'p'")
      appendRows(source, "(1, 'later', 'p'), (3, 'excluded', 'p')")
      process(status)
      assertBagEqual(
        target,
        "SELECT 1 AS id, 'one' AS value, 'p' AS part UNION ALL SELECT 2, 'two', 'p' UNION ALL SELECT 1, 'later', 'p'"
      )
    }
  }
}
