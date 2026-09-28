package org.openivm.spark.streaming

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.connector.expressions.Expressions
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.clustering.ClusteringMetadataDomain
import org.apache.spark.sql.delta.skipping.clustering.ClusteringColumnInfo
import org.scalatest.funspec.AnyFunSpec

class StreamingTableDestinationLayoutSpec extends AnyFunSpec with StreamingTableTestFixture {

  describe("destination layout declarations") {
    it("creates a native two-key clustered destination and persists the logical layout") {
      val source = "strt_layout_cluster_source"
      val target = "strt_layout_cluster_target"
      createSource(source)

      val status = createStreaming(
        target,
        readStream(source),
        s"SELECT id, value, part FROM STREAM $source",
        clusters = Seq(Seq("part"), Seq("id"))
      )
      appendRows(source, "(1, 'one', 'east'), (2, 'two', 'west')")
      process(status)

      val snapshot = DeltaLog.forTable(spark, new Path(targetPath(target))).update()
      snapshot.metadata.partitionColumns shouldBe empty
      val logicalColumns = ClusteringMetadataDomain
        .fromSnapshot(snapshot)
        .getOrElse(fail("missing Delta clustering metadata"))
        .clusteringColumns
        .map { physical =>
          Expressions.column(ClusteringColumnInfo(snapshot.schema, physical).logicalName).fieldNames.toSeq
        }
      logicalColumns shouldBe Seq(Seq("part"), Seq("id"))

      val manifest = StreamingTableMetadata.readManifest(
        spark,
        StreamingTableMetadata.resolveDeltaTarget(spark, Seq(target), requireTableIdMarker = true)
      )
      manifest.semanticJson should include("\"clusterColumns\"")
      assertBagEqual(
        target,
        "SELECT 1 AS id, 'one' AS value, 'east' AS part UNION ALL SELECT 2, 'two', 'west'"
      )
      an[org.apache.spark.sql.AnalysisException] should be thrownBy {
        createStreaming(
          target,
          readStream(source),
          s"SELECT id, value, part FROM STREAM $source",
          clusters = Seq(Seq("id"), Seq("part"))
        )
      }
      val rebuilt = createStreaming(
        target,
        readStream(source),
        s"SELECT id, value, part FROM STREAM $source",
        clusters = Seq(Seq("id"), Seq("part")),
        options = Map("onQueryChange" -> "rebuild")
      )
      process(rebuilt)
      val rebuiltSnapshot = DeltaLog.forTable(spark, new Path(targetPath(target))).update()
      val rebuiltColumns = ClusteringMetadataDomain
        .fromSnapshot(rebuiltSnapshot)
        .getOrElse(fail("missing rebuilt Delta clustering metadata"))
        .clusteringColumns
        .map(physical =>
          Expressions.column(ClusteringColumnInfo(rebuiltSnapshot.schema, physical).logicalName).fieldNames.toSeq
        )
      rebuiltColumns shouldBe Seq(Seq("id"), Seq("part"))
    }

    it("rejects invalid clustering replacements without stopping the existing writer") {
      val source = "strt_layout_invalid_source"
      val target = "strt_layout_invalid_target"
      createSource(source)
      val status = createStreaming(
        target,
        readStream(source),
        s"SELECT id, value, part FROM STREAM $source"
      )
      val active = process(status)

      an[org.apache.spark.sql.AnalysisException] should be thrownBy {
        createStreaming(
          target,
          readStream(source),
          s"SELECT id, value, part FROM STREAM $source",
          clusters = Seq(Seq("missing_column"))
        )
      }
      active.isActive shouldBe true
      appendRows(source, "(1, 'still-active', 'p')")
      active.processAllAvailable()
      assertBagEqual(target, "SELECT 1 AS id, 'still-active' AS value, 'p' AS part")
    }

    it("rejects a stats-ineligible clustered replacement without stopping the existing writer") {
      val source = "strt_layout_stats_source"
      val target = "strt_layout_stats_target"
      createSource(source)
      val status = createStreaming(
        target,
        readStream(source),
        s"SELECT id, value, part FROM STREAM $source"
      )
      val active = process(status)
      val originalTarget =
        StreamingTableMetadata.resolveDeltaTarget(spark, Seq(target), requireTableIdMarker = true)

      an[org.apache.spark.sql.AnalysisException] should be thrownBy {
        createStreaming(
          target,
          readStream(source),
          s"SELECT id, value, part FROM STREAM $source",
          clusters = Seq(Seq("id")),
          properties = Map("delta.dataSkippingNumIndexedCols" -> "0"),
          options = Map("onQueryChange" -> "rebuild")
        )
      }
      active.isActive shouldBe true
      val currentTarget =
        StreamingTableMetadata.resolveDeltaTarget(spark, Seq(target), requireTableIdMarker = true)
      currentTarget.dataPath shouldBe originalTarget.dataPath
      currentTarget.deltaTableId shouldBe originalTarget.deltaTableId
      pathExists(originalTarget.checkpointLocation) shouldBe true
      appendRows(source, "(2, 'stats-guarded', 'p')")
      active.processAllAvailable()
      assertBagEqual(target, "SELECT 2 AS id, 'stats-guarded' AS value, 'p' AS part")
    }

    it("rejects incompatible partitioned and clustered destinations before creation") {
      val source = "strt_layout_incompatible_source"
      val target = "strt_layout_incompatible_target"
      createSource(source)

      an[org.apache.spark.sql.AnalysisException] should be thrownBy {
        createStreaming(
          target,
          readStream(source),
          s"SELECT id, value, part FROM STREAM $source",
          partitions = Seq("part"),
          clusters = Seq(Seq("id"))
        )
      }
      spark.catalog.tableExists(target) shouldBe false

      val duplicateTarget = "strt_layout_duplicate_target"
      an[org.apache.spark.sql.AnalysisException] should be thrownBy {
        createStreaming(
          duplicateTarget,
          readStream(source),
          s"SELECT id, value, part FROM STREAM $source",
          clusters = Seq(Seq("id"), Seq("ID"))
        )
      }
      spark.catalog.tableExists(duplicateTarget) shouldBe false
    }

    it("keeps legacy-unclustered declarations hash-compatible across resume") {
      val source = "strt_layout_legacy_source"
      val target = "strt_layout_legacy_target"
      createSource(source)
      val first = createStreaming(
        target,
        readStream(source),
        s"SELECT id, value, part FROM STREAM $source"
      )
      val firstManifest = StreamingTableMetadata.readManifest(
        spark,
        StreamingTableMetadata.resolveDeltaTarget(spark, Seq(target), requireTableIdMarker = true)
      )
      firstManifest.semanticJson should not include "\"clusterColumns\""
      StreamingTableManager.stop(spark, Seq(target))

      val resumed = createStreaming(
        target,
        readStream(source),
        s"SELECT id, value, part FROM STREAM $source"
      )
      resumed.definitionHash shouldBe first.definitionHash
      resumed.queryId shouldBe first.queryId
    }
  }
}
