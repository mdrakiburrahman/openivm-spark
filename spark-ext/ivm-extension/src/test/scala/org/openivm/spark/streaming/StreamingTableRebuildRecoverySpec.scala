package org.openivm.spark.streaming

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta.DeltaLog
import org.openivm.spark.common.StreamingDependencyCatalog
import org.scalatest.funspec.AnyFunSpec

import scala.collection.mutable

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

    it("drops a transitive downstream chain leaves-first before rebuilding the upstream") {
      val source = "strt_cascade_chain_source"
      val a      = "strt_cascade_chain_a"
      val b      = "strt_cascade_chain_b"
      val c      = "strt_cascade_chain_c"
      createSource(source)
      val aStatus = createStreaming(a, readStream(source), s"SELECT id, value, part FROM STREAM $source")
      appendRows(source, "(1, 'one', 'p')")
      process(aStatus)
      val bStatus = createStreaming(b, readStream(a), s"SELECT id, value, part FROM STREAM $a")
      process(bStatus)
      val cStatus = createStreaming(c, readStream(b), s"SELECT id, value, part FROM STREAM $b")
      process(cStatus)
      val bCheckpoint = StreamingTableMetadata
        .resolveDeltaTarget(spark, Seq(b), requireTableIdMarker = true)
        .checkpointLocation
      val cCheckpoint = StreamingTableMetadata
        .resolveDeltaTarget(spark, Seq(c), requireTableIdMarker = true)
        .checkpointLocation
      val oldAId = StreamingTableMetadata
        .resolveDeltaTarget(spark, Seq(a), requireTableIdMarker = true)
        .deltaTableId
      val order = mutable.ArrayBuffer.empty[String]
      StreamingTableManager.setBeforeCascadeDropHookForTesting(target => order += target.name.last)

      val rebuilt = createStreaming(
        a,
        readStream(source).where("id = 1"),
        s"SELECT id, value, part FROM STREAM $source WHERE id = 1",
        options = Map("onQueryChange" -> "rebuild")
      )
      process(rebuilt)

      order.toSeq shouldBe Seq(c, b)
      spark.catalog.tableExists(b) shouldBe false
      spark.catalog.tableExists(c) shouldBe false
      pathExists(bCheckpoint) shouldBe false
      pathExists(cCheckpoint) shouldBe false
      spark.catalog.tableExists(a) shouldBe true

      val newA = StreamingTableMetadata.resolveDeltaTarget(spark, Seq(a), requireTableIdMarker = true)
      newA.deltaTableId should not be oldAId
      val recreatedB = createStreaming(b, readStream(a), s"SELECT id, value, part FROM STREAM $a")
      process(recreatedB)
      val bRecord = StreamingDependencyCatalog
        .lookup(spark, StreamingTableMetadata.canonicalIdentity(spark, Seq(b)))
        .get
      bRecord.sources.map(_.parentDeltaTableId) should contain only newA.deltaTableId
      val recreatedC = createStreaming(c, readStream(b), s"SELECT id, value, part FROM STREAM $b")
      process(recreatedC)
      val newB = StreamingTableMetadata.resolveDeltaTarget(spark, Seq(b), requireTableIdMarker = true)
      val cRecord = StreamingDependencyCatalog
        .lookup(spark, StreamingTableMetadata.canonicalIdentity(spark, Seq(c)))
        .get
      cRecord.sources.map(_.parentDeltaTableId) should contain only newB.deltaTableId
    }

    it("deduplicates a fan-out diamond and leaves unrelated ancestors intact") {
      val source          = "strt_cascade_diamond_source"
      val unrelatedSource = "strt_cascade_diamond_unrelated_source"
      val unrelatedTarget = "strt_cascade_diamond_unrelated_target"
      val a               = "strt_cascade_diamond_a"
      val b               = "strt_cascade_diamond_b"
      val c               = "strt_cascade_diamond_c"
      val d               = "strt_cascade_diamond_d"
      createSource(source)
      createSource(unrelatedSource)
      val aStatus = createStreaming(a, readStream(source), s"SELECT id, value, part FROM STREAM $source")
      val unrelatedStatus = createStreaming(
        unrelatedTarget,
        readStream(unrelatedSource),
        s"SELECT id, value, part FROM STREAM $unrelatedSource"
      )
      appendRows(source, "(1, 'one', 'p')")
      process(aStatus)
      process(unrelatedStatus)
      val bStatus = createStreaming(b, readStream(a), s"SELECT id, value, part FROM STREAM $a")
      val cStatus = createStreaming(c, readStream(a), s"SELECT id, value, part FROM STREAM $a")
      process(bStatus)
      process(cStatus)
      val dFrame  = readStream(b).unionByName(readStream(c))
      val dStatus = createStreaming(d, dFrame, s"SELECT * FROM STREAM $b UNION ALL SELECT * FROM STREAM $c")
      process(dStatus)
      val order = mutable.ArrayBuffer.empty[String]
      StreamingTableManager.setBeforeCascadeDropHookForTesting(target => order += target.name.last)

      val rebuilt = createStreaming(
        a,
        readStream(source).where("id = 1"),
        s"SELECT id, value, part FROM STREAM $source WHERE id = 1",
        options = Map("onQueryChange" -> "rebuild")
      )
      process(rebuilt)

      order.count(_ == d) shouldBe 1
      order.head shouldBe d
      order.toSet shouldBe Set(b, c, d)
      spark.catalog.tableExists(unrelatedTarget) shouldBe true
      spark.catalog.tableExists(a) shouldBe true
    }

    it("resumes a partially completed downstream cascade from its durable journal") {
      val source = "strt_cascade_retry_source"
      val a      = "strt_cascade_retry_a"
      val b      = "strt_cascade_retry_b"
      val c      = "strt_cascade_retry_c"
      createSource(source)
      val aStatus = createStreaming(a, readStream(source), s"SELECT id, value, part FROM STREAM $source")
      appendRows(source, "(1, 'one', 'p')")
      process(aStatus)
      val bStatus = createStreaming(b, readStream(a), s"SELECT id, value, part FROM STREAM $a")
      process(bStatus)
      val cStatus = createStreaming(c, readStream(b), s"SELECT id, value, part FROM STREAM $b")
      process(cStatus)
      StreamingTableManager.setBeforeCascadeDropHookForTesting { target =>
        if (target.name.last == b) throw new IllegalStateException("injected cascade failure")
      }

      an[IllegalStateException] should be thrownBy {
        createStreaming(
          a,
          readStream(source).where("id = 1"),
          s"SELECT id, value, part FROM STREAM $source WHERE id = 1",
          options = Map("onQueryChange" -> "rebuild")
        )
      }
      spark.catalog.tableExists(a) shouldBe true
      spark.catalog.tableExists(b) shouldBe true
      spark.catalog.tableExists(c) shouldBe false

      StreamingTableManager.setBeforeCascadeDropHookForTesting((_: StreamingTableCascadeTarget) => ())
      val recovered = createStreaming(
        a,
        readStream(source).where("id = 1"),
        s"SELECT id, value, part FROM STREAM $source WHERE id = 1",
        options = Map("onQueryChange" -> "rebuild")
      )
      process(recovered)

      spark.catalog.tableExists(a) shouldBe true
      spark.catalog.tableExists(b) shouldBe false
      spark.catalog.tableExists(c) shouldBe false
      StreamingTableMetadata.pendingResetIntent(
        spark,
        StreamingTableMetadata.canonicalIdentity(spark, Seq(a))
      ) shouldBe None
    }

    it("fails closed before mutating the upstream when dependency metadata is incomplete") {
      val source = "strt_cascade_corrupt_source"
      val a      = "strt_cascade_corrupt_a"
      val b      = "strt_cascade_corrupt_b"
      createSource(source)
      val aStatus = createStreaming(a, readStream(source), s"SELECT id, value, part FROM STREAM $source")
      appendRows(source, "(1, 'one', 'p')")
      process(aStatus)
      val bStatus = createStreaming(b, readStream(a), s"SELECT id, value, part FROM STREAM $a")
      process(bStatus)
      val originalA =
        StreamingTableMetadata.resolveDeltaTarget(spark, Seq(a), requireTableIdMarker = true)
      val bIdentity = StreamingTableMetadata.canonicalIdentity(spark, Seq(b))
      StreamingDependencyCatalog.removeTargetRecordForTesting(spark, bIdentity)

      an[org.apache.spark.sql.AnalysisException] should be thrownBy {
        createStreaming(
          a,
          readStream(source).where("id = 1"),
          s"SELECT id, value, part FROM STREAM $source WHERE id = 1",
          options = Map("onQueryChange" -> "rebuild")
        )
      }

      val unchangedA =
        StreamingTableMetadata.resolveDeltaTarget(spark, Seq(a), requireTableIdMarker = true)
      unchangedA.deltaTableId shouldBe originalA.deltaTableId
      spark.catalog.tableExists(a) shouldBe true
      spark.catalog.tableExists(b) shouldBe true
      aStatus.queryId.flatMap(id => Option(spark.streams.get(id))).exists(_.isActive) shouldBe true
      bStatus.queryId.flatMap(id => Option(spark.streams.get(id))).exists(_.isActive) shouldBe true
    }

    it("recovers a missing root dependency record while preserving descendant cleanup") {
      val source = "strt_cascade_missing_root_source"
      val a      = "strt_cascade_missing_root_a"
      val b      = "strt_cascade_missing_root_b"
      createSource(source)
      val aStatus = createStreaming(a, readStream(source), s"SELECT id, value, part FROM STREAM $source")
      appendRows(source, "(1, 'one', 'p')")
      process(aStatus)
      val bStatus = createStreaming(b, readStream(a), s"SELECT id, value, part FROM STREAM $a")
      process(bStatus)
      val aIdentity = StreamingTableMetadata.canonicalIdentity(spark, Seq(a))
      StreamingDependencyCatalog.removeTargetRecordForTesting(spark, aIdentity)

      val rebuilt = createStreaming(
        a,
        readStream(source).where("id = 1"),
        s"SELECT id, value, part FROM STREAM $source WHERE id = 1",
        options = Map("onQueryChange" -> "rebuild")
      )
      process(rebuilt)

      spark.catalog.tableExists(a) shouldBe true
      spark.catalog.tableExists(b) shouldBe false
      StreamingDependencyCatalog.lookup(spark, aIdentity).isDefined shouldBe true
    }
  }
}
