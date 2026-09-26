package org.openivm.spark.streaming

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta.DeltaLog
import org.scalatest.funspec.AnyFunSpec

import java.io.File
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class StreamingTableRuntimeSpec extends AnyFunSpec with StreamingTableTestFixture {

  describe("native streaming-table runtime") {
    it("writes a Delta projection/filter in the caller session without changing caller configuration") {
      val source = "strt_projection_source"
      val target = "strt_projection_target"
      createSource(source)
      val stateProvider = spark.conf.getOption("spark.sql.streaming.stateStore.providerClass")
      val query         = readStream(source).where("id >= 2").select("id", "value", "part")

      val status = createStreaming(
        target,
        query,
        s"SELECT id, value, part FROM STREAM $source WHERE id >= 2"
      )

      status.status shouldBe "initializing"
      status.queryId should not be empty
      val manifest = StreamingTableMetadata.readManifest(
        spark,
        StreamingTableMetadata.resolveDeltaTarget(spark, Seq(target), requireTableIdMarker = true)
      )
      manifest.sourcePaths should not be empty
      manifest.semanticJson should include("\"provider\":\"delta\"")
      manifest.semanticJson should include("\"deltaPath\"")
      val native = process(status)
      native.sparkSession should be theSameInstanceAs spark
      spark.streams.active.map(_.id.toString) should contain(status.queryId.get)
      spark.conf.getOption("spark.sql.streaming.stateStore.providerClass") shouldBe stateProvider

      appendRows(source, "(1, 'skip', 'a'), (2, 'keep', 'b'), (2, 'again', 'b')")
      native.processAllAvailable()
      assertBagEqual(target, "SELECT 2 AS id, 'keep' AS value, 'b' AS part UNION ALL SELECT 2, 'again', 'b'")
    }

    it("creates catalog-native partitioned LOCATION targets with table properties and a manifest") {
      val source   = "strt_layout_source"
      val target   = "strt_layout_target"
      val location = new File("target", s"streaming-location-${System.nanoTime()}").getAbsolutePath
      createSource(source)

      val status = createStreaming(
        target,
        readStream(source),
        s"SELECT id, value, part FROM STREAM $source",
        location = Some(location),
        partitions = Seq("part"),
        properties = Map("delta.enableChangeDataFeed" -> "true", "openivm.test.property" -> "present")
      )
      process(status)
      appendRows(source, "(1, 'one', 'east'), (2, 'two', 'west')")
      process(status)

      hasDefinition(target) shouldBe true
      targetPath(target) shouldBe StreamingTableMetadata.normalizePath(spark, location)
      val log = DeltaLog.forTable(spark, new Path(targetPath(target))).update()
      log.metadata.partitionColumns shouldBe Seq("part")
      log.metadata.configuration.get("openivm.test.property") shouldBe Some("present")
      assertBagEqual(target, s"SELECT id, value, part FROM `$source`")
      StreamingTableManager.drop(spark, Seq(target), ifExists = false).status shouldBe "dropped"
      pathExists(location) shouldBe false
    }

    it("uses caller-session temporary views and UDFs without cloning the session") {
      val source = "strt_session_source"
      val target = "strt_session_target"
      val view   = "strt_session_view"
      createSource(source)
      spark.udf.register("strt_suffix", (value: String) => s"$value-suffix")
      spark.readStream
        .format("delta")
        .option("maxFilesPerTrigger", "1")
        .table(source)
        .createOrReplaceTempView(view)
      val query = spark.table(view).selectExpr("id", "strt_suffix(value) AS value", "part")

      val status = createStreaming(target, query, s"SELECT id, strt_suffix(value), part FROM STREAM $source")
      val manifest = StreamingTableMetadata.readManifest(
        spark,
        StreamingTableMetadata.resolveDeltaTarget(spark, Seq(target), requireTableIdMarker = true)
      )
      manifest.sourcePaths should not be empty
      manifest.semanticJson should include("\"provider\":\"delta\"")
      manifest.semanticJson should include("\"deltaPath\"")
      manifest.operationalJson should include("maxfilespertrigger")
      appendRows(source, "(4, 'caller', 'session')")
      process(status)

      assertBagEqual(target, "SELECT 4 AS id, 'caller-suffix' AS value, 'session' AS part")
      spark.catalog.tableExists(view) shouldBe true
    }

    it("keeps one same-target writer while independent targets can start concurrently") {
      implicit val executionContext: ExecutionContext = ExecutionContext.global
      val sourceA                                     = "strt_concurrent_source_a"
      val sourceB                                     = "strt_concurrent_source_b"
      val targetA                                     = "strt_concurrent_target_a"
      val targetB                                     = "strt_concurrent_target_b"
      createSource(sourceA)
      createSource(sourceB)
      val queryA = readStream(sourceA)
      val queryB = readStream(sourceB)
      val specA = StreamingTableSpec(
        Seq(targetA),
        s"SELECT id, value, part FROM STREAM $sourceA",
        queryA.queryExecution.logical
      )
      val specB = StreamingTableSpec(
        Seq(targetB),
        s"SELECT id, value, part FROM STREAM $sourceB",
        queryB.queryExecution.logical
      )

      val first    = Future(StreamingTableManager.create(spark, specA))
      val second   = Future(StreamingTableManager.create(spark, specA))
      val other    = Future(StreamingTableManager.create(spark, specB))
      val statuses = Await.result(Future.sequence(Seq(first, second, other)), 120.seconds)

      statuses.take(2).map(_.queryId).distinct.size shouldBe 1
      statuses.last.queryId should not be empty
      spark.streams.active.map(_.name).count(_.startsWith("openivm_streaming_")) should be >= 2
      statuses.flatMap(_.queryId).foreach(id => Option(spark.streams.get(id)).foreach(_.stop()))
    }

    it("stops and resumes the exact checkpoint without replaying committed input") {
      val source = "strt_resume_source"
      val target = "strt_resume_target"
      createSource(source)
      val query = readStream(source)

      val first = createStreaming(target, query, s"SELECT id, value, part FROM STREAM $source")
      appendRows(source, "(1, 'before-stop', 'p')")
      process(first)
      val stopped = StreamingTableManager.stop(spark, Seq(target))
      stopped.status shouldBe "stopped"

      val second = createStreaming(
        target,
        query,
        s"SELECT id, value, part FROM STREAM $source",
        options = Map("triggerInterval" -> "1 second")
      )
      first.queryId shouldBe second.queryId
      first.runId should not be second.runId
      second.status shouldBe "restarted"
      appendRows(source, "(2, 'after-restart', 'p')")
      process(second)
      assertBagEqual(
        target,
        "SELECT 1 AS id, 'before-stop' AS value, 'p' AS part UNION ALL " +
          "SELECT 2, 'after-restart', 'p'"
      )
    }

    it("fails changed semantics by default and performs an explicit owned rebuild") {
      val source = "strt_rebuild_source"
      val target = "strt_rebuild_target"
      createSource(source)
      val original = readStream(source).select("id", "value", "part")

      val first = createStreaming(target, original, s"SELECT id, value, part FROM STREAM $source")
      appendRows(source, "(1, 'one', 'p'), (2, 'two', 'p')")
      process(first)
      val changed = readStream(source).where("id = 2").select("id", "value", "part")

      an[org.apache.spark.sql.AnalysisException] should be thrownBy {
        createStreaming(target, changed, s"SELECT id, value, part FROM STREAM $source WHERE id = 2")
      }
      assertBagEqual(
        target,
        "SELECT 1 AS id, 'one' AS value, 'p' AS part UNION ALL SELECT 2, 'two', 'p'"
      )

      val rebuilt = createStreaming(
        target,
        changed,
        s"SELECT id, value, part FROM STREAM $source WHERE id = 2",
        options = Map("onQueryChange" -> "rebuild")
      )
      process(rebuilt)
      rebuilt.status shouldBe "rebuilding"
      assertBagEqual(target, "SELECT 2 AS id, 'two' AS value, 'p' AS part")
    }

    it("reports owned tables, retains state on STOP, and deletes only owned data on DROP") {
      val source = "strt_lifecycle_source"
      val target = "strt_lifecycle_target"
      createSource(source)
      val status = createStreaming(target, readStream(source), s"SELECT id, value, part FROM STREAM $source")
      process(status)
      appendRows(source, "(7, 'seven', 'p')")
      process(status)
      val path = targetPath(target)

      StreamingTableManager.show(spark, None).map(_.tableName) should contain(status.tableName)
      StreamingTableManager.stop(spark, Seq(target)).status shouldBe "stopped"
      spark.table(target).count() shouldBe 1L
      StreamingTableManager.drop(spark, Seq(target), ifExists = false).status shouldBe "dropped"
      spark.catalog.tableExists(target) shouldBe false
      pathExists(path) shouldBe false
      val archives = archivedCheckpoints(path)
      archives should have size 1
      checkpointArchiveReason(archives.head) shouldBe "drop"
      StreamingTableManager.drop(spark, Seq(target), ifExists = true).status shouldBe "not_found"
    }

    it("refuses to adopt an existing unowned Delta target") {
      val source = "strt_unowned_source"
      val target = "strt_unowned_target"
      createSource(source)
      spark.sql(s"CREATE TABLE `$target` (id INT, value STRING, part STRING) USING DELTA").collect()

      an[org.apache.spark.sql.AnalysisException] should be thrownBy {
        createStreaming(target, readStream(source), s"SELECT id, value, part FROM STREAM $source")
      }
      spark.catalog.tableExists(target) shouldBe true
    }

    it("rejects a batch SELECT before creating a target or falling back to batch execution") {
      val target = "strt_batch_target"
      val batch  = spark.sql("SELECT 1 AS id, 'batch' AS value, 'p' AS part")

      an[org.apache.spark.sql.AnalysisException] should be thrownBy {
        createStreaming(target, batch, "SELECT 1 AS id, 'batch' AS value, 'p' AS part")
      }
      spark.catalog.tableExists(target) shouldBe false
    }

    it("fails closed when an owned target has a missing definition manifest") {
      val source = "strt_missing_manifest_source"
      val target = "strt_missing_manifest_target"
      createSource(source)
      val query  = readStream(source)
      val status = createStreaming(target, query, s"SELECT id, value, part FROM STREAM $source")
      process(status)
      StreamingTableManager.stop(spark, Seq(target))

      val targetInfo = StreamingTableMetadata.resolveDeltaTarget(
        spark,
        Seq(target),
        requireTableIdMarker = true
      )
      val manifest = StreamingTableMetadata.definitionPath(targetInfo)
      val fs       = manifest.getFileSystem(spark.sessionState.newHadoopConf())
      fs.delete(manifest, false) shouldBe true

      an[org.apache.spark.sql.AnalysisException] should be thrownBy {
        createStreaming(target, query, s"SELECT id, value, part FROM STREAM $source")
      }
      spark.catalog.tableExists(target) shouldBe true
    }
  }
}
