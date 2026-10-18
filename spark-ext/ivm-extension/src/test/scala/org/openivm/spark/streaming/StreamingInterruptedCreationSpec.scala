package org.openivm.spark.streaming

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.scalatest.funspec.AnyFunSpec

class StreamingInterruptedCreationSpec extends AnyFunSpec with StreamingTableTestFixture {

  private def createTarget(source: String, target: String): StreamingTableStatus =
    createStreaming(target, readStream(source), s"SELECT id, value, part FROM STREAM $source")

  private def shown(target: String): Seq[StreamingTableStatus] =
    StreamingTableManager.show(spark, None, Some(target))

  private def plannedLocation(target: String): String =
    StreamingTableMetadata.plannedTargetPath(spark, Seq(target), None).get

  /** Remove a managed target's catalog entry but keep its files, as when the catalog never committed it. */
  private def forgetCatalogEntry(target: String): Unit = {
    val location = new Path(spark.sessionState.catalog.getTableMetadata(TableIdentifier(target)).location)
    val fs       = location.getFileSystem(spark.sessionState.newHadoopConf())
    val parked   = new Path(location.getParent, s"${location.getName}.parked")
    if (!fs.rename(location, parked)) fail(s"Could not park $location")
    spark.sessionState.catalog.externalCatalog.dropTable("default", target, ignoreIfNotExists = false, purge = true)
    spark.sessionState.catalog.invalidateAllCachedTables()
    if (!fs.rename(parked, location)) fail(s"Could not restore $location")
  }

  describe("streaming-table creation that never committed its catalog table") {

    it("reports the target absent once its run stops and recreates it with exactly-once data") {
      val source = "strt_uncommitted_source"
      val target = "strt_uncommitted_target"
      createSource(source)
      appendRows(source, "(1, 'a', 'x'), (2, 'b', 'y')")
      val first = createTarget(source, target)
      process(first)
      forgetCatalogEntry(target)

      val running = shown(target)
      running.map(_.isActive) shouldBe Seq(true)
      running.flatMap(_.lastFailure) shouldBe empty

      first.queryId.flatMap(id => Option(spark.streams.get(id))).foreach(_.stop())
      shown(target) shouldBe empty

      appendRows(source, "(3, 'c', 'x')")
      val second = createTarget(source, target)
      second.queryId should not be first.queryId
      process(second)
      assertBagEqual(target, s"SELECT id, value, part FROM $source")
      shown(target).map(_.queryId) shouldBe Seq(second.queryId)
    }

    it("removes the residue of a first CREATE interrupted before its catalog commit") {
      val source = "strt_interrupted_commit_source"
      val target = "strt_interrupted_commit_target"
      createSource(source)
      appendRows(source, "(1, 'a', 'x')")
      StreamingTableMetadata.setAfterTargetCreatedHookForTesting { _ =>
        forgetCatalogEntry(target)
        throw new IllegalStateException("interrupted before the catalog commit")
      }
      try {
        val failure = intercept[Exception](createTarget(source, target))
        failure.getMessage should include("interrupted before the catalog commit")
      } finally StreamingTableMetadata.setAfterTargetCreatedHookForTesting(_ => ())

      spark.catalog.tableExists(target) shouldBe false
      pathExists(plannedLocation(target)) shouldBe false
      shown(target) shouldBe empty

      val created = createTarget(source, target)
      process(created)
      appendRows(source, "(2, 'b', 'y')")
      process(created)
      assertBagEqual(target, s"SELECT id, value, part FROM $source")
    }

    it("replaces residue left without a catalog table by an earlier session") {
      val source = "strt_orphan_residue_source"
      val target = "strt_orphan_residue_target"
      createSource(source)
      appendRows(source, "(1, 'a', 'x')")
      val first = createTarget(source, target)
      process(first)
      StreamingTableManager.stop(spark, Seq(target)).status shouldBe "stopped"
      forgetCatalogEntry(target)
      val residue = StreamingTableRegistry.registeredTargets(spark).filter(_.name.lastOption.contains(target))
      residue.foreach(stale => StreamingTableRegistry.forget(spark, StreamingTableRegistry.targetKey(stale)))
      pathExists(plannedLocation(target)) shouldBe true

      val recreated = createTarget(source, target)
      recreated.queryId should not be first.queryId
      process(recreated)
      assertBagEqual(target, s"SELECT id, value, part FROM $source")
    }

    it("refuses to replace a Delta table it did not create at the target path") {
      val source = "strt_unowned_residue_source"
      val target = "strt_unowned_residue_target"
      createSource(source)
      val location = plannedLocation(target)
      spark.range(3).write.format("delta").save(location)

      val failure = intercept[AnalysisException](createTarget(source, target))
      failure.getMessage should include("did not create")
      spark.read.format("delta").load(location).count() shouldBe 3L
      spark.catalog.tableExists(target) shouldBe false
    }
  }
}
