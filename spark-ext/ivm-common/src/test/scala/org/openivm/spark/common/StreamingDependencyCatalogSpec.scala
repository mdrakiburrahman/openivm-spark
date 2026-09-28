package org.openivm.spark.common

import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.rocksdb.{OpenIvmRocksDBBatchOps, OpenIvmRocksDBRegistry, RocksDBCodec}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

class StreamingDependencyCatalogSpec extends AnyFunSpec with BeforeAndAfterAll with Matchers {

  private val warehouse =
    new File(s"target/test-streaming-dependencies-${UUID.randomUUID().toString.take(8)}")
  private var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    warehouse.mkdirs()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("StreamingDependencyCatalogSpec")
      .config("spark.sql.warehouse.dir", warehouse.getAbsolutePath)
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override protected def afterAll(): Unit =
    try {
      if (spark != null) {
        OpenIvmRocksDBRegistry.closeAllForSparkContext(spark.sparkContext.applicationId)
        spark.stop()
      }
      deleteRecursively(warehouse)
    } finally {
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      super.afterAll()
    }

  describe("StreamingDependencyCatalog") {
    it("atomically publishes, replaces, and removes reverse dependency edges") {
      val parentOne = target("parent-one")
      val parentTwo = target("parent-two")
      val child = target("child").copy(
        sources = Seq(source(parentOne))
      )
      StreamingDependencyCatalog.publish(spark, parentOne)
      StreamingDependencyCatalog.publish(spark, parentTwo)
      StreamingDependencyCatalog.publish(spark, child)

      StreamingDependencyCatalog.lookup(spark, child.identity) shouldBe Some(child)
      StreamingDependencyCatalog.directChildren(spark, parentOne.identity) shouldBe Seq(child)

      val replacement = child.copy(sources = Seq(source(parentTwo)))
      StreamingDependencyCatalog.publish(spark, replacement)

      StreamingDependencyCatalog.directChildren(spark, parentOne.identity) shouldBe empty
      StreamingDependencyCatalog.directChildren(spark, parentTwo.identity) shouldBe Seq(replacement)

      StreamingDependencyCatalog.remove(spark, child.identity)
      StreamingDependencyCatalog.lookup(spark, child.identity) shouldBe None
      StreamingDependencyCatalog.directChildren(spark, parentTwo.identity) shouldBe empty
    }

    it("persists dependency records across a registry close and reopen") {
      val parent = target("persist-parent")
      val child  = target("persist-child").copy(sources = Seq(source(parent)))
      StreamingDependencyCatalog.publish(spark, parent)
      StreamingDependencyCatalog.publish(spark, child)

      OpenIvmRocksDBRegistry.closeAllForSparkContext(spark.sparkContext.applicationId)

      StreamingDependencyCatalog.lookup(spark, child.identity) shouldBe Some(child)
      StreamingDependencyCatalog.directChildren(spark, parent.identity) shouldBe Seq(child)
    }

    it("fails closed when a reverse edge references a missing child record") {
      val parent = target("corrupt-parent")
      val child  = target("corrupt-child").copy(sources = Seq(source(parent)))
      StreamingDependencyCatalog.publish(spark, parent)
      StreamingDependencyCatalog.publish(spark, child)

      val db = OpenIvmRocksDBRegistry.getOrOpen(
        spark,
        OpenIvmStatePaths.streamingDependencyDbPath(spark),
        Seq("targets", "children")
      )
      db.withBatch { batch =>
        OpenIvmRocksDBBatchOps.delete(
          db,
          batch,
          "targets",
          RocksDBCodec.utf8(child.identity)
        )
      }

      an[org.apache.spark.sql.AnalysisException] should be thrownBy {
        StreamingDependencyCatalog.directChildren(spark, parent.identity)
      }
    }
  }

  private def target(identity: String): StreamingDependencyTarget =
    StreamingDependencyTarget(
      identity = identity,
      name = Seq("spark_catalog", "default", identity),
      dataPath = new File(warehouse, identity).getAbsolutePath,
      deltaTableId = s"delta-$identity",
      tableId = s"table-$identity",
      definitionHash = "a" * 64,
      formatVersion = 1,
      sources = Seq.empty
    )

  private def source(parent: StreamingDependencyTarget): StreamingDependencySource =
    StreamingDependencySource(
      parentIdentity = parent.identity,
      parentPath = parent.dataPath,
      parentDeltaTableId = parent.deltaTableId,
      parentTableId = parent.tableId,
      formatVersion = parent.formatVersion
    )

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
    file.delete()
    ()
  }
}
