package org.openivm.spark.commands

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.openivm.spark.common.rocksdb.OpenIvmRocksDBRegistry
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.{Files, Path, StandardCopyOption}
import java.util.UUID
import scala.jdk.CollectionConverters._

/** A restarted driver restores the newest published state snapshot, which can lag committed source
  * and materialized-view data. Copies of the local state stand in for published snapshots, so
  * garbage collection, which treats unsynchronized local state as durable, is off.
  */
class StateRestoreConsistencySpec extends AnyFunSpec with BeforeAndAfterAll with Matchers {
  private var spark: SparkSession = _
  private val root                = new File(s"target/test-state-restore-${UUID.randomUUID().toString.take(8)}")
  private val warehouse           = new File(root, "warehouse")
  private val state               = new File(root, "state")

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-StateRestoreConsistencySpec")
      .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension,org.openivm.spark.OpenIvmSparkExtensions"
      )
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.openivm.enabled", "true")
      .config("spark.openivm.changeFeed.mode", "intercept")
      .config("spark.openivm.statePath", state.getAbsolutePath)
      .config("spark.openivm.gc.enabled", "false")
      .config("spark.sql.warehouse.dir", warehouse.getAbsolutePath)
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
    MvCatalog.ensureTables(spark)
    StagingCatalog.ensureTables(spark)
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    delete(root.toPath)
  }

  private def delete(path: Path): Unit =
    if (Files.exists(path)) {
      val paths = Files.walk(path)
      try paths.iterator().asScala.toSeq.reverse.foreach(Files.delete)
      finally paths.close()
    }

  private def copyTree(from: Path, to: Path): Unit = {
    val paths = Files.walk(from)
    try
      paths.iterator().asScala.foreach { source =>
        val target = to.resolve(from.relativize(source).toString)
        if (Files.isDirectory(source)) Files.createDirectories(target)
        else Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING)
      }
    finally paths.close()
  }

  private def stateTree: Path = new File(state, "_openivm").toPath

  /** Copy of the local state tree, equivalent to a published state-sync generation. */
  private def publishGeneration(): Path = {
    val generation = new File(root, s"generation-${UUID.randomUUID().toString.take(8)}").toPath
    copyTree(stateTree, generation)
    generation
  }

  /** Driver loss followed by a fresh driver restoring `generation`. */
  private def crashAndRestore(generation: Path): Unit = {
    OpenIvmRocksDBRegistry.closeAll()
    spark.catalog.clearCache()
    spark.catalog.listTables("global_temp").collect().foreach(table => spark.catalog.dropGlobalTempView(table.name))
    delete(stateTree)
    copyTree(generation, stateTree)
  }

  private def mismatch(actual: DataFrame, expected: DataFrame): String = {
    val missing = expected.exceptAll(actual).collect().mkString(", ")
    val extra   = actual.exceptAll(expected).collect().mkString(", ")
    if (missing.isEmpty && extra.isEmpty) "" else s"missing=[$missing] extra=[$extra]"
  }

  private def assertMvsMatch(source: String, projection: String, aggregate: String): Unit = {
    spark.sql(s"REFRESH MATERIALIZED VIEW $projection")
    spark.sql(s"REFRESH MATERIALIZED VIEW $aggregate")
    val projectionDiff =
      mismatch(spark.table(projection).select("id", "name"), spark.sql(s"SELECT id, name FROM $source"))
    val aggregateDiff = mismatch(
      spark.table(aggregate).select("name", "n"),
      spark.sql(s"SELECT name, count(*) AS n FROM $source GROUP BY name")
    )
    withClue(s"projection: $projectionDiff; aggregate: $aggregateDiff") {
      projectionDiff shouldBe ""
      aggregateDiff shouldBe ""
    }
  }

  private def createFixture(prefix: String): (String, String, String) = {
    val source     = s"${prefix}_src"
    val projection = s"${prefix}_proj"
    val aggregate  = s"${prefix}_agg"
    spark.sql(s"CREATE TABLE $source (id INT, name STRING) USING delta")
    spark.sql(s"INSERT INTO $source VALUES (1, 'a'), (2, 'b')")
    spark.sql(s"CREATE MATERIALIZED VIEW $projection AS SELECT id, name FROM $source")
    spark.sql(s"CREATE MATERIALIZED VIEW $aggregate AS SELECT name, count(*) AS n FROM $source GROUP BY name")
    (source, projection, aggregate)
  }

  private def assertBagEqual(actual: DataFrame, expected: DataFrame): Unit = {
    val diff = mismatch(actual, expected)
    withClue(diff)(diff shouldBe "")
  }

  describe("a restored state generation that lags committed data") {
    it("keeps source changes staged after the last published generation") {
      val (source, projection, aggregate) = createFixture("lost")
      val generation                      = publishGeneration()
      spark.sql(s"INSERT INTO $source VALUES (3, 'c'), (4, 'a')")
      crashAndRestore(generation)
      assertMvsMatch(source, projection, aggregate)
    }

    it("does not re-apply changes an MV committed after the last published generation") {
      val (source, projection, aggregate) = createFixture("replay")
      spark.sql(s"INSERT INTO $source VALUES (3, 'c'), (4, 'a')")
      val generation = publishGeneration()
      spark.sql(s"REFRESH MATERIALIZED VIEW $projection")
      spark.sql(s"REFRESH MATERIALIZED VIEW $aggregate")
      crashAndRestore(generation)
      assertMvsMatch(source, projection, aggregate)
    }

    it("cascades a lagging upstream refresh to downstream views from their recorded version") {
      val source   = "cascade_src"
      val upstream = "cascade_proj"
      val leaf     = "cascade_leaf"
      spark.sql(s"CREATE TABLE $source (id INT, name STRING) USING delta")
      spark.sql(s"INSERT INTO $source VALUES (1, 'a'), (2, 'b')")
      spark.sql(s"CREATE MATERIALIZED VIEW $upstream AS SELECT id, name FROM $source")
      spark.sql(s"CREATE MATERIALIZED VIEW $leaf AS SELECT name, count(*) AS n FROM $upstream GROUP BY name")
      spark.sql(s"INSERT INTO $source VALUES (3, 'c'), (4, 'a')")
      val generation = publishGeneration()
      spark.sql(s"REFRESH MATERIALIZED VIEW $upstream")
      crashAndRestore(generation)
      spark.sql(s"DELETE FROM $source WHERE id = 2")
      spark.sql(s"REFRESH MATERIALIZED VIEW $upstream")
      spark.sql(s"REFRESH MATERIALIZED VIEW $leaf")
      assertBagEqual(spark.table(upstream).select("id", "name"), spark.sql(s"SELECT id, name FROM $source"))
      assertBagEqual(
        spark.table(leaf).select("name", "n"),
        spark.sql(s"SELECT name, count(*) AS n FROM $source GROUP BY name")
      )
    }

    it("recomputes every lagging view of a cascade") {
      val source   = "chain_src"
      val upstream = "chain_proj"
      val leaf     = "chain_leaf"
      spark.sql(s"CREATE TABLE $source (id INT, name STRING) USING delta")
      spark.sql(s"INSERT INTO $source VALUES (1, 'a'), (2, 'b')")
      spark.sql(s"CREATE MATERIALIZED VIEW $upstream AS SELECT id, name FROM $source")
      spark.sql(s"CREATE MATERIALIZED VIEW $leaf AS SELECT name, count(*) AS n FROM $upstream GROUP BY name")
      spark.sql(s"INSERT INTO $source VALUES (3, 'c'), (4, 'a')")
      val generation = publishGeneration()
      spark.sql(s"REFRESH MATERIALIZED VIEW $upstream")
      spark.sql(s"REFRESH MATERIALIZED VIEW $leaf")
      crashAndRestore(generation)
      spark.sql(s"REFRESH MATERIALIZED VIEW $upstream")
      spark.sql(s"REFRESH MATERIALIZED VIEW $leaf")
      assertBagEqual(
        spark.table(leaf).select("name", "n"),
        spark.sql(s"SELECT name, count(*) AS n FROM $source GROUP BY name")
      )
    }
  }
}
