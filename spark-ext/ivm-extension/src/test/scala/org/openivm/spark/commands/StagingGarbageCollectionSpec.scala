package org.openivm.spark.commands

import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.{FeatureGate, MvCatalog, OpenIvmGarbageCollector, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.Files
import java.util.UUID
import scala.jdk.CollectionConverters._

/** Staging storage is reclaimed once no durable state references it, and never before. */
class StagingGarbageCollectionSpec extends AnyFunSpec with BeforeAndAfterAll with Matchers {
  private var spark: SparkSession = _
  private val warehouse           = new File(s"target/test-warehouse-gc-${UUID.randomUUID().toString.take(8)}")
  private val DayMs               = 24L * 60L * 60L * 1000L

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-StagingGarbageCollectionSpec")
      .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension,org.openivm.spark.OpenIvmSparkExtensions"
      )
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.openivm.enabled", "true")
      .config("spark.sql.warehouse.dir", warehouse.getAbsolutePath)
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
    MvCatalog.ensureTables(spark)
    StagingCatalog.ensureTables(spark)
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    if (warehouse.exists()) {
      val paths = Files.walk(warehouse.toPath)
      try paths.iterator().asScala.toSeq.reverse.foreach(Files.delete)
      finally paths.close()
    }
  }

  private def stagedPaths(table: String): Seq[String] =
    StagingCatalog.collectFor(spark, "__probe__", Seq(s"default.$table")).map(_.stagingPath)

  private def localPath(location: String): File =
    if (location.startsWith("file:")) new File(new java.net.URI(location).getPath) else new File(location)

  /** Write a small Delta table at `location` and age it and its direct children by `ageMs`. */
  private def writeLocation(location: String, ageMs: Long): Unit = {
    spark.range(1).write.format("delta").save(location)
    val stamp = System.currentTimeMillis() - ageMs
    val dir   = localPath(location)
    (dir +: Option(dir.listFiles()).toSeq.flatten).foreach(_.setLastModified(stamp))
  }

  describe("staging garbage collection") {
    it("deletes a staging location once every dependent consumed it") {
      spark.sql("CREATE TABLE gc_src (id INT, name STRING) USING delta")
      spark.sql("INSERT INTO gc_src VALUES (1, 'a')")
      spark.sql("CREATE MATERIALIZED VIEW gc_proj AS SELECT id, name FROM gc_src")
      spark.sql("CREATE MATERIALIZED VIEW gc_agg AS SELECT name, count(*) AS n FROM gc_src GROUP BY name")
      spark.sql("INSERT INTO gc_src VALUES (2, 'b'), (3, 'a')")
      val staged = stagedPaths("gc_src")
      staged should not be empty
      all(staged.map(localPath(_).exists())) shouldBe true

      spark.sql("REFRESH MATERIALIZED VIEW gc_proj")
      all(staged.map(localPath(_).exists())) shouldBe true

      spark.sql("REFRESH MATERIALIZED VIEW gc_agg")
      staged.filter(localPath(_).exists()) shouldBe empty
      StagingCatalog.pendingGarbage(spark) shouldBe empty
      spark.table("gc_agg").where("name = 'a'").select("n").head().getLong(0) shouldBe 2L
    }

    it("deletes the view deltas of a view without consumers") {
      spark.sql("CREATE TABLE gc_leaf_src (id INT, name STRING) USING delta")
      spark.sql("INSERT INTO gc_leaf_src VALUES (1, 'a')")
      spark.sql("CREATE MATERIALIZED VIEW gc_leaf AS SELECT name, count(*) AS n FROM gc_leaf_src GROUP BY name")
      Seq("(2, 'b')", "(3, 'a')").foreach { rows =>
        spark.sql(s"INSERT INTO gc_leaf_src VALUES $rows")
        spark.sql("REFRESH MATERIALIZED VIEW gc_leaf")
      }
      val namespaces = Option(new File(warehouse, "_ivm/view_deltas").listFiles()).toSeq.flatten
      namespaces
        .filter(_.getName.contains("gc_leaf"))
        .flatMap(dir => Option(dir.listFiles()).toSeq.flatten) shouldBe empty
      StagingCatalog.pendingGarbage(spark) shouldBe empty
      spark.table("gc_leaf").where("name = 'a'").select("n").head().getLong(0) shouldBe 2L
    }

    it("sweeps unreferenced staging locations only after the grace period") {
      val root       = s"${warehouse.getAbsolutePath}/_ivm/staging/default_gc_orphan/INSERT"
      val abandoned  = s"$root/abandoned-${UUID.randomUUID()}"
      val inFlight   = s"$root/in-flight-${UUID.randomUUID()}"
      val viewDelta  = s"${warehouse.getAbsolutePath}/_ivm/view_deltas/default_gc_orphan/${UUID.randomUUID()}"
      val consumable = s"${warehouse.getAbsolutePath}/_openivm/triggers/default_gc_orphan/${UUID.randomUUID()}"
      writeLocation(abandoned, 2 * DayMs)
      writeLocation(inFlight, 0L)
      writeLocation(viewDelta, 2 * DayMs)
      writeLocation(consumable, 2 * DayMs)
      val plan = OpenIvmGarbageCollector.prepare(spark).get
      OpenIvmGarbageCollector.sweepNow(
        spark,
        plan.copy(referenced = plan.referenced + consumable),
        System.currentTimeMillis()
      )
      localPath(abandoned).exists() shouldBe false
      localPath(viewDelta).exists() shouldBe false
      localPath(inFlight).exists() shouldBe true
      localPath(consumable).exists() shouldBe true
    }

    it("keeps locations that published staging records reference") {
      spark.sql("CREATE TABLE gc_held (id INT) USING delta")
      spark.sql("CREATE MATERIALIZED VIEW gc_held_mv AS SELECT id FROM gc_held")
      spark.sql("INSERT INTO gc_held VALUES (1)")
      val staged = stagedPaths("gc_held")
      staged.foreach { location =>
        val stamp = System.currentTimeMillis() - 2 * DayMs
        val dir   = localPath(location)
        (dir +: Option(dir.listFiles()).toSeq.flatten).foreach(_.setLastModified(stamp))
      }
      OpenIvmGarbageCollector.sweepNow(spark, OpenIvmGarbageCollector.prepare(spark).get, System.currentTimeMillis())
      all(staged.map(localPath(_).exists())) shouldBe true
      spark.sql("REFRESH MATERIALIZED VIEW gc_held_mv")
      spark.table("gc_held_mv").count() shouldBe 1L
    }

    it("expires mirrored telemetry spans") {
      val telemetry = new File(warehouse, s"telemetry-${UUID.randomUUID().toString.take(8)}")
      telemetry.mkdirs()
      val expired = new File(telemetry, "old__span.jsonl")
      val current = new File(telemetry, "new__span.jsonl")
      Files.write(expired.toPath, "{}".getBytes)
      Files.write(current.toPath, "{}".getBytes)
      expired.setLastModified(System.currentTimeMillis() - 8 * DayMs)
      spark.conf.set(FeatureGate.OneLakeTelemetryDirKey, telemetry.toURI.toString)
      try
        OpenIvmGarbageCollector.sweepNow(
          spark,
          OpenIvmGarbageCollector.Plan(Set.empty, Set.empty),
          System.currentTimeMillis()
        )
      finally spark.conf.unset(FeatureGate.OneLakeTelemetryDirKey)
      expired.exists() shouldBe false
      current.exists() shouldBe true
    }
  }
}
