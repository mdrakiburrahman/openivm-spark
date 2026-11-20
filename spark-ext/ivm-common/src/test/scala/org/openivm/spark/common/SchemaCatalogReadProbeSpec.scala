package org.openivm.spark.common

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.openivm.spark.common.rocksdb.{OpenIvmRocksDBRegistry, OpenIvmStateSync, RocksDBCodec}
import org.openivm.spark.telemetry.metrics.OpenIvmMetrics
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, StandardOpenOption}
import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.{Callable, CyclicBarrier, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicLong
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/** Diagnostic only: a counterexample must prevent adopting the point-read prototype. */
class SchemaCatalogReadProbeSpec extends AnyFunSpec with BeforeAndAfterAll with Matchers {
  private var spark: SparkSession = _
  private val root = new File(s"target/test-warehouse-schema-catalog-${UUID.randomUUID().toString.take(8)}")
  private val enumerations = new AtomicLong()
  private val validationNanos = new AtomicLong()
  private val evidence = new File(
    sys.env.getOrElse("OPENIVM_SCHEMA_PROBE_EVIDENCE", "../.logs/schema-catalog-probe/evidence.jsonl")
  ).toPath

  private def label(id: TableIdentifier): String = id.database.fold(id.table)(db => s"$db.${id.table}")

  private def metadata(name: String, identity: String = UUID.randomUUID().toString): MvMetadata =
    MvMetadata(
      CatalystSqlParser.parseTableIdentifier(name),
      "SELECT id FROM schema_catalog_probe_input",
      0,
      "SIMPLE_PROJECTION",
      0L,
      Seq("schema_catalog_probe_input"),
      "diagnostic",
      new File(root, name).getAbsolutePath,
      new Timestamp(1700000000000L),
      Map(MvMetadata.DefinitionIdentityKey -> identity)
    )

  private def record(values: (String, Any)*): Unit = synchronized {
    val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
    val node = mapper.createObjectNode()
    values.foreach {
      case (key, value: Long)    => node.put(key, value)
      case (key, value: Int)     => node.put(key, value)
      case (key, value: Boolean) => node.put(key, value)
      case (key, value)          => node.put(key, value.toString)
    }
    Option(evidence.getParent).foreach(Files.createDirectories(_))
    Files.write(
      evidence,
      (mapper.writeValueAsString(node) + "\n").getBytes(StandardCharsets.UTF_8),
      StandardOpenOption.CREATE,
      StandardOpenOption.APPEND
    )
    ()
  }

  private def full(session: SparkSession): Seq[MvMetadata] = {
    enumerations.incrementAndGet()
    // Exactly the defensive read used by allMvsCached in REFRESH.
    try MvCatalog.list(session)
    catch { case _: Throwable => Seq.empty }
  }

  private def fromAll(sources: Seq[String], rows: Seq[MvMetadata]): Map[String, String] = {
    val byName = rows.map(row => label(row.name) -> row).toMap
    sources.flatMap { source =>
      byName.get(source).orElse(byName.get(source.split("\\.").last)).map(row => source -> MvCatalog.mvIdentity(row))
    }.toMap
  }

  private def baseline(session: SparkSession, sources: Seq[String]): Map[String, String] =
    fromAll(sources, full(session))

  private def points(session: SparkSession, sources: Seq[String]): Map[String, String] = {
    val started = System.nanoTime()
    val selected =
      try {
        val keys = sources.flatMap(source => Seq(source, source.split("\\.").last)).distinct
        val rows = keys.flatMap { key =>
          MvCatalog.lookup(session, CatalystSqlParser.parseTableIdentifier(key)).map { row =>
            require(label(row.name) == key, "noncanonical catalog name")
            require(row.properties.get(MvMetadata.DefinitionIdentityKey).exists(_.nonEmpty), "legacy identity")
            key -> row
          }
        }.toMap
        if (sources.forall(source => rows.contains(source) || rows.contains(source.split("\\.").last)))
          Some(sources.flatMap { source =>
            rows.get(source).orElse(rows.get(source.split("\\.").last)).map(row => source -> MvCatalog.mvIdentity(row))
          }.toMap)
        else None
      } catch { case NonFatal(_) => None }
    validationNanos.addAndGet(System.nanoTime() - started)
    selected.getOrElse(baseline(session, sources))
  }

  private def schemaCommand(session: SparkSession, sources: Seq[String], enabled: Boolean): String = {
    val schemas = sources.map(source => source -> session.table(source).schema).toMap
    MvCatalog.schemaFingerprint(schemas, if (enabled) points(session, sources) else baseline(session, sources))
  }

  private def counter(name: String): Long = OpenIvmMetrics.counter(name).getCount

  override def beforeAll(): Unit = {
    root.mkdirs()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    spark = SparkSession.builder().master("local[1]").appName("SchemaCatalogReadProbeSpec")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", root.getAbsolutePath)
      .getOrCreate()
    MvCatalog.ensureTables(spark)
  }

  private def delete(path: Path): Unit = {
    if (Files.exists(path)) {
      val paths = Files.walk(path)
      try paths.iterator().asScala.toVector.reverse.foreach(Files.delete)
      finally paths.close()
    }
  }

  override def afterAll(): Unit = {
    try {
      if (spark != null) {
        OpenIvmRocksDBRegistry.closeAllForSparkContext(spark.sparkContext.applicationId)
        spark.stop()
      }
    } finally {
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      delete(root.toPath)
    }
  }

  it("measures four concurrent few-source schema commands against 200 actual catalog shards") {
    (0 until 200).foreach(index => MvCatalog.upsert(spark, metadata(s"scprobe.up_$index", s"identity-$index")))
    (0 until 4).foreach { index =>
      spark.sql(s"CREATE DATABASE IF NOT EXISTS scprobe")
      spark.sql(s"CREATE TABLE scprobe.up_$index (id INT) USING DELTA").collect()
    }
    OpenIvmStatePaths.existingMvDbPaths(spark).size shouldBe 200
    val pool = Executors.newFixedThreadPool(4)
    try {
      // Warm both variants, then counterbalance ABBA / BAAB on the same state and artifact.
      Seq(false, true).foreach(on => schemaCommand(spark, Seq("scprobe.up_0"), on))
      Seq(false, true, true, false, true, false, false, true).zipWithIndex.foreach { case (on, round) =>
        val gate = new CyclicBarrier(4)
        val fullBefore = enumerations.get()
        val validationBefore = validationNanos.get()
        val hitsBefore = counter("rocksdb.registry.get_or_open.hit")
        val missesBefore = counter("rocksdb.registry.get_or_open.miss")
        val multiBefore = counter("rocksdb.scope.mv.operation.multi_get.count")
        val opensBefore = counter("rocksdb.scope.mv.operation.session.native.open.count")
        val started = System.nanoTime()
        val futures = (0 until 4).map { index =>
          pool.submit(new Callable[(String, Long)] {
            override def call(): (String, Long) = {
              val session = spark.newSession()
              gate.await(30, TimeUnit.SECONDS)
              val commandStarted = System.nanoTime()
              val result = schemaCommand(session, Seq(s"scprobe.up_$index"), on)
              result -> (System.nanoTime() - commandStarted)
            }
          })
        }
        val results = futures.map(_.get(90, TimeUnit.SECONDS))
        val elapsed = System.nanoTime() - started
        results.map(_._1).distinct.size shouldBe 4
        record(
          "kind" -> "schema_command_batch", "round" -> round, "point_prototype" -> on,
          "shards" -> 200, "commands" -> 4, "full_enumerations" -> (enumerations.get() - fullBefore),
          "shard_get_or_open" -> (counter("rocksdb.registry.get_or_open.hit") - hitsBefore +
            counter("rocksdb.registry.get_or_open.miss") - missesBefore),
          "metadata_multi_get" -> (counter("rocksdb.scope.mv.operation.multi_get.count") - multiBefore),
          "native_opens" -> (counter("rocksdb.scope.mv.operation.session.native.open.count") - opensBefore),
          "batch_elapsed_ns" -> elapsed, "command_sum_ns" -> results.map(_._2).sum,
          "point_validation_sum_ns" -> (validationNanos.get() - validationBefore)
        )
      }
    } finally pool.shutdownNow()
  }

  it("preserves exact namespace precedence and the existing unqualified fallback, not physical aliases") {
    val short = metadata("scprobe_collision", "short")
    val left = metadata("scprobe_left.scprobe_collision", "left")
    val right = metadata("scprobe_right.scprobe_collision", "right")
    Seq(short, left, right).foreach(MvCatalog.upsert(spark, _))
    val sources = Seq("scprobe_left.scprobe_collision", "scprobe_right.scprobe_collision", "third.scprobe_collision")
    points(spark, sources) shouldBe baseline(spark, sources)
    points(spark, sources)("third.scprobe_collision") shouldBe "short"
    val physicalAlias = metadata("scprobe.physical_alias").copy(location = left.location)
    MvCatalog.upsert(spark, physicalAlias)
    baseline(spark, Seq("encoded.operational_alias")) shouldBe empty
    points(spark, Seq("encoded.operational_alias")) shouldBe empty
    record("kind" -> "negative", "case" -> "namespace_and_lexical_alias", "passed" -> true)
  }

  it("falls back for legacy definition identities and absent lookups without creating shards") {
    val legacy = metadata("scprobe.legacy").copy(properties = Map.empty)
    MvCatalog.upsert(spark, legacy)
    val before = enumerations.get()
    points(spark, Seq("scprobe.legacy")) shouldBe baseline(spark, Seq("scprobe.legacy"))
    enumerations.get() - before shouldBe 2L
    val paths = OpenIvmStatePaths.existingMvDbPaths(spark).toSet
    points(spark, Seq("scprobe.absent")) shouldBe empty
    OpenIvmStatePaths.existingMvDbPaths(spark).toSet shouldBe paths
    record("kind" -> "negative", "case" -> "legacy_and_missing", "passed" -> true)
  }

  it("revalidates drop/recreate, property mutation, and in-flight source-version advancement") {
    val initial = metadata("scprobe.recreated", "before")
    MvCatalog.upsert(spark, initial)
    points(spark, Seq("scprobe.recreated")) shouldBe Map("scprobe.recreated" -> "before")
    MvCatalog.remove(spark, initial.name)
    points(spark, Seq("scprobe.recreated")) shouldBe empty
    MvCatalog.upsert(spark, initial.copy(properties = Map(MvMetadata.DefinitionIdentityKey -> "after")))
    points(spark, Seq("scprobe.recreated")) shouldBe Map("scprobe.recreated" -> "after")
    MvCatalog.updateProperties(
      spark, initial.name, Map(MvMetadata.DefinitionIdentityKey -> "after", MvMetadata.SourceVersionAdvancePreMvVersionKey -> "0")
    )
    intercept[IllegalStateException](baseline(spark, Seq("scprobe.recreated")))
    intercept[IllegalStateException](points(spark, Seq("scprobe.recreated")))
    MvCatalog.updateProperties(spark, initial.name, Map(MvMetadata.DefinitionIdentityKey -> "after"))
    record("kind" -> "negative", "case" -> "recreate_and_pending_advance", "passed" -> true)
  }

  it("observes a completed mutation from a concurrent writer without a success cache") {
    val initial = metadata("scprobe.concurrent", "before")
    MvCatalog.upsert(spark, initial)
    val pool = Executors.newSingleThreadExecutor()
    try {
      (0 until 8).foreach { index =>
        val identity = s"generation-$index"
        pool.submit(new Callable[Unit] {
          override def call(): Unit =
            MvCatalog.upsert(spark, initial.copy(properties = Map(MvMetadata.DefinitionIdentityKey -> identity)))
        }).get(30, TimeUnit.SECONDS)
        points(spark, Seq("scprobe.concurrent")) shouldBe Map("scprobe.concurrent" -> identity)
      }
    } finally pool.shutdownNow()
    record("kind" -> "negative", "case" -> "concurrent_writer_visibility", "passed" -> true)
  }

  it("restores a fresh local tree from a real file-backed state-sync publication") {
    val source = spark.newSession()
    val local = new File(root, "restore-source")
    val remote = new File(root, "restore-publications")
    source.conf.set(FeatureGate.StatePathKey, local.getAbsolutePath)
    source.conf.set(FeatureGate.StateSyncUriKey, remote.toURI.toString)
    val expected = metadata("scprobe.restored", "restored")
    MvCatalog.ensureTables(source)
    MvCatalog.upsert(source, expected)
    OpenIvmStateSync.backupNow(source)
    val restored = spark.newSession()
    restored.conf.set(FeatureGate.StatePathKey, new File(root, "restore-fresh").getAbsolutePath)
    restored.conf.set(FeatureGate.StateSyncUriKey, remote.toURI.toString)
    MvCatalog.ensureTables(restored)
    MvCatalog.lookup(restored, expected.name) shouldBe Some(expected)
    points(restored, Seq("scprobe.restored")) shouldBe baseline(restored, Seq("scprobe.restored"))
    record("kind" -> "negative", "case" -> "fresh_restore", "passed" -> true)
  }

  it("rejects point-read adoption when an unrelated failed shard changes the schema gate result") {
    val sources = Seq("scprobe.up_0")
    val expected = baseline(spark, sources)
    expected shouldBe Map("scprobe.up_0" -> "identity-0")
    val broken = new File(OpenIvmStatePaths.perMvDbPath(spark, "scprobe.unreadable")).toPath
    Files.createDirectories(broken)
    Files.write(broken.resolve("CURRENT"), RocksDBCodec.utf8("MANIFEST-DOES-NOT-EXIST\n"))
    try {
      val off = baseline(spark, sources)
      val on = points(spark, sources)
      off shouldBe empty
      on shouldBe expected
      val schemas = Map("scprobe.up_0" -> StructType(Seq(StructField("id", IntegerType))))
      val offFingerprint = MvCatalog.schemaFingerprint(schemas, off)
      val onFingerprint = MvCatalog.schemaFingerprint(schemas, on)
      offFingerprint should not be onFingerprint
      record(
        "kind" -> "counterexample", "case" -> "unrelated_failed_shard",
        "baseline_identity_count" -> off.size, "point_identity_count" -> on.size,
        "same_schema_fingerprint" -> false, "production_optimization_adopted" -> false
      )
    } finally {
      OpenIvmRocksDBRegistry.closeAndDelete(broken.toString)(_ => delete(broken))
    }
  }
}
