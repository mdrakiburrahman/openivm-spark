package org.openivm.spark.commands

import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.{FeatureGate, MvCatalog, OpenIvmGarbageCollector, StagingCatalog}
import org.openivm.spark.common.rocksdb.{OpenIvmRocksDBRegistry, OpenIvmStateSync}
import org.openivm.spark.common.StagingDelta
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

import java.io.File
import java.nio.file.Files
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.lang.management.ManagementFactory
import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.jdk.CollectionConverters._

/** Staging storage is reclaimed once no durable state references it, and never before. */
class StagingGarbageCollectionSpec extends AnyFunSpec with BeforeAndAfterAll with Matchers with Eventually {
  private var spark: SparkSession = _
  private val warehouse           = new File(s"target/test-warehouse-gc-${UUID.randomUUID().toString.take(8)}")
  private val DayMs               = 24L * 60L * 60L * 1000L
  private val GcScanTiming        = new org.scalatest.Tag("org.openivm.spark.commands.GcScanTiming")

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
    OpenIvmStateSync.setBackupPassHookForTesting(null)
    OpenIvmStateSync.setStateSyncUriHookForTesting(null)
    if (spark != null) spark.stop()
    if (warehouse.exists()) {
      val paths = Files.walk(warehouse.toPath)
      try paths.iterator().asScala.toSeq.reverse.foreach(Files.delete)
      finally paths.close()
    }
  }

  private def runRecoveryInSecondJvm(
      stateUri: String,
      warehousePath: String,
      statePath: String,
      tombstonePath: String,
      livePaths: Seq[String]
  ): Unit = {
    val java = new File(System.getProperty("java.home"), "bin/java").getAbsolutePath
    val moduleArgs = ManagementFactory.getRuntimeMXBean.getInputArguments.asScala.filter { arg =>
      arg.startsWith("--add-opens=") || arg.startsWith("--add-exports=")
    }
    val command = Seq(java, "-Xmx2g") ++ moduleArgs ++ Seq(
      "-cp",
      System.getProperty("java.class.path"),
      "org.openivm.spark.commands.StagingGarbageCollectionRecoveryChild",
      stateUri,
      warehousePath,
      statePath,
      tombstonePath
    ) ++ livePaths
    val log = new File(warehouse, s"gc-recovery-child-${UUID.randomUUID().toString.take(8)}.log")
    val process = new ProcessBuilder(command: _*)
      .directory(new File(System.getProperty("user.dir")))
      .redirectErrorStream(true)
      .redirectOutput(log)
      .start()
    val completed = process.waitFor(240, TimeUnit.SECONDS)
    if (!completed) {
      process.destroyForcibly()
      fail(s"second JVM did not finish; see ${log.getAbsolutePath}")
    }
    val output = if (log.exists()) new String(Files.readAllBytes(log.toPath), StandardCharsets.UTF_8) else ""
    withClue(s"second JVM exit=${process.exitValue()} output:\n$output") {
      process.exitValue() shouldBe 0
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
    it("skips cached empty inventories when a refresh sweep is not due", GcScanTiming) {
      val source = s"default.gc_scan_${UUID.randomUUID().toString.replace('-', '_')}"
      val path   = s"${warehouse.getAbsolutePath}/_ivm/staging/${source.replace('.', '_')}/INSERT/${UUID.randomUUID()}"
      StagingCatalog.record(
        spark,
        StagingDelta(
          source,
          StagingDelta.OpTypes.Insert,
          path,
          new java.sql.Timestamp(System.currentTimeMillis()),
          Seq.empty
        )
      )
      val dbCount = 1

      val scanCounts = scala.collection.mutable.Map.empty[String, Int].withDefaultValue(0)
      StagingCatalog.setGcScanHookForTesting(kind => scanCounts.update(kind, scanCounts(kind) + 1))
      def measured(sweepDue: Boolean): (Option[StagingCatalog.GarbageCollectionPaths], Map[String, Int], Long) = {
        scanCounts.clear()
        val started = System.nanoTime()
        val result  = StagingCatalog.garbageCollectionPaths(spark, sweepDue)
        (result, scanCounts.toMap, System.nanoTime() - started)
      }
      try {
        val controlStarted = System.nanoTime()
        StagingCatalog.pendingGarbage(spark) shouldBe empty
        StagingCatalog.referencedStagingPaths(spark) should contain(path)
        val controlNs  = System.nanoTime() - controlStarted
        val coldNotDue = measured(sweepDue = false)
        coldNotDue._1 shouldBe None
        coldNotDue._2 shouldBe Map("tombstone" -> dbCount)
        val warmNotDue = measured(sweepDue = false)
        warmNotDue._1 shouldBe None
        warmNotDue._2 shouldBe empty
        val due = measured(sweepDue = true)
        due._1.get.tombstoned shouldBe empty
        due._1.get.referenced should contain(path)
        due._2 shouldBe Map("tombstone" -> dbCount, "referenced" -> dbCount)

        def scansJson(scans: Map[String, Int]): String =
          s"""{"tombstone":${scans.getOrElse("tombstone", 0)},"referenced":${scans.getOrElse("referenced", 0)}}"""
        val report =
          s"""{"base_commit":"38f571cc1c4c5d1b14df1cdc6499aa26940f10bd","base_db_count":$dbCount,"control_separate_scans_ns":$controlNs,"control_tombstone_scans":$dbCount,"control_reference_scans":$dbCount,"candidate_cold_not_due_scans":${scansJson(
              coldNotDue._2
            )},"candidate_cold_not_due_ns":${coldNotDue._3},"candidate_warm_not_due_scans":${scansJson(
              warmNotDue._2
            )},"candidate_warm_not_due_ns":${warmNotDue._3},"candidate_due_scans":${scansJson(
              due._2
            )},"candidate_due_ns":${due._3}}"""
        val reportFile = new File("target/gc-no-sweep-observations.json")
        Files.write(reportFile.toPath, report.getBytes(StandardCharsets.UTF_8))
      } finally StagingCatalog.setGcScanHookForTesting(null)
    }

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

    it("keeps consumed staging files until the default asynchronous backup succeeds") {
      spark.sql("CREATE TABLE gc_async_src (id INT) USING delta")
      spark.sql("CREATE MATERIALIZED VIEW gc_async_mv AS SELECT id FROM gc_async_src")
      spark.sql("INSERT INTO gc_async_src VALUES (1)")
      val staged = stagedPaths("gc_async_src")
      staged should not be empty
      val entered  = new CountDownLatch(1)
      val release  = new CountDownLatch(1)
      val finished = new CountDownLatch(1)
      spark.conf.getOption(FeatureGate.StateSyncAsyncEnabledKey) shouldBe None
      FeatureGate.stateSyncAsyncEnabled(spark) shouldBe true
      OpenIvmStateSync.setStateSyncUriHookForTesting(_ => Some("file:/state-sync-gc-async"))
      OpenIvmStateSync.setBackupPassHookForTesting { (_, _, _) =>
        entered.countDown()
        try release.await(60, TimeUnit.SECONDS) shouldBe true
        finally finished.countDown()
      }
      try {
        spark.sql("REFRESH MATERIALIZED VIEW gc_async_mv").collect()
        entered.await(30, TimeUnit.SECONDS) shouldBe true
        all(staged.map(localPath(_).exists())) shouldBe true
        spark.table("gc_async_mv").count() shouldBe 1L
        release.countDown()
        eventually(timeout(Span(30, Seconds)), interval(Span(20, Millis))) {
          staged.filter(localPath(_).exists()) shouldBe empty
          StagingCatalog.pendingGarbage(spark).intersect(staged.toSet) shouldBe empty
        }
      } finally {
        release.countDown()
        try finished.await(60, TimeUnit.SECONDS) shouldBe true
        finally {
          OpenIvmStateSync.setBackupPassHookForTesting(null)
          OpenIvmStateSync.setStateSyncUriHookForTesting(null)
        }
      }
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
    it("recovers acknowledged pending garbage in a second JVM and cleans only eligible paths") {
      val source = s"default.gc_retry_${UUID.randomUUID().toString.replace('-', '_')}"
      val stagingPath = new File(
        warehouse,
        s"_ivm/staging/${source.replace('.', '_')}/INSERT/${UUID.randomUUID()}"
      )
      Files.createDirectories(stagingPath.toPath)
      Files.write(stagingPath.toPath.resolve("marker"), Array[Byte](1, 2, 3))
      StagingCatalog.record(
        spark,
        StagingDelta(
          source,
          StagingDelta.OpTypes.Insert,
          stagingPath.getAbsolutePath,
          new java.sql.Timestamp(System.currentTimeMillis()),
          Seq.empty
        )
      )
      StagingCatalog.pruneUnconsumed(spark, source)
      StagingCatalog.pendingGarbage(spark) should contain(stagingPath.getAbsolutePath)

      val liveSuffix = UUID.randomUUID().toString.replace('-', '_')
      val liveSource = s"gc_cross_live_$liveSuffix"
      spark.sql(s"CREATE TABLE $liveSource (id INT) USING delta")
      spark.sql(s"CREATE MATERIALIZED VIEW ${liveSource}_mv AS SELECT id FROM $liveSource")
      spark.sql(s"INSERT INTO $liveSource VALUES (1)")
      val livePaths = stagedPaths(liveSource)
      livePaths should not be empty
      all(livePaths.map(localPath(_).exists())) shouldBe true

      val remote         = new File(warehouse, s"state-sync-gc-retry-${UUID.randomUUID()}")
      val recoveredState = new File(warehouse, s"_recovered_openivm_${UUID.randomUUID().toString.take(8)}")
      OpenIvmStateSync.setStateSyncUriHookForTesting(_ => Some(remote.toURI.toString))
      spark.conf.set(FeatureGate.StateSyncAsyncEnabledKey, "false")
      val failedPlan = OpenIvmGarbageCollector.prepareForRefresh(spark).get
      OpenIvmStateSync.setBackupPassHookForTesting { (_, _, _) =>
        throw new java.io.IOException("injected GC-state publication failure")
      }
      try {
        intercept[java.io.IOException] {
          OpenIvmStateSync.backupForCommand(
            spark,
            () => OpenIvmGarbageCollector.collect(spark, Some(failedPlan))
          )
        }.getMessage shouldBe "injected GC-state publication failure"
        stagingPath.exists() shouldBe true
        StagingCatalog.pendingGarbage(spark) should contain(stagingPath.getAbsolutePath)
        all(livePaths.map(localPath(_).exists())) shouldBe true
      } finally OpenIvmStateSync.setBackupPassHookForTesting(null)

      OpenIvmStateSync.backupNow(spark)
      stagingPath.exists() shouldBe true
      StagingCatalog.pendingGarbage(spark) should contain(stagingPath.getAbsolutePath)
      all(livePaths.map(localPath(_).exists())) shouldBe true

      OpenIvmRocksDBRegistry.closeAll()
      spark.stop()
      spark = null
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      runRecoveryInSecondJvm(
        remote.toURI.toString,
        warehouse.getAbsolutePath,
        recoveredState.getAbsolutePath,
        stagingPath.getAbsolutePath,
        livePaths
      )
      stagingPath.exists() shouldBe false
      all(livePaths.map(localPath(_).exists())) shouldBe true

      spark = SparkSession
        .builder()
        .master("local[1]")
        .appName("openivm-spark-StagingGarbageCollectionSpec-resumed")
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
      OpenIvmStateSync.setStateSyncUriHookForTesting(null)
    }

  }
}

private[commands] object StagingGarbageCollectionRecoveryChild {
  private def localFile(location: String): File =
    if (location.startsWith("file:")) new File(new java.net.URI(location).getPath) else new File(location)

  def main(args: Array[String]): Unit = {
    require(
      args.length >= 5,
      s"expected state URI, warehouse, fresh state path, tombstone, and live paths; got ${args.length}"
    )
    val Array(stateUri, warehousePath, statePath, tombstonePath) = args.take(4)
    val livePaths                                                = args.drop(4).toSeq
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-StagingGarbageCollectionRecoveryChild")
      .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension,org.openivm.spark.OpenIvmSparkExtensions"
      )
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.openivm.enabled", "true")
      .config("spark.sql.warehouse.dir", warehousePath)
      .config(FeatureGate.StatePathKey, statePath)
      .config(FeatureGate.StateSyncUriKey, stateUri)
      .config(FeatureGate.StateSyncAsyncEnabledKey, "false")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
    try {
      OpenIvmStateSync.maybeRestore(spark)
      MvCatalog.ensureTables(spark)
      StagingCatalog.ensureTables(spark)
      val plan = OpenIvmGarbageCollector
        .prepareForRefresh(spark)
        .getOrElse(
          throw new AssertionError("restored pending garbage did not produce a GC plan")
        )
      require(plan.tombstoned.contains(tombstonePath), "restored snapshot did not contain the pending tombstone")
      require(
        livePaths.forall(plan.referenced.contains),
        "restored snapshot did not preserve every live staging reference"
      )
      require(
        localFile(tombstonePath).exists(),
        "eligible path was deleted before the child JVM committed its restored state"
      )
      require(
        livePaths.forall(path => localFile(path).exists()),
        "a referenced path disappeared before recovery cleanup"
      )

      OpenIvmStateSync.backupNow(spark)
      OpenIvmGarbageCollector.collect(spark, Some(plan))
      require(!localFile(tombstonePath).exists(), "committed eligible tombstone was not cleaned")
      require(livePaths.forall(path => localFile(path).exists()), "GC deleted a path still referenced by live state")
      require(!StagingCatalog.pendingGarbage(spark).contains(tombstonePath), "cleaned tombstone remained pending")
    } finally {
      OpenIvmRocksDBRegistry.closeAll()
      spark.stop()
    }
  }
}
