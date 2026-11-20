package org.openivm.spark.common.rocksdb

import java.io.IOException
import java.lang.management.ManagementFactory
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit
import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.{CdfWatermarkCatalog, FeatureGate}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import scala.jdk.CollectionConverters._

private[rocksdb] object GuardedSqlPersistenceProbe {
  def main(args: Array[String]): Unit = {
    val root     = Paths.get(args(0))
    val phase    = args(1)
    val store    = new GuardedSnapshotDecisionProbe.LocalStore(root.resolve("archive"), loseInstall = true)
    val protocol = new GuardedSnapshotDecisions(store)
    val local    = root.resolve(s"state-$phase")
    require(Files.notExists(local))
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName(s"guarded-sql-$phase")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "localhost")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.shuffle.partitions", "1")
      .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension,org.openivm.spark.OpenIvmSparkExtensions"
      )
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.catalogImplementation", "hive")
      .config("spark.hadoop.javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=$root/metastore;create=true")
      .config("spark.sql.warehouse.dir", root.resolve("warehouse").toString)
      .config(FeatureGate.EnabledKey, "true")
      .config("spark.openivm.changeFeed.mode", "cdf")
      .config(FeatureGate.StatePathKey, local.toString)
      .config(FeatureGate.StateSyncAsyncEnabledKey, "false")
      .config(FeatureGate.GcEnabledKey, "false")
      .config(FeatureGate.GuardedStateSyncUriKey, store.canonicalRoot)
      .getOrCreate()
    val base                         = "default.gsql_source"
    val mv                           = "default.gsql_mv"
    val expected                     = s"SELECT k, COUNT(*) AS n FROM $base GROUP BY k"
    val refresh                      = s"REFRESH MATERIALIZED VIEW $mv"
    val create                       = s"CREATE MATERIALIZED VIEW IF NOT EXISTS $mv AS $expected"
    val receipt                      = root.resolve("prepare-receipt")
    def generation                   = protocol.latestPublished().map(_.generation.value)
    def sql(text: String): Unit      = { spark.sql(text).collect(); () }
    def version(table: String): Long = spark.sql(s"DESCRIBE HISTORY $table LIMIT 1").head().getLong(0)
    def parity(): Unit = {
      val actual = spark.sql(s"SELECT k, n FROM $mv")
      val oracle = spark.sql(expected)
      require(actual.exceptAll(oracle).count() == 0L && oracle.exceptAll(actual).count() == 0L)
    }
    try {
      if (phase == "prepare") {
        spark.conf.set(FeatureGate.StateSyncUriKey, root.resolve("must-not-exist").toUri.toString)
        try {
          OpenIvmStateSync.attachGuarded(spark, store)
          sys.error("mixed state sync settings were accepted")
        } catch { case e: IOException if e.getMessage.contains("no legacy URI") => () }
        finally spark.conf.unset(FeatureGate.StateSyncUriKey)
      }
      OpenIvmStateSync.attachGuarded(spark, store)
      if (phase == "prepare") {
        sql(s"CREATE TABLE $base(k INT) USING DELTA TBLPROPERTIES ('delta.enableChangeDataFeed'='true')")
        sql(s"INSERT INTO $base VALUES (1), (2)")
        OpenIvmStateSync.setBeforeSnapshotCommitHookForTesting(() => throw new IOException("injected before decision"))
        try {
          sql(create)
          sys.error("CREATE acknowledged a sealed but unpublished archive")
        } catch {
          case e: Exception
              if Iterator
                .iterate[Throwable](e)(_.getCause)
                .takeWhile(_ != null)
                .exists(_.getMessage == "injected before decision") =>
            ()
        } finally OpenIvmStateSync.setBeforeSnapshotCommitHookForTesting(null)
        require(generation.isEmpty)
        sql(create)
        val created = generation.get
        parity()
        sql(s"INSERT INTO $base VALUES (1), (1), (3)")
        sql(refresh)
        require(generation.exists(_ != created))
        parity()
        val offset = version(base)
        require(CdfWatermarkCatalog.get(spark, mv, base).contains(offset))
        require(Files.isRegularFile(store.archive(GuardedSnapshotDecisions.Generation(created))))
        Files.write(
          receipt,
          Seq(ProcessHandle.current().pid().toString, generation.get, offset.toString, version(mv).toString).asJava,
          UTF_8
        )
      } else {
        val prior = Files.readAllLines(receipt, UTF_8).asScala.toVector
        require(prior(0).toLong != ProcessHandle.current().pid())
        require(generation.contains(prior(1)))
        require(Files.notExists(local))
        // A normal catalog access must restore the watermark before REFRESH can change it.
        require(CdfWatermarkCatalog.get(spark, mv, base).contains(prior(2).toLong))
        require(version(mv) == prior(3).toLong)
        parity()
        sql(refresh)
        require(CdfWatermarkCatalog.get(spark, mv, base).contains(prior(2).toLong))
        require(version(mv) == prior(3).toLong)
        parity()
        require(Files.isRegularFile(store.archive(GuardedSnapshotDecisions.Generation(prior(1)))))
      }
      require(Files.notExists(root.resolve("must-not-exist")))
    } finally spark.stop()
  }
}

class GuardedSqlPersistenceSpec extends AnyFunSpec with Matchers {
  it("acknowledges guarded SQL only after publication and restores the exact CDF offset on startup") {
    val root = Files.createTempDirectory(Paths.get("target").toAbsolutePath, "guarded-sql-")
    Files.write(Files.createDirectory(root.resolve("archive")).resolve("format"), Seq("guarded-v1").asJava, UTF_8)
    val java = Paths.get(System.getProperty("java.home"), "bin", "java").toString
    val modules = ManagementFactory.getRuntimeMXBean.getInputArguments.asScala
      .filter(arg => arg.startsWith("--add-opens=") || arg.startsWith("--add-exports="))
    for (phase <- Seq("prepare", "restore")) {
      val log = root.resolve(s"$phase.log")
      val process = new ProcessBuilder(
        (Seq(java, "-Xmx2g") ++ modules ++ Seq(
          "-cp",
          System.getProperty("java.class.path"),
          "org.openivm.spark.common.rocksdb.GuardedSqlPersistenceProbe",
          root.toString,
          phase
        )): _*
      ).redirectErrorStream(true).redirectOutput(log.toFile).start()
      try {
        withClue(s"$phase failed; see $log: ") {
          process.waitFor(150, TimeUnit.SECONDS) shouldBe true
          process.exitValue() shouldBe 0
        }
      } finally if (process.isAlive) process.destroyForcibly()
    }
  }
}
