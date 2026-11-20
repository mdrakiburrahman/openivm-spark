package org.openivm.spark.common.rocksdb

import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.{FeatureGate, OpenIvmGarbageCollector, StagingCatalog, StagingDelta}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.lang.management.ManagementFactory
import java.nio.file.{Files, Path, Paths}
import java.security.MessageDigest
import java.sql.Timestamp
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._

private[rocksdb] object GcReferencePublicationWitness {
  def session(warehouse: Path, destination: Path): SparkSession =
    SparkSession.builder()
      .master("local[1]")
      .appName("openivm-gc-reference-publication-witness")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "localhost")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.warehouse.dir", warehouse.toString)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config(FeatureGate.StateSyncUriKey, destination.toUri.toString)
      .getOrCreate()

  def main(args: Array[String]): Unit = {
    val spark = session(Paths.get(args(1)), Paths.get(args(2)))
    val location = args(3)
    try {
      OpenIvmStateSync.maybeRestore(spark)
      if (args(0) == "publish") {
        require(!StagingCatalog.referencedStagingPaths(spark).contains(location))
        StagingCatalog.record(
          spark,
          StagingDelta("default.witness_source", "INSERT", location, new Timestamp(1000L), Nil)
        )
        OpenIvmStateSync.backupNow(spark)
      } else {
        require(StagingCatalog.referencedStagingPaths(spark).contains(location))
        require(spark.read.format("delta").load(location).collect().map(_.getLong(0)).toSeq == Seq(0L))
      }
    } finally {
      OpenIvmRocksDBRegistry.closeAll()
      spark.stop()
    }
  }
}

class GcReferencePublicationWitnessSpec extends AnyFunSpec with Matchers {
  private def fingerprint(root: Path): Map[String, (Long, Seq[Byte])] = {
    val files = Files.walk(root)
    try files.iterator().asScala.map { path =>
      val digest =
        if (Files.isRegularFile(path)) MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(path)).toSeq
        else Seq.empty[Byte]
      root.relativize(path).toString -> (Files.getLastModifiedTime(path).toMillis, digest)
    }.toMap
    finally files.close()
  }

  private def freshProcess(role: String, root: Path, destination: Path, location: String): Unit = {
    val java = new File(System.getProperty("java.home"), "bin/java").getAbsolutePath
    val modules = ManagementFactory.getRuntimeMXBean.getInputArguments.asScala
      .filter(arg => arg.startsWith("--add-opens=") || arg.startsWith("--add-exports="))
    val log = root.resolve(s"$role.log").toFile
    val process = new ProcessBuilder(
      (Seq(java, "-Xmx1g") ++ modules ++ Seq(
        "-cp",
        System.getProperty("java.class.path"),
        "org.openivm.spark.common.rocksdb.GcReferencePublicationWitness",
        role,
        root.resolve(role).toString,
        destination.toString,
        location
      )): _*
    ).redirectErrorStream(true).redirectOutput(log).start()
    try {
      process.waitFor(150, TimeUnit.SECONDS) shouldBe true
      withClue(s"$role JVM failed; see $log: ") { process.exitValue() shouldBe 0 }
    } finally {
      if (process.isAlive) {
        process.destroyForcibly()
        process.waitFor(10, TimeUnit.SECONDS)
      }
    }
  }

  describe("OneLake collector admission, with no deletion or remote requests") {
    it("cannot infer reference liveness from unchanged Delta object versions") {
      val root = Files.createTempDirectory(Paths.get("target").toAbsolutePath, "gc-live-reference-")
      val destination = Files.createDirectory(root.resolve("published"))
      val warehouse = root.resolve("collector")
      val payload = warehouse.resolve("_ivm/staging/default_witness_source/INSERT/witness")
      val spark = GcReferencePublicationWitness.session(warehouse, destination)
      try {
        spark.range(1).write.format("delta").save(payload.toUri.toString)
        val location = payload.toUri.toString
        val candidateFingerprint = fingerprint(payload)
        val plan = OpenIvmGarbageCollector.prepare(spark).get
        plan.referenced shouldBe empty
        plan.tombstoned shouldBe empty

        freshProcess("publish", root, destination, location)
        fingerprint(payload) shouldBe candidateFingerprint
        OpenIvmGarbageCollector.prepare(spark).get.referenced shouldBe empty
        freshProcess("restore", root, destination, location)
        fingerprint(payload) shouldBe candidateFingerprint
      } finally {
        OpenIvmRocksDBRegistry.closeAll()
        spark.stop()
      }
    }
  }
}
