package org.openivm.spark.common

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import io.delta.tables.DeltaTable
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.Property
import org.apache.logging.log4j.core.layout.PatternLayout
import org.apache.logging.log4j.core.{LogEvent, Logger}
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql.SparkSession
import org.openivm.spark.telemetry.OpenIvmExecutionSpan
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.collection.mutable.ArrayBuffer

object DeltaTableVersionSpec {
  val taskEntered  = new CountDownLatch(1)
  val releaseTasks = new CountDownLatch(1)
}

/**
 * Guards the catalog-publication contention fix: the latest committed Delta
 * version must be resolved from the driver-side log snapshot, without
 * submitting a Spark job that has to queue behind concurrent data writes.
 */
class DeltaTableVersionSpec extends AnyFunSpec with BeforeAndAfterAll with Matchers {

  private val Json = new ObjectMapper()

  private var spark: SparkSession = _

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-delta-version-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-DeltaTableVersionSpec")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try {
      DeltaTableVersionSpec.releaseTasks.countDown()
      if (spark != null) spark.stop()
    } finally deleteDir(new File(warehouseDir))
  }

  private def deleteDir(f: File): Unit = {
    if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(deleteDir))
    f.delete()
    ()
  }

  private def newLocation(prefix: String): String =
    s"$warehouseDir/${prefix}_${UUID.randomUUID().toString.replace("-", "").take(8)}"

  private def historyVersion(tableOrPath: String): Long =
    if (tableOrPath.contains("/"))
      DeltaTable.forPath(spark, tableOrPath).history(1).collect().head.getAs[Long]("version")
    else DeltaTable.forName(spark, tableOrPath).history(1).collect().head.getAs[Long]("version")

  /** Runs `body` and reports how many Spark jobs it submitted. */
  private def countingJobs[A](body: => A): (A, Int) = {
    val jobs = new AtomicInteger(0)
    val listener = new SparkListener {
      override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
        jobs.incrementAndGet()
        ()
      }
    }
    spark.sparkContext.addSparkListener(listener)
    try {
      val result = body
      // The listener bus is asynchronous; let queued events drain before reading.
      Thread.sleep(500L)
      (result, jobs.get())
    } finally spark.sparkContext.removeSparkListener(listener)
  }

  private final class BufferingAppender(name: String)
      extends AbstractAppender(name, null, PatternLayout.createDefaultLayout(), false, Property.EMPTY_ARRAY) {
    private val buffer = ArrayBuffer.empty[String]

    override def append(event: LogEvent): Unit =
      buffer.synchronized {
        buffer += event.getMessage.getFormattedMessage
        ()
      }

    def messages: Seq[String] = buffer.synchronized(buffer.toVector)
  }

  private def spanPayloads(body: => Unit): Seq[JsonNode] = {
    val appender = new BufferingAppender(s"openivm-delta-version-${UUID.randomUUID()}")
    val root     = LogManager.getRootLogger.asInstanceOf[Logger]
    appender.start()
    root.addAppender(appender)
    try {
      body
      appender.messages.collect {
        case line if line.startsWith("OPENIVM_EXECUTION_SPAN ") =>
          Json.readTree(line.stripPrefix("OPENIVM_EXECUTION_SPAN "))
      }
    } finally {
      root.removeAppender(appender)
      appender.stop()
    }
  }

  describe("DeltaTableVersion") {
    it("matches the Delta history version for paths and registered tables across commits") {
      val location = newLocation("parity")
      spark.sql(s"CREATE TABLE delta.`$location` USING DELTA AS SELECT 1 AS id, 'a' AS name")
      DeltaTableVersion.latest(spark, location) shouldBe historyVersion(location)

      spark.sql(s"INSERT INTO delta.`$location` VALUES (2, 'b')")
      spark.sql(s"INSERT INTO delta.`$location` VALUES (3, 'c')")
      DeltaTableVersion.latest(spark, location) shouldBe historyVersion(location)
      DeltaTableVersion.latest(spark, location) shouldBe 2L

      val ident = s"dtv_named_${UUID.randomUUID().toString.replace("-", "").take(8)}"
      spark.sql(s"CREATE TABLE IF NOT EXISTS $ident USING DELTA LOCATION '$location'")
      DeltaTableVersion.latest(spark, ident) shouldBe historyVersion(ident)
      DeltaTableVersion.latest(spark, s"default.$ident") shouldBe historyVersion(location)
    }

    it("submits no Spark job, unlike the Delta history read it replaces") {
      val location = newLocation("jobs")
      spark.sql(s"CREATE TABLE delta.`$location` USING DELTA AS SELECT 1 AS id")
      spark.sql(s"INSERT INTO delta.`$location` VALUES (2)")

      val (snapshotVersion, snapshotJobs) = countingJobs(DeltaTableVersion.latest(spark, location))
      val (historyRead, historyJobs)      = countingJobs(historyVersion(location))

      snapshotVersion shouldBe historyRead
      snapshotJobs shouldBe 0
      historyJobs should be >= 1
    }

    it("resolves a version while every task slot is held by a running job") {
      val location = newLocation("contended")
      spark.sql(s"CREATE TABLE delta.`$location` USING DELTA AS SELECT 1 AS id")
      spark.sql(s"INSERT INTO delta.`$location` VALUES (2)")
      val expected = DeltaTableVersion.latest(spark, location)

      val blocker = new Thread(
        () => {
          spark.sparkContext
            .parallelize(Seq(1), 1)
            .map { value =>
              DeltaTableVersionSpec.taskEntered.countDown()
              DeltaTableVersionSpec.releaseTasks.await(60L, TimeUnit.SECONDS)
              value
            }
            .count()
          ()
        },
        "delta-version-slot-blocker"
      )
      blocker.setDaemon(true)
      blocker.start()

      val historyThread = new Thread(
        () => {
          historyVersion(location)
          ()
        },
        "delta-version-history-read"
      )
      historyThread.setDaemon(true)

      try {
        DeltaTableVersionSpec.taskEntered.await(60L, TimeUnit.SECONDS) shouldBe true

        val startedAt = System.nanoTime()
        DeltaTableVersion.latest(spark, location) shouldBe expected
        val elapsedMs = (System.nanoTime() - startedAt) / 1000000L
        elapsedMs should be < 5000L

        // The replaced history read needs a task slot, so it cannot make
        // progress while the blocker holds the only one.
        historyThread.start()
        historyThread.join(3000L)
        historyThread.isAlive shouldBe true
      } finally {
        DeltaTableVersionSpec.releaseTasks.countDown()
        historyThread.join(60000L)
        blocker.join(60000L)
      }
    }

    it("reports a missing Delta log through Option and failure contracts") {
      val emptyDir = newLocation("empty")
      new File(emptyDir).mkdirs()

      DeltaTableVersion.latest(spark, emptyDir) shouldBe DeltaTableVersion.NoCommits
      DeltaTableVersion.latestOption(spark, emptyDir) shouldBe None
      DeltaTableVersion.latestOption(spark, "default.dtv_does_not_exist") shouldBe None
      the[IllegalStateException] thrownBy DeltaTableVersion
        .requireLatest(spark, emptyDir) getMessage () should include("no committed Delta version")
    }

    it("records lookup timing on the surrounding execution span") {
      val location = newLocation("telemetry")
      spark.sql(s"CREATE TABLE delta.`$location` USING DELTA AS SELECT 1 AS id")

      val payloads = spanPayloads {
        val span = OpenIvmExecutionSpan.start("default.dtv_mv", "create")
        DeltaTableVersion.latest(spark, location)
        DeltaTableVersion.latest(spark, location)
        span.complete("create_executed", "driver-dtv")
        span.emitIfNeeded("create_executed", "driver-dtv")
      }

      payloads should have size 1
      val payload = payloads.head
      payload.get("delta_version_lookup_count").asLong() shouldBe 2L
      payload.get("delta_version_lookup_ms").asLong() should be >= 0L
      payload.has("catalog_ms") shouldBe false
    }
  }
}
