package org.openivm.spark.common

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import py4j.GatewayServer

import java.io.File
import java.net.InetAddress
import java.nio.file.Files
import java.util.{Properties, UUID}
import java.util.concurrent.{Callable, ConcurrentLinkedQueue, CountDownLatch, ExecutionException, Executors, TimeUnit}
import scala.collection.JavaConverters._

class ScopedSqlBridgeSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll with Eventually {
  private var spark: SparkSession = _
  private val root   = new File(s"target/scoped-sql-spec-${UUID.randomUUID().toString.take(8)}").getAbsoluteFile
  private val jobs   = new ConcurrentLinkedQueue[Properties]()
  private val mapper = new ObjectMapper()

  private def requirePythonGateway(): Unit = {
    val log = new File(root, "python-prerequisite.log")
    val process =
      try
        new ProcessBuilder("python3", "-c", "import py4j.java_gateway")
          .redirectErrorStream(true)
          .redirectOutput(log)
          .start()
      catch {
        case _: java.io.IOException =>
          cancel("Pooled-gateway integration requires python3 and Spark's Py4J package; see scoped-sql-bridge.txt")
      }
    try {
      assume(
        process.waitFor(10, TimeUnit.SECONDS) && process.exitValue() == 0,
        "Pooled-gateway integration requires Spark's Py4J package on PYTHONPATH; see scoped-sql-bridge.txt"
      )
    } finally {
      if (process.isAlive) {
        process.destroyForcibly()
        process.waitFor(10, TimeUnit.SECONDS)
      }
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    root.mkdirs()
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("scoped-sql-bridge")
      .config("spark.ui.enabled", "false")
      .config("spark.scheduler.mode", "FAIR")
      .config("spark.sql.warehouse.dir", new File(root, "warehouse").getAbsolutePath)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config(FeatureGate.QueryLogEnabledKey, "true")
      .withExtensions(_.injectParser((_, parser) => ScopedSqlBridgeFixture.parser(parser)))
      .getOrCreate()
    spark.sparkContext.addSparkListener(new SparkListener {
      override def onJobStart(event: SparkListenerJobStart): Unit = {
        jobs.add(event.properties.clone().asInstanceOf[Properties])
        ()
      }
    })
    ScopedSqlBridgeFixture.source = new File(root, "source").getAbsolutePath
    spark.range(1).write.format("delta").save(ScopedSqlBridgeFixture.source)
  }

  override def afterAll(): Unit = {
    try {
      if (spark != null) spark.stop()
      val paths = Files.walk(root.toPath)
      try paths.iterator().asScala.toVector.reverse.foreach(Files.delete)
      finally paths.close()
    } finally super.afterAll()
  }

  describe("JVM-atomic scoped SQL") {
    it("reproduces pooled RPC thread drift, then isolates 32 overlapping requests and scheduler attribution") {
      requirePythonGateway()
      val fixture = new ScopedSqlGatewayFixture(spark)
      val server = new GatewayServer.GatewayServerBuilder(fixture)
        .javaAddress(InetAddress.getLoopbackAddress)
        .javaPort(0)
        .build()
      server.start()
      val log    = new File(root, "gateway.log")
      val script = new File(getClass.getResource("/scoped_sql_gateway.py").toURI)
      val process = new ProcessBuilder("python3", script.toString, server.getListeningPort.toString)
        .redirectErrorStream(true)
        .redirectOutput(log)
        .start()
      try {
        process.waitFor(180, TimeUnit.SECONDS) shouldBe true
        withClue(new String(Files.readAllBytes(log.toPath), "UTF-8")) {
          process.exitValue() shouldBe 0
        }
        val requests = (0 until 32).map(index => s"pooled-$index") ++ Seq("reused", "direct")
        requests.foreach { request =>
          ScopedSqlBridgeFixture.observed.get(request) shouldBe
            s"$request|group-$request|description-$request|pool-$request|true"
          eventually(timeout(Span(10, Seconds))) {
            val attributed = jobs.asScala.filter(_.getProperty(QueryLogExport.RequestIdProperty) == request).toVector
            attributed should not be empty
            attributed.foreach { props =>
              props.getProperty("spark.jobGroup.id") shouldBe "group-" + request
              props.getProperty("spark.scheduler.pool") shouldBe "pool-" + request
            }
          }
        }
        val pooledThreads = (0 until 32).map(index => ScopedSqlBridgeFixture.threads.get(s"pooled-$index")).toSet
        pooledThreads.size shouldBe 32
        pooledThreads should contain(ScopedSqlBridgeFixture.threads.get("reused"))
      } finally {
        fixture.releaseConnection()
        fixture.releaseOverlap()
        if (process.isAlive) {
          process.destroyForcibly()
          process.waitFor(10, TimeUnit.SECONDS)
        }
        server.shutdown()
      }
    }

    it("seals independent SQL outcomes and restores cloned properties on a reused JVM thread") {
      val fixture = new ScopedSqlGatewayFixture(spark)
      fixture.prepare(0)
      fixture.releaseOverlap()
      val thread = fixture.executeAndVerifyRestore("jvm-success", fail = false)
      intercept[IllegalStateException](fixture.executeAndVerifyRestore("jvm-failure", fail = true))
      fixture.executeAndVerifyRestore("jvm-reused", fail = false) shouldBe thread
      Seq("jvm-success", "jvm-reused").foreach { request =>
        val state = mapper.readTree(fixture.snapshot(request))
        state.path("sql_succeeded").asBoolean() shouldBe true
        state.path("capture_complete").asBoolean() shouldBe true
      }
      mapper.readTree(fixture.snapshot("jvm-failure")).path("sql_succeeded").asBoolean() shouldBe false
    }

    it("materializes lazy SQL before sealing failure and restores null-valued properties") {
      val context = spark.sparkContext
      ScopedSqlTestProperties.set(context, new Properties())
      val fail: () => Int = () => throw new IllegalStateException("lazy synthetic failure")
      spark.udf.register("scoped_fail", fail)
      intercept[Exception] {
        ScopedSqlBridge.execute(spark, "SELECT scoped_fail()", "lazy", "lazy-group", "", "lazy-pool", true)
      }
      mapper
        .readTree(QueryLogExport.snapshotJson(spark, "lazy", 100, 65536))
        .path("sql_succeeded")
        .asBoolean() shouldBe false
      context.getLocalProperty(QueryLogExport.RequestIdProperty) shouldBe null
      context.getLocalProperty("spark.jobGroup.id") shouldBe null
      context.getLocalProperty("spark.scheduler.pool") shouldBe null
    }

    it("rejects duplicate captures without executing SQL or changing prior state") {
      QueryLogExport.begin(spark, "duplicate")
      val prior = ScopedSqlTestProperties.get(spark.sparkContext).clone().asInstanceOf[Properties]
      intercept[IllegalArgumentException] {
        ScopedSqlBridge.execute(spark, "SCOPED FIXTURE never ok", "duplicate", "g", "", "p", true)
      }.getMessage should include("CAPTURE_EXISTS")
      ScopedSqlBridgeFixture.threads.containsKey("never") shouldBe false
      ScopedSqlTestProperties.get(spark.sparkContext) shouldBe prior
      QueryLogExport.end(spark, "duplicate", sqlSucceeded = false)
    }

    it("retains an independently inspectable success marker after the Python process loses its connection") {
      requirePythonGateway()
      val fixture = new ScopedSqlGatewayFixture(spark)
      fixture.prepare(1)
      val server = new GatewayServer.GatewayServerBuilder(fixture)
        .javaAddress(InetAddress.getLoopbackAddress)
        .javaPort(0)
        .build()
      server.start()
      val log    = new File(root, "disconnect.log")
      val script = new File(getClass.getResource("/scoped_sql_gateway.py").toURI)
      val process = new ProcessBuilder("python3", script.toString, server.getListeningPort.toString, "disconnect")
        .redirectErrorStream(true)
        .redirectOutput(log)
        .start()
      try {
        fixture.awaitOverlap()
        mapper.readTree(fixture.snapshot("disconnected")).path("sql_succeeded").isNull shouldBe true
        process.destroyForcibly()
        process.waitFor(10, TimeUnit.SECONDS) shouldBe true
        fixture.releaseOverlap()
        eventually(timeout(Span(30, Seconds))) {
          val state = mapper.readTree(fixture.snapshot("disconnected"))
          state.path("sql_succeeded").asBoolean() shouldBe true
          state.path("capture_complete").asBoolean() shouldBe true
        }
      } finally {
        fixture.releaseOverlap()
        if (process.isAlive) {
          process.destroyForcibly()
          process.waitFor(10, TimeUnit.SECONDS)
        }
        server.shutdown()
      }
    }

    it("records Spark job-group cancellation as failure rather than transport success") {
      ScopedSqlBridgeFixture.taskEntered = new CountDownLatch(1)
      ScopedSqlBridgeFixture.taskProceed = new CountDownLatch(1)
      spark.udf.register("scoped_wait", () => ScopedSqlBridgeFixture.awaitCancellation())
      val worker = Executors.newSingleThreadExecutor()
      val future = worker.submit(new Callable[Unit] {
        override def call(): Unit =
          ScopedSqlBridge.execute(spark, "SELECT scoped_wait()", "cancelled", "cancel-group", "", "cancel-pool", true)
      })
      try {
        ScopedSqlBridgeFixture.taskEntered.await(30, TimeUnit.SECONDS) shouldBe true
        spark.sparkContext.cancelJobGroup("cancel-group")
        intercept[ExecutionException](future.get(30, TimeUnit.SECONDS))
        mapper
          .readTree(QueryLogExport.snapshotJson(spark, "cancelled", 100, 65536))
          .path("sql_succeeded")
          .asBoolean() shouldBe false
      } finally {
        ScopedSqlBridgeFixture.taskProceed.countDown()
        worker.shutdownNow()
        worker.awaitTermination(30, TimeUnit.SECONDS) shouldBe true
      }
    }

    it("does not turn interruption into success and preserves primary errors through all cleanup") {
      Thread.currentThread().interrupt()
      try {
        intercept[InterruptedException] {
          ScopedSqlBridge.execute(spark, "SCOPED FIXTURE interrupted ok", "interrupted", "g", "", "p", true)
        }
        Thread.currentThread().isInterrupted shouldBe true
      } finally Thread.interrupted()
      mapper
        .readTree(QueryLogExport.snapshotJson(spark, "interrupted", 100, 65536))
        .path("sql_succeeded")
        .asBoolean() shouldBe false
      val primary = new IllegalStateException("primary")
      val end     = new IllegalArgumentException("end")
      val restore = new IllegalArgumentException("restore")
      val thrown = intercept[IllegalStateException] {
        ScopedSqlBridge.preservingFailure(
          () => ScopedSqlBridge.preservingFailure(() => throw primary, () => throw end),
          () => throw restore
        )
      }
      thrown should be theSameInstanceAs primary
      thrown.getSuppressed.toVector shouldBe Vector(end, restore)
      intercept[IllegalArgumentException] {
        ScopedSqlBridge.preservingFailure(() => (), () => throw end)
      } should be theSameInstanceAs end
    }
  }
}
