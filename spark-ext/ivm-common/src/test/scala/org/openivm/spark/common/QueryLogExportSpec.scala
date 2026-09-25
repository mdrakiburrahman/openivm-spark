package org.openivm.spark.common

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.rocksdb.OpenIvmRocksDBRegistry
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.concurrent.Eventually
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}

import java.io.{File, IOException}
import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit}
import scala.jdk.CollectionConverters._
import scala.collection.mutable

abstract class QueryLogExportTestBase(slug: String)
    extends AnyFunSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with Eventually {
  protected var spark: SparkSession            = _
  protected def extraConf: Map[String, String] = Map.empty
  private val mapper                           = new ObjectMapper()
  private val requests                         = mutable.ArrayBuffer.empty[String]
  private val invocations                      = mutable.ArrayBuffer.empty[QueryLogExport.Invocation]
  private val warehouse = new File(s"target/test-warehouse-$slug-${UUID.randomUUID().toString.take(8)}")

  override def beforeAll(): Unit = {
    super.beforeAll()
    warehouse.mkdirs()
    startSpark()
  }

  protected def startSpark(overrides: Map[String, String] = Map.empty): Unit = {
    val builder = SparkSession
      .builder()
      .master("local[2]")
      .appName(slug)
      .config("spark.sql.warehouse.dir", warehouse.getAbsolutePath)
      .config("spark.ui.enabled", "false")
      .config(FeatureGate.QueryLogEnabledKey, "true")
      .config(FeatureGate.ProfileRefreshKey, "false")
    (extraConf ++ overrides).foreach { case (key, value) => builder.config(key, value) }
    spark = builder.getOrCreate()
  }

  override def afterEach(): Unit = {
    try {
      RefreshSqlLogAsyncFlusher.beforeWriteForTesting = _ => ()
      RefreshSqlLogAsyncFlusher.awaitQuiescence(30000L) shouldBe true
      invocations.foreach(_.finish("test_aborted"))
      requests.foreach { request =>
        if (state(request).path("sql_succeeded").isNull) QueryLogExport.end(spark, request, sqlSucceeded = false)
        QueryLogExport.release(spark, request)
      }
      invocations.clear()
      requests.clear()
    } finally super.afterEach()
  }

  override def afterAll(): Unit = {
    try {
      if (spark != null) spark.stop()
      OpenIvmRocksDBRegistry.closeAll()
      def delete(file: File): Unit = {
        if (file.isDirectory) Option(file.listFiles()).foreach(_.foreach(delete))
        file.delete()
        ()
      }
      delete(warehouse)
    } finally super.afterAll()
  }

  protected def begin(request: String): Unit = {
    QueryLogExport.begin(spark, request)
    requests.synchronized { requests += request }
    ()
  }

  protected def invocation(request: String, nativeId: String, mode: String = "refresh"): QueryLogExport.Invocation = {
    val context  = spark.sparkContext
    val previous = context.getLocalProperty(QueryLogExport.RequestIdProperty)
    context.setLocalProperty(QueryLogExport.RequestIdProperty, request)
    try {
      val result = QueryLogExport.startInvocation(spark, nativeId, "default.export_fixture", mode).get
      invocations.synchronized { invocations += result }
      result
    } finally context.setLocalProperty(QueryLogExport.RequestIdProperty, previous)
  }

  protected def row(
      invocation: QueryLogExport.Invocation,
      order: Int = 0,
      category: String = "rewritten_stmt",
      sql: String = "SELECT 1",
      attempt: Int = 0
  ): RefreshSqlLogRow =
    RefreshSqlLogRow(
      invocation.refreshId,
      invocation.viewName,
      new Timestamp(1700000000000L),
      order,
      attempt,
      invocation.mode,
      category,
      "select",
      if (category == "original_query") -1L else 1L,
      sql
    )

  protected def append(invocation: QueryLogExport.Invocation, rows: RefreshSqlLogRow*): Unit = {
    rows.foreach(row => invocation.accept(row) shouldBe true)
    RefreshSqlLogAsyncFlusher.submit(spark, rows, Some(invocation))
  }

  protected def finish(
      request: String,
      invocation: QueryLogExport.Invocation,
      outcome: String = "incremental_executed"
  ): Unit = {
    invocation.finish(outcome)
    QueryLogExport.end(spark, request, sqlSucceeded = true)
  }

  protected def state(request: String, maxRows: Int = 100000, maxBytes: Int = 67108864): JsonNode =
    mapper.readTree(QueryLogExport.snapshotJson(spark, request, maxRows, maxBytes))

  protected def terminal(request: String, status: String = "complete"): JsonNode = {
    eventually(timeout(Span(30, Seconds))) {
      val current = state(request)
      current.path("status").asText() shouldBe status
      current.path("pending_flushes").asInt() shouldBe 0
    }
    state(request)
  }

  protected def records(state: JsonNode): Vector[JsonNode] = state.path("records").elements().asScala.toVector
}

class QueryLogExportSpec extends QueryLogExportTestBase("query-log-export") {
  describe("request-scoped query-log export") {
    it("preserves full UTF-8, timestamp-key collisions, statement attempts and conservative representations") {
      QueryLogExport.apiVersion() shouldBe 1
      val apiMethod = Class.forName("org.openivm.spark.common.QueryLogExport").getMethod("apiVersion")
      java.lang.reflect.Modifier.isStatic(apiMethod.getModifiers) shouldBe true
      apiMethod.invoke(null) shouldBe Integer.valueOf(1)
      FeatureGate.profileRefreshEnabled(spark) shouldBe false
      begin("text")
      val capture = invocation("text", "text_native")
      val fullSql = "SELECT '" + ("é\u0000\"\\\n😀" * 3000) + "' AS value"
      val input = Vector(
        row(capture, -1, "original_query", fullSql),
        row(capture, 0, "rewritten_stmt", fullSql),
        row(capture, 0, "explain_formatted", "== Physical Plan ==\nFixture"),
        row(capture, 0, "rewritten_stmt", fullSql, attempt = 1),
        row(capture, 1, "register_source_delta", "CREATE VIEW diagnostic AS SELECT 1"),
        row(capture, 2, "future_category", "INSERT INTO diagnostic VALUES (1)"),
        row(capture, 3, "full_refresh_stmt", "INSERT OVERWRITE TABLE fixture SELECT 1")
          .copy(stmtKind = "insert_overwrite"),
        row(capture, 4, "full_refresh_stmt", "DataFrameWriter.format(\"delta\")")
          .copy(stmtKind = "replace_where_writer")
      )
      append(capture, input: _*)
      finish("text", capture)
      val result = terminal("text")
      result.path("schema").asText() shouldBe "openivm.query-log-export"
      result.path("version").asInt() shouldBe 1
      result.path("application_id").asText() shouldBe spark.sparkContext.applicationId
      result.path("capture_complete").asBoolean() shouldBe true
      result.path("record_count").asInt() shouldBe input.size
      result.path("truncated").asBoolean() shouldBe false
      records(result).zip(input).foreach { case (actual, expected) =>
        actual.path("refresh_id").asText() shouldBe expected.refreshId
        actual.path("view_name").asText() shouldBe expected.viewName
        actual.path("profile_timestamp").asText() shouldBe expected.profileTimestamp.toInstant.toString
        actual.path("stmt_order").asInt() shouldBe expected.stmtOrder
        actual.path("attempt_idx").asInt() shouldBe expected.attemptIdx
        actual.path("mode").asText() shouldBe expected.mode
        actual.path("category").asText() shouldBe expected.category
        actual.path("stmt_kind").asText() shouldBe expected.stmtKind
        actual.path("duration_ms").asLong() shouldBe expected.durationMs
        actual.path("sql_text").asText() shouldBe expected.sqlText
      }
      records(result).map(_.path("representation_kind").asText()) shouldBe Vector(
        "original_query",
        "submitted_sql",
        "explain_plan",
        "submitted_sql",
        "diagnostic",
        "diagnostic",
        "submitted_sql",
        "diagnostic"
      )
    }

    it("isolates exact request prefixes across SQL workers, sessions and multiple native invocations") {
      val requests = Vector("isolation\u0000/a", "isolation\u0000/a-extra")
      requests.foreach(begin)
      val pool = Executors.newFixedThreadPool(2)
      try {
        val tasks = requests.zipWithIndex.map { case (request, index) =>
          pool.submit(new Callable[Unit] {
            override def call(): Unit = {
              val first = invocation(request, s"native_${index}_first", "create")
              append(first, row(first, sql = s"SELECT $index"))
              first.finish("create_executed")
              val second = invocation(request, s"native_${index}_second")
              append(second, row(second, sql = s"SELECT ${index + 10}"))
              second.finish("incremental_executed")
            }
          })
        }
        tasks.foreach(_.get(20, TimeUnit.SECONDS))
        requests.zipWithIndex.foreach { case (request, index) =>
          QueryLogExport.end(spark.newSession(), request, sqlSucceeded = true)
          val result = terminal(request)
          result.path("invocations").size() shouldBe 2
          records(result).map(_.path("sql_text").asText()) shouldBe Vector(s"SELECT $index", s"SELECT ${index + 10}")
          val otherSession = spark.newSession()
          QueryLogExport.snapshotJson(otherSession, request, 100, 10000) shouldBe
            QueryLogExport.snapshotJson(spark, request, 100, 10000)
        }
      } finally pool.shutdownNow()
    }

    it("accounts for multiple flushes including cleanup after the initial span flush") {
      begin("late")
      val capture = invocation("late", "late_native")
      append(capture, row(capture))
      eventually(timeout(Span(30, Seconds))) { state("late").path("pending_flushes").asInt() shouldBe 0 }
      state("late").path("status").asText() shouldBe "running"
      state("late").path("capture_complete").asBoolean() shouldBe false
      val entered = new CountDownLatch(1)
      val proceed = new CountDownLatch(1)
      RefreshSqlLogAsyncFlusher.beforeWriteForTesting = rows =>
        if (rows.exists(_.category == "drop_cleanup")) {
          entered.countDown()
          if (!proceed.await(20, TimeUnit.SECONDS)) throw new IOException("fixture flush timed out")
        }
      try {
        append(capture, row(capture, 1, "drop_cleanup", "DROP VIEW fixture_delta"))
        entered.await(10, TimeUnit.SECONDS) shouldBe true
        finish("late", capture)
        val pending = state("late")
        pending.path("status").asText() shouldBe "pending_flush"
        pending.path("capture_complete").asBoolean() shouldBe false
        pending.path("record_count").asInt() shouldBe 2
        records(pending) shouldBe empty
        intercept[IllegalArgumentException] { QueryLogExport.release(spark, "late") }
      } finally proceed.countDown()
      records(terminal("late")).map(_.path("category").asText()) shouldBe Vector("rewritten_stmt", "drop_cleanup")
    }

    it("reads a completed request without waiting for unrelated queued work") {
      begin("ready")
      val ready = invocation("ready", "ready_native")
      append(ready, row(ready))
      finish("ready", ready)
      terminal("ready")
      begin("unrelated")
      val blocked = invocation("unrelated", "blocked_native")
      val entered = new CountDownLatch(1)
      val proceed = new CountDownLatch(1)
      val reader  = Executors.newSingleThreadExecutor()
      RefreshSqlLogAsyncFlusher.beforeWriteForTesting = rows =>
        if (rows.exists(_.refreshId == blocked.refreshId)) {
          entered.countDown()
          if (!proceed.await(20, TimeUnit.SECONDS)) throw new IOException("fixture flush timed out")
        }
      try {
        append(blocked, row(blocked))
        finish("unrelated", blocked)
        entered.await(10, TimeUnit.SECONDS) shouldBe true
        val result = reader
          .submit(new Callable[JsonNode] {
            override def call(): JsonNode = state("ready")
          })
          .get(3, TimeUnit.SECONDS)
        result.path("status").asText() shouldBe "complete"
        state("unrelated").path("status").asText() shouldBe "pending_flush"
      } finally {
        proceed.countDown()
        reader.shutdownNow()
      }
      terminal("unrelated")
    }

    it("surfaces asynchronous write failure without throwing from the SQL submission or end") {
      begin("write-failure")
      val capture = invocation("write-failure", "write_failure_native")
      RefreshSqlLogAsyncFlusher.beforeWriteForTesting = _ => throw new IOException("synthetic write failure")
      append(capture, row(capture))
      finish("write-failure", capture)
      val result = terminal("write-failure", "failed")
      result.path("sql_succeeded").asBoolean() shouldBe true
      result.path("capture_complete").asBoolean() shouldBe false
      result.path("failure").path("code").asText() shouldBe "ASYNC_WRITE_FAILED"
      records(result) shouldBe empty
    }

    it("fails scoped queue overflow immediately instead of persisting synchronously on a SQL worker") {
      begin("queue-blocker")
      val blocker   = invocation("queue-blocker", "queue_blocker_native")
      val entered   = new CountDownLatch(1)
      val proceed   = new CountDownLatch(1)
      val submitter = Executors.newSingleThreadExecutor()
      RefreshSqlLogAsyncFlusher.beforeWriteForTesting = _ => {
        entered.countDown()
        if (!proceed.await(20, TimeUnit.SECONDS)) throw new IOException("fixture flush timed out")
      }
      try {
        append(blocker, row(blocker))
        finish("queue-blocker", blocker)
        entered.await(10, TimeUnit.SECONDS) shouldBe true
        RefreshSqlLogAsyncFlusher.fillQueueWithMarkersForTesting()
        begin("queue-overflow")
        val overflow     = invocation("queue-overflow", "queue_overflow_native")
        val inlineBefore = RefreshSqlLogAsyncFlusher.stats("droppedInlineFallbacks")
        submitter
          .submit(new Callable[Unit] {
            override def call(): Unit = {
              append(overflow, row(overflow))
              finish("queue-overflow", overflow)
            }
          })
          .get(3, TimeUnit.SECONDS)
        state("queue-overflow").path("failure").path("code").asText() shouldBe "ASYNC_QUEUE_FULL"
        state("queue-overflow").path("capture_complete").asBoolean() shouldBe false
        RefreshSqlLogAsyncFlusher.stats("droppedInlineFallbacks") shouldBe inlineBefore
      } finally {
        proceed.countDown()
        submitter.shutdownNow()
      }
      terminal("queue-blocker")
    }

    it("requires an ended native no-op lifecycle as positive evidence for an empty success") {
      begin("no-op")
      val capture = invocation("no-op", "noop_native")
      state("no-op").path("capture_complete").asBoolean() shouldBe false
      capture.finish("no_pending_deltas")
      state("no-op").path("capture_complete").asBoolean() shouldBe false
      QueryLogExport.end(spark, "no-op", sqlSucceeded = true)
      val result = terminal("no-op")
      result.path("capture_complete").asBoolean() shouldBe true
      result.path("record_count").asInt() shouldBe 0
      result.path("invocations").get(0).path("outcome").asText() shouldBe "no_pending_deltas"
      records(result) shouldBe empty
      begin("unexplained-empty")
      finish("unexplained-empty", invocation("unexplained-empty", "empty_native"))
      state("unexplained-empty").path("failure").path("code").asText() shouldBe "EMPTY_TRACE"
    }

    it("distinguishes a captured SQL failure from no lifecycle or an unknown request and preserves the SQL error") {
      begin("sql-error")
      val capture  = invocation("sql-error", "error_native")
      val original = new IllegalStateException("synthetic SQL rejection")
      val thrown = intercept[IllegalStateException] {
        try throw original
        finally {
          capture.finish("metadata_not_found")
          QueryLogExport.end(spark, "sql-error", sqlSucceeded = false)
        }
      }
      thrown should be theSameInstanceAs original
      val result = terminal("sql-error", "failed")
      result.path("capture_complete").asBoolean() shouldBe true
      result.path("failure").path("code").asText() shouldBe "SQL_FAILED"
      begin("no-lifecycle")
      QueryLogExport.end(spark, "no-lifecycle", sqlSucceeded = false)
      state("no-lifecycle").path("capture_complete").asBoolean() shouldBe false
      state("no-lifecycle").path("failure").path("code").asText() shouldBe "NO_LIFECYCLE"
      QueryLogExport.end(spark, "unknown", sqlSucceeded = false)
      state("unknown").path("status").asText() shouldBe "missing"
    }

    it("detects a missing persisted index instead of mistaking scan emptiness for a no-op") {
      begin("lost")
      val capture = invocation("lost", "lost_native")
      append(capture, row(capture))
      finish("lost", capture)
      terminal("lost")
      RefreshSqlLogCatalog.removeExport(spark, spark.sparkContext.applicationId, "lost")
      val result = state("lost")
      result.path("status").asText() shouldBe "failed"
      result.path("capture_complete").asBoolean() shouldBe false
      result.path("failure").path("code").asText() shouldBe "CAPTURE_READ_FAILED"
    }

    it("releases only the acknowledged export index and preserves the cumulative legacy SHOW rows") {
      val requests = Vector("release", "release-neighbor")
      requests.foreach { request =>
        begin(request)
        val capture = invocation(request, request + "_native")
        append(capture, row(capture))
        finish(request, capture)
        terminal(request)
      }
      QueryLogExport.release(spark, "release")
      QueryLogExport.release(spark, "release")
      state("release").path("status").asText() shouldBe "missing"
      state("release-neighbor").path("status").asText() shouldBe "complete"
      RefreshSqlLogCatalog.readExport(spark, spark.sparkContext.applicationId, "release", 10, 10000) shouldBe empty
      RefreshSqlLogCatalog.scanAll(spark).map(_.refreshId) should contain allOf (
        "release_native", "release-neighbor_native"
      )
    }

    it("keeps disabled logging inactive and never reuses a prior application's request") {
      begin("old-app")
      val old = invocation("old-app", "old_native")
      finish("old-app", old, "no_pending_deltas")
      terminal("old-app")
      val oldApplication = spark.sparkContext.applicationId
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      startSpark(Map(FeatureGate.QueryLogEnabledKey -> "false"))
      spark.sparkContext.applicationId should not be oldApplication
      state("old-app").path("status").asText() shouldBe "missing"
      begin("disabled")
      QueryLogExport.startInvocation(spark, "disabled_native", "default.fixture", "refresh") shouldBe None
      QueryLogExport.end(spark, "disabled", sqlSucceeded = true)
      state("disabled").path("failure").path("code").asText() shouldBe "QUERY_LOG_DISABLED"
      state("disabled").path("capture_complete").asBoolean() shouldBe false
    }
  }
}
